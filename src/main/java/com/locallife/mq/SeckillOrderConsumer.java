package com.locallife.mq;

import com.locallife.config.RabbitMQConfig;
import com.locallife.entity.VoucherOrder;
import com.locallife.service.IVoucherOrderService;
import com.locallife.utils.RedisConstants;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.IOException;

/**
 * 秒杀订单消费者。
 *
 * 第一版（2026-11-07 上午）是最小闭环：默认 AUTO ack，异常一律吞掉，
 *   目的是先跑通"能发能收"。代价是处理失败的消息会被静默丢弃。
 *
 * 第二版（2026-11-07 晚）补上可靠性 L1：
 *   ① 手动 ack：只有真正处理成功才确认，失败的消息不会消失
 *   ② 按异常类型分流：重复消费直接丢弃，真异常进死信队列
 *   ③ 处理完成后清掉 pending 记录（否则对账会一直以为这单没落库）
 *   ④ prefetch=1、死信队列在 application.yaml 和 RabbitMQConfig 里配
 *
 * @author 阿栈
 * @since 2026-11-07
 */
@Slf4j
@Component
public class SeckillOrderConsumer {

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 监听业务队列。
     *
     * 方法签名里直接写 VoucherOrder，靠的是 RabbitMQConfig 里那个
     * Jackson2JsonMessageConverter —— 消息体是 JSON，转成对象由它负责。
     * 如果转换器没配对，这里会报 ConvertException 或者收到 null。
     *
     * Channel 和 deliveryTag 是手动 ack 必须的两个东西：
     *   channel    —— 真正发 ack/nack 的通道
     *   deliveryTag —— 这条消息在本通道里的投递序号，ack 的时候要带上它
     *
     * 为什么要 throws IOException：basicAck / basicNack 都会抛受检异常，必须处理或声明。
     */
    @RabbitListener(queues = RabbitMQConfig.SECKILL_QUEUE)
    public void handleSeckillOrder(
            VoucherOrder voucherOrder,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) throws IOException {

        log.info("收到秒杀订单消息，orderId={}, userId={}, voucherId={}",
                voucherOrder.getId(), voucherOrder.getUserId(), voucherOrder.getVoucherId());

        try {
            // 第一步：真正落库（一人一单校验 + 扣 DB 库存 + 写订单表）
            // 内部加了 @Transactional，扣库存和插订单在同一个事务里。
            // 拿不到锁 / DB 库存不足 会抛异常，走到下面的分支。
            voucherOrderService.createVoucherOrder(voucherOrder);

            // 第二步：落库成功，先清 pending 记录，再确认消息
            // 顺序很重要：先删 pending 再 ack。反过来的话，万一删 pending 前服务挂了，
            // 消息已 ack 不会重投，pending 又留着 → 对账会把这单当"丢了"再补发一次。
            clearPending(voucherOrder);

            // 第三步：manual 模式下必须手动确认，不写这一句消息会一直 unacked
            // multiple=false 表示只确认这一条
            channel.basicAck(deliveryTag, false);
            log.info("订单落库完成并已确认，orderId={}", voucherOrder.getId());

        } catch (DuplicateKeyException e) {
            // 撞 uk_user_voucher 唯一索引 = 重复消费。
            //
            // 这不是错误，是 MQ"至少一次投递"的正常现象。必须 ack 掉，
            // 否则消息会被反复重投，永远卡在这一条。
            // 也不能 nack 进死信 —— 那只会让死信队列里堆一堆本来就没问题的重复消息。
            log.warn("订单重复消费，直接确认丢弃，orderId={}, userId={}",
                    voucherOrder.getId(), voucherOrder.getUserId());
            clearPending(voucherOrder);
            channel.basicAck(deliveryTag, false);

        } catch (Exception e) {
            // 其余异常（拿锁失败、DB 库存不足、数据库连接断了等）→ 拒绝消息。
            //
            // basicNack 第二个参数 multiple=false：只拒这一条
            // basicNack 第三个参数 requeue=false：不再回原队列
            //
            // 为什么 requeue 必须是 false：
            //   true 会把消息放回队列头，立刻又被这个消费者取到，再失败再回队 ——
            //   经典队列没有"重投计数器"，程序无法判断"已经试过几次该放弃了"，
            //   于是变成死循环烧 CPU，还会把后面正常消息全堵住。
            //   false 才会走 x-dead-letter-exchange，进到 seckill.order.dlq 等人工排查。
            log.error("订单处理失败，转入死信队列，orderId={}, userId={}",
                    voucherOrder.getId(), voucherOrder.getUserId(), e);
            channel.basicNack(deliveryTag, false, false);
        }
    }

    /**
     * 清掉这条订单的 pending 记录。
     *
     * pending 是「Redis 已扣库存、但还没确认落库」的账本。落库成功了就该销账，
     * 否则定时对账会一直以为这单丢了，每隔 15 分钟补发一次。
     *
     * 内部吞异常是故意的：删 pending 失败不影响"订单已经落库"这个事实，
     * 不能因为这一步失败就 nack 让整条消息重来 —— 那样反而制造重复消费。
     * 对账任务也会自己清理"DB 里已有但 pending 还在"的残留记录，兜得住。
     */
    private void clearPending(VoucherOrder voucherOrder) {
        try {
            String pendingKey = RedisConstants.SECKILL_PENDING_KEY + voucherOrder.getVoucherId();
            stringRedisTemplate.opsForHash().delete(pendingKey, voucherOrder.getUserId().toString());
        } catch (Exception e) {
            log.warn("清理 pending 记录失败（不影响主流程，对账会兜底），orderId={}",
                    voucherOrder.getId(), e);
        }
    }

    // ==================== 第一版实现（已停用，原样保留备查）====================
    // 停用原因：AUTO ack + 吞异常 = 处理失败的消息被静默丢弃，没有任何兜底。
    // 要恢复这版：把下面方法的 @RabbitListener 注释去掉，同时把上面那个方法的注解注掉。
    // 两个方法不能同时监听同一个队列，否则会变成两个消费者抢消息，行为混乱。
    //
    // @RabbitListener(queues = RabbitMQConfig.SECKILL_QUEUE)
    // public void handleSeckillOrderOld(VoucherOrder voucherOrder) {
    //     log.info("收到秒杀订单消息，orderId={}, userId={}, voucherId={}",
    //             voucherOrder.getId(), voucherOrder.getUserId(), voucherOrder.getVoucherId());
    //     try {
    //         voucherOrderService.createVoucherOrder(voucherOrder);
    //         log.info("订单落库完成，orderId={}", voucherOrder.getId());
    //     } catch (Exception e) {
    //         log.error("订单落库失败（消息已被 ack 丢弃），orderId={}", voucherOrder.getId(), e);
    //     }
    // }
}
