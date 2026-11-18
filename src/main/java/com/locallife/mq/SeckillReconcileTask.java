package com.locallife.mq;

import com.locallife.config.RabbitMQConfig;
import com.locallife.entity.SeckillVoucher;
import com.locallife.entity.VoucherOrder;
import com.locallife.service.ISeckillVoucherService;
import com.locallife.service.IVoucherOrderService;
import com.locallife.utils.RedisConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 秒杀订单定时对账（2026-11-07 第二轮新增，可靠性 L2）。
 *
 * 【这个类到底是干什么的】
 * 前面的可靠性手段能保证「消息不重复」「消息尽量不丢」，但都防不住一种情况：
 * Redis 已经把库存扣了、用户也被告知"抢到了"，但消息最终没落成订单。
 * 唯一索引在这里帮不上忙 —— 它是防重的，不是防漏的（DB 里压根没有这行，索引无从触发）。
 *
 * 所以需要一个"点名"的机制：把 pending 账本和 DB 订单表对一遍，缺的补上。
 *
 * 【设计取舍：为什么不做"丢了还是还没到"的判断】
 * 理论上 pending 里有、DB 里没有，可能是真丢了，也可能只是消息还在队列里排队。
 * 我们选择【不判断】，直接重发，理由是重发本身幂等：
 *   - 真丢了   → 补上，正好
 *   - 还在排队 → 多发一条重复消息，消费时撞 uk_user_voucher 唯一索引，
 *                被 SeckillOrderConsumer 捕获后 ack 丢弃，数据不受影响
 * 代价只是几条多余消息，换来代码从 130 行降到 50 行、且没有时序 bug。
 *
 * 如果以后消息量上来了想优化，把 pending 换成 ZSet（score=创建时间戳），
 * 对账时用 rangeByScore 只捞"存在超过 N 分钟"的记录即可。
 *
 * 【前置条件】
 *   1. seckill.lua 里第 3.6 步要往 pending 写记录（hset）
 *   2. 消费者落库成功后要删 pending
 *   3. 启动类要加 @EnableScheduling
 *
 * @author 阿栈
 * @since 2026-11-07
 */
@Slf4j
@Component
public class SeckillReconcileTask {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private IVoucherOrderService voucherOrderService;

    @Resource
    private RabbitTemplate rabbitTemplate;

    /**
     * 每 15 分钟跑一次。
     *
     * 为什么不设更频繁：跑一次就会把 pending 里所有没落库的全补一遍，
     * 频率太高会放大"误判在途消息"造成的重复消息量。15 分钟是个折中。
     */
    @Scheduled(cron = "0 0/15 * * * ?")
    public void reconcile() {
        log.debug("秒杀订单对账开始");

        // 第一步：取出所有秒杀券
        List<SeckillVoucher> vouchers = seckillVoucherService.list();

        // 第二步：逐张券把"欠用户的订单"补一遍
        for (int i = 0; i < vouchers.size(); i++) {
            SeckillVoucher voucher = vouchers.get(i);
            Long voucherId = voucher.getVoucherId();

            try {
                resendMissingOrders(voucherId);
            } catch (Exception e) {
                // 单张券出错不能影响其他券的对账，这里吞掉继续下一个
                log.error("对账失败，voucherId={}", voucherId, e);
            }
        }

        log.debug("秒杀订单对账结束");
    }

    /**
     * 把 pending 里"DB 中不存在"的订单全部重发。
     *
     * 注意两点：
     *   ① 这里【不动库存】。名额已经承诺给用户了，我们补的是"兑现"，不是"收回"。
     *      如果这里把库存加回去，就等于同一张券卖两次 —— 超卖。
     *   ② 重发必须用【原来的 orderId】。重新生成一个的话，
     *      用户在页面上看到的订单号就永远查不到了。
     *
     * @param voucherId 优惠券 id
     */
    private void resendMissingOrders(Long voucherId) {
        String pendingKey = RedisConstants.SECKILL_PENDING_KEY + voucherId;

        // 第一步：取出这张券所有"待确认"记录
        // 结构是 Hash：field = userId，value = orderId
        Map<Object, Object> pendingMap = stringRedisTemplate.opsForHash().entries(pendingKey);

        // 第二步：没有待确认记录，说明账是干净的，直接返回
        if (pendingMap.isEmpty() == true) {
            return;
        }

        // 第三步：把所有 orderId 收集起来，准备批量查库
        List<Long> orderIds = new ArrayList<>();
        for (Object value : pendingMap.values()) {
            orderIds.add(Long.valueOf(value.toString()));
        }

        // 第四步：一次性批量查 DB
        // 千万不要在循环里单条 getById —— 那是 N+1 查询，pending 有 1000 条就查 1000 次库
        List<VoucherOrder> existOrders = voucherOrderService.listByIds(orderIds);

        Set<Long> existIdSet = new HashSet<>();
        for (int i = 0; i < existOrders.size(); i++) {
            existIdSet.add(existOrders.get(i).getId());
        }

        // 第五步：逐个处理
        for (Map.Entry<Object, Object> entry : pendingMap.entrySet()) {
            String userIdStr = entry.getKey().toString();
            String orderIdStr = entry.getValue().toString();
            Long orderId = Long.valueOf(orderIdStr);

            // 情况一：DB 里已经有这单
            // 说明落库成功了，只是删 pending 那一步没成功（比如删的时候 Redis 抖了一下）。
            // 补一下销账就行，不用重发。
            if (existIdSet.contains(orderId) == true) {
                stringRedisTemplate.opsForHash().delete(pendingKey, userIdStr);
                log.info("pending 残留记录已清理（DB 中订单已存在），orderId={}", orderIdStr);
                continue;
            }

            // 情况二：DB 里没有 → 不管它是真丢了还是还在排队，直接重发
            // 真丢了    → 补上
            // 还在排队  → 重复消息撞唯一索引后被 ack 丢弃，无害
            log.warn("发现未落库订单，补发消息，voucherId={}, userId={}, orderId={}",
                    voucherId, userIdStr, orderIdStr);

            VoucherOrder resendOrder = new VoucherOrder();
            resendOrder.setId(orderId);
            resendOrder.setUserId(Long.valueOf(userIdStr));
            resendOrder.setVoucherId(voucherId);

            try {
                rabbitTemplate.convertAndSend(
                        RabbitMQConfig.SECKILL_EXCHANGE,
                        RabbitMQConfig.SECKILL_ROUTING_KEY,
                        resendOrder,
                        // 带上 CorrelationData，confirm 回调失败时能定位到是哪一单
                        new CorrelationData(orderIdStr)
                );
                // ⚠️ 重发成功也不删 pending。
                // 必须等消费端真正落库后由 SeckillOrderConsumer 删。
                // 如果这里提前删了，而 MQ 下一秒就挂了、消息其实没进去，这单就永远没人管了。
            } catch (Exception e) {
                // 补发失败不要紧，下一轮对账还会再捞到这条记录
                log.error("补发消息失败，等下一轮对账，orderId={}", orderIdStr, e);
            }
        }
    }
}
