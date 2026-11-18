package com.locallife.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.locallife.config.RabbitMQConfig;
import com.locallife.dto.Result;
import com.locallife.entity.VoucherOrder;
import com.locallife.mapper.VoucherOrderMapper;
import com.locallife.service.ISeckillVoucherService;
import com.locallife.service.IVoucherOrderService;
import com.locallife.utils.RedisConstants;
import com.locallife.utils.RedisIdWorker;
import com.locallife.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 2026-11-07 新增：RabbitMQ 生产端。
     * 异步下单从 Redis Stream 换成 RabbitMQ 之后，发消息靠它。
     */
    @Resource
    private RabbitTemplate rabbitTemplate;

    /**
     * 秒杀订单消息队列的 key / 消费组 / 消费者名。
     *
     * 注意：XACK 失败是【静默】的 —— 对不存在的 key 做 XACK 只返回 0，不抛异常。
     * 之前这里 ack 写死成 "s1"（课程课件里的 stream 名），而 seckill.lua 里 XADD 到的是
     * "stream.orders"，两边对不上，导致消息落库成功但全部卡在 pending-list 里。
     * 所以抽成常量，读写共用同一个名字。
     */
    private static final String STREAM_KEY = "stream.orders";
    private static final String GROUP_NAME = "g1";
    private static final String CONSUMER_NAME = "c1";

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }


    // ==================== 2026-11-07 改造：Redis Stream 消费线程已停用 ====================
    // 停用方式：把下面的 @PostConstruct 注掉，init() 就不会被 Spring 调用，消费线程不会启动。
    // VoucherOrderHandler 整个类原样保留（没有删），方便对照和回滚。
    //
    // 要恢复 Stream 版：① 去掉 @PostConstruct 的注释；② 把下面 seckillVoucher 里被注释的旧实现放回来。
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    /**
     * 消费线程的运行标志。
     * 应用关闭时置成 false，让 while 循环自己退出来。
     */
    private volatile boolean running = true;

    // @PostConstruct
    private void init() {
        // SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    /**
     * 应用关闭时停掉消费线程。
     *
     * 不加这个会有严重后果：Spring 上下文销毁之后，消费线程还在 while 里空转，
     * 每次 read 都抛 "LettuceConnectionFactory was destroyed and cannot be used anymore"，
     * 而 catch 里只打日志不退出 —— 于是变成无限循环刷日志。
     * 实测一次启动失败就刷出了 119 MB 日志，CPU 也打满。
     */
    @PreDestroy
    public void destroy() {
        running = false;
        SECKILL_ORDER_EXECUTOR.shutdownNow();
    }

    private class VoucherOrderHandler implements Runnable {

        @Override
        public void run() {
            while (running) {
                try {
                    // 1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from(GROUP_NAME, CONSUMER_NAME),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(STREAM_KEY, ReadOffset.lastConsumed())
                    );
                    // 2.判断订单信息是否为空
                    if (list == null || list.isEmpty()) {
                        // 如果为null，说明没有消息，继续下一次循环
                        continue;
                    }
                    // 解析数据
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 3.创建订单
                    createVoucherOrder(voucherOrder);
                    // 4.确认消息 XACK
                    stringRedisTemplate.opsForStream().acknowledge(STREAM_KEY, GROUP_NAME, record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (running) {
                try {
                    // 1.获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from(GROUP_NAME, CONSUMER_NAME),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(STREAM_KEY, ReadOffset.from("0"))
                    );
                    // 2.判断订单信息是否为空
                    if (list == null || list.isEmpty()) {
                        // 如果为null，说明没有异常消息，结束循环
                        break;
                    }
                    // 解析数据
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 3.创建订单
                    createVoucherOrder(voucherOrder);
                    // 4.确认消息 XACK
                    stringRedisTemplate.opsForStream().acknowledge(STREAM_KEY, GROUP_NAME, record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    // 退避 1 秒再重试。
                    // 少了这一句，一条永远失败的消息（或者 Redis 已经断开）会在这里空转，
                    // 因为 ReadOffset.from("0") 每次都从 pending 的第一条重新读，
                    // 读完照旧失败，catch 接着进下一轮 —— 日志会被瞬间刷爆。
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException interrupted) {
                        // 被 shutdownNow 打断，补回中断标记后退出
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }
    }

    /*private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    private class VoucherOrderHandler implements Runnable{

        @Override
        public void run() {
            while (true){
                try {
                    // 1.获取队列中的订单信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 2.创建订单
                    createVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }
            }
        }
    }*/

    /**
     * 真正的落库动作：一人一单校验 + 扣 DB 库存 + 写订单。
     *
     * 2026-11-07 上午改动：从 private 改成 public，并加 @Override。
     * 原因：换成 RabbitMQ 之后，这段逻辑不再被内部消费线程调用，
     * 而是由消费者 SeckillOrderConsumer 通过 IVoucherOrderService 调用。
     *
     * 2026-11-07 晚上改动（可靠性 L1）：
     *   ① 加 @Transactional(rollbackFor = Exception.class)
     *      原来没有事务，扣库存和插订单是两个独立动作 —— save 失败的话库存白扣了。
     *   ② 拿不到锁、DB 库存不足，都从「log 后 return」改成「抛异常」
     *      原来 return 等于告诉消费者"处理成功了"，消息会被 ack 掉，订单就这么没了。
     *      抛异常才能让消费者 nack → 重试 → 进死信队列。
     *   ③ 「重复下单」保持 return 不抛异常
     *      因为它不是错误，是 MQ 至少一次投递的正常现象，属于幂等命中，应该 ack 掉。
     *
     * ⚠️ 已知瑕疵：锁的 unlock 写在 finally 里，会在事务提交【之前】执行。
     *    理论上存在「锁已释放、事务未提交」的窗口，另一个线程进来查 count 还是 0。
     *    这里不加"锁包事务"那套代理写法，因为：① 异步消费是串行的（prefetch=1）；
     *    ② 就算真撞上，DB 的 uk_user_voucher 唯一索引会挡住，不会产生两单。
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        // 创建锁对象
        RLock redisLock = redissonClient.getLock("lock:order:" + userId);
        // 尝试获取锁
        boolean isLock = redisLock.tryLock();
        // 判断
        if (!isLock) {
            // 2026-11-07 改：原来这里 return，消息会被 ack 掉、订单直接丢。
            // 现在抛异常，交给消费者的重试 + 死信队列处理。
            throw new IllegalStateException("获取下单锁失败，userId=" + userId + "，交给 MQ 重试");
        }

        try {
            // 5.1.查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            // 5.2.判断是否存在
            if (count > 0) {
                // 用户已经购买过了 —— 这是幂等命中，不是错误，正常返回让消费者 ack
                log.warn("重复下单，幂等跳过，userId={}, voucherId={}", userId, voucherId);
                return;
            }

            // 6.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1") // set stock = stock - 1
                    .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
                    .update();
            if (!success) {
                // 2026-11-07 改：原来这里 return，等于把「Redis 扣成功但 DB 扣不动」这种
                // 库存不一致悄悄吞掉。现在抛异常暴露出来，进死信队列等人工看。
                throw new IllegalStateException("DB 库存不足，Redis 与 DB 可能不一致，voucherId=" + voucherId);
            }

            // 7.创建订单
            save(voucherOrder);
        } finally {
            // 释放锁
            redisLock.unlock();
        }
    }

    /**
     * 2026-11-07 改造：秒杀下单，异步链路从 Redis Stream 换成 RabbitMQ。
     *
     * 整体七步，顺序很重要：
     *   1. 取当前用户
     *   2. 先生成订单号（消息体里要带，也是后面幂等和对账的依据）
     *   3. 跑 Lua，原子地判断资格 + 扣库存 + 记用户
     *   4. 按 Lua 返回值分流
     *   5. 组装订单消息
     *   6. 投递到 MQ
     *   7. 立刻返回订单号
     */
    @Override
    public Result seckillVoucher(Long voucherId) {
        // 第一步：拿到当前登录用户（ThreadLocal 里取）
        Long userId = UserHolder.getUser().getId();

        // 第二步：先生成订单 id。
        // 必须在发消息之前生成，因为消息体里要带它；
        // 而且它是后面「幂等」和「对账补发」的唯一依据 —— 补发时必须用同一个号，
        // 重新生成一个的话，用户手里那个号就对不上了。
        long orderId = redisIdWorker.nextId("order");

        // 第三步：执行 Lua 脚本，原子地判断资格 + 扣库存 + 记用户。
        // 注意：Lua 里已经不再 xadd 了（发不了 MQ 消息），只做 Redis 这边的事。
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId)
        );
        int code = result.intValue();

        // 第四步：按返回值分流。中间变量 code 只是为了好读，java 的 switch 也行。
        // 1 = 库存不足；2 = 重复下单；3 = 库存 key 不存在；0 = 有资格
        if (code == 1) {
            return Result.fail("库存不足");
        }
        if (code == 2) {
            return Result.fail("不能重复下单");
        }
        if (code == 3) {
            // 2026-11-07 新增：Lua 发现库存 key 不存在。
            // 这种情况说明 Redis 里的库存没预热、或者数据丢了（比如 Redis 重启但没开持久化）。
            // 以前这里会一路往下走，最后在 Lua 里抛 nil 比较异常变成 500，什么都看不出来。
            log.error("秒杀库存未预热或已丢失，voucherId={}", voucherId);
            return Result.fail("活动尚未开始，请稍后再试");
        }

        // 第五步：组装订单消息体。
        // 只带这三个字段就够了 —— 其余字段（支付方式、状态、时间）让数据库自己填默认值，
        // 消息体越小越好，也避免 LocalDateTime 序列化的额外麻烦。
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);

        // 第六步：投递到 RabbitMQ
        try {
            rabbitTemplate.convertAndSend(
                    RabbitMQConfig.SECKILL_EXCHANGE,
                    RabbitMQConfig.SECKILL_ROUTING_KEY,
                    voucherOrder,
                    // 2026-11-07 第二轮新增：带上 CorrelationData。
                    // 作用是把 orderId 捎给 confirm 回调（RabbitConfirmCallback），
                    // 万一消息没到交换机，日志里能直接看到是哪一单出问题。
                    // 不传的话回调里 correlationData 是 null，只能看到"有消息失败了"，定位不了。
                    new CorrelationData(String.valueOf(orderId))
            );
        } catch (AmqpException e) {
            // 走到这里说明消息压根没发出去（连不上 MQ、序列化失败等）。
            //
            // 2026-11-07 第二轮改造：从这里开始【回滚 Redis】。
            //
            // 为什么必须回滚：
            //   Lua 已经把库存扣了、把用户记进已购集合了、还写了 pending 记录。
            //   但消息没发出去，这单永远不会被消费 → 库存永久少 1，属于少卖。
            //   三个东西必须一起回滚，少回滚一个都会留下脏数据：
            //     ① 库存 +1        —— 否则这张券白少一张
            //     ② SREM 该用户    —— 否则这个用户再也下不了单（被误判成已购买）
            //     ③ HDEL pending   —— 否则对账会一直以为「这单丢了」，反复补发
            log.error("秒杀消息投递失败，开始回滚 Redis，orderId={}, userId={}, voucherId={}",
                    orderId, userId, voucherId, e);
            rollbackStock(voucherId, userId);
            return Result.fail("系统繁忙，请重试");

        } catch (Exception e) {
            // 兜底：比如序列化抛了非 AmqpException 的异常。
            // 同样不能留着悬空库存，否则就是「扣了没卖」。
            log.error("秒杀消息投递未知异常，开始回滚 Redis，orderId={}, userId={}, voucherId={}",
                    orderId, userId, voucherId, e);
            rollbackStock(voucherId, userId);
            return Result.fail("下单失败，请重试");
        }

        // 第七步：立刻返回订单号，前端据此轮询结果（此时订单还没真正落库）
        return Result.ok(orderId);
    }

    /**
     * 2026-11-07 第二轮新增：发送消息失败时回滚 Redis。
     *
     * 回滚三样东西，缺一不可：
     *   ① 库存 +1
     *   ② 把用户从已购集合里移除（否则他再点一次会被判成"已购买"）
     *   ③ 删掉 pending 记录（否则对账会一直以为这单丢了，反复补发）
     *
     * ⚠️ 注意：这个方法自己也可能失败（比如 Redis 恰好连不上）。
     *    所以不能把「回滚成功」当成必然，最终还得靠 SeckillReconcileTask 定时对账兜底。
     *
     * @param voucherId 优惠券 id
     * @param userId    用户 id
     */
    private void rollbackStock(Long voucherId, Long userId) {
        String stockKey = RedisConstants.SECKILL_STOCK_KEY + voucherId;
        String orderKey = RedisConstants.SECKILL_ORDER_KEY + voucherId;
        String pendingKey = RedisConstants.SECKILL_PENDING_KEY + voucherId;

        try {
            // 第一步：库存加回去
            stringRedisTemplate.opsForValue().increment(stockKey);
            // 第二步：释放这个用户的购买资格
            stringRedisTemplate.opsForSet().remove(orderKey, userId.toString());
            // 第三步：删掉待确认记录
            stringRedisTemplate.opsForHash().delete(pendingKey, userId.toString());
            log.info("Redis 回滚完成，voucherId={}, userId={}", voucherId, userId);
        } catch (Exception e) {
            // 回滚失败只能记日志，等对账任务兜底。
            // 这里不能再往外抛 —— 用户已经拿不到订单了，再抛只会让前端看到 500。
            log.error("Redis 回滚失败！等定时对账兜底，voucherId={}, userId={}",
                    voucherId, userId, e);
        }
    }

    // ==================== 下面这份是 Redis Stream 版实现，已停用，原样保留备查 ====================
    /*
    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIdWorker.nextId("order");
        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );
        int r = result.intValue();
        // 2.判断结果是否为0
        if (r != 0) {
            // 2.1.不为0 ，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        // 3.返回订单id
        return Result.ok(orderId);
    }
    */

    /*@Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString()
        );
        int r = result.intValue();
        // 2.判断结果是否为0
        if (r != 0) {
            // 2.1.不为0 ，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        // 2.2.为0 ，有购买资格，把下单信息保存到阻塞队列
        VoucherOrder voucherOrder = new VoucherOrder();
        // 2.3.订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        // 2.4.用户id
        voucherOrder.setUserId(userId);
        // 2.5.代金券id
        voucherOrder.setVoucherId(voucherId);
        // 2.6.放入阻塞队列
        orderTasks.add(voucherOrder);

        // 3.返回订单id
        return Result.ok(orderId);
    }*/
    /*@Override
    public Result seckillVoucher(Long voucherId) {
        // 1.查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        // 2.判断秒杀是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            // 尚未开始
            return Result.fail("秒杀尚未开始！");
        }
        // 3.判断秒杀是否已经结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            // 尚未开始
            return Result.fail("秒杀已经结束！");
        }
        // 4.判断库存是否充足
        if (voucher.getStock() < 1) {
            // 库存不足
            return Result.fail("库存不足！");
        }

        return createVoucherOrder(voucherId);
    }



    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        // 5.一人一单
        Long userId = UserHolder.getUser().getId();

        // 创建锁对象
        RLock redisLock = redissonClient.getLock("lock:order:" + userId);
        // 尝试获取锁
        boolean isLock = redisLock.tryLock();
        // 判断
        if(!isLock){
            // 获取锁失败，直接返回失败或者重试
            return Result.fail("不允许重复下单！");
        }

        try {
            // 5.1.查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            // 5.2.判断是否存在
            if (count > 0) {
                // 用户已经购买过了
                return Result.fail("用户已经购买过一次！");
            }

            // 6.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1") // set stock = stock - 1
                    .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
                    .update();
            if (!success) {
                // 扣减失败
                return Result.fail("库存不足！");
            }

            // 7.创建订单
            VoucherOrder voucherOrder = new VoucherOrder();
            // 7.1.订单id
            long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);
            // 7.2.用户id
            voucherOrder.setUserId(userId);
            // 7.3.代金券id
            voucherOrder.setVoucherId(voucherId);
            save(voucherOrder);

            // 7.返回订单id
            return Result.ok(orderId);
        } finally {
            // 释放锁
            redisLock.unlock();
        }

    }*/
    /*@Transactional
    public Result createVoucherOrder(Long voucherId) {
        // 5.一人一单
        Long userId = UserHolder.getUser().getId();

        // 创建锁对象
        SimpleRedisLock redisLock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        // 尝试获取锁
        boolean isLock = redisLock.tryLock(1200);
        // 判断
        if(!isLock){
            // 获取锁失败，直接返回失败或者重试
            return Result.fail("不允许重复下单！");
        }

        try {
            // 5.1.查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            // 5.2.判断是否存在
            if (count > 0) {
                // 用户已经购买过了
                return Result.fail("用户已经购买过一次！");
            }

            // 6.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1") // set stock = stock - 1
                    .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
                    .update();
            if (!success) {
                // 扣减失败
                return Result.fail("库存不足！");
            }

            // 7.创建订单
            VoucherOrder voucherOrder = new VoucherOrder();
            // 7.1.订单id
            long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);
            // 7.2.用户id
            voucherOrder.setUserId(userId);
            // 7.3.代金券id
            voucherOrder.setVoucherId(voucherId);
            save(voucherOrder);

            // 7.返回订单id
            return Result.ok(orderId);
        } finally {
            // 释放锁
            redisLock.unlock();
        }

    }*/

    /*@Transactional
    public Result createVoucherOrder(Long voucherId) {
        // 5.一人一单
        Long userId = UserHolder.getUser().getId();

        synchronized (userId.toString().intern()) {
            // 5.1.查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            // 5.2.判断是否存在
            if (count > 0) {
                // 用户已经购买过了
                return Result.fail("用户已经购买过一次！");
            }

            // 6.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1") // set stock = stock - 1
                    .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
                    .update();
            if (!success) {
                // 扣减失败
                return Result.fail("库存不足！");
            }

            // 7.创建订单
            VoucherOrder voucherOrder = new VoucherOrder();
            // 7.1.订单id
            long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);
            // 7.2.用户id
            voucherOrder.setUserId(userId);
            // 7.3.代金券id
            voucherOrder.setVoucherId(voucherId);
            save(voucherOrder);

            // 7.返回订单id
            return Result.ok(orderId);
        }
    }*/
}
