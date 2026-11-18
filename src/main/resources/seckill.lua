-- 1.参数列表
-- 1.1.优惠券id
local voucherId = ARGV[1]
-- 1.2.用户id
local userId = ARGV[2]
-- 1.3.订单id
local orderId = ARGV[3]

-- 2.数据key
-- 2.1.库存key
local stockKey = 'seckill:stock:' .. voucherId
-- 2.2.订单key
local orderKey = 'seckill:order:' .. voucherId

-- 3.脚本业务
-- 3.1.判断库存是否充足 get stockKey
--
-- 2026-11-07 修复：先判空，再比较大小。
-- 原来直接写 tonumber(redis.call('get', stockKey)) <= 0，有个坑：
--   key 不存在时 get 返回 false（Lua 的布尔值），tonumber(false) 返回 nil，
--   而 nil 和数字比较会直接抛错 "attempt to compare nil with number"。
--   结果是：库存 key 一旦丢了（Redis 重启没开持久化 / 没预热 / 被清空），
--   秒杀接口不是返回"库存不足"，而是直接 500，而且没有任何提示。
local stock = redis.call('get', stockKey)
if (stock == false) then
    -- key 不存在：说明库存没预热，或者 Redis 数据丢了
    -- 返回 3 让 Java 侧给一个明确提示，而不是抛异常
    return 3
end
if (tonumber(stock) <= 0) then
    -- 3.2.库存不足，返回1
    return 1
end
-- 3.2.判断用户是否下单 SISMEMBER orderKey userId
if(redis.call('sismember', orderKey, userId) == 1) then
    -- 3.3.存在，说明是重复下单，返回2
    return 2
end
-- 3.4.扣库存 incrby stockKey -1
redis.call('incrby', stockKey, -1)
-- 3.5.下单（保存用户）sadd orderKey userId
redis.call('sadd', orderKey, userId)

-- ========== 2026-11-07 改造：异步下单从 Redis Stream 换成 RabbitMQ ==========
-- 下面这行是 Stream 版的「Lua 里直接发消息」，现已停用，保留备查。
-- 停用原因：Lua 跑在 Redis 内部，发不了 RabbitMQ 消息。
-- 所以现在 Lua 只负责「原子地扣库存 + 记用户」，发消息挪到 Java 侧（VoucherOrderServiceImpl）做。
-- 代价是「扣库存」和「发消息」不再是一个原子操作，中间有一个短暂的不一致窗口：
-- 极端情况下库存扣了、消息却没发出去 —— 这个缺口留到可靠性 L1/L2 阶段补。
--
-- 3.6.发送消息到队列中， XADD stream.orders * k1 v1 k2 v2 ...
-- redis.call('xadd', 'stream.orders', '*', 'userId', userId, 'voucherId', voucherId, 'id', orderId)

-- ========== 2026-11-07 第二轮改造：记下「待确认订单」，供对账补发 ==========
-- 为什么要多存这一步：
--   Lua 只扣了 Redis 的库存，MQ 消息是 Java 侧发的。万一消息发失败、或者消费失败，
--   这批库存就「扣了但没变成订单」—— 少卖。而对账任务要补发，就必须知道
--   「是哪个用户、原本的订单号是多少」。
--   订单号不能用重新生成的，必须用这个原号，否则用户手里那个号就对不上了。
--
-- 为什么用 Hash 而不是 ZSet：
--   对账策略是「不判断时间，只要没落库就补发」，不需要按时间筛，Hash 最省事。
--   如果以后想加宽限期（只补存在超过 N 分钟的记录），把这一行换成
--   zadd pendingKey 时间戳 userId:orderId，对账改用 rangeByScore 即可。
--
-- key 格式：seckill:pending:{voucherId}
-- field = userId，value = orderId
local pendingKey = 'seckill:pending:' .. voucherId
redis.call('hset', pendingKey, userId, orderId)

-- 3.7.返回 0 表示「有资格」，Java 侧拿到 0 之后再去发 MQ 消息
return 0