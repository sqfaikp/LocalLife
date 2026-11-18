package com.locallife.utils;

public class RedisConstants {
    public static final String LOGIN_CODE_KEY = "login:code:";
    public static final Long LOGIN_CODE_TTL = 2L;
    public static final String LOGIN_USER_KEY = "login:token:";
    public static final Long LOGIN_USER_TTL = 36000L;

    public static final Long CACHE_NULL_TTL = 2L;

    public static final Long CACHE_SHOP_TTL = 30L;
    public static final String CACHE_SHOP_KEY = "cache:shop:";

    public static final String LOCK_SHOP_KEY = "lock:shop:";
    public static final Long LOCK_SHOP_TTL = 10L;

    public static final String SECKILL_STOCK_KEY = "seckill:stock:";

    /**
     * 2026-11-07 新增：已下单用户集合 key 前缀。
     * 必须和 seckill.lua 里的 'seckill:order:' 完全一致 —— 对账时要用它的 size
     * 去和 DB 订单数比对，写错一个字两边就对不上。
     */
    public static final String SECKILL_ORDER_KEY = "seckill:order:";

    /**
     * 2026-11-07 新增：待确认订单 key 前缀。
     * 结构是 Hash：field = userId，value = orderId。
     *
     * 它记录的是「已经在 Redis 里扣了库存、但还不确定有没有落库」的订单。
     * 消费成功就删掉；定时对账发现「还留着但 DB 里没有」就用原 orderId 重发。
     * 没有它，对账根本不知道「欠了用户哪些单」，只能干瞪眼。
     */
    public static final String SECKILL_PENDING_KEY = "seckill:pending:";

    public static final String BLOG_LIKED_KEY = "blog:liked:";
    public static final String FEED_KEY = "feed:";
    public static final String SHOP_GEO_KEY = "shop:geo:";
    public static final String USER_SIGN_KEY = "sign:";
}
