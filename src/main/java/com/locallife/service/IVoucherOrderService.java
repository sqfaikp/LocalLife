package com.locallife.service;

import com.locallife.dto.Result;
import com.locallife.entity.VoucherOrder;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IVoucherOrderService extends IService<VoucherOrder> {

    Result seckillVoucher(Long voucherId);

    /**
     * 2026-11-07 新增：把订单消息真正落库。
     *
     * 方法签名从实现类的 private 提升到接口，是因为异步下单换成 RabbitMQ 之后，
     * 落库这段逻辑要由消费者（SeckillOrderConsumer）来调用，不再是内部线程自己调。
     *
     * @param voucherOrder 从 MQ 消息体反序列化出来的订单，至少带着 id / userId / voucherId
     */
    void createVoucherOrder(VoucherOrder voucherOrder);
}
