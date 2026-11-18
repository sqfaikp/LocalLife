package com.locallife.mq;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

/**
 * 生产端消息投递结果回调（2026-11-07 第二轮新增，可靠性 L1）。
 *
 * 它解决两个问题：
 *   ① confirm  —— 消息到底有没有到达【交换机】？
 *   ② return   —— 到了交换机，但有没有【路由到队列】？
 *
 * 这两件事都不能靠 convertAndSend 的返回值判断 —— 那个方法正常返回只代表
 * "消息交给了客户端库"，既不保证到了 Broker，更不保证进了队列。
 *
 * 配合 application.yaml 里这三个配置才生效：
 *   publisher-confirm-type: correlated
 *   publisher-returns: true
 *   template.mandatory: true
 * 少任何一个，对应回调都不会触发（而且不会报错，只是静默不回调，很难发现）。
 *
 * @author 阿栈
 * @since 2026-11-07
 */
@Slf4j
@Component
public class RabbitConfirmCallback implements RabbitTemplate.ConfirmCallback, RabbitTemplate.ReturnCallback {

    @Resource
    private RabbitTemplate rabbitTemplate;

    /**
     * Bean 初始化完成后，把回调挂到 RabbitTemplate 上。
     *
     * 为什么不写在构造函数里：RabbitTemplate 是注入进来的，构造函数执行时
     * Spring 还没保证它完全初始化好。@PostConstruct 在所有依赖注入完成后执行，更稳妥。
     *
     * ⚠️ 这里用的是 setReturnCallback（旧 API）而不是 setReturnsCallback（新 API）。
     *    原因：本项目 Spring Boot 2.3.12 实际解析到的 spring-rabbit 是 2.2.18，
     *    ReturnsCallback / ReturnedMessage 是 spring-amqp 2.3 才引入的，
     *    在 2.2 里根本没有这两个类，写了会编译不过（找不到符号）。
     *    功能上两者等价，只是回调方法的参数形式不同：
     *      旧：returnedMessage(Message message, int replyCode, String replyText, String exchange, String routingKey)
     *      新：returnedMessage(ReturnedMessage returned)
     */
    @PostConstruct
    public void init() {
        rabbitTemplate.setConfirmCallback(this);
        rabbitTemplate.setReturnCallback(this);
        log.info("RabbitMQ 生产端 confirm / return 回调已注册");
    }

    /**
     * 消息是否到达交换机。
     *
     * @param correlationData 发消息时带过来的关联数据，里面放了 orderId，
     *                        失败时靠它定位是哪一单
     * @param ack             true = 到了交换机；false = 没到
     * @param cause           没到的原因（连接断了、交换机不存在等）
     *
     * ⚠️ 注意 ack=true 不代表消息一定被消费了，只代表【交换机收到了】。
     *    能不能路由到队列，看下面的 returnedMessage。
     */
    @Override
    public void confirm(CorrelationData correlationData, boolean ack, String cause) {
        String orderId = "null";
        if (correlationData != null) {
            orderId = correlationData.getId();
        }

        if (ack == true) {
            log.debug("消息已到达交换机，orderId={}", orderId);
            return;
        }

        // 没到交换机 = MQ 层面出问题了（Broker 挂了、连接断了、交换机没声明）
        // 这时候消息是真的丢了，只能靠 SeckillReconcileTask 的对账补发捞回来
        log.error("消息未到达交换机！orderId={}, 原因={}，等定时对账补发", orderId, cause);
    }

    /**
     * 消息到了交换机，但路由不到任何队列。
     *
     * 最常见的原因：
     *   - routingKey 写错（比如 seckill.order 写成了 seckill.orders）
     *   - 队列忘了绑定到交换机
     *   - 队列被人从管理台删了
     *
     * 这类错误基本都是代码或配置问题，必须人工介入，不能静默吞掉 ——
     * 否则表现就是"接口返回成功、订单就是不生成"，排查起来极其痛苦。
     *
     * 参数说明（spring-amqp 2.2 的回调签名）：
     *   message    原始消息体，想看内容可以 new String(message.getBody())
     *   replyCode  AMQP 回复码，312 = NO_ROUTE（路由不到）
     *   replyText  回复文字
     *   exchange   消息发往的交换机
     *   routingKey 发消息时用的 routingKey
     */
    @Override
    public void returnedMessage(Message message, int replyCode, String replyText,
                                String exchange, String routingKey) {
        log.error("消息路由失败！exchange={}, routingKey={}, replyCode={}, replyText={}",
                exchange, routingKey, replyCode, replyText);
    }
}
