package com.locallife.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * RabbitMQ 的交换机 / 队列 / 绑定 声明，以及消息转换器。
 *
 * 第一版（2026-11-07 上午）只做「能发能收」的最小闭环：一个直连交换机 + 一个业务队列 + 一条绑定。
 * 当时故意没做的：死信交换机 / 死信队列、生产端 confirm / return、消费端手动 ack + prefetch。
 *
 * 第二版（2026-11-07 晚）补上可靠性 L1：新增死信交换机 + 死信队列，业务队列绑定死信。
 * 剩下两项（confirm / return、手动 ack + prefetch）在 application.yaml 里配，需要配合 Java 回调。
 *
 * ⚠️ 改队列参数前必须先删掉 MQ 里已存在的旧队列！
 *    seckill.order.queue 如果已经建过、且没有 x-dead-letter-exchange 参数，
 *    这里重新声明时会报 PRECONDITION_FAILED（ inequivalent arg 'x-dead-letter-exchange'）。
 *    处理办法：管理台 Queues 页面删掉 seckill.order.queue，或者执行 rabbitmqctl delete_queue。
 *    删掉后重启应用，会按新参数自动重建。
 *
 * @author 阿栈
 * @since 2026-11-07
 */
@Configuration
public class RabbitMQConfig {

    // ========== 第一步：把名字定义成常量，避免到处写字符串写错 ==========
    /** 业务交换机（直连类型，按 routingKey 精确匹配） */
    public static final String SECKILL_EXCHANGE = "seckill.exchange";
    /** 业务队列 */
    public static final String SECKILL_QUEUE = "seckill.order.queue";
    /** 业务 routingKey */
    public static final String SECKILL_ROUTING_KEY = "seckill.order";

    // ========== 2026-11-07 第二轮新增：死信相关常量 ==========
    /**
     * 死信交换机。
     * 业务队列里的消息被 nack(requeue=false)、TTL 过期、队列满时，
     * 会按业务队列上的 x-dead-letter-exchange 参数转发到这里。
     */
    public static final String SECKILL_DLX_EXCHANGE = "seckill.dlx.exchange";
    /** 死信队列：真正不行了的消息躺在这儿，等人排查 */
    public static final String SECKILL_DLQ_QUEUE = "seckill.order.dlq";
    /** 死信 routingKey */
    public static final String SECKILL_DLQ_ROUTING_KEY = "seckill.order.dlq";

    // ========== 第二步：声明业务交换机 ==========
    /**
     * 用 DirectExchange（直连）而不是 Fanout/Topic：
     * 秒杀下单只有一个消费方、只按 routingKey 精确投递，直连最省事也最好排查。
     */
    @Bean
    public DirectExchange seckillExchange() {
        // 参数依次是：名字、durable（是否持久化）、autoDelete（无人使用时是否自动删除）
        DirectExchange exchange = new DirectExchange(SECKILL_EXCHANGE, true, false);
        return exchange;
    }

    // ========== 第三步：声明业务队列（第二轮改造：绑定死信交换机）==========
    /**
     * durable = true：MQ 重启后队列还在。
     * 注意这只保证「队列本身」不丢，消息要不要持久化是发消息时决定的。
     *
     * 2026-11-07 第二轮改动：加上 x-dead-letter-exchange / x-dead-letter-routing-key。
     *
     * 为什么要加：
     *   消费端改成手动 ack 之后，处理失败会 basicNack(requeue=false)。
     *   如果队列没配死信，这条消息就被【直接丢弃】—— 管理台队列变短、死信队列是空的，
     *   连「丢了多少条」都统计不出来，等于一个静默丢消息的黑洞。
     *   配上死信之后，失败消息会转到 seckill.order.dlq，可以人工捞出来重放。
     *
     * 旧写法（第一版，保留备查，别再改回去）：
     *   Queue queue = QueueBuilder.durable(SECKILL_QUEUE).build();
     *   return queue;
     */
    @Bean
    public Queue seckillQueue() {
        Map<String, Object> args = new HashMap<>();
        // 消息被拒绝 / 过期 / 队列满时，转发到死信交换机
        args.put("x-dead-letter-exchange", SECKILL_DLX_EXCHANGE);
        args.put("x-dead-letter-routing-key", SECKILL_DLQ_ROUTING_KEY);

        Queue queue = QueueBuilder
                .durable(SECKILL_QUEUE)
                .withArguments(args)
                .build();
        return queue;
    }

    // ========== 第四步：把队列绑到交换机上 ==========
    /**
     * 少了这条绑定，消息能到交换机但路由不到队列，
     * 而且默认情况下不会有任何报错，消息会被静默丢弃。
     */
    @Bean
    public Binding seckillBinding() {
        Binding binding = BindingBuilder
                .bind(seckillQueue())
                .to(seckillExchange())
                .with(SECKILL_ROUTING_KEY);
        return binding;
    }

    // ========== 2026-11-07 第二轮新增：死信交换机 + 死信队列 + 绑定 ==========
    /**
     * 死信交换机：类型和业务交换机保持一致（Direct），省得再想一遍路由规则。
     */
    @Bean
    public DirectExchange seckillDlxExchange() {
        DirectExchange exchange = new DirectExchange(SECKILL_DLX_EXCHANGE, true, false);
        return exchange;
    }

    /**
     * 死信队列：只要求 durable，不再往下转发。
     * 进到这里说明重试也救不回来了，需要人工看日志、必要时手动重放。
     */
    @Bean
    public Queue seckillDlqQueue() {
        Queue queue = QueueBuilder.durable(SECKILL_DLQ_QUEUE).build();
        return queue;
    }

    @Bean
    public Binding seckillDlqBinding() {
        Binding binding = BindingBuilder
                .bind(seckillDlqQueue())
                .to(seckillDlxExchange())
                .with(SECKILL_DLQ_ROUTING_KEY);
        return binding;
    }

    // ========== 第五步：消息转换器（必须配，否则发对象直接报错）==========
    /**
     * 默认的 SimpleMessageConverter 只能发 String / byte[] / Serializable 对象，
     * 直接 convertAndSend 一个 VoucherOrder 会抛 IllegalArgumentException。
     *
     * 声明成 Bean 之后，Spring Boot 会自动把它同时设置到
     * RabbitTemplate（生产端）和 SimpleRabbitListenerContainerFactory（消费端），
     * 两边必须用同一个转换器，否则消费端反序列化会报 ConvertException。
     */
    @Bean
    public MessageConverter jacksonMessageConverter() {
        ObjectMapper objectMapper = new ObjectMapper();

        // 注册 Java 8 时间模块。
        // VoucherOrder 里有 createTime / payTime / useTime 这些 LocalDateTime 字段，
        // 不注册的话 Jackson 会抛 "Java 8 date/time type not supported by default"。
        objectMapper.registerModule(new JavaTimeModule());

        // 时间输出成 ISO 字符串而不是时间戳，方便在管理台里直接肉眼看消息内容
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter(objectMapper);
        return converter;
    }
}
