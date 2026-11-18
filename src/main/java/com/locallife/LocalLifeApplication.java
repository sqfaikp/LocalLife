package com.locallife;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 启动类。
 *
 * 2026-11-07 第二轮改动：加 @EnableScheduling。
 * 原因：SeckillReconcileTask 用的是 @Scheduled 定时任务，
 * 没有这个注解 Spring 根本不会去扫描和执行 @Scheduled 方法 ——
 * 而且不会有任何报错，表现就是"对账任务好像从来没跑过"，极难发现。
 */
@EnableScheduling
@MapperScan("com.locallife.mapper")
@SpringBootApplication
public class LocalLifeApplication {

    public static void main(String[] args) {
        SpringApplication.run(LocalLifeApplication.class, args);
    }

}
