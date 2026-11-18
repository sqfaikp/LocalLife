# LocalLife · 本地生活服务平台后端

一个基于 Spring Boot 的本地生活服务后端，覆盖商铺浏览、探店笔记、关注点赞、优惠券秒杀等典型场景。
重点不在于功能数量，而在于**高并发场景下的缓存、锁与异步下单方案**。

---

## 技术栈

| 分类 | 技术 |
| --- | --- |
| 框架 | Spring Boot 2.3.12、Spring MVC、Spring AOP |
| 持久层 | MyBatis-Plus 3.4.3、MySQL 8.0、HikariCP |
| 缓存 | Redis（Lettuce 连接池）、Spring Data Redis |
| 分布式锁 | Redisson 3.13.6 |
| 消息队列 | RabbitMQ（Spring AMQP） |
| 工具 | Lombok、Hutool 5.7 |
| 构建 | Maven 3.9 |

---

## 功能模块

- **用户**：手机验证码登录、Token 续期、每日签到（BitMap）、用户信息查询
- **商铺**：按类型分页查询、商铺详情（缓存）、按经纬度搜索附近商铺（GEO）
- **探店笔记**：发布图文、分页滚动查询、点赞（SortedSet 按时间排序）
- **社交**：关注 / 取关、共同关注（Set 交集）
- **优惠券秒杀**：库存预扣、一人一单、异步下单、订单对账
- **文件上传**：图片上传到本地目录

---

## 关键技术实现

### 1. 登录鉴权：双拦截器 + Redis Hash

验证码写入 Redis（2 分钟过期）；登录成功后生成随机 Token，用户信息以 Hash 结构存 Redis 并设置 30 分钟 TTL。

两个拦截器配合使用：

- `RefreshTokenInterceptor`：拦截所有请求，有 Token 就刷新 TTL 并把用户存入 `ThreadLocal`
- `LoginInterceptor`：只拦需要登录的路径，`ThreadLocal` 为空则返回 401

这样既保证"用着就不会掉线"，又不需要在业务代码里到处取 Token。

### 2. 缓存：穿透与击穿

封装 `CacheClient`，三种策略可按需切换：

| 问题 | 方案 | 说明 |
| --- | --- | --- |
| 缓存穿透 | 空值缓存 | 查不到也写一个空串进 Redis，TTL 2 分钟，避免同一不存在的 ID 反复打到库 |
| 缓存击穿（强一致） | 互斥锁 | 未命中时 `SETNX` 抢锁，只放一个线程查库并回填，其余线程自旋重试 |
| 缓存击穿（高可用） | 逻辑过期 | value 里带过期时间字段，过期后异步起线程重建，请求始终直接返回旧数据 |

`ShopServiceImpl#queryById` 里三种实现都保留着，切换只需注释 / 反注释。

### 3. 秒杀：Lua 原子校验 + MQ 异步下单

下单一共四道关：

1. **Lua 脚本原子执行**（`seckill.lua`）：判断库存、判断是否重复下单、扣库存、写"已下单用户"集合 —— 全部在一次 Redis 调用内完成，杜绝判断与扣减之间的并发窗口
2. **RabbitMQ 异步落库**：Lua 校验通过后只负责发一条消息，接口立刻返回订单号，数据库写入由消费者完成，把 RT 从"等一次 DB 事务"降到"一次 Redis + 一次投递"
3. **生产端可靠性**：开启 `publisher-confirm` + `publisher-returns` + `mandatory`，消息没到交换机或没路由到队列都会回调并记录
4. **消费端可靠性**：手动 ACK + `prefetch=1`；业务异常时 `nack(requeue=false)` 进死信队列，避免消息被反复重投打爆 CPU

此外还有 `SeckillReconcileTask` 定时对账：比对 Redis 中"已扣库存但未确认落库"的订单与数据库实际订单，修复中间态。

> 早期版本用的是 Redis Stream 消费组，代码（`VoucherOrderHandler`）保留着没有删，方便对照两种方案的差异。

### 4. 分布式锁

- `SimpleRedisLock`：`SETNX + UUID` 实现，释放时用 Lua 保证"判断是自己的锁"与"删除"的原子性
- `RedissonClient`：生产环境使用，带看门狗自动续期，避免业务执行超时导致锁提前释放

### 5. 全局唯一订单 ID

`RedisIdWorker`：`时间戳 << 32 | Redis 自增序列`。既保证趋势递增（利于 B+ 树索引），又保证分布式环境下不重复。

### 6. 附近商铺

Redis GEO：店铺坐标写入 `geo:shop:typeId`，查询时用 `GEOSEARCH` 按半径搜索并按距离排序，再批量回表组装结果。

---

## 快速开始

### 环境要求

| 组件 | 版本 | 默认端口 |
| --- | --- | --- |
| JDK | 8 或 17 | - |
| MySQL | 8.0 | 3306 |
| Redis | 5.0+（需支持 Stream） | 6380 |
| RabbitMQ | 3.x | 5672 |
| Maven | 3.6+ | - |

> Redis 默认用 6380 是为了避开本机已有的 6379 实例，可在 `application.yaml` 里改成你自己的端口。

### 1. 初始化数据库

```bash
mysql -u root -p -e "CREATE DATABASE locallife DEFAULT CHARACTER SET utf8mb4;"
mysql -u root -p locallife < src/main/resources/db/locallife.sql
```

### 2. 修改配置

编辑 `src/main/resources/application.yaml`，把数据库、Redis、RabbitMQ 的连接信息改成你本机的：

```yaml
spring:
  datasource:
    username: root
    password: 你的密码
```

### 3. 启动

```bash
# 方式一：Maven
mvn spring-boot:run

# 方式二：打包后运行
mvn clean package -DskipTests
java -jar target/locallife-0.0.1-SNAPSHOT.jar
```

Windows 上可以直接双击 `tools/run-app.bat`，它会自动选 JDK 17 并打包运行。

服务启动在 <http://127.0.0.1:8081>。

### 4. 秒杀压测（可选）

`tools/` 下有一套完整脚本：

| 脚本 | 作用 |
| --- | --- |
| `start-redis.bat` | 按 `redis-locallife.conf` 启动 6380 端口的 Redis，并预热秒杀库存 |
| `gen-tokens.ps1` | 批量注册用户并换取登录 Token，输出 `tokens.csv` |
| `seckill-test.bat` | 一键跑 JMeter 压测（重置数据 → 压测 → 校验） |
| `reset-seckill.ps1` | 把库存、订单、Redis 队列恢复到初始状态 |
| `verify-seckill.ps1` | 校验有没有超卖、重复下单 |

---

## 目录结构

```
src/main/java/com/locallife
├── config          # MVC、MyBatis-Plus、Redisson、RabbitMQ 配置
├── controller      # 接口层
├── dto             # 请求 / 响应对象
├── entity          # 数据库实体
├── mapper          # MyBatis-Plus Mapper
├── mq              # 消息消费者、发送确认回调、对账任务
├── service         # 业务接口与实现
└── utils           # 缓存工具、分布式锁、拦截器、ID 生成器
```

---

## 说明

- 本项目为个人学习与实践项目，业务模型参考了常见本地生活类产品。
- 配置文件中的数据库密码仅为本地开发使用，部署到服务器前请改为环境变量注入。
