# 预热秒杀数据到 Redis（端口 6380）
# 作用：1) 创建订单消息队列的消费组 g1；2) 把 tb_seckill_voucher 的库存写入 seckill:stock:{id}
# 每次重启 Redis 或新增秒杀券后执行一次即可。
$ErrorActionPreference = 'Stop'
$MYSQL = 'mysql'
$REDIS_CLI = (Join-Path $PSScriptRoot 'redis-5.0.14.1\redis-cli.exe')
$REDIS_PORT = '6380'

$env:MYSQL_PWD = '20210623'
$rows = & $MYSQL -uroot -N -B locallife -e "select voucher_id, stock from tb_seckill_voucher;" 2>$null
foreach ($row in $rows) {
    $parts = $row -split "`t"
    if ($parts.Count -ge 2) {
        & $REDIS_CLI -p $REDIS_PORT set "seckill:stock:$($parts[0])" $parts[1] | Out-Null
        Write-Host "seckill:stock:$($parts[0]) = $($parts[1])"
    }
}

# 消费组：已存在会报 BUSYGROUP，忽略即可
& $REDIS_CLI -p $REDIS_PORT xgroup create stream.orders g1 0 MKSTREAM 2>$null | Out-Null
Write-Host '消费组 g1 已就绪（stream.orders）'
