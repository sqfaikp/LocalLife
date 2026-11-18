param(
    [int]$VoucherId = 2,
    [int]$Stock = 100,
    [string]$RedisCli = (Join-Path $PSScriptRoot 'redis-5.0.14.1\redis-cli.exe'),
    [int]$RedisPort = 6380,
    [string]$Mysql = 'mysql',
    [string]$MysqlUser = 'root',
    [string]$MysqlPwd = '20210623',
    [string]$Database = 'locallife'
)

# 压测前把环境重置干净：
#   1) Redis 库存回到 Stock，清掉"已下单用户"集合、清空消息队列并重建消费组
#   2) MySQL 库存回到 Stock，删掉这张券的历史订单
# 注意：只影响 VoucherId 这一张券，不动其它数据

$ErrorActionPreference = 'Stop'
$env:MYSQL_PWD = $MysqlPwd

Write-Host "== 重置秒杀环境 (voucherId=$VoucherId, stock=$Stock) ==" -ForegroundColor Cyan

& $RedisCli -p $RedisPort set "seckill:stock:$VoucherId" $Stock | Out-Null
& $RedisCli -p $RedisPort del "seckill:order:$VoucherId" | Out-Null
& $RedisCli -p $RedisPort del 'stream.orders' | Out-Null
& $RedisCli -p $RedisPort xgroup create 'stream.orders' g1 0 MKSTREAM | Out-Null

$sql = "UPDATE tb_seckill_voucher SET stock=$Stock WHERE voucher_id=$VoucherId; DELETE FROM tb_voucher_order WHERE voucher_id=$VoucherId;"
& $Mysql "-u$MysqlUser" -N -B $Database -e $sql | Out-Null

$redisStock = (& $RedisCli -p $RedisPort --raw get "seckill:stock:$VoucherId") -join ''
$dbStock = (& $Mysql "-u$MysqlUser" -N -B $Database -e "SELECT stock FROM tb_seckill_voucher WHERE voucher_id=$VoucherId;") -join ''
$orders = (& $Mysql "-u$MysqlUser" -N -B $Database -e "SELECT COUNT(*) FROM tb_voucher_order WHERE voucher_id=$VoucherId;") -join ''

Write-Host ("Redis 库存: {0}" -f $redisStock)
Write-Host ("MySQL 库存: {0}" -f $dbStock)
Write-Host ("历史订单: {0}" -f $orders)
Write-Host "消息队列 stream.orders 已清空，消费组 g1 已重建" -ForegroundColor Green
