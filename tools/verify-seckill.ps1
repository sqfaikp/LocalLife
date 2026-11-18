param(
    [int]$VoucherId = 2,
    [int]$InitialStock = 100,
    [string]$Jtl = (Join-Path $PSScriptRoot 'result.jtl'),
    [string]$RedisCli = (Join-Path $PSScriptRoot 'redis-5.0.14.1\redis-cli.exe'),
    [int]$RedisPort = 6380,
    [string]$Mysql = 'mysql',
    [string]$MysqlUser = 'root',
    [string]$MysqlPwd = '20210623',
    [string]$Database = 'locallife'
)

# 压测结果校验：看有没有超卖、有没有重复下单、库存对不对得上

$env:MYSQL_PWD = $MysqlPwd
$ok = $true

Write-Host ""
Write-Host "== 压测结果校验 ==" -ForegroundColor Cyan

# 下单是异步的（Redis Stream 消费者线程在后台建单），先等它把消息处理完：
# 订单数连续 3 次不再变化（或最多等 20 秒）才认为处理结束
Write-Host "等待异步下单线程处理完消息..." -NoNewline
$lastCount = -1
$stable = 0
for ($i = 0; $i -lt 40; $i++) {
    $c = [int]((& $Mysql "-u$MysqlUser" -N -B $Database -e "SELECT COUNT(*) FROM tb_voucher_order WHERE voucher_id=$VoucherId;") -join '')
    if ($c -eq $lastCount) { $stable++ } else { $stable = 0 }
    $lastCount = $c
    if ($stable -ge 3) { break }
    Write-Host "." -NoNewline
    Start-Sleep -Milliseconds 500
}
Write-Host " 完成"

$dbStock = [int]((& $Mysql "-u$MysqlUser" -N -B $Database -e "SELECT stock FROM tb_seckill_voucher WHERE voucher_id=$VoucherId;") -join '')
$orderRows = (& $Mysql "-u$MysqlUser" -N -B $Database -e "SELECT COUNT(*), COUNT(DISTINCT user_id) FROM tb_voucher_order WHERE voucher_id=$VoucherId;") -join ''
$orderCount = [int](($orderRows -split "`t")[0])
$userCount = [int](($orderRows -split "`t")[1])
$redisStock = (& $RedisCli -p $RedisPort --raw get "seckill:stock:$VoucherId") -join ''

Write-Host ("MySQL 剩余库存 : {0}   (初始 {1})" -f $dbStock, $InitialStock)
Write-Host ("Redis 剩余库存 : {0}" -f $redisStock)
Write-Host ("成功订单数     : {0}" -f $orderCount)
Write-Host ("下单用户数     : {0}" -f $userCount)

# 1. 超卖
if ($dbStock -lt 0 -or $orderCount -gt $InitialStock) {
    Write-Host "[FAIL] 出现超卖！订单数 $orderCount > 初始库存 $InitialStock，或库存为负" -ForegroundColor Red
    $ok = $false
} else {
    Write-Host "[PASS] 没有超卖" -ForegroundColor Green
}

# 2. 一人一单
if ($orderCount -ne $userCount) {
    Write-Host "[FAIL] 有用户重复下单：订单 $orderCount 条，但只有 $userCount 个不同用户" -ForegroundColor Red
    $ok = $false
} else {
    Write-Host "[PASS] 一人一单（订单数 = 用户数）" -ForegroundColor Green
}

# 3. 库存一致性
if ("$redisStock" -ne "$dbStock") {
    Write-Host "[WARN] Redis 库存($redisStock) 和 MySQL 库存($dbStock) 不一致" -ForegroundColor Yellow
} else {
    Write-Host "[PASS] Redis / MySQL 库存一致" -ForegroundColor Green
}

# 4. 是否把库存打空
if ($orderCount -lt $InitialStock) {
    Write-Host ("[WARN] 只成功 {0} 单，库存没打完（并发不够 / token 不够 / 请求提前失败）" -f $orderCount) -ForegroundColor Yellow
} else {
    Write-Host "[PASS] 库存被打空，订单数正好等于初始库存" -ForegroundColor Green
}

# 5. JMeter 原始结果
if (Test-Path $Jtl) {
    Write-Host ""
    Write-Host "-- JMeter 采样统计 --" -ForegroundColor Cyan
    $rows = @(Import-Csv $Jtl)
    if ($rows.Count -gt 0) {
        $total = $rows.Count
        $success = @($rows | Where-Object { $_.success -eq 'true' }).Count
        $elapsed = $rows | ForEach-Object { [double]$_.elapsed }
        $avg = ($elapsed | Measure-Object -Average).Average
        $max = ($elapsed | Measure-Object -Maximum).Maximum
        $p95 = ($elapsed | Sort-Object)[[int]([Math]::Floor($elapsed.Count * 0.95))]
        Write-Host ("总请求   : {0}" -f $total)
        Write-Host ("HTTP 成功: {0}  ({1:P1})" -f $success, ($success / $total))
        Write-Host ("平均响应 : {0:N0} ms   最大 {1:N0} ms   P95 {2:N0} ms" -f $avg, $max, $p95)
        Write-Host ""
        Write-Host "响应分布（业务层成败都算 HTTP 200）:"
        $rows | Group-Object responseCode | Sort-Object Count -Descending | ForEach-Object {
            Write-Host ("  HTTP {0}: {1}" -f $_.Name, $_.Count)
        }
    }
} else {
    Write-Host "[WARN] 没找到 jtl 文件：$Jtl" -ForegroundColor Yellow
}

Write-Host ""
if ($ok) {
    Write-Host "结论: 通过 ✅" -ForegroundColor Green
    exit 0
} else {
    Write-Host "结论: 有问题 ❌" -ForegroundColor Red
    exit 1
}
