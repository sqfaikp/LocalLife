param(
    [int]$Count = 1000,
    [string]$BaseUrl = 'http://127.0.0.1:8081',
    [int]$RedisPort = 6380,
    [string]$RedisCli = (Join-Path $PSScriptRoot 'redis-5.0.14.1\redis-cli.exe'),
    [string]$OutFile = (Join-Path $PSScriptRoot 'tokens.csv'),
    [int]$Batch = 100
)

# 批量造压测用的登录 token
# 原理：POST /user/code 发码 -> 从 Redis 读 login:code:{手机号} -> POST /user/login 换 token
# 输出 tokens.csv（第一行是表头 token，JMeter 里配置了忽略首行）

$ErrorActionPreference = 'Stop'
$sw = [Diagnostics.Stopwatch]::StartNew()

Write-Host "== 批量生成 token ==" -ForegroundColor Cyan
Write-Host "目标数量: $Count   后端: $BaseUrl"

try {
    Invoke-WebRequest "$BaseUrl/shop-type/list" -UseBasicParsing -TimeoutSec 5 | Out-Null
} catch {
    Write-Host "[ERROR] 后端没有启动，先启动 Spring Boot 再来。($BaseUrl)" -ForegroundColor Red
    exit 1
}

$tokens = New-Object System.Collections.Generic.List[string]
$fail = 0

for ($start = 1; $start -le $Count; $start += $Batch) {
    $end = [Math]::Min($start + $Batch - 1, $Count)
    $phones = @()
    for ($i = $start; $i -le $end; $i++) {
        $phones += ('138' + $i.ToString('00000000'))
    }

    # 1. 发验证码（后端会把码写进 Redis，2 分钟有效）
    foreach ($p in $phones) {
        try { Invoke-RestMethod "$BaseUrl/user/code?phone=$p" -Method Post -TimeoutSec 10 | Out-Null } catch { }
    }

    # 2. 一次性从 Redis 批量取验证码
    $keys = @($phones | ForEach-Object { "login:code:$_" })
    $codes = & $RedisCli -p $RedisPort --raw mget @keys

    # 3. 逐个登录换 token
    for ($i = 0; $i -lt $phones.Count; $i++) {
        $code = if ($codes -is [array]) { $codes[$i] } else { $codes }
        if ([string]::IsNullOrWhiteSpace($code)) { $fail++; continue }
        try {
            $body = @{ phone = $phones[$i]; code = "$code" } | ConvertTo-Json -Compress
            $r = Invoke-RestMethod "$BaseUrl/user/login" -Method Post -ContentType 'application/json' -Body $body -TimeoutSec 10
            if ($r.success -and $r.data) { $tokens.Add([string]$r.data) } else { $fail++ }
        } catch {
            $fail++
        }
    }
    Write-Host ("  进度: {0}/{1}  (失败 {2})" -f $tokens.Count, $Count, $fail)
}

$lines = @('token') + $tokens
[IO.File]::WriteAllLines($OutFile, $lines, [Text.UTF8Encoding]::new($false))

$sw.Stop()
Write-Host ""
Write-Host ("完成: {0} 个 token -> {1}" -f $tokens.Count, $OutFile) -ForegroundColor Green
Write-Host ("耗时: {0:N1} 秒，失败: {1}" -f $sw.Elapsed.TotalSeconds, $fail)
if ($tokens.Count -lt $Count) {
    Write-Host "[WARN] token 数量不足线程数，压测时多余线程会直接停止" -ForegroundColor Yellow
}
