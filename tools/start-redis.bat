@echo off
rem Start dedicated Redis 5.0 for LocalLife (port 6380, no password), then preheat seckill data.
rem NOTE: keep this file ASCII-only. cmd.exe reads .bat with the OEM code page (936),
rem so UTF-8 Chinese comments get split into garbage commands.
cd /d "%~dp0redis-5.0.14.1"
start "locallife-redis-6380" redis-server.exe redis-locallife.conf
timeout /t 2 >nul
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0preheat-seckill.ps1"
echo.
echo Redis 5.0.14 started: 127.0.0.1:6380 (original Redis 3.2 still on 6379, untouched)
