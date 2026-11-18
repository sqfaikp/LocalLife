@echo off
chcp 65001 >nul
setlocal

rem ============================================================
rem  LocalLife seckill load test - one click
rem  usage: seckill-test.bat [threads] [voucherId] [stock]
rem  example: seckill-test.bat 1000 2 100
rem ============================================================

set "TOOLS=%~dp0"
if "%TOOLS:~-1%"=="\" set "TOOLS=%TOOLS:~0,-1%"
set "JMETER_BAT=jmeter.bat"
rem If jmeter.bat is not in PATH, set the full path here, e.g.
rem set "JMETER_BAT=D:\apache-jmeter-5.6.3\bin\jmeter.bat"

set "THREADS=%~1"
if "%THREADS%"=="" set "THREADS=1000"
set "VOUCHER=%~2"
if "%VOUCHER%"=="" set "VOUCHER=2"
set "STOCK=%~3"
if "%STOCK%"=="" set "STOCK=100"
set "CSV=%TOOLS:\=/%/tokens.csv"

echo ============================================
echo  LocalLife seckill load test
echo  threads=%THREADS%  voucher=%VOUCHER%  stock=%STOCK%
echo ============================================

if not exist "%JMETER_BAT%" (
  echo [ERROR] JMeter not found: %JMETER_BAT%
  exit /b 1
)

if not exist "%TOOLS%\tokens.csv" (
  echo [ERROR] tokens.csv not found.
  echo         run first: powershell -ExecutionPolicy Bypass -File "%TOOLS%\gen-tokens.ps1" -Count %THREADS%
  exit /b 1
)

echo.
echo [1/3] reset redis + mysql ...
powershell -NoProfile -ExecutionPolicy Bypass -File "%TOOLS%\reset-seckill.ps1" -VoucherId %VOUCHER% -Stock %STOCK%
if errorlevel 1 goto fail

echo.
echo [2/3] run jmeter ...
if exist "%TOOLS%\result.jtl" del /q "%TOOLS%\result.jtl"
if exist "%TOOLS%\report" rmdir /s /q "%TOOLS%\report"
call "%JMETER_BAT%" -n -t "%TOOLS%\seckill.jmx" -l "%TOOLS%\result.jtl" -j "%TOOLS%\jmeter.log" -e -o "%TOOLS%\report" -Jthreads=%THREADS% -JvoucherId=%VOUCHER% -JcsvFile=%CSV%
if errorlevel 1 goto fail

echo.
echo [3/3] verify ...
powershell -NoProfile -ExecutionPolicy Bypass -File "%TOOLS%\verify-seckill.ps1" -VoucherId %VOUCHER% -InitialStock %STOCK%
if errorlevel 1 goto fail

echo.
echo Done. HTML report: %TOOLS%\report\index.html
exit /b 0

:fail
echo.
echo [FAILED] see messages above, jmeter log: %TOOLS%\jmeter.log
exit /b 1
