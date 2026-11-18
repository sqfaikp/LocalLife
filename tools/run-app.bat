@echo off
rem ============================================================
rem  LocalLife - one click start (Windows)
rem  Usage: double click this file, or run tools\run-app.bat
rem  Note : requires JDK 17 (or 8). JDK 25 cannot build/run this project.
rem ============================================================
setlocal

rem Prefer JAVA_HOME; otherwise fall back to a local JDK 17 install.
if not defined JAVA_HOME set "JAVA_HOME=D:\software\jdk17"

set "JAVA_EXE=%JAVA_HOME%\bin\java.exe"
if not exist "%JAVA_EXE%" set "JAVA_EXE=java"

set "APP_DIR=%~dp0.."
set "JAR=%APP_DIR%\target\locallife-0.0.1-SNAPSHOT.jar"

if not exist "%JAR%" (
    echo [INFO] jar not found, building with maven...
    call "%APP_DIR%\mvnw.cmd" -DskipTests package 2>nul || mvn -f "%APP_DIR%\pom.xml" -DskipTests package
)

echo [INFO] Starting LocalLife backend: http://127.0.0.1:8081
"%JAVA_EXE%" "-Dfile.encoding=UTF-8" -jar "%JAR%"
pause
