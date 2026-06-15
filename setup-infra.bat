@echo off
setlocal enabledelayedexpansion

:: setup-infra.bat - Automates local infrastructure startup and InfluxDB 3 token generation for Windows

echo Starting Kafka and InfluxDB...
docker compose up -d influxdb kafka

echo Waiting for InfluxDB health check...
:wait_influx
curl -s http://localhost:8181/health > nul
if errorlevel 1 (
    echo Still waiting for InfluxDB...
    timeout /t 2 > nul
    goto wait_influx
)

echo Generating new InfluxDB 3 admin token...
:: Execute docker command and capture the line containing "Token:"
for /f "tokens=2" %%i in ('docker exec influxdb influxdb3 create token --admin ^| findstr "Token:"') do (
    set NEW_TOKEN=%%i
)

if "%NEW_TOKEN%"=="" (
    echo Error: Failed to generate InfluxDB token.
    exit /b 1
)

echo Token generated: !NEW_TOKEN:~0,10!...

:: Update .env file
echo Updating .env file...
if exist .env (
    powershell -Command "(gc .env) -replace '^INFLUXDB_TOKEN=.*', 'INFLUXDB_TOKEN=%NEW_TOKEN%' | Out-File -encoding ASCII .env"
) else (
    echo INFLUXDB_TOKEN=%NEW_TOKEN% > .env
)

echo Running initialization containers...
docker compose up -d

echo Infrastructure is ready!
