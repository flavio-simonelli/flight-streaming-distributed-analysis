@echo off
SET IMAGE_NAME=flight-simulator
SET CONTAINER_NAME=simulator

echo Building Docker image...
docker build -t %IMAGE_NAME% .
if %errorlevel% neq 0 (
    echo Build failed.
    exit /b %errorlevel%
)

:: Remove old container if it exists
echo Checking for old container...
docker rm -f %CONTAINER_NAME% 2>nul

echo Starting Flight Simulator Container...
docker run -it --network sae-net ^
  --name %CONTAINER_NAME% ^
  -e INPUT_PARQUET_PATH="/app/data/flights.parquets" ^
  -e OUTPUT_TYPE="kafka" ^
  -e KAFKA_BROKERS="kafka.flight-analysis.local:9092" ^
  -e KAFKA_TOPIC="flights-stream" ^
  -e MAX_RECORDS=50 ^
  -v "%cd%/data:/app/data" ^
  %IMAGE_NAME%
