@echo off
setlocal enabledelayedexpansion

:: --- ENVIRONMENT INITIALIZATION ---
:: Determine the location of the deployment scripts to ensure all relative 
:: path resolutions are consistent regardless of the working directory.
set "SCRIPTS_DIR=%~dp0"
set "PROJECT_ROOT=%SCRIPTS_DIR%..\"

:: Load environment variables from the .env file located in the deploy folder.
:: This provides the script with required S3 bucket names and AWS region settings.
call "%SCRIPTS_DIR%load_env.bat"
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Failed to initialize environment variables. Ensure .env exists.
    exit /b 1
)

:: Validate that essential AWS variables are properly defined before proceeding.
if "%BUCKET_NAME%"=="" (
    echo [ERROR] BUCKET_NAME is not defined in the environment.
    exit /b 1
)

echo ----------------------------------------------------
echo [INFO] AWS S3 BUCKET PROVISIONING
echo ----------------------------------------------------
echo BUCKET: %BUCKET_NAME%
echo REGION: %REGION%
echo ----------------------------------------------------

:: --- BUCKET VALIDATION AND CREATION ---
:: Check if the target S3 bucket exists; create it if it is missing in the specified region.
aws s3api head-bucket --bucket %BUCKET_NAME% 2>nul
if %ERRORLEVEL% equ 0 (
    echo [INFO] Bucket already exists. Skipping creation.
) else (
    echo [INFO] Bucket does not exist. Creating it now in %REGION%...
    aws s3 mb s3://%BUCKET_NAME% --region %REGION%
    if !ERRORLEVEL! neq 0 (
        echo [ERROR] Failed to create bucket. Verify IAM permissions or naming rules.
        exit /b 1
    )
    echo [INFO] Bucket created successfully.
)

:: --- FOLDER STRUCTURE PROVISIONING ---
:: Create the logical folder structure within the bucket.
call "%SCRIPTS_DIR%create_bucket_folders.bat" "logs" "deploy" "flink" "grafana" "telegraf" "influxdb"

:: --- DEPLOYMENT ASSETS SYNCHRONIZATION ---
:: Sync the local deploy directory containing templates, compose files, and bash scripts.
echo [INFO] Syncing deployment scripts and configurations...
aws s3 sync "%SCRIPTS_DIR%." "s3://%BUCKET_NAME%/deploy/" ^
    --exclude "*.bat" --include "*/*.bat" ^
    --exclude "*.sh" --include "*/*.sh" ^
    --exclude ".env*" --include "envs/*.env" ^
    --exclude "template/*"

:: Sync Telegraf configuration
if exist "%PROJECT_ROOT%telegraf\" (
    echo [INFO] Syncing Telegraf configuration...
    aws s3 sync "%PROJECT_ROOT%telegraf/" "s3://%BUCKET_NAME%/telegraf/"
)

:: Sync InfluxDB configurations & initialization scripts
if exist "%PROJECT_ROOT%influxdb\" (
    echo [INFO] Syncing InfluxDB initialization assets...
    aws s3 sync "%PROJECT_ROOT%influxdb/" "s3://%BUCKET_NAME%/influxdb/"
)

:: Sync Grafana dashboard and datasource provisioning configurations from data/grafana
if exist "%PROJECT_ROOT%data\grafana\" (
    echo [INFO] Syncing Grafana dashboards and datasources...
    aws s3 sync "%PROJECT_ROOT%data/grafana/" "s3://%BUCKET_NAME%/grafana/"
)

:: Upload the Flink application JAR and configuration files.
if exist "%PROJECT_ROOT%flink\target\flight-analysis-1.0.jar" (
    echo [INFO] Uploading Flink application JAR...
    aws s3 cp "%PROJECT_ROOT%flink/target/flight-analysis-1.0.jar" "s3://%BUCKET_NAME%/flink/flight-analysis.jar"
) else if exist "%PROJECT_ROOT%flink\target\flight-analysis-1.0-shaded.jar" (
    echo [INFO] Uploading Flink shaded application JAR...
    aws s3 cp "%PROJECT_ROOT%flink/target/flight-analysis-1.0-shaded.jar" "s3://%BUCKET_NAME%/flink/flight-analysis.jar"
)

echo.
echo ----------------------------------------------------
echo [INFO] S3 Synchronization completed successfully!
echo Assets location: s3://%BUCKET_NAME%/
echo ----------------------------------------------------

exit /b 0
