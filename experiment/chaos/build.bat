@echo off
:: Stop showing commands on the terminal
setlocal enabledelayedexpansion

set IMAGE_NAME=fmasci/flink-chaos-simulator
set TAG=0.1.0

echo ==========================================
echo  Starting Build ^& Push Process
echo ==========================================

:: Build the Docker image locally
echo =^> Building Docker image: %IMAGE_NAME%:%TAG%...
docker build -t %IMAGE_NAME%:%TAG% .
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Docker build failed.
    exit /b %ERRORLEVEL%
)

:: Push the built image to Docker Hub repository
echo =^> Pushing image to Docker Hub...
docker push %IMAGE_NAME%:%TAG%
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Docker push failed.
    exit /b %ERRORLEVEL%
)

echo ==========================================
echo  Process Completed Successfully!
echo ==========================================
