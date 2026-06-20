@echo off
setlocal enabledelayedexpansion

:: --- FULL DEPLOYMENT PIPELINE ORCHESTRATOR ---
:: This script executes the complete deployment sequence for the Flink & Kafka cluster:
:: 1. Provisions network infrastructure (VPC, Subnets, DNS, Security Groups).
:: 2. Initializes and synchronizes deployment assets to the Amazon S3 storage bucket.
:: 3. Spins up the EC2 instances via CloudFormation, installing Docker and running services.

echo ==========================================================
echo [START] INITIATING FULL EC2 DEPLOYMENT PIPELINE
echo ==========================================================

:: 1. Provisions VPC and Network Stack
call deploy-network.bat
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Network infrastructure provisioning failed. Aborting.
    exit /b 1
)

:: 2. Sync files and compilation packages to S3
call init-bucket.bat
if %ERRORLEVEL% neq 0 (
    echo [ERROR] S3 bucket synchronization failed. Aborting.
    exit /b 1
)

:: 3. Provision and configure EC2 cluster instances
call deploy-node.bat all
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Node cluster provisioning failed. Aborting.
    exit /b 1
)

echo ==========================================================
echo [SUCCESS] FULL CLUSTER DEPLOYMENT COMPLETED!
echo ==========================================================
exit /b 0
