@echo off
setlocal enabledelayedexpansion

:: --- NETWORK INFRASTRUCTURE DEPLOYMENT ORCHESTRATOR ---
:: This script manages the provisioning of the core VPC network components, 
:: including subnets, security groups, and private DNS zones.

:: --- ENVIRONMENT INITIALIZATION ---
call load_env.bat
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Environment initialization failed. Ensure load_env.bat and .env are present.
    exit /b 1
)

echo ----------------------------------------------------
echo [INFO] NETWORK INFRASTRUCTURE PROVISIONING
echo VPC:    %VPC_NAME%
echo Subnet: %SUBNET_NAME%
echo ----------------------------------------------------

:: Generate the stack name derived from the VPC configuration.
set "VPC_STACK_NAME=%VPC_NAME%-stack"

:: --- INFRASTRUCTURE VALIDATION ---
:: Check for the existence of the subnet before attempting deployment to avoid redundant stack updates.
echo [INFO] Validating existing network infrastructure...
aws ec2 describe-subnets --filters "Name=tag:Name,Values=%SUBNET_NAME%" --query "Subnets[0].SubnetId" --output text 2>nul | findstr /v "None" >nul

if %ERRORLEVEL% equ 0 (
    echo [INFO] Network infrastructure '%SUBNET_NAME%' already exists. Skipping CloudFormation deployment.
) else (
    echo [INFO] Infrastructure not found. Initiating CloudFormation deployment...

    :: Deploy the core VPC Infrastructure stack using the cluster-vpc template.
    aws cloudformation deploy ^
      --stack-name "%VPC_STACK_NAME%" ^
      --template-file "template/cluster-vpc.yaml" ^
      --parameter-overrides ^
          VpcName="%VPC_NAME%" ^
          SubnetName="%SUBNET_NAME%" ^
          VpcCIDR=%VPC_CIDR% ^
          PublicSubnetCIDR=%SUBNET_CIDR% ^
          SgName="%SG_NAME%" ^
          PrivateDomainName="%PRIVATE_DOMAIN_NAME%" ^
      --no-fail-on-empty-changeset

    if !ERRORLEVEL! neq 0 (
        echo [ERROR] CloudFormation network deployment failed. Check the AWS Console for details.
        exit /b 1
    )
)

:: --- FINAL VERIFICATION ---
:: Verify and display the deployed network state for operational awareness.
echo [INFO] Finalizing network status check...

set "FOUND_SUBNET_ID=None"
for /f "tokens=*" %%i in ('aws ec2 describe-subnets --filters "Name=tag:Name,Values=%SUBNET_NAME%" --query "Subnets[0].SubnetId" --output text 2^>nul') do (
    set "FOUND_SUBNET_ID=%%i"
)

if "%FOUND_SUBNET_ID%"=="None" (
    echo [ERROR] Subnet provisioning could not be verified. 
    set "EXIT_CODE=1"
) else (
    echo [INFO] Network infrastructure is ready.
    echo        Subnet ID: %FOUND_SUBNET_ID%
    
    :: Retrieve the parent VPC ID for comprehensive reporting.
    for /f "tokens=*" %%j in ('aws ec2 describe-subnets --subnet-ids %FOUND_SUBNET_ID% --query "Subnets[0].VpcId" --output text') do (
        echo        Parent VPC: %%j
    )
    set "EXIT_CODE=0"
)

exit /b %EXIT_CODE%
