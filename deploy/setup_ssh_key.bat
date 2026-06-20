@echo off
setlocal enabledelayedexpansion

:: --- SSH KEY PROVISIONING TOOL ---
:: This utility ensures that the required SSH key pair is available both in the 
:: AWS account and on the local development machine with secure permissions.

:: --- ENVIRONMENT INITIALIZATION ---
if "%BUCKET_NAME%"=="" (
    call load_env.bat
    if !ERRORLEVEL! neq 0 (
        echo [ERROR] Environment initialization failed during SSH setup.
        exit /b 1
    )
)

echo [INFO] Provisioning SSH Access: %SSH_KEY_NAME%

:: Ensure the local directory for cryptographic keys exists.
if not exist "%SSH_KEY_DIR%" (
    mkdir "%SSH_KEY_DIR%"
)

:: --- KEY PAIR DISCOVERY AND GENERATION ---
:: Query AWS EC2 to verify if the specified key pair already exists.
aws ec2 describe-key-pairs --key-names "%SSH_KEY_NAME%" >nul 2>&1

if %ERRORLEVEL% neq 0 (
    echo [INFO] Key pair '%SSH_KEY_NAME%' not found in AWS. Generating new pair...
    
    :: Create the key pair in AWS and capture the private key material locally.
    aws ec2 create-key-pair --key-name "%SSH_KEY_NAME%" --query "KeyMaterial" --output text > "%SSH_KEY_DIR%\%SSH_KEY_NAME%.pem"
    
    :: Apply restrictive Windows permissions - Required for OpenSSH compliance.
    :: This removes inherited ACLs and grants exclusive read access to the current user.
    echo [INFO] Applying restrictive file permissions to the private key...
    icacls "%SSH_KEY_DIR%\%SSH_KEY_NAME%.pem" /inheritance:r >nul
    icacls "%SSH_KEY_DIR%\%SSH_KEY_NAME%.pem" /grant:r "%USERNAME%:(R)" >nul
    
    echo [INFO] Key pair successfully created and secured in %SSH_KEY_DIR%.
) else (
    if not exist "%SSH_KEY_DIR%\%SSH_KEY_NAME%.pem" (
        echo [WARNING] Key pair exists in AWS but the local .pem file is missing in %SSH_KEY_DIR%.
        echo           Ensure you have the correct private key to access the cluster nodes.
    ) else (
        :: Consistently re-apply permissions to maintain a secure state across executions.
        icacls "%SSH_KEY_DIR%\%SSH_KEY_NAME%.pem" /inheritance:r >nul
        icacls "%SSH_KEY_DIR%\%SSH_KEY_NAME%.pem" /grant:r "%USERNAME%:(R)" >nul
        echo [INFO] SSH Key pair is ready and secured both locally and in AWS.
    )
)

exit /b 0
