@echo off

:: --- ENVIRONMENT LOADER ---
:: This script parses the project's .env file and exports its key-value pairs 
:: as local environment variables for the current session.

:: The script first checks the current directory for a .env file, then 
:: attempts to find it in the parent directory if the current one is missing.
set "ENV_PATH=.env"
if not exist "%ENV_PATH%" set "ENV_PATH=../.env"

if not exist "%ENV_PATH%" (
    echo [ERROR] Source environment file was not found.
    exit /b 1
)

:: Iterate through the environment file, skipping comments and empty lines.
:: Each valid entry is exported to the local environment scope.
:: Using findstr /v /b "#" ensures lines starting with '#' are ignored.
for /f "usebackq tokens=*" %%i in (`findstr /v /b "#" "%ENV_PATH%"`) do (
    set "%%i"
)

echo [INFO] Environment variables successfully imported from %ENV_PATH%.
exit /b 0
