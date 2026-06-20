@echo off

:: --- S3 BUCKET FOLDER INITIALIZER ---
:: This utility ensures that the logical directory structure required
:: by the project exists within the target S3 bucket.

:: --- FOLDER PROVISIONING LOOP ---
:: Process each folder name passed as a command-line argument.
:loop
if "%~1"=="" goto :done
    set "FOLDER_NAME=%~1"

    echo [INFO] Validating existence of S3 prefix: %FOLDER_NAME%/
    
    :: Use 'aws s3 ls' to verify if the prefix (logical folder) exists in the bucket.
    aws s3 ls "s3://%BUCKET_NAME%/%FOLDER_NAME%/" 2>nul

    if %ERRORLEVEL% neq 0 (
        echo [INFO] Prefix '%FOLDER_NAME%/' missing. Creating placeholder object...
        
        :: In S3, 'folders' are simulated by creating an empty object with a trailing slash in its key.
        aws s3api put-object --bucket %BUCKET_NAME% --key %FOLDER_NAME%/ >nul
        
        if !ERRORLEVEL! equ 0 (
            echo [INFO] Successfully initialized S3 prefix: %FOLDER_NAME%/
        ) else (
            echo [ERROR] Failed to initialize S3 prefix: %FOLDER_NAME%/. Verify bucket existence and IAM permissions.
        )
    ) else (
        echo [INFO] S3 prefix '%FOLDER_NAME%/' is already present.
    )

    :: Shift to the next argument provided to the script.
    shift
    goto :loop

:done
exit /b 0
