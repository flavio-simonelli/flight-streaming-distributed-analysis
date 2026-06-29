@echo off
setlocal enabledelayedexpansion

:: --- DEPLOYMENT ORCHESTRATOR FOR FLINK/KAFKA CLUSTER ---
:: This script manages the deployment of various node types across the EC2 cluster.
:: It supports targeted deployment (kafka, influxdb, flink-master, flink-worker, metrics)
:: or full cluster deployment (all).

:: --- PARAMETER CHECK ---
set "TARGET=%~1"
if "%TARGET%"=="" set "TARGET=all"

:: Validate the provided target against supported node types.
if /I "%TARGET%" NEQ "kafka" if /I "%TARGET%" NEQ "influxdb" if /I "%TARGET%" NEQ "flink-master" if /I "%TARGET%" NEQ "flink-worker" if /I "%TARGET%" NEQ "metrics" if /I "%TARGET%" NEQ "all" (
    echo [ERROR] Invalid deployment target: %TARGET%
    echo Usage: deploy-node.bat [kafka ^^| influxdb ^^| flink-master ^^| flink-worker ^^| metrics ^^| all] [count]
    exit /b 1
)

:: --- ENVIRONMENT INITIALIZATION ---
call load_env.bat
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Failed to load environment variables. Ensure .env exists in the current directory.
    exit /b 1
)

:: --- SSH KEY PROVISIONING ---
call setup_ssh_key.bat
if %ERRORLEVEL% neq 0 (
    echo [ERROR] SSH key setup failed. Deployment aborted.
    exit /b 1
)

echo [INFO] Retrieving Cloud Infrastructure context...

:: --- INFRASTRUCTURE DISCOVERY ---
set "SUBNET_ID=None"
for /f "tokens=*" %%i in ('aws ec2 describe-subnets --filters "Name=tag:Name,Values=%SUBNET_NAME%" --query "Subnets[0].SubnetId" --output text') do set "SUBNET_ID=%%i"

set "SG_ID=None"
for /f "tokens=*" %%i in ('aws ec2 describe-security-groups --filters "Name=group-name,Values=%SG_NAME%" --query "SecurityGroups[0].GroupId" --output text') do set "SG_ID=%%i"

set "ZONE_ID=None"
for /f "tokens=*" %%i in ('aws route53 list-hosted-zones-by-name --dns-name %PRIVATE_DOMAIN_NAME% --query "HostedZones[0].Id" --output text') do (
    set "RAW_ZONE_ID=%%i"
    set "ZONE_ID=!RAW_ZONE_ID:/hostedzone/=!"
)

echo [INFO] Infrastructure Discovery Results:
echo        Subnet: %SUBNET_ID%
echo        Security Group: %SG_ID%
echo        DNS Zone: %ZONE_ID%

if "%SUBNET_ID%"=="None" (
    echo [ERROR] Required network infrastructure is missing. Run deploy-network.bat first.
    exit /b 1
)

:: --- PARAMETER OVERRIDES ---
if /I "%TARGET%"=="kafka" (
    if "%~2" NEQ "" (
        set "KAFKA_COUNT=%~2"
        echo [INFO] Overriding KAFKA_COUNT to: !KAFKA_COUNT!
    )
)

if /I "%TARGET%"=="flink-worker" (
    if "%~2" NEQ "" (
        set "FLINK_WORKER_COUNT=%~2"
        echo [INFO] Overriding FLINK_WORKER_COUNT to: !FLINK_WORKER_COUNT!
    )
)

set "KH_PATH=%USERPROFILE%\.ssh\known_hosts"

:: --- NODE DEPLOYMENT PHASE ---
echo [INFO] Initiating deployment for target: %TARGET%...

:: --- KAFKA CLUSTER ---
if /I "%TARGET%"=="influxdb" goto :skip_kafka
if /I "%TARGET%"=="flink-master" goto :skip_kafka
if /I "%TARGET%"=="flink-worker" goto :skip_kafka
if /I "%TARGET%"=="metrics" goto :skip_kafka

echo [INFO] Preparing Kafka Quorum Voters map...
set "QUORUM_VOTERS="
for /L %%i in (1,1,%KAFKA_COUNT%) do (
    if "%%i"=="1" (
        set "QUORUM_VOTERS=1@kafka-1.%PRIVATE_DOMAIN_NAME%:9093"
    ) else (
        set "QUORUM_VOTERS=!QUORUM_VOTERS!,%%i@kafka-%%i.%PRIVATE_DOMAIN_NAME%:9093"
    )
)

echo [INFO] Deploying %KAFKA_COUNT% Kafka Broker Nodes...
for /L %%N in (1,1,%KAFKA_COUNT%) do (
    set "NODE_HOST=kafka-%%N.%PRIVATE_DOMAIN_NAME%"
    set "STACK_NAME=Flight-Stream-kafka-node-%%N"

    if %%N==1 (
        set "INSTANCE_TYPE=t3.large"
    ) else (
        set "INSTANCE_TYPE=t3.medium"
    )

    echo [INFO] Cleaning up stale SSH host keys for !NODE_HOST!...
    powershell -Command "if (Test-Path '%KH_PATH%') { $c = Get-Content '%KH_PATH%'; $c | Where-Object { $_ -notmatch 'kafka-%%N' } | Set-Content '%KH_PATH%' }"

    echo [INFO] Preparing node-specific environment for Kafka Broker %%N...
    set "TEMP_ENV=%TEMP%\kafka-%%N.env"
    
    :: Copy .env contents
    copy /Y .env "!TEMP_ENV!" >nul
    echo.>>"!TEMP_ENV!"
    echo KAFKA_NODE_ID=%%N>>"!TEMP_ENV!"
    echo NODE_HOSTNAME=!NODE_HOST!>>"!TEMP_ENV!"
    echo KAFKA_CONTROLLER_QUORUM_VOTERS=!QUORUM_VOTERS!>>"!TEMP_ENV!"
    
    if exist envs\kafka.env (
        type envs\kafka.env >> "!TEMP_ENV!"
    )

    :: Upload to S3
    aws s3 cp "!TEMP_ENV!" "s3://%BUCKET_NAME%/deploy/envs/kafka-%%N.env" >nul
    del /f "!TEMP_ENV!"

    echo [INFO] Launching Kafka Broker Node %%N...
    aws cloudformation deploy ^
      --stack-name "!STACK_NAME!" ^
      --template-file "template/cluster-node.yaml" ^
      --parameter-overrides ^
          InstanceType=!INSTANCE_TYPE! ^
          SubnetId=%SUBNET_ID% ^
          SecurityGroupId=%SG_ID% ^
          HostedZoneId=%ZONE_ID% ^
          KeyName=%SSH_KEY_NAME% ^
          NodeHostname="!NODE_HOST!" ^
          InstanceName="Kafka-Node-%%N" ^
          S3Bucket=%BUCKET_NAME% ^
          EnvironmentFile="kafka-%%N.env" ^
          DeployScripts="deploy-kafka.sh" ^
      --capabilities CAPABILITY_IAM ^
      --no-fail-on-empty-changeset
)

echo [INFO] Creating DNS alias for Kafka Bootstrap Server (kafka.%PRIVATE_DOMAIN_NAME%)...
set "DNS_BATCH_FILE=%TEMP%\dns-kafka-aliases.json"
(
echo {
echo   "Comment": "Creating CNAME for Kafka Bootstrap",
echo   "Changes": [
echo     { "Action": "UPSERT", "ResourceRecordSet": { "Name": "kafka.%PRIVATE_DOMAIN_NAME%", "Type": "CNAME", "TTL": 300, "ResourceRecords": [{ "Value": "kafka-1.%PRIVATE_DOMAIN_NAME%" }] } }
echo   ]
echo }
) > "%DNS_BATCH_FILE%"
aws route53 change-resource-record-sets --hosted-zone-id "%ZONE_ID%" --change-batch file://"%DNS_BATCH_FILE%" >nul
del /f "%DNS_BATCH_FILE%"

:skip_kafka

:: --- INFLUXDB & TELEGRAF NODE ---
if /I "%TARGET%"=="kafka" goto :skip_influxdb
if /I "%TARGET%"=="flink-master" goto :skip_influxdb
if /I "%TARGET%"=="flink-worker" goto :skip_influxdb
if /I "%TARGET%"=="metrics" goto :skip_influxdb

set "INFLUXDB_HOST=influxdb.%PRIVATE_DOMAIN_NAME%"
echo [INFO] Cleaning up stale SSH host keys for %INFLUXDB_HOST%...
powershell -Command "if (Test-Path '%KH_PATH%') { $c = Get-Content '%KH_PATH%'; $c | Where-Object { $_ -notmatch 'influxdb' } | Set-Content '%KH_PATH%' }"

echo [INFO] Preparing InfluxDB environment...
set "TEMP_ENV=%TEMP%\influxdb.env"
copy /Y .env "!TEMP_ENV!" >nul
echo.>>"!TEMP_ENV!"
echo NODE_HOSTNAME=%INFLUXDB_HOST%>>"!TEMP_ENV!"
if exist envs\influxdb.env (
    type envs\influxdb.env >> "!TEMP_ENV!"
)
aws s3 cp "!TEMP_ENV!" "s3://%BUCKET_NAME%/deploy/envs/influxdb.env" >nul
del /f "!TEMP_ENV!"

echo [INFO] Deploying InfluxDB Node via CloudFormation...
aws cloudformation deploy ^
  --stack-name "Flight-Stream-influxdb-node" ^
  --template-file "template/cluster-node.yaml" ^
  --parameter-overrides ^
      SubnetId=%SUBNET_ID% ^
      SecurityGroupId=%SG_ID% ^
      HostedZoneId=%ZONE_ID% ^
      KeyName=%SSH_KEY_NAME% ^
      NodeHostname="%INFLUXDB_HOST%" ^
      InstanceName="InfluxDB-Node" ^
      S3Bucket=%BUCKET_NAME% ^
      EnvironmentFile="influxdb.env" ^
      DeployScripts="deploy-influxdb.sh" ^
  --capabilities CAPABILITY_IAM ^
  --no-fail-on-empty-changeset

:skip_influxdb

:: --- FLINK MASTER (JOBMANAGER) ---
if /I "%TARGET%"=="kafka" goto :skip_flink_master
if /I "%TARGET%"=="influxdb" goto :skip_flink_master
if /I "%TARGET%"=="flink-worker" goto :skip_flink_master
if /I "%TARGET%"=="metrics" goto :skip_flink_master

set "MASTER_HOST=jobmanager.%PRIVATE_DOMAIN_NAME%"
echo [INFO] Cleaning up stale SSH host keys for %MASTER_HOST%...
powershell -Command "if (Test-Path '%KH_PATH%') { $c = Get-Content '%KH_PATH%'; $c | Where-Object { $_ -notmatch 'jobmanager' } | Set-Content '%KH_PATH%' }"

echo [INFO] Preparing Flink Master environment...
set "TEMP_ENV=%TEMP%\flink-master.env"
copy /Y .env "!TEMP_ENV!" >nul
echo.>>"!TEMP_ENV!"
echo NODE_HOSTNAME=%MASTER_HOST%>>"!TEMP_ENV!"
if exist envs\flink-master.env (
    type envs\flink-master.env >> "!TEMP_ENV!"
)
aws s3 cp "!TEMP_ENV!" "s3://%BUCKET_NAME%/deploy/envs/flink-master.env" >nul
del /f "!TEMP_ENV!"

echo [INFO] Deploying Flink JobManager Node via CloudFormation...
aws cloudformation deploy ^
  --stack-name "Flight-Stream-flink-master-node" ^
  --template-file "template/cluster-node.yaml" ^
  --parameter-overrides ^
      InstanceType=c6i.large ^
      SubnetId=%SUBNET_ID% ^
      SecurityGroupId=%SG_ID% ^
      HostedZoneId=%ZONE_ID% ^
      KeyName=%SSH_KEY_NAME% ^
      NodeHostname="%MASTER_HOST%" ^
      InstanceName="Flink-JobManager-Node" ^
      S3Bucket=%BUCKET_NAME% ^
      EnvironmentFile="flink-master.env" ^
      DeployScripts="deploy-flink-master.sh" ^
  --capabilities CAPABILITY_IAM ^
  --no-fail-on-empty-changeset

echo [INFO] Creating DNS alias for Flink Dashboard (flink.%PRIVATE_DOMAIN_NAME%)...
set "DNS_BATCH_FILE=%TEMP%\dns-flink-aliases.json"
(
echo {
echo   "Comment": "Creating alias for Flink JobManager Dashboard",
echo   "Changes": [
echo     { "Action": "UPSERT", "ResourceRecordSet": { "Name": "flink.%PRIVATE_DOMAIN_NAME%", "Type": "CNAME", "TTL": 300, "ResourceRecords": [{ "Value": "%MASTER_HOST%" }] } }
echo   ]
echo }
) > "%DNS_BATCH_FILE%"
aws route53 change-resource-record-sets --hosted-zone-id "%ZONE_ID%" --change-batch file://"%DNS_BATCH_FILE%" >nul
del /f "%DNS_BATCH_FILE%"

:skip_flink_master

:: --- FLINK WORKERS (TASKMANAGERS) ---
if /I "%TARGET%"=="kafka" goto :skip_flink_worker
if /I "%TARGET%"=="influxdb" goto :skip_flink_worker
if /I "%TARGET%"=="flink-master" goto :skip_flink_worker
if /I "%TARGET%"=="metrics" goto :skip_flink_worker

echo [INFO] Deploying %FLINK_WORKER_COUNT% Flink TaskManager Nodes...
for /L %%N in (1,1,%FLINK_WORKER_COUNT%) do (
    set "NODE_HOST=flink-worker-%%N.%PRIVATE_DOMAIN_NAME%"
    set "STACK_NAME=Flight-Stream-flink-worker-%%N"

    echo [INFO] Cleaning up stale SSH host keys for !NODE_HOST!...
    powershell -Command "if (Test-Path '%KH_PATH%') { $c = Get-Content '%KH_PATH%'; $c | Where-Object { $_ -notmatch 'flink-worker-%%N' } | Set-Content '%KH_PATH%' }"

    echo [INFO] Preparing node-specific environment for Flink TaskManager %%N...
    set "TEMP_ENV=%TEMP%\flink-worker-%%N.env"
    
    copy /Y .env "!TEMP_ENV!" >nul
    echo.>>"!TEMP_ENV!"
    echo NODE_HOSTNAME=!NODE_HOST!>>"!TEMP_ENV!"
    
    if exist envs\flink-worker.env (
        type envs\flink-worker.env >> "!TEMP_ENV!"
    )

    aws s3 cp "!TEMP_ENV!" "s3://%BUCKET_NAME%/deploy/envs/flink-worker-%%N.env" >nul
    del /f "!TEMP_ENV!"

    echo [INFO] Launching Flink TaskManager Node %%N...
    aws cloudformation deploy ^
      --stack-name "!STACK_NAME!" ^
      --template-file "template/cluster-node.yaml" ^
      --parameter-overrides ^
          InstanceType=c6i.large ^
          SubnetId=%SUBNET_ID% ^
          SecurityGroupId=%SG_ID% ^
          HostedZoneId=%ZONE_ID% ^
          KeyName=%SSH_KEY_NAME% ^
          NodeHostname="!NODE_HOST!" ^
          InstanceName="Flink-Worker-%%N" ^
          S3Bucket=%BUCKET_NAME% ^
          EnvironmentFile="flink-worker-%%N.env" ^
          DeployScripts="deploy-flink-worker.sh" ^
      --capabilities CAPABILITY_IAM ^
      --no-fail-on-empty-changeset
)

:skip_flink_worker

:: --- METRICS NODE (GRAFANA) ---
if /I "%TARGET%"=="kafka" goto :skip_metrics
if /I "%TARGET%"=="influxdb" goto :skip_metrics
if /I "%TARGET%"=="flink-master" goto :skip_metrics
if /I "%TARGET%"=="flink-worker" goto :skip_metrics

set "METRICS_HOST=metrics.%PRIVATE_DOMAIN_NAME%"
echo [INFO] Cleaning up stale SSH host keys for %METRICS_HOST%...
powershell -Command "if (Test-Path '%KH_PATH%') { $c = Get-Content '%KH_PATH%'; $c | Where-Object { $_ -notmatch 'metrics' } | Set-Content '%KH_PATH%' }"

echo [INFO] Preparing Metrics environment...
set "TEMP_ENV=%TEMP%\metrics.env"
copy /Y .env "!TEMP_ENV!" >nul
echo.>>"!TEMP_ENV!"
echo NODE_HOSTNAME=%METRICS_HOST%>>"!TEMP_ENV!"
if exist envs\metrics.env (
    type envs\metrics.env >> "!TEMP_ENV!"
)
aws s3 cp "!TEMP_ENV!" "s3://%BUCKET_NAME%/deploy/envs/metrics.env" >nul
del /f "!TEMP_ENV!"

echo [INFO] Deploying Grafana Metrics Node via CloudFormation...
aws cloudformation deploy ^
  --stack-name "Flight-Stream-metrics-node" ^
  --template-file "template/cluster-node.yaml" ^
  --parameter-overrides ^
      SubnetId=%SUBNET_ID% ^
      SecurityGroupId=%SG_ID% ^
      HostedZoneId=%ZONE_ID% ^
      KeyName=%SSH_KEY_NAME% ^
      NodeHostname="%METRICS_HOST%" ^
      InstanceName="Metrics-Node" ^
      S3Bucket=%BUCKET_NAME% ^
      EnvironmentFile="metrics.env" ^
      DeployScripts="deploy-metrics.sh" ^
  --capabilities CAPABILITY_IAM ^
  --no-fail-on-empty-changeset

echo [INFO] Creating DNS alias for Grafana (grafana.%PRIVATE_DOMAIN_NAME%)...
set "DNS_BATCH_FILE=%TEMP%\dns-grafana-aliases.json"
(
echo {
echo   "Comment": "Creating alias for Grafana UI",
echo   "Changes": [
echo     { "Action": "UPSERT", "ResourceRecordSet": { "Name": "grafana.%PRIVATE_DOMAIN_NAME%", "Type": "CNAME", "TTL": 300, "ResourceRecords": [{ "Value": "%METRICS_HOST%" }] } }
echo   ]
echo }
) > "%DNS_BATCH_FILE%"
aws route53 change-resource-record-sets --hosted-zone-id "%ZONE_ID%" --change-batch file://"%DNS_BATCH_FILE%" >nul
del /f "%DNS_BATCH_FILE%"

:skip_metrics

echo ----------------------------------------------------
echo [SUCCESS] Deployment completed for target: %TARGET%
echo ----------------------------------------------------
exit /b 0
