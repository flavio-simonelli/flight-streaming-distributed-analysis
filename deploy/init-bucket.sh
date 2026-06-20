#!/bin/bash

# --- ENVIRONMENT INITIALIZATION ---
# Determine the location of the deployment scripts to ensure all relative 
# path resolutions are consistent regardless of the working directory.
SCRIPTS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
PROJECT_ROOT="${SCRIPTS_DIR}/.."

# Load environment variables from the .env file located in the deploy folder.
# This provides the script with required S3 bucket names and AWS region settings.
source "${SCRIPTS_DIR}/load_env.sh"
if [ $? -ne 0 ]; then
    echo "[ERROR] Failed to initialize environment variables. Ensure .env exists."
    exit 1
fi

# Validate that essential AWS variables are properly defined before proceeding.
if [ -z "$BUCKET_NAME" ]; then
    echo "[ERROR] BUCKET_NAME is not defined in the environment."
    exit 1
fi

echo "----------------------------------------------------"
echo "[INFO] AWS S3 BUCKET PROVISIONING"
echo "----------------------------------------------------"
echo "BUCKET: ${BUCKET_NAME}"
echo "REGION: ${REGION}"
echo "----------------------------------------------------"

# --- BUCKET VALIDATION AND CREATION ---
# Check if the target S3 bucket exists; create it if it is missing in the specified region.
aws s3api head-bucket --bucket "${BUCKET_NAME}" >/dev/null 2>&1
if [ $? -eq 0 ]; then
    echo "[INFO] Bucket already exists. Skipping creation."
else
    echo "[INFO] Bucket does not exist. Creating it now in ${REGION}..."
    aws s3 mb "s3://${BUCKET_NAME}" --region "${REGION}"
    if [ $? -ne 0 ]; then
        echo "[ERROR] Failed to create bucket. Verify IAM permissions or naming rules."
        exit 1
    fi
    echo "[INFO] Bucket created successfully."
fi

# --- FOLDER STRUCTURE PROVISIONING ---
# Create the logical folder structure within the bucket.
"${SCRIPTS_DIR}/create_bucket_folders.sh" "logs" "deploy" "flink" "grafana" "telegraf" "influxdb"

# --- DEPLOYMENT ASSETS SYNCHRONIZATION ---
# Sync the local deploy directory containing templates, compose files, and bash scripts.
echo "[INFO] Syncing deployment scripts and configurations..."
aws s3 sync "${SCRIPTS_DIR}/" "s3://${BUCKET_NAME}/deploy/" \
    --exclude "*.bat" --include "*/*.bat" \
    --exclude "*.sh" --include "*/*.sh" \
    --exclude ".env*" --include "envs/*.env" \
    --exclude "template/*"

# Sync Telegraf configuration
if [ -d "${PROJECT_ROOT}/telegraf/" ]; then
    echo "[INFO] Syncing Telegraf configuration..."
    aws s3 sync "${PROJECT_ROOT}/telegraf/" "s3://${BUCKET_NAME}/telegraf/"
fi

# Sync InfluxDB configurations & initialization scripts
if [ -d "${PROJECT_ROOT}/influxdb/" ]; then
    echo "[INFO] Syncing InfluxDB initialization assets..."
    aws s3 sync "${PROJECT_ROOT}/influxdb/" "s3://${BUCKET_NAME}/influxdb/"
fi

# Sync Grafana dashboard and datasource provisioning configurations from data/grafana
if [ -d "${PROJECT_ROOT}/data/grafana/" ]; then
    echo "[INFO] Syncing Grafana dashboards and datasources..."
    aws s3 sync "${PROJECT_ROOT}/data/grafana/" "s3://${BUCKET_NAME}/grafana/"
fi

# Upload the Flink application JAR and configuration files.
if [ -f "${PROJECT_ROOT}/flink/target/flight-analysis-1.0.jar" ]; then
    echo "[INFO] Uploading Flink application JAR..."
    aws s3 cp "${PROJECT_ROOT}/flink/target/flight-analysis-1.0.jar" "s3://${BUCKET_NAME}/flink/flight-analysis.jar"
elif [ -f "${PROJECT_ROOT}/flink/target/flight-analysis-1.0-shaded.jar" ]; then
    echo "[INFO] Uploading Flink shaded application JAR..."
    aws s3 cp "${PROJECT_ROOT}/flink/target/flight-analysis-1.0-shaded.jar" "s3://${BUCKET_NAME}/flink/flight-analysis.jar"
fi

echo ""
echo "----------------------------------------------------"
echo "[INFO] S3 Synchronization completed successfully!"
echo "Assets location: s3://${BUCKET_NAME}/"
echo "----------------------------------------------------"

exit 0
