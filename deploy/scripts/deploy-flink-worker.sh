#!/bin/bash

# Deployment script for Flink TaskManager (Flink Worker) Node.
# Each worker instance runs tasks assigned by the JobManager.

echo "[DEPLOY] Starting Flink TaskManager Deployment..."

BUCKET_NAME="${1:-flink-flight-analysis}"

cd /home/ec2-user

echo "[DEPLOY] Syncing docker compose file from S3..."
aws s3 cp "s3://${BUCKET_NAME}/deploy/compose/flink-worker-compose.yml" "docker-compose.yml"

if [ -f "/home/ec2-user/.env" ]; then
    mv /home/ec2-user/.env .env
fi

echo "[DEPLOY] Launching Flink TaskManager containers..."
docker-compose up -d

echo "[DEPLOY] Adjusting file permissions..."
sudo chown -R ec2-user:ec2-user /home/ec2-user

echo "[DEPLOY] Flink TaskManager deployed successfully."
