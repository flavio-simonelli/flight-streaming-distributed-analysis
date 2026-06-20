#!/bin/bash

# Deployment script for Flink JobManager (Flink Master) Node.
# Hosts the orchestrator of Flink execution.

echo "[DEPLOY] Starting Flink JobManager Deployment..."

BUCKET_NAME="${1:-flink-flight-analysis}"

cd /home/ec2-user

mkdir -p flink

echo "[DEPLOY] Syncing docker compose file and Flink JAR from S3..."
aws s3 cp "s3://${BUCKET_NAME}/deploy/compose/flink-master-compose.yml" "docker-compose.yml"
aws s3 cp "s3://${BUCKET_NAME}/flink/flight-analysis.jar" "flink/flight-analysis.jar"
aws s3 cp "s3://${BUCKET_NAME}/deploy/scripts/flink-submit.sh" "flink-submit.sh"
chmod +x flink-submit.sh

if [ -f "/home/ec2-user/.env" ]; then
    mv /home/ec2-user/.env .env
fi

echo "[DEPLOY] Launching Flink JobManager containers..."
docker-compose up -d

echo "[DEPLOY] Adjusting file permissions..."
sudo chown -R ec2-user:ec2-user /home/ec2-user

echo "[DEPLOY] Flink JobManager deployed successfully."
