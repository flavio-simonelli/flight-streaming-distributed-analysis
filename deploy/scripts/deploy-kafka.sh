#!/bin/bash

# Deployment script for a Kafka Broker Node.
# Each instance runs a Kafka broker configured via Docker Compose in KRaft mode.

echo "[DEPLOY] Starting Kafka Node Deployment..."

BUCKET_NAME="${1:-flink-flight-analysis}"

cd /home/ec2-user

echo "[DEPLOY] Syncing docker compose file from S3..."
aws s3 cp "s3://${BUCKET_NAME}/deploy/compose/kafka-compose.yml" "docker-compose.yml"

if [ -f "/home/ec2-user/.env" ]; then
    mv /home/ec2-user/.env .env
fi

echo "[DEPLOY] Launching Kafka containers..."
docker-compose up -d

echo "[DEPLOY] Adjusting file permissions..."
sudo chown -R ec2-user:ec2-user /home/ec2-user

echo "[DEPLOY] Kafka Node deployed successfully."
