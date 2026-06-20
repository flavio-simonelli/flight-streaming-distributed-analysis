#!/bin/bash

# Deployment script for the Metrics (Grafana) Node.
# Hosts the Grafana dashboard server.

echo "[DEPLOY] Starting Grafana Metrics Node Deployment..."

BUCKET_NAME="${1:-flink-flight-analysis}"

cd /home/ec2-user

# Create configuration directories
mkdir -p grafana data/grafana

# Download configurations
aws s3 cp "s3://${BUCKET_NAME}/deploy/compose/metrics-compose.yml" "docker-compose.yml"
aws s3 cp "s3://${BUCKET_NAME}/grafana/" "grafana/" --recursive

if [ -f "/home/ec2-user/.env" ]; then
    mv /home/ec2-user/.env .env
fi

echo "[DEPLOY] Launching Grafana container..."
docker-compose up -d

echo "[DEPLOY] Adjusting file permissions..."
sudo chown -R ec2-user:ec2-user /home/ec2-user

echo "[DEPLOY] Grafana Metrics Node deployed successfully."
