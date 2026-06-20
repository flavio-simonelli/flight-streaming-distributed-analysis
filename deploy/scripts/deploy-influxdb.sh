#!/bin/bash

# Deployment script for InfluxDB and Telegraf Node.
# Hosts the database and the kafka ingestion agent (telegraf).

echo "[DEPLOY] Starting InfluxDB and Telegraf Node Deployment..."

BUCKET_NAME="${1:-flink-flight-analysis}"

cd /home/ec2-user

# Create configuration directories
mkdir -p influxdb telegraf data/influxdb

# Download configurations
aws s3 cp "s3://${BUCKET_NAME}/deploy/compose/influxdb-compose.yml" "docker-compose.yml"
aws s3 cp "s3://${BUCKET_NAME}/influxdb/influx-init.sh" "influxdb/influx-init.sh"
aws s3 cp "s3://${BUCKET_NAME}/telegraf/telegraf.conf" "telegraf/telegraf.conf"

# Make initialization script executable
chmod +x influxdb/influx-init.sh

if [ -f "/home/ec2-user/.env" ]; then
    mv /home/ec2-user/.env .env
fi

echo "[DEPLOY] Launching InfluxDB and Telegraf containers..."
docker-compose up -d

echo "[DEPLOY] Adjusting file permissions..."
sudo chown -R ec2-user:ec2-user /home/ec2-user

echo "[DEPLOY] InfluxDB and Telegraf Node deployed successfully."
