#!/bin/bash

# setup-infra.sh - Automates local infrastructure startup and InfluxDB 3 token generation

# Start core infrastructure
echo "Starting Kafka and InfluxDB..."
docker compose up -d influxdb kafka

# Wait for InfluxDB to be ready
echo "Waiting for InfluxDB health check..."
until curl -s http://localhost:8181/health > /dev/null; do
  echo "Still waiting for InfluxDB..."
  sleep 2
done

# Generate a fresh admin token
echo "Generating new InfluxDB 3 admin token..."
NEW_TOKEN=$(docker exec influxdb influxdb3 create token --admin | grep "Token:" | awk '{print $2}')

if [ -z "$NEW_TOKEN" ]; then
    echo "Error: Failed to generate InfluxDB token."
    exit 1
fi

echo "Token generated: ${NEW_TOKEN:0:10}..."

# Update the .env file automatically
if [ -f .env ]; then
    # Use sed to replace the INFLUXDB_TOKEN line
    # For macOS/BSD sed compatibility, use a temporary file if needed, but standard Linux sed works fine here.
    sed -i "s/^INFLUXDB_TOKEN=.*/INFLUXDB_TOKEN=$NEW_TOKEN/" .env
    echo ".env file updated with new token."
else
    echo "INFLUXDB_TOKEN=$NEW_TOKEN" > .env
    echo ".env file created with new token."
fi

# Run initialization containers
echo "Running initialization containers..."
docker compose up -d

echo "Infrastructure is ready!"
