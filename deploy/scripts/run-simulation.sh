#!/usr/bin/env bash

# ==========================================
# DOCKER CONFIGURATION
# ==========================================
CONTAINER_NAME="flight-simulator"
IMAGE_TAG="fmasci/sae-simulator:0.1.0"
NETWORK_NAME="sae-net"

# Volume mapping for flight data
# Takes the 'data' folder from the current script location
LOCAL_DATA_DIR="$(pwd)/data"
CONTAINER_DATA_DIR="/app/data"

# ==========================================
# ENVIRONMENT VARIABLES (FLIGHT SIMULATOR)
# ==========================================
INPUT_PARQUET_PATH="/app/data/flights.parquet"
OUTPUT_TYPE="kafka"
SPEEDUP_FACTOR=43200

# Kafka Settings
KAFKA_BROKERS="kafka.flight-analysis.local:9094"
KAFKA_TOPIC="flights-stream"

# Optimizations and Concurrency
SPIN_THRESHOLD_MS=60000
PARQUET_READER_CONCURRENCY=2

# Out-of-Order Injection
OUT_OF_ORDER_FACTOR=0.00
OUT_OF_ORDER_MAX_DELAY_MINUTES=30

# ==========================================
# STARTUP LOGIC
# ==========================================

# Check if the Docker network exists, create it if it doesn't
if ! docker network inspect "${NETWORK_NAME}" >/dev/null 2>&1; then
    echo "Network '${NETWORK_NAME}' not found. Creating..."
    docker network create "${NETWORK_NAME}"
fi

# Remove any existing container with the same name (running or stopped)
if docker ps -a --format '{{.Names}}' | grep -Eq "^${CONTAINER_NAME}$"; then
    echo "An old container named '${CONTAINER_NAME}' already exists. Removing..."
    docker rm -f "${CONTAINER_NAME}"
fi

echo "Starting container ${CONTAINER_NAME}..."

# Run the container with the mapped volume
docker run -d \
  --name "${CONTAINER_NAME}" \
  --network "${NETWORK_NAME}" \
  -v "${LOCAL_DATA_DIR}:${CONTAINER_DATA_DIR}" \
  -e INPUT_PARQUET_PATH="${INPUT_PARQUET_PATH}" \
  -e OUTPUT_TYPE="${OUTPUT_TYPE}" \
  -e SPEEDUP_FACTOR="${SPEEDUP_FACTOR}" \
  -e KAFKA_BROKERS="${KAFKA_BROKERS}" \
  -e KAFKA_TOPIC="${KAFKA_TOPIC}" \
  -e SPIN_THRESHOLD_MS="${SPIN_THRESHOLD_MS}" \
  -e PARQUET_READER_CONCURRENCY="${PARQUET_READER_CONCURRENCY}" \
  -e OUT_OF_ORDER_FACTOR="${OUT_OF_ORDER_FACTOR}" \
  -e OUT_OF_ORDER_MAX_DELAY_MINUTES="${OUT_OF_ORDER_MAX_DELAY_MINUTES}" \
  "${IMAGE_TAG}"

echo "Container started successfully. Monitor logs with:"
echo "docker logs -f ${CONTAINER_NAME}"