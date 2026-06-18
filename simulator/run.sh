#!/bin/bash

# Interrupt the script if any command fails
set -e

IMAGE_NAME="flight-simulator"
CONTAINER_NAME="simulator"

echo "Building Docker image..."
docker build -t $IMAGE_NAME .

# Remove any existing container with the same name
if [ "$(docker ps -aq -f name=$CONTAINER_NAME)" ]; then
    echo "Removing old container..."
    docker rm -f $CONTAINER_NAME
fi

# Load PARQUET_READER_CONCURRENCY from .env if present, otherwise default to 4
PARQUET_READER_CONCURRENCY=4
if [ -f .env ]; then
    ENV_VAL=$(grep -E "^PARQUET_READER_CONCURRENCY=" .env | cut -d'=' -f2)
    PARQUET_READER_CONCURRENCY=${ENV_VAL:-4}
fi

echo "Starting Flight Simulator Container..."
docker run -it --network sae-net \
  --name $CONTAINER_NAME \
  -e INPUT_PARQUET_PATH="/app/data/flights.parquet" \
  -e OUTPUT_TYPE="kafka" \
  -e KAFKA_BROKERS="kafka.flight-analysis.local:9092" \
  -e KAFKA_TOPIC="flights-stream" \
  -e MAX_RECORDS=50 \
  -e PARQUET_READER_CONCURRENCY="$PARQUET_READER_CONCURRENCY" \
  -v "$(pwd)/data:/app/data" \
  $IMAGE_NAME
