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

echo "Starting Flight Simulator Container..."
docker run -it --network sae-net \
  --name $CONTAINER_NAME \
  -e INPUT_ARCHIVE_PATH="/app/data/project-1-data.tar.gz" \
  -e OUTPUT_TYPE="kafka" \
  -e KAFKA_BROKERS="kafka.flight-analysis.local:9092" \
  -e KAFKA_TOPIC="flights-stream" \
  -e MAX_RECORDS=50 \
  -v "$(pwd)/data:/app/data" \
  $IMAGE_NAME
