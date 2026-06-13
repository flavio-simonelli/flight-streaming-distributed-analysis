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
docker run -it \
  --name $CONTAINER_NAME \
  -e INPUT_PARQUET_PATH="/app/data/flights.parquets" \
  -e OUTPUT_TYPE="terminal" \
  -e MAX_RECORDS=50 \
  -v "$(pwd)/data:/app/data" \
  $IMAGE_NAME
