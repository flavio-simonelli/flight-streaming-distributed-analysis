#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

IMAGE_NAME="fmasci/flink-chaos-simulator"
TAG="0.1.0"

echo "=========================================="
echo " Starting Build & Push Process            "
echo "=========================================="

# Build the Docker image locally
echo "=> Building Docker image: ${IMAGE_NAME}:${TAG}..."
docker build -t "${IMAGE_NAME}:${TAG}" .

# Push the built image to Docker Hub repository
echo "=> Pushing image to Docker Hub..."
docker push "${IMAGE_NAME}:${TAG}"

echo "=========================================="
echo " Process Completed Successfully!          "
echo "=========================================="
