#!/bin/bash

LOG_FILE="/var/log/docker-install.log"
exec > >(tee -a "$LOG_FILE") 2>&1

echo "[INFO] Starting Docker and Docker Compose installation..."

echo "[DEBUG] Updating system packages"
sudo yum update -y

echo "[DEBUG] Enabling docker repository via amazon-linux-extras"
sudo amazon-linux-extras enable docker

echo "[DEBUG] Installing Docker"
sudo amazon-linux-extras install docker -y

echo "[DEBUG] Starting and enabling Docker service"
sudo systemctl start docker
sudo systemctl enable docker

echo "[DEBUG] Adding ec2-user to docker group"
sudo usermod -aG docker ec2-user

echo "[DEBUG] Downloading and installing Docker Compose"
DOCKER_COMPOSE_VERSION="v2.39.2"
sudo curl -L "https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

echo "[DEBUG] Verifying installations"
docker --version
docker-compose --version

echo "[INFO] Docker and Docker Compose installed successfully!"
