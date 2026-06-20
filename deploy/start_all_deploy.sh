#!/bin/bash

# --- FULL DEPLOYMENT PIPELINE ORCHESTRATOR ---
# This script executes the complete deployment sequence for the Flink & Kafka cluster:
# 1. Provisions network infrastructure (VPC, Subnets, DNS, Security Groups).
# 2. Initializes and synchronizes deployment assets to the Amazon S3 storage bucket.
# 3. Spins up the EC2 instances via CloudFormation, installing Docker and running services.

echo "=========================================================="
echo "[START] INITIATING FULL EC2 DEPLOYMENT PIPELINE"
echo "=========================================================="

# 1. Provisions VPC and Network Stack
chmod +x deploy-network.sh init-bucket.sh deploy-node.sh
./deploy-network.sh
if [ $? -ne 0 ]; then
    echo "[ERROR] Network infrastructure provisioning failed. Aborting."
    exit 1
fi

# 2. Sync files and compilation packages to S3
./init-bucket.sh
if [ $? -ne 0 ]; then
    echo "[ERROR] S3 bucket synchronization failed. Aborting."
    exit 1
fi

# 3. Provision and configure EC2 cluster instances
./deploy-node.sh all
if [ $? -ne 0 ]; then
    echo "[ERROR] Node cluster provisioning failed. Aborting."
    exit 1
fi

echo "=========================================================="
echo "[SUCCESS] FULL CLUSTER DEPLOYMENT COMPLETED!"
echo "=========================================================="
exit 0
