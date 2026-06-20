#!/bin/bash

# --- NETWORK INFRASTRUCTURE DEPLOYMENT ORCHESTRATOR ---
# This script manages the provisioning of the core VPC network components, 
# including subnets, security groups, and private DNS zones.

# --- ENVIRONMENT INITIALIZATION ---
source load_env.sh
if [ $? -ne 0 ]; then
    echo "[ERROR] Environment initialization failed. Ensure load_env.sh and .env are present."
    exit 1
fi

echo "----------------------------------------------------"
echo "[INFO] NETWORK INFRASTRUCTURE PROVISIONING"
echo "VPC:    ${VPC_NAME}"
echo "Subnet: ${SUBNET_NAME}"
echo "----------------------------------------------------"

# Generate the stack name derived from the VPC configuration.
VPC_STACK_NAME="${VPC_NAME}-stack"

# --- INFRASTRUCTURE VALIDATION ---
# Check for the existence of the subnet before attempting deployment to avoid redundant stack updates.
echo "[INFO] Validating existing network infrastructure..."

aws ec2 describe-subnets --filters "Name=tag:Name,Values=${SUBNET_NAME}" --query "Subnets[0].SubnetId" --output text 2>/dev/null | grep -v "None" >/dev/null

if [ $? -eq 0 ]; then
    echo "[INFO] Network infrastructure '${SUBNET_NAME}' already exists. Skipping CloudFormation deployment."
else
    echo "[INFO] Infrastructure not found. Initiating CloudFormation deployment..."

    # Deploy the core VPC Infrastructure stack using the cluster-vpc template.
    aws cloudformation deploy \
      --stack-name "${VPC_STACK_NAME}" \
      --template-file "template/cluster-vpc.yaml" \
      --parameter-overrides \
          VpcName="${VPC_NAME}" \
          SubnetName="${SUBNET_NAME}" \
          VpcCIDR="${VPC_CIDR}" \
          PublicSubnetCIDR="${SUBNET_CIDR}" \
          SgName="${SG_NAME}" \
          PrivateDomainName="${PRIVATE_DOMAIN_NAME}" \
      --no-fail-on-empty-changeset

    if [ $? -ne 0 ]; then
        echo "[ERROR] CloudFormation network deployment failed. Check the AWS Console for details."
        exit 1
    fi
fi

# --- FINAL VERIFICATION ---
# Verify and display the deployed network state for operational awareness.
echo "[INFO] Finalizing network status check..."

FOUND_SUBNET_ID=$(aws ec2 describe-subnets --filters "Name=tag:Name,Values=${SUBNET_NAME}" --query "Subnets[0].SubnetId" --output text 2>/dev/null)

if [ -z "$FOUND_SUBNET_ID" ] || [ "$FOUND_SUBNET_ID" == "None" ]; then
    echo "[ERROR] Subnet provisioning could not be verified."
    EXIT_CODE=1
else
    echo "[INFO] Network infrastructure is ready."
    echo "       Subnet ID: ${FOUND_SUBNET_ID}"
    
    # Retrieve the parent VPC ID for comprehensive reporting.
    PARENT_VPC=$(aws ec2 describe-subnets --subnet-ids "${FOUND_SUBNET_ID}" --query "Subnets[0].VpcId" --output text 2>/dev/null)
    echo "       Parent VPC: ${PARENT_VPC}"
    
    EXIT_CODE=0
fi

exit $EXIT_CODE
