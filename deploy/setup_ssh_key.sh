#!/bin/bash

# --- SSH KEY PROVISIONING TOOL ---
# This utility ensures that the required SSH key pair is available both in the 
# AWS account and on the local development machine with secure permissions.

# --- ENVIRONMENT INITIALIZATION ---
if [ -z "$BUCKET_NAME" ]; then
    source load_env.sh
    if [ $? -ne 0 ]; then
        echo "[ERROR] Environment initialization failed during SSH setup."
        exit 1
    fi
fi

echo "[INFO] Provisioning SSH Access: ${SSH_KEY_NAME}"

# Ensure the local directory for cryptographic keys exists.
if [ ! -d "$SSH_KEY_DIR" ]; then
    mkdir -p "$SSH_KEY_DIR"
fi

# --- KEY PAIR DISCOVERY AND GENERATION ---
# Query AWS EC2 to verify if the specified key pair already exists.
aws ec2 describe-key-pairs --key-names "${SSH_KEY_NAME}" >/dev/null 2>&1

if [ $? -ne 0 ]; then
    echo "[INFO] Key pair '${SSH_KEY_NAME}' not found in AWS. Generating new pair..."
    
    # Create the key pair in AWS and capture the private key material locally.
    aws ec2 create-key-pair --key-name "${SSH_KEY_NAME}" --query "KeyMaterial" --output text > "${SSH_KEY_DIR}/${SSH_KEY_NAME}.pem"
    
    # Apply restrictive Unix permissions - Required for OpenSSH compliance.
    # This grants exclusive read access to the current user (chmod 400).
    echo "[INFO] Applying restrictive file permissions to the private key..."
    chmod 400 "${SSH_KEY_DIR}/${SSH_KEY_NAME}.pem"
    
    echo "[INFO] Key pair successfully created and secured in ${SSH_KEY_DIR}."
else
    if [ ! -f "${SSH_KEY_DIR}/${SSH_KEY_NAME}.pem" ]; then
        echo "[WARNING] Key pair exists in AWS but the local .pem file is missing in ${SSH_KEY_DIR}."
        echo "          Ensure you have the correct private key to access the cluster nodes."
    else
        # Consistently re-apply permissions to maintain a secure state across executions.
        chmod 400 "${SSH_KEY_DIR}/${SSH_KEY_NAME}.pem"
        echo "[INFO] SSH Key pair is ready and secured both locally and in AWS."
    fi
fi

return 0 2>/dev/null || exit 0
