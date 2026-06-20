#!/bin/bash

# --- S3 BUCKET FOLDER INITIALIZER ---
# This utility ensures that the logical directory structure required
# by the project exists within the target S3 bucket.

# --- FOLDER PROVISIONING LOOP ---
# Process each folder name passed as a command-line argument.
for FOLDER_NAME in "$@"; do

    echo "[INFO] Validating existence of S3 prefix: ${FOLDER_NAME}/"
    
    # Use 'aws s3 ls' to verify if the prefix (logical folder) exists in the bucket.
    aws s3 ls "s3://${BUCKET_NAME}/${FOLDER_NAME}/" > /dev/null 2>&1

    if [ $? -ne 0 ]; then
        echo "[INFO] Prefix '${FOLDER_NAME}/' missing. Creating placeholder object..."
        
        # In S3, 'folders' are simulated by creating an empty object with a trailing slash in its key.
        aws s3api put-object --bucket "${BUCKET_NAME}" --key "${FOLDER_NAME}/" > /dev/null 2>&1
        
        if [ $? -eq 0 ]; then
            echo "[INFO] Successfully initialized S3 prefix: ${FOLDER_NAME}/"
        else
            echo "[ERROR] Failed to initialize S3 prefix: ${FOLDER_NAME}/. Verify bucket existence and IAM permissions."
        fi
    else
        echo "[INFO] S3 prefix '${FOLDER_NAME}/' is already present."
    fi

done

exit 0
