#!/bin/bash

# --- ENVIRONMENT LOADER ---
# This script parses the project's .env file and exports its key-value pairs 
# as local environment variables for the current session.
# The script first checks the current directory for a .env file, then 
# attempts to find it in the parent directory if the current one is missing.

ENV_PATH=".env"
if [ ! -f "$ENV_PATH" ]; then
    ENV_PATH="../.env"
fi

if [ ! -f "$ENV_PATH" ]; then
    echo "[ERROR] Source environment file was not found."
    # Use return instead of exit so it doesn't close the terminal when sourced
    return 1 2>/dev/null || exit 1
fi

# Iterate through the environment file, skipping comments and empty lines.
# Each valid entry is exported to the local environment scope.
# Using grep -v "^#" ensures lines starting with '#' are ignored.

while IFS= read -r line || [ -n "$line" ]; do
    # Skip empty lines and comments
    if [[ -n "$line" && ! "$line" =~ ^# ]]; then
        export "$line"
    fi
done < "$ENV_PATH"

echo "[INFO] Environment variables successfully imported from ${ENV_PATH}."
return 0 2>/dev/null || exit 0
