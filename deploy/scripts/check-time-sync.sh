#!/bin/bash

# --- AMAZON TIME SYNC SERVICE CHECKER ---
# This script verifies if the EC2 instance is successfully synchronized
# with the Amazon Time Sync Service (169.254.169.123) using chrony.

echo "=========================================================="
echo "[START] CHECKING AMAZON TIME SYNC SERVICE STATUS"
echo "=========================================================="

# 1. Check if chronyc command is installed
if ! command -v chronyc &> /dev/null; then
    echo "[WARNING] 'chronyc' CLI not found. Attempting to check systemd-timesyncd..."
    if command -v timedatectl &> /dev/null; then
        timedatectl status
    else
        echo "[ERROR] No NTP client CLI (chronyc or timedatectl) detected on this instance."
    fi
    exit 1
fi

# 2. Check if chronyd daemon is active
if ! systemctl is-active --quiet chronyd; then
    echo "[WARNING] 'chronyd' service is not running. Attempting to start it..."
    sudo systemctl start chronyd
    sleep 2
    if ! systemctl is-active --quiet chronyd; then
        echo "[ERROR] Failed to start 'chronyd' service. Clock synchronization is inactive."
        exit 2
    fi
fi

# 3. Check if the Amazon NTP link-local IP (169.254.169.123) is registered as a source
SOURCES_OUTPUT=$(chronyc sources)
if echo "$SOURCES_OUTPUT" | grep -q "169.254.169.123"; then
    echo "[SUCCESS] Amazon Time Sync Service (169.254.169.123) is registered as an NTP source."
else
    echo "[ERROR] Amazon Time Sync Service (169.254.169.123) is NOT found in chrony sources!"
    echo "Current chrony sources list:"
    echo "$SOURCES_OUTPUT"
    exit 3
fi

# 4. Check if the system clock is actively synchronized with the Amazon NTP source
# Chrony marks the selected synchronized source with a '*' character prefix.
ACTIVE_SOURCE=$(echo "$SOURCES_OUTPUT" | grep -E "^\*\s+169.254.169.123|^\^\*\s+169.254.169.123")
if [ -n "$ACTIVE_SOURCE" ] || echo "$SOURCES_OUTPUT" | grep -q "^\^\*"; then
    echo "[SUCCESS] System clock is actively synchronized with an NTP reference."
    echo "----------------------------------------------------------"
    chronyc tracking | grep -E "Reference ID|System time|Last offset|RMS offset"
    echo "----------------------------------------------------------"
    exit 0
else
    echo "[WARNING] NTP source is registered, but the system clock is not yet synchronized."
    echo "Attempting to force manual synchronization step..."
    sudo chronyc -a makestep
    sleep 1
    chronyc tracking
    exit 4
fi
