#!/bin/bash

# Helper script for submitting Flink stream analytical jobs on the EC2 cluster.
# This script automates the 'docker exec' command targeting the jobmanager container.

# Usage: ./flink-submit.sh [parallelism] [other_arguments]

PARALLELISM="${1:-1}"

# Shift parameters so we can pass the rest as arguments to Flink
shift 1

echo "[FLINK] Submitting job to Flink JobManager..."
echo "[FLINK] Parallelism: $PARALLELISM"

# Run Flink submission command inside the JobManager container
docker exec jobmanager flink run \
  -d \
  -c "it.uniroma2.sae.FlightAnalysisJob" \
  -p "$PARALLELISM" \
  /opt/flink/flink-jobs/flight-analysis.jar \
  "$@"

echo "[FLINK] Submission command successfully transmitted to the JobManager."
