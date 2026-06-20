#!/bin/bash

# Helper script for submitting Flink stream analytical jobs on the EC2 cluster.
# This script automates the 'docker exec' command targeting the jobmanager container.

# Usage: ./flink-submit.sh [main_class] [parallelism] [other_arguments]
# Default Main Class: it.uniroma2.sae.FlightAnalysisJob

MAIN_CLASS="${1:-it.uniroma2.sae.FlightAnalysisJob}"
PARALLELISM="${2:-1}"

# Shift parameters so we can pass the rest as arguments to Flink
shift 2

echo "[FLINK] Submitting job to Flink JobManager..."
echo "[FLINK] Main Class:  $MAIN_CLASS"
echo "[FLINK] Parallelism: $PARALLELISM"

# Run Flink submission command inside the JobManager container
docker exec jobmanager flink run \
  -d \
  -c "$MAIN_CLASS" \
  -p "$PARALLELISM" \
  /opt/flink/flink-jobs/flight-analysis.jar \
  "$@"

echo "[FLINK] Submission command successfully transmitted to the JobManager."
