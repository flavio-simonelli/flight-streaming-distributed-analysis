#!/bin/bash
set -e

if [ -n "$INFLUXDB_METRICS_BUCKET" ]; then
  influx bucket create \
    -n "$INFLUXDB_METRICS_BUCKET" \
    -o "$DOCKER_INFLUXDB_INIT_ORG" \
    -r 0 \
    -t "$DOCKER_INFLUXDB_INIT_ADMIN_TOKEN"
fi