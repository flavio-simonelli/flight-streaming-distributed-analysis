#!/bin/bash

# --- DEPLOYMENT ORCHESTRATOR FOR FLINK/KAFKA CLUSTER ---
# This script manages the deployment of various node types across the EC2 cluster.
# It supports targeted deployment (kafka, influxdb, flink-master, flink-worker, metrics)
# or full cluster deployment (all).

# --- PARAMETER CHECK ---
TARGET="${1:-all}"
TARGET=$(echo "$TARGET" | tr '[:upper:]' '[:lower:]')

# Validate the provided target against supported node types.
if [[ ! "$TARGET" =~ ^(kafka|influxdb|flink-master|flink-worker|metrics|all)$ ]]; then
    echo "[ERROR] Invalid deployment target: ${TARGET}"
    echo "Usage: ./deploy-node.sh [kafka | influxdb | flink-master | flink-worker | metrics | all] [count]"
    exit 1
fi

# --- ENVIRONMENT INITIALIZATION ---
source load_env.sh
if [ $? -ne 0 ]; then
    echo "[ERROR] Failed to load environment variables. Ensure .env exists in the current directory."
    exit 1
fi

# --- SSH KEY PROVISIONING ---
source setup_ssh_key.sh
if [ $? -ne 0 ]; then
    echo "[ERROR] SSH key setup failed. Deployment aborted."
    exit 1
fi

echo "[INFO] Retrieving Cloud Infrastructure context..."

# --- INFRASTRUCTURE DISCOVERY ---
SUBNET_ID=$(aws ec2 describe-subnets --filters "Name=tag:Name,Values=${SUBNET_NAME}" --query "Subnets[0].SubnetId" --output text 2>/dev/null)
SG_ID=$(aws ec2 describe-security-groups --filters "Name=group-name,Values=${SG_NAME}" --query "SecurityGroups[0].GroupId" --output text 2>/dev/null)

RAW_ZONE_ID=$(aws route53 list-hosted-zones-by-name --dns-name "${PRIVATE_DOMAIN_NAME}" --query "HostedZones[0].Id" --output text 2>/dev/null)
ZONE_ID=${RAW_ZONE_ID#/hostedzone/}

echo "[INFO] Infrastructure Discovery Results:"
echo "       Subnet: ${SUBNET_ID}"
echo "       Security Group: ${SG_ID}"
echo "       DNS Zone: ${ZONE_ID}"

if [ -z "$SUBNET_ID" ] || [ "$SUBNET_ID" == "None" ]; then
    echo "[ERROR] Required network infrastructure is missing. Run deploy-network.sh first."
    exit 1
fi

# --- PARAMETER OVERRIDES ---
if [[ "$TARGET" == "kafka" && -n "$2" ]]; then
    KAFKA_COUNT=$2
    echo "[INFO] Overriding KAFKA_COUNT to: ${KAFKA_COUNT}"
fi

if [[ "$TARGET" == "flink-worker" && -n "$2" ]]; then
    FLINK_WORKER_COUNT=$2
    echo "[INFO] Overriding FLINK_WORKER_COUNT to: ${FLINK_WORKER_COUNT}"
fi

KH_PATH="${HOME}/.ssh/known_hosts"

# --- SUBROUTINES ---

deploy_kafka_node() {
    local N=$1
    local QUORUM_VOTERS=$2
    local NODE_HOST="kafka-${N}.${PRIVATE_DOMAIN_NAME}"
    local STACK_NAME="${VPC_NAME}-kafka-node-${N}"

    if [ "$N" -eq 1 ]; then
        INSTANCE_TYPE="t3.large"
    else
        INSTANCE_TYPE="t3.medium"
    fi

    echo "[INFO] Cleaning up stale SSH host keys for ${NODE_HOST}..."
    if [ -f "$KH_PATH" ]; then sed -i.bak "/${NODE_HOST}/d" "$KH_PATH"; fi

    echo "[INFO] Preparing node-specific environment for Kafka Broker ${N}..."
    local TEMP_ENV="/tmp/kafka-${N}.env"
    
    # Start with global settings
    cat .env > "$TEMP_ENV"
    echo "" >> "$TEMP_ENV"
    
    # Append node-specific overrides
    echo "KAFKA_NODE_ID=${N}" >> "$TEMP_ENV"
    echo "NODE_HOSTNAME=${NODE_HOST}" >> "$TEMP_ENV"
    echo "KAFKA_CONTROLLER_QUORUM_VOTERS=${QUORUM_VOTERS}" >> "$TEMP_ENV"
    
    # Append template env properties
    if [ -f "envs/kafka.env" ]; then
        cat envs/kafka.env >> "$TEMP_ENV"
    fi

    # Upload to S3
    aws s3 cp "$TEMP_ENV" "s3://${BUCKET_NAME}/deploy/envs/kafka-${N}.env"
    rm -f "$TEMP_ENV"

    echo "[INFO] Launching Kafka Broker Node ${N}..."
    aws cloudformation deploy \
      --stack-name "${STACK_NAME}" \
      --template-file "template/cluster-node.yaml" \
      --parameter-overrides \
          InstanceType="${INSTANCE_TYPE}" \
          SubnetId="${SUBNET_ID}" \
          SecurityGroupId="${SG_ID}" \
          HostedZoneId="${ZONE_ID}" \
          KeyName="${SSH_KEY_NAME}" \
          NodeHostname="${NODE_HOST}" \
          InstanceName="Kafka-Node-${N}" \
          S3Bucket="${BUCKET_NAME}" \
          EnvironmentFile="kafka-${N}.env" \
          DeployScripts="deploy-kafka.sh" \
      --capabilities CAPABILITY_IAM \
      --no-fail-on-empty-changeset
}

deploy_flink_worker_node() {
    local N=$1
    local NODE_HOST="flink-worker-${N}.${PRIVATE_DOMAIN_NAME}"
    local STACK_NAME="${VPC_NAME}-flink-worker-${N}"

    echo "[INFO] Cleaning up stale SSH host keys for ${NODE_HOST}..."
    if [ -f "$KH_PATH" ]; then sed -i.bak "/${NODE_HOST}/d" "$KH_PATH"; fi

    echo "[INFO] Preparing node-specific environment for Flink TaskManager ${N}..."
    local TEMP_ENV="/tmp/flink-worker-${N}.env"
    
    # Start with global settings
    cat .env > "$TEMP_ENV"
    echo "" >> "$TEMP_ENV"
    
    # Append node-specific overrides
    echo "NODE_HOSTNAME=${NODE_HOST}" >> "$TEMP_ENV"
    
    # Append template env properties
    if [ -f "envs/flink-worker.env" ]; then
        cat envs/flink-worker.env >> "$TEMP_ENV"
    fi

    # Upload to S3
    aws s3 cp "$TEMP_ENV" "s3://${BUCKET_NAME}/deploy/envs/flink-worker-${N}.env"
    rm -f "$TEMP_ENV"

    echo "[INFO] Launching Flink TaskManager Node ${N}..."
    aws cloudformation deploy \
      --stack-name "${STACK_NAME}" \
      --template-file "template/cluster-node.yaml" \
      --parameter-overrides \
          SubnetId="${SUBNET_ID}" \
          SecurityGroupId="${SG_ID}" \
          HostedZoneId="${ZONE_ID}" \
          KeyName="${SSH_KEY_NAME}" \
          NodeHostname="${NODE_HOST}" \
          InstanceName="Flink-Worker-${N}" \
          S3Bucket="${BUCKET_NAME}" \
          EnvironmentFile="flink-worker-${N}.env" \
          DeployScripts="deploy-flink-worker.sh" \
      --capabilities CAPABILITY_IAM \
      --no-fail-on-empty-changeset
}

# --- NODE DEPLOYMENT PHASE ---
echo "[INFO] Initiating deployment for target: ${TARGET}..."

# --- KAFKA CLUSTER ---
if [[ "$TARGET" == "kafka" || "$TARGET" == "all" ]]; then
    echo "[INFO] Preparing Kafka Quorum Voters map..."
    QUORUM_VOTERS=""
    for (( i=1; i<=KAFKA_COUNT; i++ )); do
        if [ -z "$QUORUM_VOTERS" ]; then
            QUORUM_VOTERS="${i}@kafka-${i}.${PRIVATE_DOMAIN_NAME}:9093"
        else
            QUORUM_VOTERS="${QUORUM_VOTERS},${i}@kafka-${i}.${PRIVATE_DOMAIN_NAME}:9093"
        fi
    done
    
    echo "[INFO] Deploying ${KAFKA_COUNT} Kafka Broker Nodes..."
    for (( N=1; N<=KAFKA_COUNT; N++ )); do
        deploy_kafka_node "$N" "$QUORUM_VOTERS"
    done

    echo "[INFO] Creating DNS alias for Kafka Bootstrap Server (kafka.${PRIVATE_DOMAIN_NAME})..."
    DNS_BATCH_FILE="/tmp/dns-kafka-aliases.json"
    cat <<EOF > "$DNS_BATCH_FILE"
{
  "Comment": "Creating CNAME for Kafka Bootstrap",
  "Changes": [
    { "Action": "UPSERT", "ResourceRecordSet": { "Name": "kafka.${PRIVATE_DOMAIN_NAME}", "Type": "CNAME", "TTL": 300, "ResourceRecords": [{ "Value": "kafka-1.${PRIVATE_DOMAIN_NAME}" }] } }
  ]
}
EOF
    aws route53 change-resource-record-sets --hosted-zone-id "${ZONE_ID}" --change-batch file://"${DNS_BATCH_FILE}"
    rm -f "$DNS_BATCH_FILE"
fi

# --- INFLUXDB & TELEGRAF NODE ---
if [[ "$TARGET" == "influxdb" || "$TARGET" == "all" ]]; then
    INFLUXDB_HOST="influxdb.${PRIVATE_DOMAIN_NAME}"
    echo "[INFO] Cleaning up stale SSH host keys for ${INFLUXDB_HOST}..."
    if [ -f "$KH_PATH" ]; then sed -i.bak "/${INFLUXDB_HOST}/d" "$KH_PATH"; fi

    echo "[INFO] Preparing InfluxDB environment..."
    TEMP_ENV="/tmp/influxdb.env"
    cat .env > "$TEMP_ENV"
    echo "" >> "$TEMP_ENV"
    echo "NODE_HOSTNAME=${INFLUXDB_HOST}" >> "$TEMP_ENV"
    if [ -f "envs/influxdb.env" ]; then
        cat envs/influxdb.env >> "$TEMP_ENV"
    fi
    aws s3 cp "$TEMP_ENV" "s3://${BUCKET_NAME}/deploy/envs/influxdb.env"
    rm -f "$TEMP_ENV"

    echo "[INFO] Deploying InfluxDB Node via CloudFormation..."
    aws cloudformation deploy \
      --stack-name "${VPC_NAME}-influxdb-node" \
      --template-file "template/cluster-node.yaml" \
      --parameter-overrides \
          InstanceType="c6i.large" \
          SubnetId="${SUBNET_ID}" \
          SecurityGroupId="${SG_ID}" \
          HostedZoneId="${ZONE_ID}" \
          KeyName="${SSH_KEY_NAME}" \
          NodeHostname="${INFLUXDB_HOST}" \
          InstanceName="InfluxDB-Node" \
          S3Bucket="${BUCKET_NAME}" \
          EnvironmentFile="influxdb.env" \
          DeployScripts="deploy-influxdb.sh" \
      --capabilities CAPABILITY_IAM \
      --no-fail-on-empty-changeset
fi

# --- FLINK MASTER (JOBMANAGER) ---
if [[ "$TARGET" == "flink-master" || "$TARGET" == "all" ]]; then
    MASTER_HOST="jobmanager.${PRIVATE_DOMAIN_NAME}"
    echo "[INFO] Cleaning up stale SSH host keys for ${MASTER_HOST}..."
    if [ -f "$KH_PATH" ]; then sed -i.bak "/${MASTER_HOST}/d" "$KH_PATH"; fi

    echo "[INFO] Preparing Flink Master environment..."
    TEMP_ENV="/tmp/flink-master.env"
    cat .env > "$TEMP_ENV"
    echo "" >> "$TEMP_ENV"
    echo "NODE_HOSTNAME=${MASTER_HOST}" >> "$TEMP_ENV"
    if [ -f "envs/flink-master.env" ]; then
        cat envs/flink-master.env >> "$TEMP_ENV"
    fi
    aws s3 cp "$TEMP_ENV" "s3://${BUCKET_NAME}/deploy/envs/flink-master.env"
    rm -f "$TEMP_ENV"

    echo "[INFO] Deploying Flink JobManager Node via CloudFormation..."
    aws cloudformation deploy \
      --stack-name "${VPC_NAME}-flink-master-node" \
      --template-file "template/cluster-node.yaml" \
      --parameter-overrides \
          InstanceType="c6i.large" \
          SubnetId="${SUBNET_ID}" \
          SecurityGroupId="${SG_ID}" \
          HostedZoneId="${ZONE_ID}" \
          KeyName="${SSH_KEY_NAME}" \
          NodeHostname="${MASTER_HOST}" \
          InstanceName="Flink-JobManager-Node" \
          S3Bucket="${BUCKET_NAME}" \
          EnvironmentFile="flink-master.env" \
          DeployScripts="deploy-flink-master.sh" \
      --capabilities CAPABILITY_IAM \
      --no-fail-on-empty-changeset

    echo "[INFO] Creating DNS alias for Flink Dashboard (flink.${PRIVATE_DOMAIN_NAME})..."
    DNS_BATCH_FILE="/tmp/dns-flink-aliases.json"
    cat <<EOF > "$DNS_BATCH_FILE"
{
  "Comment": "Creating alias for Flink JobManager Dashboard",
  "Changes": [
    { "Action": "UPSERT", "ResourceRecordSet": { "Name": "flink.${PRIVATE_DOMAIN_NAME}", "Type": "CNAME", "TTL": 300, "ResourceRecords": [{ "Value": "${MASTER_HOST}" }] } }
  ]
}
EOF
    aws route53 change-resource-record-sets --hosted-zone-id "${ZONE_ID}" --change-batch file://"${DNS_BATCH_FILE}"
    rm -f "$DNS_BATCH_FILE"
fi

# --- FLINK WORKERS (TASKMANAGERS) ---
if [[ "$TARGET" == "flink-worker" || "$TARGET" == "all" ]]; then
    echo "[INFO] Deploying ${FLINK_WORKER_COUNT} Flink TaskManager Nodes..."
    for (( N=1; N<=FLINK_WORKER_COUNT; N++ )); do
        deploy_flink_worker_node "$N"
    done
fi

# --- METRICS NODE (GRAFANA) ---
if [[ "$TARGET" == "metrics" || "$TARGET" == "all" ]]; then
    METRICS_HOST="metrics.${PRIVATE_DOMAIN_NAME}"
    echo "[INFO] Cleaning up stale SSH host keys for ${METRICS_HOST}..."
    if [ -f "$KH_PATH" ]; then sed -i.bak "/${METRICS_HOST}/d" "$KH_PATH"; fi

    echo "[INFO] Preparing Metrics environment..."
    TEMP_ENV="/tmp/metrics.env"
    cat .env > "$TEMP_ENV"
    echo "" >> "$TEMP_ENV"
    echo "NODE_HOSTNAME=${METRICS_HOST}" >> "$TEMP_ENV"
    if [ -f "envs/metrics.env" ]; then
        cat envs/metrics.env >> "$TEMP_ENV"
    fi
    aws s3 cp "$TEMP_ENV" "s3://${BUCKET_NAME}/deploy/envs/metrics.env"
    rm -f "$TEMP_ENV"

    echo "[INFO] Deploying Grafana Metrics Node via CloudFormation..."
    aws cloudformation deploy \
      --stack-name "${VPC_NAME}-metrics-node" \
      --template-file "template/cluster-node.yaml" \
      --parameter-overrides \
          SubnetId="${SUBNET_ID}" \
          SecurityGroupId="${SG_ID}" \
          HostedZoneId="${ZONE_ID}" \
          KeyName="${SSH_KEY_NAME}" \
          NodeHostname="${METRICS_HOST}" \
          InstanceName="Metrics-Node" \
          S3Bucket="${BUCKET_NAME}" \
          EnvironmentFile="metrics.env" \
          DeployScripts="deploy-metrics.sh" \
      --capabilities CAPABILITY_IAM \
      --no-fail-on-empty-changeset

    echo "[INFO] Creating DNS alias for Grafana (grafana.${PRIVATE_DOMAIN_NAME})..."
    DNS_BATCH_FILE="/tmp/dns-grafana-aliases.json"
    cat <<EOF > "$DNS_BATCH_FILE"
{
  "Comment": "Creating alias for Grafana UI",
  "Changes": [
    { "Action": "UPSERT", "ResourceRecordSet": { "Name": "grafana.${PRIVATE_DOMAIN_NAME}", "Type": "CNAME", "TTL": 300, "ResourceRecords": [{ "Value": "${METRICS_HOST}" }] } }
  ]
}
EOF
    aws route53 change-resource-record-sets --hosted-zone-id "${ZONE_ID}" --change-batch file://"${DNS_BATCH_FILE}"
    rm -f "$DNS_BATCH_FILE"
fi

echo "----------------------------------------------------"
echo "[SUCCESS] Deployment completed for target: ${TARGET}"
echo "----------------------------------------------------"
exit 0
