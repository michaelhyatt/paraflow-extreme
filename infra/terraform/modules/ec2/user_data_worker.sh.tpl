#!/bin/bash
set -ex

# ============================================================================
# Paraflow Worker Bootstrap Script
# Enhanced with comprehensive observability and stage tracking
# ============================================================================

# Bootstrap configuration
COMPONENT="worker"
JOB_ID="${job_id}"
AWS_REGION="${aws_region}"
LOG_GROUP="${log_group_name}"
ENABLE_DETAILED_MONITORING="${enable_detailed_monitoring}"
BOOTSTRAP_TIMEOUT="${bootstrap_timeout_seconds}"
BENCHMARK_MODE="${benchmark_mode}"

# Status tracking file
BOOTSTRAP_STATUS_FILE="/var/log/paraflow-bootstrap-status"
BOOTSTRAP_START_TIME=$(date +%s)

# ============================================================================
# Logging Functions
# ============================================================================

log_to_file() {
    local level="$1"
    local stage="$2"
    local message="$3"
    local timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    local elapsed=$(($(date +%s) - BOOTSTRAP_START_TIME))
    echo "[$timestamp] [$level] [STAGE:$stage] [elapsed:$${elapsed}s] $message" | tee -a /var/log/user-data.log
}

log_info() {
    log_to_file "INFO" "$1" "$2"
}

log_error() {
    log_to_file "ERROR" "$1" "$2"
}

log_success() {
    log_to_file "SUCCESS" "$1" "$2"
}

# Update bootstrap status file
update_status() {
    local stage="$1"
    local status="$2"
    local message="$3"
    local timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    local elapsed=$(($(date +%s) - BOOTSTRAP_START_TIME))

    cat > "$BOOTSTRAP_STATUS_FILE" <<EOF
{
    "component": "$COMPONENT",
    "job_id": "$JOB_ID",
    "stage": "$stage",
    "status": "$status",
    "message": "$message",
    "timestamp": "$timestamp",
    "elapsed_seconds": $elapsed,
    "instance_type": "$(curl -s http://169.254.169.254/latest/meta-data/instance-type || echo 'unknown')",
    "instance_id": "$(curl -s http://169.254.169.254/latest/meta-data/instance-id || echo 'unknown')"
}
EOF

    # Also log to syslog for CloudWatch
    logger -t "paraflow-$COMPONENT" "STAGE:$stage STATUS:$status MESSAGE:$message"
}

# Check for timeout
check_timeout() {
    local elapsed=$(($(date +%s) - BOOTSTRAP_START_TIME))
    if [ "$elapsed" -gt "$BOOTSTRAP_TIMEOUT" ]; then
        log_error "TIMEOUT" "Bootstrap timeout exceeded ($elapsed > $BOOTSTRAP_TIMEOUT seconds)"
        update_status "TIMEOUT" "FAILED" "Bootstrap exceeded timeout of $BOOTSTRAP_TIMEOUT seconds"
        exit 1
    fi
}

# ============================================================================
# Stage Definitions
# ============================================================================
STAGE_INIT="INIT"
STAGE_CLOUDWATCH_AGENT="CLOUDWATCH_AGENT"
STAGE_DOCKER_INSTALL="DOCKER_INSTALL"
STAGE_DOCKER_START="DOCKER_START"
STAGE_ECR_AUTH="ECR_AUTH"
STAGE_IMAGE_PULL="IMAGE_PULL"
STAGE_CONTAINER_START="CONTAINER_START"
STAGE_APP_READY="APP_READY"
STAGE_COMPLETED="COMPLETED"

# ============================================================================
# Main Bootstrap Sequence
# ============================================================================

# Redirect all output to log file
exec > >(tee -a /var/log/user-data.log | logger -t user-data -s 2>/dev/console) 2>&1

echo "============================================================================"
echo "Paraflow Worker Bootstrap Starting"
echo "Instance Type: $(curl -s http://169.254.169.254/latest/meta-data/instance-type || echo 'unknown')"
echo "Instance ID: $(curl -s http://169.254.169.254/latest/meta-data/instance-id || echo 'unknown')"
echo "Region: $AWS_REGION"
echo "Job ID: $JOB_ID"
echo "Benchmark Mode: $BENCHMARK_MODE"
echo "Worker Threads: ${worker_threads}"
echo "Batch Size: ${batch_size}"
echo "============================================================================"

update_status "$STAGE_INIT" "IN_PROGRESS" "Bootstrap initialization starting"
log_info "$STAGE_INIT" "Paraflow Worker bootstrap starting..."

# ============================================================================
# Stage: CloudWatch Agent Installation
# ============================================================================

if [ "$ENABLE_DETAILED_MONITORING" = "true" ]; then
    log_info "$STAGE_CLOUDWATCH_AGENT" "Installing CloudWatch agent for enhanced monitoring..."
    update_status "$STAGE_CLOUDWATCH_AGENT" "IN_PROGRESS" "Installing CloudWatch agent"
    check_timeout

    # Install CloudWatch agent
    dnf install -y amazon-cloudwatch-agent

    # Configure CloudWatch agent
    cat > /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json <<EOF
{
    "agent": {
        "metrics_collection_interval": 10,
        "run_as_user": "root"
    },
    "logs": {
        "logs_collected": {
            "files": {
                "collect_list": [
                    {
                        "file_path": "/var/log/user-data.log",
                        "log_group_name": "$LOG_GROUP",
                        "log_stream_name": "{instance_id}/$COMPONENT/user-data",
                        "timestamp_format": "%Y-%m-%dT%H:%M:%SZ"
                    },
                    {
                        "file_path": "/var/log/docker",
                        "log_group_name": "$LOG_GROUP",
                        "log_stream_name": "{instance_id}/$COMPONENT/docker-daemon"
                    },
                    {
                        "file_path": "/var/log/messages",
                        "log_group_name": "$LOG_GROUP",
                        "log_stream_name": "{instance_id}/$COMPONENT/system"
                    }
                ]
            }
        }
    },
    "metrics": {
        "namespace": "Paraflow/$JOB_ID",
        "metrics_collected": {
            "cpu": {
                "measurement": ["cpu_usage_idle", "cpu_usage_user", "cpu_usage_system"],
                "metrics_collection_interval": 10
            },
            "mem": {
                "measurement": ["mem_used_percent", "mem_available"],
                "metrics_collection_interval": 10
            },
            "disk": {
                "measurement": ["disk_used_percent"],
                "resources": ["/"],
                "metrics_collection_interval": 60
            },
            "net": {
                "measurement": ["bytes_recv", "bytes_sent"],
                "metrics_collection_interval": 10
            }
        },
        "append_dimensions": {
            "InstanceId": "\$${aws:InstanceId}",
            "InstanceType": "\$${aws:InstanceType}",
            "Component": "$COMPONENT",
            "JobId": "$JOB_ID"
        }
    }
}
EOF

    # Start CloudWatch agent
    systemctl enable amazon-cloudwatch-agent
    systemctl start amazon-cloudwatch-agent

    log_success "$STAGE_CLOUDWATCH_AGENT" "CloudWatch agent installed and started"
    update_status "$STAGE_CLOUDWATCH_AGENT" "COMPLETED" "CloudWatch agent running"
else
    log_info "$STAGE_CLOUDWATCH_AGENT" "Detailed monitoring disabled, skipping CloudWatch agent"
    update_status "$STAGE_CLOUDWATCH_AGENT" "SKIPPED" "Detailed monitoring disabled"
fi

# ============================================================================
# Stage: Docker Installation
# ============================================================================

log_info "$STAGE_DOCKER_INSTALL" "Installing Docker..."
update_status "$STAGE_DOCKER_INSTALL" "IN_PROGRESS" "Installing Docker"
check_timeout

dnf install -y docker

log_success "$STAGE_DOCKER_INSTALL" "Docker installed successfully"
update_status "$STAGE_DOCKER_INSTALL" "COMPLETED" "Docker installed"

# ============================================================================
# Stage: Docker Start
# ============================================================================

log_info "$STAGE_DOCKER_START" "Starting Docker daemon..."
update_status "$STAGE_DOCKER_START" "IN_PROGRESS" "Starting Docker daemon"
check_timeout

systemctl enable docker
systemctl start docker

# Wait for Docker to be ready
DOCKER_WAIT_TIMEOUT=60
DOCKER_WAIT_START=$(date +%s)
while ! docker info > /dev/null 2>&1; do
    DOCKER_ELAPSED=$(($(date +%s) - DOCKER_WAIT_START))
    if [ "$DOCKER_ELAPSED" -gt "$DOCKER_WAIT_TIMEOUT" ]; then
        log_error "$STAGE_DOCKER_START" "Docker failed to start within $DOCKER_WAIT_TIMEOUT seconds"
        update_status "$STAGE_DOCKER_START" "FAILED" "Docker daemon failed to start"
        exit 1
    fi
    log_info "$STAGE_DOCKER_START" "Waiting for Docker daemon... ($DOCKER_ELAPSED seconds)"
    sleep 2
done

log_success "$STAGE_DOCKER_START" "Docker daemon running"
update_status "$STAGE_DOCKER_START" "COMPLETED" "Docker daemon running"

# ============================================================================
# Stage: ECR Authentication
# ============================================================================

log_info "$STAGE_ECR_AUTH" "Authenticating with ECR..."
update_status "$STAGE_ECR_AUTH" "IN_PROGRESS" "Authenticating with ECR"
check_timeout

ECR_REGISTRY="${ecr_repository}"

if aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin "$ECR_REGISTRY"; then
    log_success "$STAGE_ECR_AUTH" "ECR authentication successful"
    update_status "$STAGE_ECR_AUTH" "COMPLETED" "ECR authentication successful"
else
    log_error "$STAGE_ECR_AUTH" "ECR authentication failed"
    update_status "$STAGE_ECR_AUTH" "FAILED" "ECR authentication failed"
    exit 1
fi

# ============================================================================
# Stage: Image Pull
# ============================================================================

log_info "$STAGE_IMAGE_PULL" "Pulling Docker image: ${ecr_repository}:${image_tag}"
update_status "$STAGE_IMAGE_PULL" "IN_PROGRESS" "Pulling Docker image"
check_timeout

PULL_START=$(date +%s)
if docker pull "${ecr_repository}:${image_tag}"; then
    PULL_DURATION=$(($(date +%s) - PULL_START))
    IMAGE_SIZE=$(docker images "${ecr_repository}:${image_tag}" --format "{{.Size}}")
    log_success "$STAGE_IMAGE_PULL" "Image pulled successfully in $${PULL_DURATION}s (size: $IMAGE_SIZE)"
    update_status "$STAGE_IMAGE_PULL" "COMPLETED" "Image pulled in $${PULL_DURATION}s"
else
    log_error "$STAGE_IMAGE_PULL" "Failed to pull Docker image"
    update_status "$STAGE_IMAGE_PULL" "FAILED" "Docker image pull failed"
    exit 1
fi

# ============================================================================
# Stage: Container Start
# ============================================================================

log_info "$STAGE_CONTAINER_START" "Starting pf-worker container..."
update_status "$STAGE_CONTAINER_START" "IN_PROGRESS" "Starting container"
check_timeout

# Prepare container run command
DOCKER_CMD="docker run --rm --name pf-worker \
  -e AWS_REGION=$AWS_REGION \
  ${ecr_repository}:${image_tag} \
  pf-worker \
  --input sqs \
  --sqs-queue-url ${sqs_queue_url} \
  --destination stats \
  --threads ${worker_threads} \
  --batch-size ${batch_size} \
  --sqs-drain \
  --region $AWS_REGION \
  --progress \
  --log-level info"

log_info "$STAGE_CONTAINER_START" "Container command: $DOCKER_CMD"
update_status "$STAGE_APP_READY" "IN_PROGRESS" "Application starting"

# Run the worker and capture output
CONTAINER_START=$(date +%s)
WORKER_OUTPUT_FILE="/var/log/pf-worker-output.log"
echo "Starting pf-worker..."

docker run --rm \
  --name pf-worker \
  -e AWS_REGION=$AWS_REGION \
  ${ecr_repository}:${image_tag} \
  pf-worker \
  --input sqs \
  --sqs-queue-url ${sqs_queue_url} \
  --destination stats \
  --threads ${worker_threads} \
  --batch-size ${batch_size} \
  --sqs-drain \
  --region $AWS_REGION \
  --progress \
  --log-level info 2>&1 | tee "$WORKER_OUTPUT_FILE"

CONTAINER_EXIT_CODE=$${PIPESTATUS[0]}
CONTAINER_DURATION=$(($(date +%s) - CONTAINER_START))

# Parse worker output for metrics
parse_worker_metric() {
    local metric_name="$1"
    local pattern="$2"
    grep -oP "$pattern" "$WORKER_OUTPUT_FILE" 2>/dev/null | tail -1 || echo "0"
}

# Extract throughput metrics from worker output
WORKER_FILES_PROCESSED=$(grep "Files processed:" "$WORKER_OUTPUT_FILE" 2>/dev/null | grep -oP '\d+' | head -1 || echo "0")
WORKER_FILES_FAILED=$(grep "Files failed:" "$WORKER_OUTPUT_FILE" 2>/dev/null | grep -oP '\d+' | head -1 || echo "0")
WORKER_RECORDS_PROCESSED=$(grep "Records processed:" "$WORKER_OUTPUT_FILE" 2>/dev/null | grep -oP '[\d,]+' | head -1 | tr -d ',' || echo "0")
WORKER_RECORDS_FAILED=$(grep "Records failed:" "$WORKER_OUTPUT_FILE" 2>/dev/null | grep -oP '[\d,]+' | head -1 | tr -d ',' || echo "0")
WORKER_BYTES_READ=$(grep "Bytes read:" "$WORKER_OUTPUT_FILE" 2>/dev/null | grep -oP '[\d.]+\s*(KB|MB|GB|bytes)' | head -1 || echo "0 bytes")
WORKER_BYTES_WRITTEN=$(grep "Bytes written:" "$WORKER_OUTPUT_FILE" 2>/dev/null | grep -oP '[\d.]+\s*(KB|MB|GB|bytes)' | head -1 || echo "0 bytes")
WORKER_DURATION=$(grep "Duration:" "$WORKER_OUTPUT_FILE" 2>/dev/null | grep -oP '[\d.]+' | head -1 || echo "0")
WORKER_FILES_PER_SEC=$(grep "files/sec" "$WORKER_OUTPUT_FILE" 2>/dev/null | grep -oP '[\d.]+' | head -1 || echo "0")
WORKER_RECORDS_PER_SEC=$(grep "records/sec" "$WORKER_OUTPUT_FILE" 2>/dev/null | grep -oP '[\d,]+' | head -1 | tr -d ',' || echo "0")
WORKER_MB_PER_SEC=$(grep "MB/s read" "$WORKER_OUTPUT_FILE" 2>/dev/null | grep -oP '[\d.]+' | head -1 || echo "0")

log_info "$STAGE_CONTAINER_START" "Worker metrics: files=$WORKER_FILES_PROCESSED, records=$WORKER_RECORDS_PROCESSED, throughput=$WORKER_RECORDS_PER_SEC rec/s"

# ============================================================================
# Stage: Completion
# ============================================================================

if [ "$CONTAINER_EXIT_CODE" -eq 0 ]; then
    log_success "$STAGE_COMPLETED" "Worker completed successfully in $${CONTAINER_DURATION}s"
    update_status "$STAGE_COMPLETED" "SUCCESS" "Completed in $${CONTAINER_DURATION}s"

    # Record benchmark metrics if in benchmark mode
    if [ "$BENCHMARK_MODE" = "true" ]; then
        TOTAL_DURATION=$(($(date +%s) - BOOTSTRAP_START_TIME))
        cat >> /var/log/benchmark-metrics.json <<EOF
{
    "component": "$COMPONENT",
    "job_id": "$JOB_ID",
    "instance_type": "$(curl -s http://169.254.169.254/latest/meta-data/instance-type)",
    "bootstrap_duration_seconds": $TOTAL_DURATION,
    "container_duration_seconds": $CONTAINER_DURATION,
    "worker_threads": ${worker_threads},
    "batch_size": ${batch_size},
    "status": "SUCCESS",
    "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
    "throughput": {
        "files_processed": $WORKER_FILES_PROCESSED,
        "files_failed": $WORKER_FILES_FAILED,
        "records_processed": $WORKER_RECORDS_PROCESSED,
        "records_failed": $WORKER_RECORDS_FAILED,
        "bytes_read": "$WORKER_BYTES_READ",
        "bytes_written": "$WORKER_BYTES_WRITTEN",
        "duration_seconds": $WORKER_DURATION,
        "files_per_second": $WORKER_FILES_PER_SEC,
        "records_per_second": $WORKER_RECORDS_PER_SEC,
        "mb_per_second": $WORKER_MB_PER_SEC
    }
}
EOF
        log_info "$STAGE_COMPLETED" "Benchmark metrics recorded"
    fi
else
    log_error "$STAGE_COMPLETED" "Worker failed with exit code $CONTAINER_EXIT_CODE after $${CONTAINER_DURATION}s"
    update_status "$STAGE_COMPLETED" "FAILED" "Exit code $CONTAINER_EXIT_CODE after $${CONTAINER_DURATION}s"

    if [ "$BENCHMARK_MODE" = "true" ]; then
        cat >> /var/log/benchmark-metrics.json <<EOF
{
    "component": "$COMPONENT",
    "job_id": "$JOB_ID",
    "instance_type": "$(curl -s http://169.254.169.254/latest/meta-data/instance-type)",
    "status": "FAILED",
    "exit_code": $CONTAINER_EXIT_CODE,
    "timestamp": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
}
EOF
    fi
    exit $CONTAINER_EXIT_CODE
fi

echo "============================================================================"
echo "Paraflow Worker Bootstrap Complete"
echo "Total duration: $(($(date +%s) - BOOTSTRAP_START_TIME)) seconds"
echo "============================================================================"
