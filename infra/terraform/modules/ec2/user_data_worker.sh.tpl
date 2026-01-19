#!/bin/bash
set -e

# Paraflow Worker Bootstrap Script
COMPONENT="worker"
JOB_ID="${job_id}"
AWS_REGION="${aws_region}"
LOG_GROUP="${log_group_name}"
ENABLE_MONITORING="${enable_detailed_monitoring}"
BENCHMARK_MODE="${benchmark_mode}"
ENABLE_PROFILING="${enable_profiling}"
ARTIFACTS_BUCKET="${artifacts_bucket}"
PREPOPULATE_QUEUE="${prepopulate_queue}"
START_TIME=$(date +%s)

exec > >(tee -a /var/log/user-data.log | logger -t user-data -s 2>/dev/console) 2>&1

# Auto-detect CPU cores for thread count (use configured value of 0 means auto)
CONFIGURED_THREADS="${worker_threads}"
if [ "$CONFIGURED_THREADS" -eq 0 ] || [ "$CONFIGURED_THREADS" = "auto" ]; then
    WORKER_THREADS=$(nproc)
else
    WORKER_THREADS="$CONFIGURED_THREADS"
fi

echo "=== Paraflow Worker Bootstrap ==="
echo "Region: $AWS_REGION | Job: $JOB_ID | CPUs: $(nproc) | Threads: $WORKER_THREADS | Batch: ${batch_size}"

# CloudWatch Agent (optional) - install only if monitoring enabled
if [ "$ENABLE_MONITORING" = "true" ]; then
    # CloudWatch agent may already be installed on ECS-optimized AMI
    if ! command -v /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl &> /dev/null; then
        yum install -y amazon-cloudwatch-agent
    fi
    cat > /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json <<EOF
{"agent":{"metrics_collection_interval":10},"logs":{"logs_collected":{"files":{"collect_list":[{"file_path":"/var/log/user-data.log","log_group_name":"$LOG_GROUP","log_stream_name":"{instance_id}/$COMPONENT/user-data"}]}}},"metrics":{"namespace":"Paraflow/$JOB_ID","metrics_collected":{"cpu":{"measurement":["cpu_usage_user"],"metrics_collection_interval":10},"mem":{"measurement":["mem_used_percent"],"metrics_collection_interval":10}},"append_dimensions":{"InstanceId":"\$${aws:InstanceId}","Component":"$COMPONENT"}}}
EOF
    systemctl enable amazon-cloudwatch-agent && systemctl start amazon-cloudwatch-agent
fi

# Docker is pre-installed on ECS-Optimized AMI - just ensure it's running
systemctl start docker
for i in {1..10}; do docker info >/dev/null 2>&1 && break || sleep 1; done

# Install sysstat for iostat (used in profiling artifact collection)
if [ "$ENABLE_PROFILING" = "true" ]; then
    yum install -y sysstat 2>/dev/null || true
fi

# ECR Auth and pull
ECR="${ecr_repository}"
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin "$ECR"
docker pull "${ecr_repository}:${image_tag}"

# Wait for discoverer to populate the queue
SQS_QUEUE_URL="${sqs_queue_url}"
POLL_INTERVAL=5

if [ "$PREPOPULATE_QUEUE" = "true" ]; then
    # Prepopulate mode: wait for discoverer to COMPLETE (via SSM parameter)
    echo "Prepopulate mode: waiting for discoverer to fully populate the queue..."
    SSM_PARAM="/paraflow/$JOB_ID/discoverer-done"
    MAX_WAIT=1800  # 30 minutes max wait for full population
    WAITED=0

    while [ $WAITED -lt $MAX_WAIT ]; do
        # Check if discoverer has signaled completion
        SSM_VALUE=$(aws ssm get-parameter --region $AWS_REGION \
            --name "$SSM_PARAM" \
            --query 'Parameter.Value' \
            --output text 2>/dev/null || echo "")

        if [ -n "$SSM_VALUE" ] && [ "$SSM_VALUE" != "None" ]; then
            echo "Discoverer completed: $SSM_VALUE"

            # Get final queue status
            QUEUE_ATTRS=$(aws sqs get-queue-attributes --region $AWS_REGION \
                --queue-url "$SQS_QUEUE_URL" \
                --attribute-names ApproximateNumberOfMessages \
                --output json 2>/dev/null || echo '{}')
            VISIBLE=$(echo "$QUEUE_ATTRS" | grep -o '"ApproximateNumberOfMessages"[^,}]*' | grep -o '[0-9]*' || echo "0")
            echo "Queue has $VISIBLE messages ready - starting worker"
            break
        fi

        echo "Waiting for discoverer to complete... ($WAITED/$MAX_WAIT seconds)"
        sleep $POLL_INTERVAL
        WAITED=$((WAITED + POLL_INTERVAL))
    done

    if [ $WAITED -ge $MAX_WAIT ]; then
        echo "WARNING: Timed out waiting for discoverer to complete after $${MAX_WAIT}s"
        echo "Proceeding anyway - discoverer may have failed"
    fi
else
    # Normal mode: start as soon as first message appears
    echo "Waiting for discoverer to populate SQS queue..."
    MAX_WAIT=300  # 5 minutes max wait for first message
    WAITED=0

    while [ $WAITED -lt $MAX_WAIT ]; do
        # Check queue for messages (visible + in-flight)
        QUEUE_ATTRS=$(aws sqs get-queue-attributes --region $AWS_REGION \
            --queue-url "$SQS_QUEUE_URL" \
            --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
            --output json 2>/dev/null || echo '{}')

        VISIBLE=$(echo "$QUEUE_ATTRS" | grep -o '"ApproximateNumberOfMessages"[^,}]*' | grep -o '[0-9]*' || echo "0")
        IN_FLIGHT=$(echo "$QUEUE_ATTRS" | grep -o '"ApproximateNumberOfMessagesNotVisible"[^,}]*' | grep -o '[0-9]*' || echo "0")
        TOTAL=$((VISIBLE + IN_FLIGHT))

        if [ "$TOTAL" -gt 0 ]; then
            echo "Queue has $TOTAL messages (visible=$VISIBLE, in-flight=$IN_FLIGHT) - starting worker"
            break
        fi

        echo "Queue empty, waiting for discoverer... ($WAITED/$MAX_WAIT seconds)"
        sleep $POLL_INTERVAL
        WAITED=$((WAITED + POLL_INTERVAL))
    done

    if [ $WAITED -ge $MAX_WAIT ]; then
        echo "WARNING: Timed out waiting for messages in queue after $${MAX_WAIT}s"
        echo "Proceeding anyway - discoverer may have failed or no files matched"
    fi
fi

# Function to collect and upload profiling artifacts
collect_profiling_artifacts() {
    echo "Collecting profiling artifacts..."

    ARTIFACTS_DIR="/var/log/profiling-artifacts"
    mkdir -p $ARTIFACTS_DIR

    # Copy profiling files from container volume mount
    cp /var/log/tokio-metrics.jsonl $ARTIFACTS_DIR/ 2>/dev/null || true
    cp /var/log/profile_*.svg $ARTIFACTS_DIR/ 2>/dev/null || true
    cp /var/log/benchmark-metrics.json $ARTIFACTS_DIR/ 2>/dev/null || true
    cp /var/log/worker_metrics.json $ARTIFACTS_DIR/ 2>/dev/null || true

    # Capture final docker stats
    docker stats --no-stream --format "{{json .}}" > $ARTIFACTS_DIR/docker-stats.json 2>/dev/null || true

    #---------------------------------------------------------------------------
    # TROUBLESHOOTING DATA CAPTURE
    #---------------------------------------------------------------------------

    # Container logs (stdout/stderr from pf-worker)
    echo "Collecting container logs..."
    docker logs pf-worker > $ARTIFACTS_DIR/container-stdout.log 2> $ARTIFACTS_DIR/container-stderr.log || true

    # Docker inspect for container metadata and state
    docker inspect pf-worker > $ARTIFACTS_DIR/container-inspect.json 2>/dev/null || true

    # OS-level logs
    echo "Collecting OS logs..."
    mkdir -p $ARTIFACTS_DIR/os-logs

    # System journal (recent entries)
    journalctl --since "1 hour ago" --no-pager > $ARTIFACTS_DIR/os-logs/journal-1h.log 2>/dev/null || true

    # Docker daemon logs
    journalctl -u docker --since "1 hour ago" --no-pager > $ARTIFACTS_DIR/os-logs/docker-daemon.log 2>/dev/null || true

    # Kernel messages (dmesg) - useful for OOM kills, hardware issues
    dmesg -T > $ARTIFACTS_DIR/os-logs/dmesg.log 2>/dev/null || true

    # Cloud-init logs (EC2 user-data execution)
    cp /var/log/cloud-init.log $ARTIFACTS_DIR/os-logs/ 2>/dev/null || true
    cp /var/log/cloud-init-output.log $ARTIFACTS_DIR/os-logs/ 2>/dev/null || true

    # Key system logs
    echo "Collecting key system logs..."
    cp /var/log/messages $ARTIFACTS_DIR/os-logs/ 2>/dev/null || true
    cp /var/log/secure $ARTIFACTS_DIR/os-logs/ 2>/dev/null || true

    # ECS agent logs (if using ECS)
    cp -r /var/log/ecs $ARTIFACTS_DIR/os-logs/ 2>/dev/null || true

    # Process state at collection time
    echo "Collecting process state..."
    ps auxf > $ARTIFACTS_DIR/process-tree.txt 2>/dev/null || true
    top -bn1 > $ARTIFACTS_DIR/top-snapshot.txt 2>/dev/null || true

    # Network state
    ss -tuanp > $ARTIFACTS_DIR/network-connections.txt 2>/dev/null || true

    # Memory details
    cat /proc/meminfo > $ARTIFACTS_DIR/meminfo.txt 2>/dev/null || true
    cat /proc/vmstat > $ARTIFACTS_DIR/vmstat.txt 2>/dev/null || true

    # I/O statistics
    iostat -x 1 3 > $ARTIFACTS_DIR/iostat.txt 2>/dev/null || true

    #---------------------------------------------------------------------------
    # END TROUBLESHOOTING DATA CAPTURE
    #---------------------------------------------------------------------------

    # System information
    {
        echo "=== Instance Info ==="
        echo "Instance ID: $(curl -s http://169.254.169.254/latest/meta-data/instance-id)"
        echo "Instance Type: $(curl -s http://169.254.169.254/latest/meta-data/instance-type)"
        echo "Availability Zone: $(curl -s http://169.254.169.254/latest/meta-data/placement/availability-zone)"
        echo ""
        echo "=== CPU Info ==="
        lscpu | grep -E "^(Architecture|CPU\(s\)|Model name|CPU max MHz)"
        echo ""
        echo "=== Memory Info ==="
        free -h
        echo ""
        echo "=== Disk Info ==="
        df -h /
        echo ""
        echo "=== Uptime ==="
        uptime
        echo ""
        echo "=== Kernel Version ==="
        uname -a
    } > $ARTIFACTS_DIR/system-info.txt

    # Create tarball with human-readable datetime
    DATETIME=$(date +%Y%m%d-%H%M%S)
    INSTANCE_ID=$(curl -s http://169.254.169.254/latest/meta-data/instance-id)
    TARBALL="/tmp/profiling-$${JOB_ID}-$${DATETIME}-$${INSTANCE_ID}.tar.gz"

    tar -czf $TARBALL -C $ARTIFACTS_DIR .

    # Upload to S3
    aws s3 cp $TARBALL s3://$${ARTIFACTS_BUCKET}/profiling/$${JOB_ID}/ --region $AWS_REGION

    echo "Profiling artifacts uploaded to s3://$${ARTIFACTS_BUCKET}/profiling/$${JOB_ID}/"
}

# Run worker
echo "Starting pf-worker..."
WORKER_LOG="/var/log/pf-worker-output.log"
CONTAINER_START=$(date +%s)

# Build docker run command with optional profiling flags
# Note: We don't use --rm when profiling is enabled so we can collect container logs/stats after exit
if [ "$ENABLE_PROFILING" = "true" ]; then
    DOCKER_ARGS="--name pf-worker -e AWS_REGION=$AWS_REGION -v /var/log:/var/log"
    WORKER_ARGS="--input sqs --sqs-queue-url ${sqs_queue_url} --destination stats --threads $WORKER_THREADS --batch-size ${batch_size} --sqs-drain --region $AWS_REGION --progress --log-level info --metrics-file /var/log/tokio-metrics.jsonl --profile-dir /var/log --profile-interval 60"
else
    DOCKER_ARGS="--rm --name pf-worker -e AWS_REGION=$AWS_REGION"
    WORKER_ARGS="--input sqs --sqs-queue-url ${sqs_queue_url} --destination stats --threads $WORKER_THREADS --batch-size ${batch_size} --sqs-drain --region $AWS_REGION --progress --log-level info"
fi

docker run $DOCKER_ARGS ${ecr_repository}:${image_tag} $WORKER_ARGS 2>&1 | tee "$WORKER_LOG"

EXIT_CODE=$${PIPESTATUS[0]}
TOTAL_DURATION=$(($(date +%s) - CONTAINER_START))

# Parse metrics (handle comma-formatted numbers like "4,069")
FILES=$(grep "Files processed:" "$WORKER_LOG" 2>/dev/null | grep -oP '[\d,]+' | head -1 | tr -d ',' || echo "0")
RECORDS=$(grep "Records processed:" "$WORKER_LOG" 2>/dev/null | grep -oP '[\d,]+' | head -1 | tr -d ',' || echo "0")
REC_SEC=$(grep "records/sec" "$WORKER_LOG" 2>/dev/null | grep -oP '[\d,]+' | head -1 | tr -d ',' || echo "0")
MB_SEC=$(grep "MB/s read" "$WORKER_LOG" 2>/dev/null | grep -oP '[\d.]+' | head -1 || echo "0")

# Parse active duration (actual file processing time, excludes startup/drain overhead)
# Format: "Active duration:    67.23s"
ACTIVE_DURATION=$(grep "Active duration:" "$WORKER_LOG" 2>/dev/null | grep -oP '[\d.]+' | head -1 || echo "0")
# Convert to integer seconds for JSON (round up to be conservative)
ACTIVE_DURATION_INT=$(echo "$ACTIVE_DURATION" | awk '{printf "%.0f", $1 + 0.5}')
# Fall back to total duration if active duration not available
if [ "$ACTIVE_DURATION_INT" = "0" ] || [ -z "$ACTIVE_DURATION_INT" ]; then
    ACTIVE_DURATION_INT=$TOTAL_DURATION
fi

echo "Metrics: files=$FILES records=$RECORDS throughput=$REC_SEC rec/s $MB_SEC MB/s active_duration=${ACTIVE_DURATION_INT}s total_duration=${TOTAL_DURATION}s"

# Benchmark metrics - use active_duration for accurate throughput reporting
if [ "$BENCHMARK_MODE" = "true" ]; then
    cat > /var/log/benchmark-metrics.json <<EOF
{"component":"$COMPONENT","job_id":"$JOB_ID","instance_type":"$(curl -s http://169.254.169.254/latest/meta-data/instance-type)","duration":$ACTIVE_DURATION_INT,"total_duration":$TOTAL_DURATION,"status":"$([ $EXIT_CODE -eq 0 ] && echo SUCCESS || echo FAILED)","throughput":{"files":$FILES,"records":$RECORDS,"rec_per_sec":$REC_SEC,"mb_per_sec":$MB_SEC}}
EOF
fi

# Collect and upload profiling artifacts before instance terminates
if [ "$ENABLE_PROFILING" = "true" ] && [ -n "$ARTIFACTS_BUCKET" ]; then
    collect_profiling_artifacts
    # Clean up container now that we've collected artifacts (container was not started with --rm)
    docker rm pf-worker 2>/dev/null || true
fi

echo "=== Worker Complete (active=${ACTIVE_DURATION_INT}s, total=${TOTAL_DURATION}s, exit=$EXIT_CODE) ==="
exit $EXIT_CODE
