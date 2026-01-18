#!/bin/bash
# ============================================================================
# Paraflow Benchmark Runner
# Deploys infrastructure, monitors execution, collects metrics, and tears down
# ============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TERRAFORM_DIR="$SCRIPT_DIR/../terraform"
RESULTS_DIR="${RESULTS_DIR:-/tmp/paraflow-benchmarks}"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)

# AWS Configuration - ensure region and profile are set for all AWS CLI commands
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-$AWS_REGION}"
# Preserve AWS_PROFILE if set (for SSO or named profiles)
if [ -n "$AWS_PROFILE" ]; then
    export AWS_PROFILE
fi

# Default configuration
INSTANCE_TYPE="${INSTANCE_TYPE:-t4g.medium}"
WORKER_THREADS="${WORKER_THREADS:-4}"
BATCH_SIZE="${BATCH_SIZE:-10000}"
MAX_FILES="${MAX_FILES:-100}"
POLL_INTERVAL="${POLL_INTERVAL:-30}"  # seconds
MAX_WAIT_TIME="${MAX_WAIT_TIME:-3600}"  # 1 hour max

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# ============================================================================
# Logging Functions
# ============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $(date -u +"%Y-%m-%dT%H:%M:%SZ") $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $(date -u +"%Y-%m-%dT%H:%M:%SZ") $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date -u +"%Y-%m-%dT%H:%M:%SZ") $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $(date -u +"%Y-%m-%dT%H:%M:%SZ") $1"
}

# ============================================================================
# Utility Functions
# ============================================================================

setup_results_dir() {
    local run_dir="$RESULTS_DIR/$TIMESTAMP-$INSTANCE_TYPE"
    mkdir -p "$run_dir"
    echo "$run_dir"
}

get_instance_id() {
    local component="$1"
    # Include all states to find instances even after completion
    aws ec2 describe-instances \
        --region "$AWS_REGION" \
        --filters "Name=tag:JobId,Values=$JOB_ID" "Name=tag:Component,Values=$component" \
        --query 'Reservations[0].Instances[0].InstanceId' \
        --output text 2>/dev/null || echo "None"
}

get_instance_status() {
    local instance_id="$1"
    if [ "$instance_id" = "None" ] || [ -z "$instance_id" ]; then
        echo "not_found"
        return
    fi
    # Use describe-instances (not describe-instance-status) to get state for any instance
    local status=$(aws ec2 describe-instances \
        --region "$AWS_REGION" \
        --instance-ids "$instance_id" \
        --query 'Reservations[0].Instances[0].State.Name' \
        --output text 2>/dev/null || echo "error")

    # Handle None or empty response
    if [ "$status" = "None" ] || [ -z "$status" ] || [ "$status" = "error" ]; then
        echo "pending"  # Likely still initializing
    else
        echo "$status"
    fi
}

fetch_bootstrap_status() {
    local instance_id="$1"
    if [ "$instance_id" = "None" ] || [ -z "$instance_id" ]; then
        echo "{}"
        return
    fi

    # Bootstrap status is now derived from CloudWatch logs or EC2 instance status
    # SSM requires IAM role configuration which isn't set up
    echo "{\"status\": \"monitoring_via_cloudwatch\"}"
}

# Track last log timestamp to avoid showing duplicate logs
LAST_LOG_TIMESTAMP=0

fetch_cloudwatch_logs_realtime() {
    local job_id="$1"
    local component="$2"
    local instance_id="$3"

    if [ -z "$instance_id" ] || [ "$instance_id" = "None" ] || [ "$instance_id" = "N/A" ]; then
        return
    fi

    local log_group="/paraflow/jobs/$job_id"
    local log_stream="${instance_id}/${component}/user-data"

    # Get logs since last timestamp (or last 60 seconds if first time)
    local start_time=$LAST_LOG_TIMESTAMP
    if [ "$start_time" -eq 0 ]; then
        start_time=$(( $(date +%s) * 1000 - 60000 ))  # 60 seconds ago in milliseconds
    fi

    # Fetch recent log events
    local logs=$(aws logs get-log-events \
        --region "$AWS_REGION" \
        --log-group-name "$log_group" \
        --log-stream-name "$log_stream" \
        --start-time "$start_time" \
        --limit 20 \
        --query 'events[*].[timestamp,message]' \
        --output text 2>/dev/null || echo "")

    if [ -n "$logs" ] && [ "$logs" != "None" ]; then
        echo "--- $component Logs (recent) ---"
        echo "$logs" | while IFS=$'\t' read -r ts msg; do
            if [ -n "$msg" ]; then
                # Update last timestamp
                if [ "$ts" -gt "$LAST_LOG_TIMESTAMP" ] 2>/dev/null; then
                    LAST_LOG_TIMESTAMP="$ts"
                fi
                # Format timestamp and print
                local formatted_ts=$(date -r $((ts / 1000)) "+%H:%M:%S" 2>/dev/null || echo "$ts")
                echo "[$formatted_ts] $msg"
            fi
        done
        echo "--- End $component Logs ---"
    fi
}

fetch_benchmark_metrics() {
    local instance_id="$1"
    local run_dir="$2"

    if [ "$instance_id" = "None" ] || [ -z "$instance_id" ]; then
        log_warn "No instance ID provided for metrics fetch"
        echo "{}" > "$run_dir/worker_metrics.json"
        return
    fi

    log_info "Fetching benchmark metrics from CloudWatch logs..."

    # Try to fetch metrics from CloudWatch logs (worker writes benchmark-metrics.json content to logs)
    local log_group="/paraflow/jobs/$JOB_ID"
    local log_stream="${instance_id}/worker/user-data"

    # Fetch recent log events to find the benchmark metrics JSON
    local logs=$(aws logs get-log-events \
        --region "$AWS_REGION" \
        --log-group-name "$log_group" \
        --log-stream-name "$log_stream" \
        --limit 100 \
        --query 'events[*].message' \
        --output text 2>/dev/null || echo "")

    if [ -n "$logs" ] && [ "$logs" != "None" ]; then
        # Try to find the benchmark metrics JSON in the logs
        # The worker outputs: Metrics: files=X records=Y throughput=Z rec/s W MB/s
        local files=$(echo "$logs" | grep -oP 'files=\K\d+' | tail -1 || echo "0")
        local records=$(echo "$logs" | grep -oP 'records=\K[\d,]+' | tail -1 | tr -d ',' || echo "0")
        local rec_per_sec=$(echo "$logs" | grep -oP 'throughput=\K[\d,]+' | tail -1 | tr -d ',' || echo "0")
        local mb_per_sec=$(echo "$logs" | grep -oP '[\d.]+(?= MB/s)' | tail -1 || echo "0")

        # Also try to extract from JSON format in logs
        if [ "$files" = "0" ]; then
            files=$(echo "$logs" | grep -oP '"files":\s*\K\d+' | tail -1 || echo "0")
        fi
        if [ "$records" = "0" ]; then
            records=$(echo "$logs" | grep -oP '"records":\s*\K\d+' | tail -1 || echo "0")
        fi
        if [ "$rec_per_sec" = "0" ]; then
            rec_per_sec=$(echo "$logs" | grep -oP '"rec_per_sec":\s*\K\d+' | tail -1 || echo "0")
        fi
        if [ "$mb_per_sec" = "0" ]; then
            mb_per_sec=$(echo "$logs" | grep -oP '"mb_per_sec":\s*\K[\d.]+' | tail -1 || echo "0")
        fi

        # Extract duration from "Worker Complete (Xs, exit=0)" message
        local duration=$(echo "$logs" | grep -oP 'Worker Complete \(\K\d+' | tail -1 || echo "0")

        log_info "Extracted metrics: files=$files, records=$records, $rec_per_sec rec/s, $mb_per_sec MB/s, duration=${duration}s"

        # Save as JSON
        cat > "$run_dir/worker_metrics.json" <<EOF
{
    "component": "worker",
    "job_id": "$JOB_ID",
    "instance_id": "$instance_id",
    "throughput": {
        "files_processed": $files,
        "records_processed": $records,
        "records_per_second": $rec_per_sec,
        "mb_per_second": $mb_per_sec,
        "files_per_second": 0
    },
    "duration": $duration,
    "source": "cloudwatch_logs"
}
EOF
        log_success "Worker metrics saved to $run_dir/worker_metrics.json"
    else
        log_warn "Could not fetch logs from CloudWatch"
        echo "{}" > "$run_dir/worker_metrics.json"
    fi
}

parse_worker_metrics() {
    local run_dir="$1"
    local metrics_file="$run_dir/worker_metrics.json"

    if [ ! -f "$metrics_file" ]; then
        return
    fi

    # Extract throughput metrics using jq if available, otherwise grep
    if command -v jq &> /dev/null; then
        WORKER_FILES_PROCESSED=$(jq -r '.throughput.files_processed // 0' "$metrics_file" 2>/dev/null || echo "0")
        WORKER_RECORDS_PROCESSED=$(jq -r '.throughput.records_processed // 0' "$metrics_file" 2>/dev/null || echo "0")
        WORKER_FILES_PER_SEC=$(jq -r '.throughput.files_per_second // 0' "$metrics_file" 2>/dev/null || echo "0")
        WORKER_RECORDS_PER_SEC=$(jq -r '.throughput.records_per_second // 0' "$metrics_file" 2>/dev/null || echo "0")
        WORKER_MB_PER_SEC=$(jq -r '.throughput.mb_per_second // 0' "$metrics_file" 2>/dev/null || echo "0")
        WORKER_BYTES_READ=$(jq -r '.throughput.bytes_read // "0 bytes"' "$metrics_file" 2>/dev/null || echo "0 bytes")
        WORKER_DURATION=$(jq -r '.duration // 0' "$metrics_file" 2>/dev/null || echo "0")
    else
        # Fallback to grep
        WORKER_FILES_PROCESSED=$(grep -oP '"files_processed":\s*\K\d+' "$metrics_file" 2>/dev/null | head -1 || echo "0")
        WORKER_RECORDS_PROCESSED=$(grep -oP '"records_processed":\s*\K\d+' "$metrics_file" 2>/dev/null | head -1 || echo "0")
        WORKER_FILES_PER_SEC=$(grep -oP '"files_per_second":\s*\K[\d.]+' "$metrics_file" 2>/dev/null | head -1 || echo "0")
        WORKER_RECORDS_PER_SEC=$(grep -oP '"records_per_second":\s*\K\d+' "$metrics_file" 2>/dev/null | head -1 || echo "0")
        WORKER_MB_PER_SEC=$(grep -oP '"mb_per_second":\s*\K[\d.]+' "$metrics_file" 2>/dev/null | head -1 || echo "0")
        WORKER_BYTES_READ=$(grep -oP '"bytes_read":\s*"\K[^"]+' "$metrics_file" 2>/dev/null | head -1 || echo "0 bytes")
        WORKER_DURATION=$(grep -oP '"duration":\s*\K\d+' "$metrics_file" 2>/dev/null | head -1 || echo "0")
    fi

    log_info "Parsed metrics: files=$WORKER_FILES_PROCESSED, records=$WORKER_RECORDS_PROCESSED, throughput=$WORKER_RECORDS_PER_SEC rec/s, $WORKER_MB_PER_SEC MB/s, duration=${WORKER_DURATION}s"
}

wait_for_completion() {
    local job_id="$1"
    local run_dir="$2"
    local start_time=$(date +%s)
    local poll_count=0

    log_info "Waiting for job completion (polling every ${POLL_INTERVAL}s, max ${MAX_WAIT_TIME}s)..."
    log_info "Using terraform instance IDs: discoverer=$DISCOVERER_INSTANCE_ID, worker=$WORKER_INSTANCE_ID"
    echo ""

    while true; do
        poll_count=$((poll_count + 1))
        local elapsed=$(($(date +%s) - start_time))

        if [ "$elapsed" -gt "$MAX_WAIT_TIME" ]; then
            log_error "Timeout waiting for job completion after ${elapsed}s"
            return 1
        fi

        echo "--- Poll #$poll_count (elapsed: ${elapsed}s) ---"

        # Check discoverer instance - use terraform ID if available, else query by tag
        local discoverer_id="$DISCOVERER_INSTANCE_ID"
        if [ "$discoverer_id" = "N/A" ] || [ -z "$discoverer_id" ]; then
            discoverer_id=$(get_instance_id "discoverer")
        fi
        local discoverer_status=$(get_instance_status "$discoverer_id")
        log_info "Discoverer: ID=$discoverer_id, Status=$discoverer_status"

        # Check worker instance - use terraform ID if available, else query by tag
        local worker_id="$WORKER_INSTANCE_ID"
        if [ "$worker_id" = "N/A" ] || [ -z "$worker_id" ]; then
            worker_id=$(get_instance_id "worker")
        fi
        local worker_status=$(get_instance_status "$worker_id")
        log_info "Worker: ID=$worker_id, Status=$worker_status"

        # Fetch CloudWatch logs from both discoverer and worker (every poll)
        log_info "Fetching CloudWatch logs..."
        fetch_cloudwatch_logs_realtime "$job_id" "discoverer" "$discoverer_id"
        fetch_cloudwatch_logs_realtime "$job_id" "worker" "$worker_id"

        # Fetch EC2 console output as fallback (every 3rd poll, before CloudWatch logs are available)
        if [ $((poll_count % 3)) -eq 1 ] && [ "$worker_id" != "None" ] && [ "$worker_id" != "N/A" ] && [ -n "$worker_id" ]; then
            local console_output=$(aws ec2 get-console-output --region "$AWS_REGION" --instance-id "$worker_id" --query 'Output' --output text 2>/dev/null | tail -50 || echo "")
            if [ -n "$console_output" ] && [ "$console_output" != "None" ]; then
                echo "--- Worker EC2 Console (bootstrap) ---"
                echo "$console_output" | tail -15
                echo "--- End EC2 Console ---"
            fi
        fi

        # Check SQS queue for messages - use terraform URL if available
        local queue_url="$SQS_QUEUE_URL"
        if [ "$queue_url" = "N/A" ] || [ -z "$queue_url" ]; then
            queue_url=$(aws sqs list-queues --region "$AWS_REGION" --queue-name-prefix "paraflow-$job_id" --query 'QueueUrls[0]' --output text 2>/dev/null || echo "")
        fi
        if [ -n "$queue_url" ] && [ "$queue_url" != "None" ] && [ "$queue_url" != "N/A" ]; then
            local visible=$(aws sqs get-queue-attributes --region "$AWS_REGION" --queue-url "$queue_url" --attribute-names ApproximateNumberOfMessages --query 'Attributes.ApproximateNumberOfMessages' --output text 2>/dev/null || echo "0")
            local in_flight=$(aws sqs get-queue-attributes --region "$AWS_REGION" --queue-url "$queue_url" --attribute-names ApproximateNumberOfMessagesNotVisible --query 'Attributes.ApproximateNumberOfMessagesNotVisible' --output text 2>/dev/null || echo "0")

            log_info "SQS Queue: $visible visible, $in_flight in-flight"

            # Save queue metrics
            echo "{\"timestamp\":\"$(date -u +"%Y-%m-%dT%H:%M:%SZ")\",\"visible\":$visible,\"in_flight\":$in_flight,\"elapsed\":$elapsed,\"worker_status\":\"$worker_status\",\"discoverer_status\":\"$discoverer_status\"}" >> "$run_dir/queue_metrics.jsonl"
        else
            log_info "SQS Queue: Not found yet (waiting for infrastructure)"
        fi

        # Check if worker instance has terminated/stopped
        if [ "$worker_status" = "terminated" ] || [ "$worker_status" = "stopped" ]; then
            echo ""
            log_info "Worker instance terminated/stopped - job likely complete"
            break
        fi

        # If queue is empty and worker not running after initial startup
        if [ "$visible" = "0" ] && [ "$in_flight" = "0" ] && [ "$worker_status" != "running" ] && [ "$elapsed" -gt 120 ]; then
            echo ""
            log_success "Queue empty and worker not running - job complete"
            break
        fi

        echo ""
        sleep "$POLL_INTERVAL"
    done

    local total_time=$(($(date +%s) - start_time))
    log_success "Job completed in ${total_time}s after $poll_count polls"

    # Set global variable for caller
    WAIT_TOTAL_TIME="$total_time"
}

collect_cloudwatch_metrics() {
    local job_id="$1"
    local run_dir="$2"
    local start_time="$3"
    local end_time="$4"

    log_info "Collecting CloudWatch metrics..."

    local namespace="Paraflow/$job_id"

    # Collect CPU metrics
    aws cloudwatch get-metric-statistics \
        --region "$AWS_REGION" \
        --namespace "$namespace" \
        --metric-name "cpu_usage_user" \
        --start-time "$start_time" \
        --end-time "$end_time" \
        --period 60 \
        --statistics Average Maximum \
        --output json > "$run_dir/cpu_metrics.json" 2>/dev/null || echo "{}" > "$run_dir/cpu_metrics.json"

    # Collect memory metrics
    aws cloudwatch get-metric-statistics \
        --region "$AWS_REGION" \
        --namespace "$namespace" \
        --metric-name "mem_used_percent" \
        --start-time "$start_time" \
        --end-time "$end_time" \
        --period 60 \
        --statistics Average Maximum \
        --output json > "$run_dir/memory_metrics.json" 2>/dev/null || echo "{}" > "$run_dir/memory_metrics.json"

    # Collect network metrics
    aws cloudwatch get-metric-statistics \
        --region "$AWS_REGION" \
        --namespace "$namespace" \
        --metric-name "bytes_recv" \
        --start-time "$start_time" \
        --end-time "$end_time" \
        --period 60 \
        --statistics Sum \
        --output json > "$run_dir/network_metrics.json" 2>/dev/null || echo "{}" > "$run_dir/network_metrics.json"

    log_success "CloudWatch metrics saved to $run_dir/"
}

collect_logs() {
    local job_id="$1"
    local run_dir="$2"

    log_info "Collecting CloudWatch logs..."

    local log_group="/paraflow/$job_id"

    # Get log streams
    aws logs describe-log-streams \
        --region "$AWS_REGION" \
        --log-group-name "$log_group" \
        --query 'logStreams[*].logStreamName' \
        --output json > "$run_dir/log_streams.json" 2>/dev/null || echo "[]" > "$run_dir/log_streams.json"

    # Download recent logs (last 4 hours)
    local end_time=$(($(date +%s) * 1000))
    local start_time=$((end_time - 14400000))  # 4 hours ago

    aws logs filter-log-events \
        --region "$AWS_REGION" \
        --log-group-name "$log_group" \
        --start-time "$start_time" \
        --end-time "$end_time" \
        --output json > "$run_dir/logs.json" 2>/dev/null || echo "{}" > "$run_dir/logs.json"

    log_success "Logs saved to $run_dir/"
}

generate_report() {
    local run_dir="$1"
    local job_id="$2"
    local instance_type="$3"
    local total_time="$4"

    log_info "Generating benchmark report..."

    local report_file="$run_dir/report.md"

    # Parse worker metrics if not already done
    if [ -z "$WORKER_FILES_PROCESSED" ]; then
        parse_worker_metrics "$run_dir"
    fi

    cat > "$report_file" <<EOF
# Paraflow Benchmark Report

## Configuration

| Parameter | Value |
|-----------|-------|
| Instance Type | $instance_type |
| Worker Threads | $WORKER_THREADS |
| Batch Size | $BATCH_SIZE |
| Max Files | $MAX_FILES |
| Job ID | $job_id |
| Timestamp | $TIMESTAMP |

## Throughput Results

| Metric | Value |
|--------|-------|
| Files Processed | ${WORKER_FILES_PROCESSED:-0} |
| Records Processed | ${WORKER_RECORDS_PROCESSED:-0} |
| Bytes Read | ${WORKER_BYTES_READ:-0 bytes} |
| Processing Duration | ${WORKER_DURATION:-0}s |
| **Files/sec** | **${WORKER_FILES_PER_SEC:-0}** |
| **Records/sec** | **${WORKER_RECORDS_PER_SEC:-0}** |
| **Read Throughput** | **${WORKER_MB_PER_SEC:-0} MB/s** |

## Timing

| Metric | Value |
|--------|-------|
| Total Execution Time | ${total_time}s |
| Bootstrap + Processing | ${total_time}s |

## Cost Analysis

EOF

    # Calculate approximate cost based on instance type
    # These are approximate on-demand prices for us-east-1
    local hourly_cost
    case "$instance_type" in
        t4g.small)   hourly_cost=0.0168 ;;
        t4g.medium)  hourly_cost=0.0336 ;;
        t4g.large)   hourly_cost=0.0672 ;;
        t4g.xlarge)  hourly_cost=0.1344 ;;
        c7g.medium)  hourly_cost=0.0363 ;;
        c7g.large)   hourly_cost=0.0725 ;;
        c7g.xlarge)  hourly_cost=0.145 ;;
        m7g.medium)  hourly_cost=0.0408 ;;
        m7g.large)   hourly_cost=0.0816 ;;
        m7g.xlarge)  hourly_cost=0.163 ;;
        *)           hourly_cost=0.05 ;;
    esac

    local hours=$(echo "scale=4; $total_time / 3600" | bc)
    local cost=$(echo "scale=6; $hours * $hourly_cost" | bc)

    # Calculate cost per million records
    local cost_per_million="N/A"
    if [ "${WORKER_RECORDS_PROCESSED:-0}" -gt 0 ]; then
        cost_per_million=$(echo "scale=6; ($cost / ${WORKER_RECORDS_PROCESSED}) * 1000000" | bc)
    fi

    cat >> "$report_file" <<EOF
| Instance Type | Hourly Cost | Run Time (hours) | Job Cost | Cost per 1M Records |
|---------------|-------------|------------------|----------|---------------------|
| $instance_type | \$$hourly_cost | $hours | \$$cost | \$$cost_per_million |

## Files

- \`worker_metrics.json\` - Worker throughput metrics from EC2 instance
- \`queue_metrics.jsonl\` - SQS queue metrics over time
- \`cpu_metrics.json\` - CloudWatch CPU metrics
- \`memory_metrics.json\` - CloudWatch memory metrics
- \`network_metrics.json\` - CloudWatch network metrics
- \`logs.json\` - CloudWatch log events

---
Generated by Paraflow Benchmark Runner
EOF

    log_success "Report generated: $report_file"
}

# ============================================================================
# Main Functions
# ============================================================================

deploy() {
    echo ""
    echo "============================================================================"
    echo " STEP 1: INFRASTRUCTURE DEPLOYMENT"
    echo "============================================================================"
    echo ""

    log_info "Deploying infrastructure with instance type: $INSTANCE_TYPE"
    log_info "Working directory: $TERRAFORM_DIR"

    cd "$TERRAFORM_DIR"

    # Create tfvars for this benchmark run
    local benchmark_tfvars="benchmark-${TIMESTAMP}.tfvars"

    log_info "Creating benchmark tfvars file: $benchmark_tfvars"
    cat > "$benchmark_tfvars" <<EOF
# Benchmark run: $TIMESTAMP
worker_instance_type       = "$INSTANCE_TYPE"
worker_threads             = $WORKER_THREADS
batch_size                 = $BATCH_SIZE
max_files                  = $MAX_FILES
enable_detailed_monitoring = true
benchmark_mode             = true
bootstrap_timeout_seconds  = 900
EOF

    log_info "Benchmark configuration:"
    log_info "  - worker_instance_type: $INSTANCE_TYPE"
    log_info "  - worker_threads: $WORKER_THREADS"
    log_info "  - batch_size: $BATCH_SIZE"
    log_info "  - max_files: $MAX_FILES"
    log_info "  - enable_detailed_monitoring: true"
    log_info "  - benchmark_mode: true"

    # Check for base tfvars
    if [ -f "example.tfvars" ]; then
        log_info "Using base config from example.tfvars"
    else
        log_warn "No example.tfvars found - using defaults only"
    fi

    # Initialize if needed
    if [ ! -d ".terraform" ]; then
        log_info "Terraform not initialized. Running terraform init..."
        terraform init
        log_success "Terraform initialized"
    else
        log_info "Terraform already initialized"
    fi

    # Plan and apply
    echo ""
    log_info "Running terraform plan..."
    log_info "Command: terraform plan -var-file=example.tfvars -var-file=$benchmark_tfvars -out=benchmark.tfplan"
    echo ""
    echo "--- Terraform Plan Output ---"
    terraform plan -var-file="example.tfvars" -var-file="$benchmark_tfvars" -out="benchmark.tfplan"
    echo "--- End Terraform Plan ---"
    echo ""
    log_success "Terraform plan complete"

    echo ""
    log_info "Applying Terraform deployment..."
    log_info "Command: terraform apply benchmark.tfplan"
    log_info "Resources being created (watch for each resource completion):"
    echo ""
    echo "--- Terraform Apply Output ---"
    terraform apply benchmark.tfplan
    echo "--- End Terraform Apply ---"
    echo ""
    log_success "Terraform apply complete"

    # Extract outputs
    echo ""
    log_info "Extracting Terraform outputs..."
    JOB_ID=$(terraform output -raw job_id 2>/dev/null || echo "benchmark-$TIMESTAMP")
    SQS_QUEUE_URL=$(terraform output -raw sqs_queue_url 2>/dev/null || echo "N/A")
    DISCOVERER_INSTANCE_ID=$(terraform output -raw discoverer_instance_id 2>/dev/null || echo "N/A")
    WORKER_INSTANCE_ID=$(terraform output -raw worker_instance_id 2>/dev/null || echo "N/A")
    local ecr_repository=$(terraform output -raw ecr_repository_url 2>/dev/null || echo "N/A")

    log_success "Infrastructure deployed successfully!"
    log_info "  Job ID: $JOB_ID"
    log_info "  SQS Queue: $SQS_QUEUE_URL"
    log_info "  ECR Repository: $ecr_repository"
    log_info "  Discoverer Instance: $DISCOVERER_INSTANCE_ID"
    log_info "  Worker Instance: $WORKER_INSTANCE_ID"

    # Cleanup plan file
    rm -f "benchmark.tfplan"
}

monitor() {
    local job_id="$1"
    local run_dir="$2"

    echo ""
    echo "============================================================================"
    echo " STEP 2: JOB MONITORING"
    echo "============================================================================"
    echo ""

    log_info "Monitoring job: $job_id"
    log_info "Results directory: $run_dir"
    log_info "Poll interval: ${POLL_INTERVAL}s"
    log_info "Max wait time: ${MAX_WAIT_TIME}s"

    local start_iso=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    log_info "Monitoring started at: $start_iso"

    echo ""
    log_info "Waiting for EC2 instances to start and complete processing..."
    wait_for_completion "$job_id" "$run_dir"
    local total_time="$WAIT_TOTAL_TIME"
    local end_iso=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

    log_info "Monitoring completed at: $end_iso"
    log_success "Total job execution time: ${total_time}s"

    echo ""
    echo "============================================================================"
    echo " STEP 3: METRICS COLLECTION"
    echo "============================================================================"
    echo ""

    # Fetch worker throughput metrics before instance terminates
    log_info "Fetching worker instance ID..."
    local worker_id="$WORKER_INSTANCE_ID"
    if [ "$worker_id" = "N/A" ] || [ -z "$worker_id" ]; then
        worker_id=$(get_instance_id "worker")
    fi
    log_info "Worker instance ID: $worker_id"

    log_info "Fetching benchmark metrics from worker instance..."
    fetch_benchmark_metrics "$worker_id" "$run_dir"

    log_info "Parsing worker metrics..."
    parse_worker_metrics "$run_dir"

    # Collect CloudWatch metrics
    log_info "Collecting CloudWatch metrics..."
    collect_cloudwatch_metrics "$job_id" "$run_dir" "$start_iso" "$end_iso"

    log_info "Collecting CloudWatch logs..."
    collect_logs "$job_id" "$run_dir"

    log_success "Metrics collection complete"

    # Set global variable for caller
    MONITOR_TOTAL_TIME="$total_time"
}

teardown() {
    local job_id="$1"

    echo ""
    echo "============================================================================"
    echo " STEP 5: INFRASTRUCTURE TEARDOWN"
    echo "============================================================================"
    echo ""

    log_info "Tearing down infrastructure for job: $job_id"
    log_info "Working directory: $TERRAFORM_DIR"

    cd "$TERRAFORM_DIR"

    log_info "Running terraform destroy..."
    log_info "Command: terraform destroy -var-file=example.tfvars -auto-approve"
    log_info "Resources being destroyed (watch for each resource removal):"
    echo ""
    echo "--- Terraform Destroy Output ---"
    terraform destroy -var-file="example.tfvars" -auto-approve
    echo "--- End Terraform Destroy ---"
    echo ""

    log_success "Infrastructure destroyed successfully"
}

run_benchmark() {
    echo ""
    echo "############################################################################"
    echo "#                                                                          #"
    echo "#                    PARAFLOW BENCHMARK RUNNER                             #"
    echo "#                                                                          #"
    echo "############################################################################"
    echo ""

    log_info "Starting benchmark run at $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    echo ""
    log_info "BENCHMARK CONFIGURATION:"
    log_info "  Instance Type:   $INSTANCE_TYPE"
    log_info "  Worker Threads:  $WORKER_THREADS"
    log_info "  Batch Size:      $BATCH_SIZE"
    log_info "  Max Files:       $MAX_FILES"
    log_info "  Poll Interval:   ${POLL_INTERVAL}s"
    log_info "  Max Wait Time:   ${MAX_WAIT_TIME}s"
    log_info "  Skip Teardown:   ${SKIP_TEARDOWN:-false}"

    # Setup
    local run_dir=$(setup_results_dir)
    echo ""
    log_info "Results directory: $run_dir"

    # Deploy - run in current shell to show output, capture job_id via file
    deploy
    local job_id="$JOB_ID"

    # Save configuration
    log_info "Saving benchmark configuration to $run_dir/config.json"
    cat > "$run_dir/config.json" <<EOF
{
    "instance_type": "$INSTANCE_TYPE",
    "worker_threads": $WORKER_THREADS,
    "batch_size": $BATCH_SIZE,
    "max_files": $MAX_FILES,
    "job_id": "$job_id",
    "timestamp": "$TIMESTAMP",
    "poll_interval": $POLL_INTERVAL,
    "max_wait_time": $MAX_WAIT_TIME
}
EOF

    # Monitor - run in current shell to show output
    monitor "$job_id" "$run_dir"
    local total_time="$MONITOR_TOTAL_TIME"

    # Generate report
    echo ""
    echo "============================================================================"
    echo " STEP 4: REPORT GENERATION"
    echo "============================================================================"
    echo ""

    generate_report "$run_dir" "$job_id" "$INSTANCE_TYPE" "$total_time"

    # Output summary CSV line for orchestrator (to stdout)
    echo ""
    log_info "BENCHMARK SUMMARY:"
    log_info "  Instance Type:      $INSTANCE_TYPE"
    log_info "  Total Duration:     ${total_time}s"
    log_info "  Files Processed:    ${WORKER_FILES_PROCESSED:-0}"
    log_info "  Records Processed:  ${WORKER_RECORDS_PROCESSED:-0}"
    log_info "  Files/sec:          ${WORKER_FILES_PER_SEC:-0}"
    log_info "  Records/sec:        ${WORKER_RECORDS_PER_SEC:-0}"
    log_info "  Throughput:         ${WORKER_MB_PER_SEC:-0} MB/s"

    # Format: instance_type,duration,files_processed,records_processed,files_per_sec,records_per_sec,mb_per_sec
    echo ""
    echo "BENCHMARK_RESULT:$INSTANCE_TYPE,$total_time,${WORKER_FILES_PROCESSED:-0},${WORKER_RECORDS_PROCESSED:-0},${WORKER_FILES_PER_SEC:-0},${WORKER_RECORDS_PER_SEC:-0},${WORKER_MB_PER_SEC:-0}"

    # Teardown (optional - can skip with --no-teardown)
    if [ "$SKIP_TEARDOWN" != "true" ]; then
        teardown "$job_id"
    else
        echo ""
        log_warn "Skipping teardown (--no-teardown specified)"
        log_warn "Remember to manually run: $0 teardown --job-id $job_id"
    fi

    echo ""
    echo "############################################################################"
    echo "#                                                                          #"
    echo "#                    BENCHMARK COMPLETE                                    #"
    echo "#                                                                          #"
    echo "############################################################################"
    echo ""

    log_success "Benchmark complete! Results saved to: $run_dir"
    log_info "View report: cat $run_dir/report.md"
}

# ============================================================================
# CLI
# ============================================================================

usage() {
    cat <<EOF
Paraflow Benchmark Runner

Usage: $0 [command] [options]

Commands:
    run         Run a complete benchmark (deploy, monitor, collect, teardown)
    deploy      Deploy infrastructure only
    monitor     Monitor running job and collect metrics
    teardown    Destroy infrastructure
    help        Show this help message

Options:
    --instance-type TYPE    Worker instance type (default: t4g.medium)
    --threads NUM           Worker threads (default: 4)
    --batch-size NUM        Batch size (default: 10000)
    --max-files NUM         Max files to process (default: 100)
    --no-teardown           Skip infrastructure teardown after benchmark
    --results-dir DIR       Directory for results (default: /tmp/paraflow-benchmarks)

Examples:
    # Run benchmark with c7g.large instance
    $0 run --instance-type c7g.large --threads 8

    # Run benchmark without teardown (for debugging)
    $0 run --instance-type t4g.medium --no-teardown

    # Deploy only
    $0 deploy --instance-type c7g.xlarge

Environment Variables:
    INSTANCE_TYPE       Worker instance type
    WORKER_THREADS      Number of worker threads
    BATCH_SIZE          Batch size for processing
    MAX_FILES           Maximum files to process
    RESULTS_DIR         Results directory
    JOB_ID              Job ID (for monitor/teardown commands)
EOF
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --instance-type)
                INSTANCE_TYPE="$2"
                shift 2
                ;;
            --threads)
                WORKER_THREADS="$2"
                shift 2
                ;;
            --batch-size)
                BATCH_SIZE="$2"
                shift 2
                ;;
            --max-files)
                MAX_FILES="$2"
                shift 2
                ;;
            --no-teardown)
                SKIP_TEARDOWN="true"
                shift
                ;;
            --results-dir)
                RESULTS_DIR="$2"
                shift 2
                ;;
            --job-id)
                JOB_ID="$2"
                shift 2
                ;;
            *)
                shift
                ;;
        esac
    done
}

main() {
    local command="${1:-help}"
    shift || true

    parse_args "$@"

    case "$command" in
        run)
            run_benchmark
            ;;
        deploy)
            deploy
            ;;
        monitor)
            if [ -z "$JOB_ID" ]; then
                log_error "Job ID required. Use --job-id or JOB_ID env var"
                exit 1
            fi
            local run_dir=$(setup_results_dir)
            monitor "$JOB_ID" "$run_dir"
            ;;
        teardown)
            if [ -z "$JOB_ID" ]; then
                log_error "Job ID required. Use --job-id or JOB_ID env var"
                exit 1
            fi
            teardown "$JOB_ID"
            ;;
        help|--help|-h)
            usage
            ;;
        *)
            log_error "Unknown command: $command"
            usage
            exit 1
            ;;
    esac
}

main "$@"
