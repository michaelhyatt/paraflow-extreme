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
    aws ec2 describe-instances \
        --filters "Name=tag:JobId,Values=$JOB_ID" "Name=tag:Component,Values=$component" "Name=instance-state-name,Values=running,pending" \
        --query 'Reservations[0].Instances[0].InstanceId' \
        --output text 2>/dev/null || echo "None"
}

get_instance_status() {
    local instance_id="$1"
    if [ "$instance_id" = "None" ] || [ -z "$instance_id" ]; then
        echo "not_found"
        return
    fi
    aws ec2 describe-instance-status \
        --instance-ids "$instance_id" \
        --query 'InstanceStatuses[0].InstanceState.Name' \
        --output text 2>/dev/null || echo "unknown"
}

fetch_bootstrap_status() {
    local instance_id="$1"
    if [ "$instance_id" = "None" ] || [ -z "$instance_id" ]; then
        echo "{}"
        return
    fi

    # Try to fetch via SSM
    aws ssm send-command \
        --instance-ids "$instance_id" \
        --document-name "AWS-RunShellScript" \
        --parameters 'commands=["cat /var/log/paraflow-bootstrap-status 2>/dev/null || echo {}"]' \
        --output-s3-bucket-name "" \
        --query 'Command.CommandId' \
        --output text 2>/dev/null || echo ""
}

fetch_benchmark_metrics() {
    local instance_id="$1"
    local run_dir="$2"

    if [ "$instance_id" = "None" ] || [ -z "$instance_id" ]; then
        log_warn "No instance ID provided for metrics fetch"
        return
    fi

    log_info "Fetching benchmark metrics from instance $instance_id..."

    # Use SSM to retrieve the benchmark metrics file
    local command_id=$(aws ssm send-command \
        --instance-ids "$instance_id" \
        --document-name "AWS-RunShellScript" \
        --parameters 'commands=["cat /var/log/benchmark-metrics.json 2>/dev/null || echo {}"]' \
        --query 'Command.CommandId' \
        --output text 2>/dev/null || echo "")

    if [ -n "$command_id" ] && [ "$command_id" != "None" ]; then
        # Wait for command to complete
        sleep 5

        # Get command output
        aws ssm get-command-invocation \
            --command-id "$command_id" \
            --instance-id "$instance_id" \
            --query 'StandardOutputContent' \
            --output text 2>/dev/null > "$run_dir/worker_metrics.json" || echo "{}" > "$run_dir/worker_metrics.json"

        log_success "Worker metrics saved to $run_dir/worker_metrics.json"
    else
        log_warn "Failed to send SSM command to fetch metrics"
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
    else
        # Fallback to grep
        WORKER_FILES_PROCESSED=$(grep -oP '"files_processed":\s*\K\d+' "$metrics_file" 2>/dev/null | head -1 || echo "0")
        WORKER_RECORDS_PROCESSED=$(grep -oP '"records_processed":\s*\K\d+' "$metrics_file" 2>/dev/null | head -1 || echo "0")
        WORKER_FILES_PER_SEC=$(grep -oP '"files_per_second":\s*\K[\d.]+' "$metrics_file" 2>/dev/null | head -1 || echo "0")
        WORKER_RECORDS_PER_SEC=$(grep -oP '"records_per_second":\s*\K\d+' "$metrics_file" 2>/dev/null | head -1 || echo "0")
        WORKER_MB_PER_SEC=$(grep -oP '"mb_per_second":\s*\K[\d.]+' "$metrics_file" 2>/dev/null | head -1 || echo "0")
        WORKER_BYTES_READ=$(grep -oP '"bytes_read":\s*"\K[^"]+' "$metrics_file" 2>/dev/null | head -1 || echo "0 bytes")
    fi

    log_info "Parsed metrics: files=$WORKER_FILES_PROCESSED, records=$WORKER_RECORDS_PROCESSED, throughput=$WORKER_RECORDS_PER_SEC rec/s, $WORKER_MB_PER_SEC MB/s"
}

wait_for_completion() {
    local job_id="$1"
    local run_dir="$2"
    local start_time=$(date +%s)

    log_info "Waiting for job completion (polling every ${POLL_INTERVAL}s, max ${MAX_WAIT_TIME}s)..."

    while true; do
        local elapsed=$(($(date +%s) - start_time))
        if [ "$elapsed" -gt "$MAX_WAIT_TIME" ]; then
            log_error "Timeout waiting for job completion after ${elapsed}s"
            return 1
        fi

        # Check SQS queue for messages
        local queue_url=$(aws sqs list-queues --queue-name-prefix "paraflow-$job_id" --query 'QueueUrls[0]' --output text 2>/dev/null || echo "")
        if [ -n "$queue_url" ] && [ "$queue_url" != "None" ]; then
            local visible=$(aws sqs get-queue-attributes --queue-url "$queue_url" --attribute-names ApproximateNumberOfMessages --query 'Attributes.ApproximateNumberOfMessages' --output text 2>/dev/null || echo "0")
            local in_flight=$(aws sqs get-queue-attributes --queue-url "$queue_url" --attribute-names ApproximateNumberOfMessagesNotVisible --query 'Attributes.ApproximateNumberOfMessagesNotVisible' --output text 2>/dev/null || echo "0")

            log_info "Queue status: $visible visible, $in_flight in-flight (elapsed: ${elapsed}s)"

            # Save queue metrics
            echo "{\"timestamp\":\"$(date -u +"%Y-%m-%dT%H:%M:%SZ")\",\"visible\":$visible,\"in_flight\":$in_flight,\"elapsed\":$elapsed}" >> "$run_dir/queue_metrics.jsonl"
        fi

        # Check if worker instance is still running
        local worker_id=$(get_instance_id "worker")
        local worker_status=$(get_instance_status "$worker_id")

        if [ "$worker_status" = "terminated" ] || [ "$worker_status" = "stopped" ]; then
            log_info "Worker instance terminated/stopped - job likely complete"
            break
        fi

        # If no messages and worker finished
        if [ "$visible" = "0" ] && [ "$in_flight" = "0" ] && [ "$worker_status" != "running" ] && [ "$elapsed" -gt 120 ]; then
            log_success "Queue empty and worker not running - job complete"
            break
        fi

        sleep "$POLL_INTERVAL"
    done

    local total_time=$(($(date +%s) - start_time))
    log_success "Job completed in ${total_time}s"
    echo "$total_time"
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
        --namespace "$namespace" \
        --metric-name "cpu_usage_user" \
        --start-time "$start_time" \
        --end-time "$end_time" \
        --period 60 \
        --statistics Average Maximum \
        --output json > "$run_dir/cpu_metrics.json" 2>/dev/null || echo "{}" > "$run_dir/cpu_metrics.json"

    # Collect memory metrics
    aws cloudwatch get-metric-statistics \
        --namespace "$namespace" \
        --metric-name "mem_used_percent" \
        --start-time "$start_time" \
        --end-time "$end_time" \
        --period 60 \
        --statistics Average Maximum \
        --output json > "$run_dir/memory_metrics.json" 2>/dev/null || echo "{}" > "$run_dir/memory_metrics.json"

    # Collect network metrics
    aws cloudwatch get-metric-statistics \
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
        --log-group-name "$log_group" \
        --query 'logStreams[*].logStreamName' \
        --output json > "$run_dir/log_streams.json" 2>/dev/null || echo "[]" > "$run_dir/log_streams.json"

    # Download recent logs (last 4 hours)
    local end_time=$(($(date +%s) * 1000))
    local start_time=$((end_time - 14400000))  # 4 hours ago

    aws logs filter-log-events \
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
    log_info "Deploying infrastructure with instance type: $INSTANCE_TYPE"

    cd "$TERRAFORM_DIR"

    # Create tfvars for this benchmark run
    local benchmark_tfvars="benchmark-${TIMESTAMP}.tfvars"

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

    log_info "Created $benchmark_tfvars"

    # Initialize if needed
    if [ ! -d ".terraform" ]; then
        log_info "Initializing Terraform..."
        terraform init
    fi

    # Plan and apply
    log_info "Planning deployment..."
    terraform plan -var-file="$benchmark_tfvars" -out="benchmark.tfplan"

    log_info "Applying deployment..."
    terraform apply "benchmark.tfplan"

    # Extract job_id from terraform output
    JOB_ID=$(terraform output -raw job_id 2>/dev/null || echo "benchmark-$TIMESTAMP")

    log_success "Infrastructure deployed. Job ID: $JOB_ID"

    # Cleanup plan file
    rm -f "benchmark.tfplan"

    echo "$JOB_ID"
}

monitor() {
    local job_id="$1"
    local run_dir="$2"

    log_info "Monitoring job: $job_id"

    local start_iso=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    local total_time=$(wait_for_completion "$job_id" "$run_dir")
    local end_iso=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

    # Fetch worker throughput metrics before instance terminates
    local worker_id=$(get_instance_id "worker")
    fetch_benchmark_metrics "$worker_id" "$run_dir"
    parse_worker_metrics "$run_dir"

    # Collect CloudWatch metrics
    collect_cloudwatch_metrics "$job_id" "$run_dir" "$start_iso" "$end_iso"
    collect_logs "$job_id" "$run_dir"

    echo "$total_time"
}

teardown() {
    local job_id="$1"

    log_info "Tearing down infrastructure for job: $job_id"

    cd "$TERRAFORM_DIR"

    terraform destroy -auto-approve

    log_success "Infrastructure destroyed"
}

run_benchmark() {
    log_info "Starting benchmark run"
    log_info "Instance Type: $INSTANCE_TYPE"
    log_info "Worker Threads: $WORKER_THREADS"
    log_info "Batch Size: $BATCH_SIZE"
    log_info "Max Files: $MAX_FILES"

    # Setup
    local run_dir=$(setup_results_dir)
    log_info "Results will be saved to: $run_dir"

    # Deploy
    local job_id=$(deploy)

    # Save configuration
    cat > "$run_dir/config.json" <<EOF
{
    "instance_type": "$INSTANCE_TYPE",
    "worker_threads": $WORKER_THREADS,
    "batch_size": $BATCH_SIZE,
    "max_files": $MAX_FILES,
    "job_id": "$job_id",
    "timestamp": "$TIMESTAMP"
}
EOF

    # Monitor
    local total_time=$(monitor "$job_id" "$run_dir")

    # Generate report
    generate_report "$run_dir" "$job_id" "$INSTANCE_TYPE" "$total_time"

    # Output summary CSV line for orchestrator (to stdout)
    # Format: instance_type,duration,files_processed,records_processed,files_per_sec,records_per_sec,mb_per_sec
    echo "BENCHMARK_RESULT:$INSTANCE_TYPE,$total_time,${WORKER_FILES_PROCESSED:-0},${WORKER_RECORDS_PROCESSED:-0},${WORKER_FILES_PER_SEC:-0},${WORKER_RECORDS_PER_SEC:-0},${WORKER_MB_PER_SEC:-0}"

    # Teardown (optional - can skip with --no-teardown)
    if [ "$SKIP_TEARDOWN" != "true" ]; then
        teardown "$job_id"
    else
        log_warn "Skipping teardown (--no-teardown specified)"
    fi

    log_success "Benchmark complete! Results in: $run_dir"
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
