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
export AWS_PAGER=""  # Disable pager to prevent hangs

# Preserve AWS_PROFILE if set (for SSO or named profiles)
# AWS CLI automatically uses AWS_PROFILE when exported - no need for --profile flag
if [ -n "$AWS_PROFILE" ]; then
    export AWS_PROFILE
    echo "Using AWS profile: $AWS_PROFILE"
else
    echo "WARNING: AWS_PROFILE is not set. AWS CLI commands may fail."
    echo "Set it with: export AWS_PROFILE=your-profile-name"
fi

# Verify AWS credentials are working for AWS CLI (not just terraform)
# This is critical - terraform may use different credential sources than AWS CLI
AWS_IDENTITY=$(aws sts get-caller-identity --no-cli-pager --output json 2>&1)
AWS_EXIT_CODE=$?

if [ $AWS_EXIT_CODE -ne 0 ]; then
    echo ""
    echo "=========================================="
    echo "ERROR: AWS CLI credentials not configured!"
    echo "=========================================="
    echo ""
    echo "Error: $AWS_IDENTITY"
    echo ""
    echo "Please set AWS_PROFILE before running this script:"
    echo ""
    echo "  export AWS_PROFILE=your-profile-name"
    echo "  ./benchmark-runner.sh run"
    echo ""
    echo "Or run with the profile inline:"
    echo ""
    echo "  AWS_PROFILE=your-profile-name ./benchmark-runner.sh run"
    echo ""
    echo "Note: Terraform may use different credentials than AWS CLI."
    echo "      Make sure AWS_PROFILE is exported in your shell."
    echo ""
    exit 1
fi

# Parse credentials - try jq first, fall back to grep
if command -v jq &> /dev/null; then
    AWS_ACCOUNT=$(echo "$AWS_IDENTITY" | jq -r '.Account // "unknown"' 2>/dev/null)
    AWS_ARN=$(echo "$AWS_IDENTITY" | jq -r '.Arn // "unknown"' 2>/dev/null)
else
    # Fallback to grep - handle both macOS and Linux
    AWS_ACCOUNT=$(echo "$AWS_IDENTITY" | grep -o '"Account"[^,}]*' | sed 's/.*: *"\([^"]*\)".*/\1/' 2>/dev/null || echo "unknown")
    AWS_ARN=$(echo "$AWS_IDENTITY" | grep -o '"Arn"[^,}]*' | sed 's/.*: *"\([^"]*\)".*/\1/' 2>/dev/null || echo "unknown")
fi

# Verify we actually got valid credentials
if [ "$AWS_ACCOUNT" = "unknown" ] || [ -z "$AWS_ACCOUNT" ] || [ "$AWS_ACCOUNT" = "null" ]; then
    echo ""
    echo "=========================================="
    echo "ERROR: Failed to parse AWS credentials!"
    echo "=========================================="
    echo ""
    echo "AWS CLI returned:"
    echo "$AWS_IDENTITY"
    echo ""
    echo "Please ensure AWS_PROFILE is set correctly:"
    echo ""
    echo "  export AWS_PROFILE=your-profile-name"
    echo "  ./benchmark-runner.sh run"
    echo ""
    exit 1
fi

echo "AWS CLI credentials verified:"
echo "  Account: $AWS_ACCOUNT"
echo "  Identity: $AWS_ARN"

# Default configuration
INSTANCE_TYPE="${INSTANCE_TYPE:-t4g.medium}"
WORKER_COUNT="${WORKER_COUNT:-1}"  # Number of worker instances (for horizontal scaling)
WORKER_THREADS="${WORKER_THREADS:-0}"  # 0 = auto-detect from CPU cores
BATCH_SIZE="${BATCH_SIZE:-10000}"
# MAX_FILES: if not set, read from example.tfvars (default to 100 if not found)
if [ -z "$MAX_FILES" ]; then
    MAX_FILES=$(grep -E '^max_files\s*=' "$TERRAFORM_DIR/example.tfvars" 2>/dev/null | sed 's/.*=\s*//' | tr -d ' ' || echo "100")
fi
MAX_FILES="${MAX_FILES:-100}"
POLL_INTERVAL="${POLL_INTERVAL:-10}"  # seconds
MAX_WAIT_TIME="${MAX_WAIT_TIME:-1800}"  # 30 minutes max (sufficient for 20k files)

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
    # AWS CLI uses exported AWS_PROFILE automatically
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
    # AWS CLI uses exported AWS_PROFILE automatically
    local status
    local err_output
    local exit_code=0

    err_output=$(aws ec2 describe-instances \
        --region "$AWS_REGION" \
        --instance-ids "$instance_id" \
        --query 'Reservations[0].Instances[0].State.Name' \
        --output text --no-cli-pager 2>&1) || exit_code=$?

    if [ $exit_code -ne 0 ]; then
        # Check if it's a credentials error
        if echo "$err_output" | grep -qi "credentials\|expired\|unauthorized\|accessdenied"; then
            echo "ERROR:credentials"
        else
            echo "ERROR:$err_output"
        fi
        return
    fi

    # Handle None or empty response
    if [ "$err_output" = "None" ] || [ -z "$err_output" ]; then
        echo "pending"  # Likely still initializing
    else
        echo "$err_output"
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

    # AWS CLI uses exported AWS_PROFILE automatically
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

    # AWS CLI uses exported AWS_PROFILE automatically
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
        # Try to find the benchmark metrics from the logs
        # Worker outputs: "Metrics: files=X records=Y throughput=Z rec/s W MB/s"
        # Use sed instead of grep -P for macOS compatibility
        local files=$(echo "$logs" | sed -n 's/.*files=\([0-9]*\).*/\1/p' | tail -1)
        local records=$(echo "$logs" | sed -n 's/.*records=\([0-9,]*\).*/\1/p' | tail -1 | tr -d ',')
        local rec_per_sec=$(echo "$logs" | sed -n 's/.*throughput=\([0-9,]*\).*/\1/p' | tail -1 | tr -d ',')
        # MB/s pattern: "rec/s X MB/s" - extract number before " MB/s"
        local mb_per_sec=$(echo "$logs" | sed -n 's/.*rec\/s[[:space:]]*\([0-9.]*\)[[:space:]]*MB\/s.*/\1/p' | tail -1)

        # Set defaults if empty
        files="${files:-0}"
        records="${records:-0}"
        rec_per_sec="${rec_per_sec:-0}"
        mb_per_sec="${mb_per_sec:-0}"

        # Also try to extract from JSON format in logs (benchmark_mode=true)
        if [ "$files" = "0" ]; then
            files=$(echo "$logs" | sed -n 's/.*"files":[[:space:]]*\([0-9]*\).*/\1/p' | tail -1)
            files="${files:-0}"
        fi
        if [ "$records" = "0" ]; then
            records=$(echo "$logs" | sed -n 's/.*"records":[[:space:]]*\([0-9]*\).*/\1/p' | tail -1)
            records="${records:-0}"
        fi
        if [ "$rec_per_sec" = "0" ]; then
            rec_per_sec=$(echo "$logs" | sed -n 's/.*"rec_per_sec":[[:space:]]*\([0-9]*\).*/\1/p' | tail -1)
            rec_per_sec="${rec_per_sec:-0}"
        fi
        if [ "$mb_per_sec" = "0" ]; then
            mb_per_sec=$(echo "$logs" | sed -n 's/.*"mb_per_sec":[[:space:]]*\([0-9.]*\).*/\1/p' | tail -1)
            mb_per_sec="${mb_per_sec:-0}"
        fi

        # Extract duration from "Worker Complete (Xs, exit=0)" message
        local duration=$(echo "$logs" | sed -n 's/.*Worker Complete (\([0-9]*\)s.*/\1/p' | tail -1)
        duration="${duration:-0}"

        # Calculate derived metrics
        local files_per_sec=0
        local bytes_read="0 bytes"
        # Ensure numeric values for comparison (default to 0 if empty/invalid)
        local files_num=${files:-0}
        local duration_num=${duration:-0}
        if [ "$duration_num" -gt 0 ] 2>/dev/null && [ "$files_num" -gt 0 ] 2>/dev/null; then
            files_per_sec=$(echo "scale=2; $files_num / $duration_num" | bc 2>/dev/null || echo "0")
        fi
        if [ "$duration" -gt 0 ] && [ "$mb_per_sec" != "0" ]; then
            # Estimate total bytes read: MB/s * duration * 1024 * 1024
            local total_mb=$(echo "scale=2; $mb_per_sec * $duration" | bc 2>/dev/null || echo "0")
            if [ "$total_mb" != "0" ]; then
                if [ "$(echo "$total_mb >= 1024" | bc 2>/dev/null)" = "1" ]; then
                    local total_gb=$(echo "scale=2; $total_mb / 1024" | bc 2>/dev/null || echo "0")
                    bytes_read="${total_gb} GB"
                else
                    bytes_read="${total_mb} MB"
                fi
            fi
        fi

        log_info "Extracted metrics: files=$files, records=$records, $rec_per_sec rec/s, $mb_per_sec MB/s, duration=${duration}s, files_per_sec=$files_per_sec"

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
        "files_per_second": $files_per_sec,
        "bytes_read": "$bytes_read"
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

# Fetch metrics from all workers and aggregate them
fetch_all_worker_metrics() {
    local run_dir="$1"
    local worker_ids="$2"  # Space-separated list of worker instance IDs
    local worker_count="${3:-1}"

    log_info "Fetching metrics from $worker_count worker(s)..."

    # Initialize aggregated metrics
    local total_files=0
    local total_records=0
    local total_mb_per_sec=0
    local total_rec_per_sec=0
    local max_duration=0
    local workers_with_metrics=0

    # Create workers directory for individual metrics
    mkdir -p "$run_dir/workers"

    local worker_index=0
    for instance_id in $worker_ids; do
        worker_index=$((worker_index + 1))
        if [ -z "$instance_id" ] || [ "$instance_id" = "None" ]; then
            continue
        fi

        log_info "Fetching metrics from worker $worker_index (instance: $instance_id)..."

        local log_group="/paraflow/jobs/$JOB_ID"
        local log_stream="${instance_id}/worker/user-data"

        # Fetch recent log events
        local logs=$(aws logs get-log-events \
            --region "$AWS_REGION" \
            --log-group-name "$log_group" \
            --log-stream-name "$log_stream" \
            --limit 100 \
            --query 'events[*].message' \
            --output text 2>/dev/null || echo "")

        if [ -n "$logs" ] && [ "$logs" != "None" ]; then
            # Extract metrics from this worker
            local files=$(echo "$logs" | sed -n 's/.*files=\([0-9]*\).*/\1/p' | tail -1)
            local records=$(echo "$logs" | sed -n 's/.*records=\([0-9,]*\).*/\1/p' | tail -1 | tr -d ',')
            local rec_per_sec=$(echo "$logs" | sed -n 's/.*throughput=\([0-9,]*\).*/\1/p' | tail -1 | tr -d ',')
            local mb_per_sec=$(echo "$logs" | sed -n 's/.*rec\/s[[:space:]]*\([0-9.]*\)[[:space:]]*MB\/s.*/\1/p' | tail -1)
            local duration=$(echo "$logs" | sed -n 's/.*Worker Complete (\([0-9]*\)s.*/\1/p' | tail -1)

            # Set defaults
            files="${files:-0}"
            records="${records:-0}"
            rec_per_sec="${rec_per_sec:-0}"
            mb_per_sec="${mb_per_sec:-0}"
            duration="${duration:-0}"

            log_info "  Worker $worker_index: files=$files, records=$records, $rec_per_sec rec/s, $mb_per_sec MB/s, ${duration}s"

            # Save individual worker metrics
            cat > "$run_dir/workers/worker_${worker_index}_metrics.json" <<EOF
{
    "worker_index": $worker_index,
    "instance_id": "$instance_id",
    "files_processed": $files,
    "records_processed": $records,
    "records_per_second": $rec_per_sec,
    "mb_per_second": $mb_per_sec,
    "duration": $duration
}
EOF

            # Aggregate metrics
            if [ "$files" -gt 0 ] 2>/dev/null || [ "$records" -gt 0 ] 2>/dev/null; then
                workers_with_metrics=$((workers_with_metrics + 1))
                total_files=$((total_files + files))
                total_records=$((total_records + records))
                # Sum throughput (workers process in parallel)
                total_rec_per_sec=$(echo "$total_rec_per_sec + $rec_per_sec" | bc 2>/dev/null || echo "$total_rec_per_sec")
                total_mb_per_sec=$(echo "$total_mb_per_sec + $mb_per_sec" | bc 2>/dev/null || echo "$total_mb_per_sec")
                # Track max duration (job completes when all workers finish)
                if [ "$duration" -gt "$max_duration" ] 2>/dev/null; then
                    max_duration="$duration"
                fi
            fi
        else
            log_warn "  Worker $worker_index: Could not fetch logs"
        fi
    done

    # Calculate derived metrics
    local files_per_sec=0
    local bytes_read="0 bytes"
    if [ "$max_duration" -gt 0 ] 2>/dev/null && [ "$total_files" -gt 0 ] 2>/dev/null; then
        files_per_sec=$(echo "scale=2; $total_files / $max_duration" | bc 2>/dev/null || echo "0")
    fi
    if [ "$max_duration" -gt 0 ] && [ "$total_mb_per_sec" != "0" ]; then
        local total_mb=$(echo "scale=2; $total_mb_per_sec * $max_duration" | bc 2>/dev/null || echo "0")
        if [ "$total_mb" != "0" ]; then
            if [ "$(echo "$total_mb >= 1024" | bc 2>/dev/null)" = "1" ]; then
                local total_gb=$(echo "scale=2; $total_mb / 1024" | bc 2>/dev/null || echo "0")
                bytes_read="${total_gb} GB"
            else
                bytes_read="${total_mb} MB"
            fi
        fi
    fi

    log_info "Aggregated metrics from $workers_with_metrics worker(s):"
    log_info "  Total files: $total_files"
    log_info "  Total records: $total_records"
    log_info "  Combined throughput: $total_rec_per_sec rec/s, $total_mb_per_sec MB/s"
    log_info "  Max duration: ${max_duration}s"

    # Save aggregated metrics
    cat > "$run_dir/worker_metrics.json" <<EOF
{
    "component": "worker",
    "job_id": "$JOB_ID",
    "worker_count": $worker_count,
    "workers_reporting": $workers_with_metrics,
    "throughput": {
        "files_processed": $total_files,
        "records_processed": $total_records,
        "records_per_second": $total_rec_per_sec,
        "mb_per_second": $total_mb_per_sec,
        "files_per_second": $files_per_sec,
        "bytes_read": "$bytes_read"
    },
    "duration": $max_duration,
    "source": "cloudwatch_logs_aggregated"
}
EOF

    log_success "Aggregated worker metrics saved to $run_dir/worker_metrics.json"
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
        # Fallback to sed (macOS compatible)
        WORKER_FILES_PROCESSED=$(sed -n 's/.*"files_processed":[[:space:]]*\([0-9]*\).*/\1/p' "$metrics_file" 2>/dev/null | head -1)
        WORKER_FILES_PROCESSED="${WORKER_FILES_PROCESSED:-0}"
        WORKER_RECORDS_PROCESSED=$(sed -n 's/.*"records_processed":[[:space:]]*\([0-9]*\).*/\1/p' "$metrics_file" 2>/dev/null | head -1)
        WORKER_RECORDS_PROCESSED="${WORKER_RECORDS_PROCESSED:-0}"
        WORKER_FILES_PER_SEC=$(sed -n 's/.*"files_per_second":[[:space:]]*\([0-9.]*\).*/\1/p' "$metrics_file" 2>/dev/null | head -1)
        WORKER_FILES_PER_SEC="${WORKER_FILES_PER_SEC:-0}"
        WORKER_RECORDS_PER_SEC=$(sed -n 's/.*"records_per_second":[[:space:]]*\([0-9]*\).*/\1/p' "$metrics_file" 2>/dev/null | head -1)
        WORKER_RECORDS_PER_SEC="${WORKER_RECORDS_PER_SEC:-0}"
        WORKER_MB_PER_SEC=$(sed -n 's/.*"mb_per_second":[[:space:]]*\([0-9.]*\).*/\1/p' "$metrics_file" 2>/dev/null | head -1)
        WORKER_MB_PER_SEC="${WORKER_MB_PER_SEC:-0}"
        WORKER_BYTES_READ=$(sed -n 's/.*"bytes_read":[[:space:]]*"\([^"]*\)".*/\1/p' "$metrics_file" 2>/dev/null | head -1)
        WORKER_BYTES_READ="${WORKER_BYTES_READ:-0 bytes}"
        WORKER_DURATION=$(sed -n 's/.*"duration":[[:space:]]*\([0-9]*\).*/\1/p' "$metrics_file" 2>/dev/null | head -1)
        WORKER_DURATION="${WORKER_DURATION:-0}"
    fi

    log_info "Parsed metrics: files=$WORKER_FILES_PROCESSED, records=$WORKER_RECORDS_PROCESSED, throughput=$WORKER_RECORDS_PER_SEC rec/s, $WORKER_MB_PER_SEC MB/s, duration=${WORKER_DURATION}s"
}

wait_for_completion() {
    local job_id="$1"
    local run_dir="$2"
    local start_time=$(date +%s)
    local poll_count=0
    local queue_had_messages=false
    local queue_empty_since=0

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

        # Re-verify credentials every 5 polls to catch expired SSO tokens
        if [ $((poll_count % 5)) -eq 0 ]; then
            # shellcheck disable=SC2086
            if ! aws sts get-caller-identity --no-cli-pager >/dev/null 2>&1; then
                log_error "AWS credentials have expired or are no longer valid!"
                log_error "Please refresh your credentials and try again."
                log_error "For SSO: aws sso login --profile your-profile-name"
                return 1
            fi
        fi

        # FIRST: Check CloudWatch logs for "Worker Complete" message (prioritize this check)
        # This catches fast jobs that complete between polls
        local worker_id="$WORKER_INSTANCE_ID"
        if [ "$worker_id" = "N/A" ] || [ -z "$worker_id" ]; then
            worker_id=$(get_instance_id "worker")
        fi

        if [ "$worker_id" != "None" ] && [ "$worker_id" != "N/A" ] && [ -n "$worker_id" ]; then
            local log_group="/paraflow/jobs/$job_id"
            local log_stream="${worker_id}/worker/user-data"
            log_info "Checking for Worker Complete in $log_group (stream: $log_stream)..."

            # Capture both stdout and stderr
            # Use || true to prevent set -e from exiting on non-zero
            local log_output=""
            local log_error=""
            local exit_code=0
            # shellcheck disable=SC2086
            log_error=$(aws logs get-log-events \
                --region "$AWS_REGION" \
                --log-group-name "$log_group" \
                --log-stream-name "$log_stream" \
                --limit 50 \
                --query 'events[*].message' \
                --output text \
                --no-cli-pager 2>&1) || exit_code=$?

            if [ $exit_code -eq 0 ]; then
                log_output="$log_error"
                if [ -n "$log_output" ] && [ "$log_output" != "None" ]; then
                    log_info "Got log output (${#log_output} chars), checking for Worker Complete..."
                    if echo "$log_output" | grep -q "Worker Complete"; then
                        echo ""
                        log_success "Worker Complete message found in logs - job finished!"
                        # Display final metrics from logs (filter out set -x trace lines starting with +)
                        echo "--- Final Worker Output ---"
                        echo "$log_output" | grep -v '^[[:space:]]*+' | tail -10
                        echo "--- End Final Output ---"
                        break
                    fi
                else
                    log_info "Log stream exists but no events yet"
                fi
            else
                # Check for credential errors vs log group not existing
                if echo "$log_error" | grep -qi "credentials\|expired\|unauthorized\|accessdenied"; then
                    log_error "AWS credentials error while querying CloudWatch logs!"
                    log_error "Run: export AWS_PROFILE=your-profile-name"
                    return 1
                elif echo "$log_error" | grep -q "ResourceNotFoundException"; then
                    # Log group/stream may not exist yet if instance just started
                    log_info "CloudWatch log stream not available yet (instance still starting)"
                else
                    log_warn "CloudWatch logs query failed (exit=$exit_code): $log_error"
                fi
            fi
        fi

        # Check discoverer instance - use terraform ID if available, else query by tag
        local discoverer_id="$DISCOVERER_INSTANCE_ID"
        if [ "$discoverer_id" = "N/A" ] || [ -z "$discoverer_id" ]; then
            discoverer_id=$(get_instance_id "discoverer")
        fi
        local discoverer_status=$(get_instance_status "$discoverer_id")

        # Check for credential errors
        if [[ "$discoverer_status" == ERROR:credentials* ]]; then
            log_error "AWS credentials error! Please check your AWS_PROFILE or credentials."
            log_error "Run: export AWS_PROFILE=your-profile-name"
            return 1
        fi
        log_info "Discoverer: ID=$discoverer_id, Status=$discoverer_status"

        # Check worker instance status (worker_id already set above)
        local worker_status=$(get_instance_status "$worker_id")

        # Check for credential errors
        if [[ "$worker_status" == ERROR:credentials* ]]; then
            log_error "AWS credentials error! Please check your AWS_PROFILE or credentials."
            log_error "Run: export AWS_PROFILE=your-profile-name"
            return 1
        fi
        log_info "Worker: ID=$worker_id, Status=$worker_status"

        # Check if instances no longer exist (InvalidInstanceID.NotFound means they were terminated/deleted)
        if [[ "$worker_status" == ERROR:*NotFound* ]] && [[ "$discoverer_status" == ERROR:*NotFound* ]]; then
            echo ""
            # Only treat as success if we've seen the queue have messages at some point
            # or if we're past the first few polls (instances had time to start)
            if [ "$queue_had_messages" = true ] || [ "$poll_count" -gt 5 ]; then
                log_success "Both instances no longer exist - job has completed and instances were terminated"
                break
            else
                log_warn "Instances not found on poll #$poll_count - they may have failed to start or are still being created"
                log_warn "Waiting for instances to appear..."
            fi
        fi

        # Fetch CloudWatch logs from both discoverer and worker (every poll)
        log_info "Fetching CloudWatch logs..."
        fetch_cloudwatch_logs_realtime "$job_id" "discoverer" "$discoverer_id"
        fetch_cloudwatch_logs_realtime "$job_id" "worker" "$worker_id"

        # Fetch EC2 console output as fallback (every 3rd poll, before CloudWatch logs are available)
        if [ $((poll_count % 3)) -eq 1 ] && [ "$worker_id" != "None" ] && [ "$worker_id" != "N/A" ] && [ -n "$worker_id" ]; then
            # shellcheck disable=SC2086
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
            # shellcheck disable=SC2086
            queue_url=$(aws sqs list-queues --region "$AWS_REGION" --queue-name-prefix "paraflow-$job_id" --query 'QueueUrls[0]' --output text 2>/dev/null || echo "")
        fi
        local visible="0"
        local in_flight="0"
        if [ -n "$queue_url" ] && [ "$queue_url" != "None" ] && [ "$queue_url" != "N/A" ]; then
            # shellcheck disable=SC2086
            visible=$(aws sqs get-queue-attributes --region "$AWS_REGION" --queue-url "$queue_url" --attribute-names ApproximateNumberOfMessages --query 'Attributes.ApproximateNumberOfMessages' --output text 2>/dev/null || echo "0")
            # shellcheck disable=SC2086
            in_flight=$(aws sqs get-queue-attributes --region "$AWS_REGION" --queue-url "$queue_url" --attribute-names ApproximateNumberOfMessagesNotVisible --query 'Attributes.ApproximateNumberOfMessagesNotVisible' --output text 2>/dev/null || echo "0")

            # Track if queue ever had messages
            local total_messages=$((visible + in_flight))
            if [ "$total_messages" -gt 0 ]; then
                queue_had_messages=true
                queue_empty_since=0
            fi

            log_info "SQS Queue: $visible visible, $in_flight in-flight (had_messages=$queue_had_messages)"

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

        # Track how long queue has been empty after having messages
        if [ "$queue_had_messages" = true ] && [ "$visible" = "0" ] && [ "$in_flight" = "0" ]; then
            if [ "$queue_empty_since" -eq 0 ]; then
                queue_empty_since=$(date +%s)
                log_info "Queue drained - waiting 30s to confirm completion..."
            else
                local empty_duration=$(($(date +%s) - queue_empty_since))
                if [ "$empty_duration" -ge 30 ]; then
                    echo ""
                    log_success "Queue drained and empty for ${empty_duration}s - job complete"
                    break
                else
                    log_info "Queue empty for ${empty_duration}s (waiting for 30s)"
                fi
            fi
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
    # shellcheck disable=SC2086
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
    # shellcheck disable=SC2086
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
    # shellcheck disable=SC2086
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
    # shellcheck disable=SC2086
    aws logs describe-log-streams \
        --region "$AWS_REGION" \
        --log-group-name "$log_group" \
        --query 'logStreams[*].logStreamName' \
        --output json > "$run_dir/log_streams.json" 2>/dev/null || echo "[]" > "$run_dir/log_streams.json"

    # Download recent logs (last 4 hours)
    local end_time=$(($(date +%s) * 1000))
    local start_time=$((end_time - 14400000))  # 4 hours ago

    # shellcheck disable=SC2086
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
    local worker_count="${DEPLOYED_WORKER_COUNT:-1}"

    # Parse worker metrics if not already done
    if [ -z "$WORKER_FILES_PROCESSED" ]; then
        parse_worker_metrics "$run_dir"
    fi

    # Extract workers_reporting from metrics file if available
    local workers_reporting="$worker_count"
    if command -v jq &> /dev/null && [ -f "$run_dir/worker_metrics.json" ]; then
        workers_reporting=$(jq -r '.workers_reporting // .worker_count // 1' "$run_dir/worker_metrics.json" 2>/dev/null || echo "$worker_count")
    fi

    # Build configuration description
    local config_desc="$instance_type"
    if [ "$worker_count" -gt 1 ] 2>/dev/null; then
        config_desc="${worker_count}× $instance_type"
    fi

    cat > "$report_file" <<EOF
# Paraflow Benchmark Report

## Configuration

| Parameter | Value |
|-----------|-------|
| Instance Type | $instance_type |
| Worker Count | $worker_count |
| Configuration | $config_desc |
| Worker Threads | $([ "$WORKER_THREADS" = "0" ] && echo "auto (nproc)" || echo "$WORKER_THREADS") |
| Batch Size | $BATCH_SIZE |
| Max Files | $MAX_FILES |
| Job ID | $job_id |
| Timestamp | $TIMESTAMP |

## Throughput Results (Combined from $workers_reporting worker(s))

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
        t4g.2xlarge) hourly_cost=0.2688 ;;
        c7g.medium)  hourly_cost=0.0363 ;;
        c7g.large)   hourly_cost=0.0725 ;;
        c7g.xlarge)  hourly_cost=0.145 ;;
        c7g.2xlarge) hourly_cost=0.29 ;;
        c7g.4xlarge) hourly_cost=0.58 ;;
        m7g.medium)  hourly_cost=0.0408 ;;
        m7g.large)   hourly_cost=0.0816 ;;
        m7g.xlarge)  hourly_cost=0.163 ;;
        *)           hourly_cost=0.05 ;;
    esac

    # Multiply hourly cost by worker count
    local combined_hourly_cost=$(echo "scale=4; $hourly_cost * $worker_count" | bc)
    local hours=$(echo "scale=4; $total_time / 3600" | bc)
    local cost=$(echo "scale=6; $hours * $combined_hourly_cost" | bc)

    # Calculate cost per million records
    local cost_per_million="N/A"
    if [ "${WORKER_RECORDS_PROCESSED:-0}" -gt 0 ]; then
        cost_per_million=$(echo "scale=6; ($cost / ${WORKER_RECORDS_PROCESSED}) * 1000000" | bc)
    fi

    cat >> "$report_file" <<EOF
| Configuration | Hourly Cost | Run Time (hours) | Job Cost | Cost per 1M Records |
|---------------|-------------|------------------|----------|---------------------|
| $config_desc | \$$combined_hourly_cost | $hours | \$$cost | \$$cost_per_million |

EOF

    # Add individual worker breakdown if multiple workers
    if [ "$worker_count" -gt 1 ] 2>/dev/null && [ -d "$run_dir/workers" ]; then
        cat >> "$report_file" <<EOF
## Individual Worker Metrics

| Worker | Instance ID | Files | Records | Records/sec | MB/s | Duration |
|--------|-------------|-------|---------|-------------|------|----------|
EOF
        for worker_file in "$run_dir/workers"/worker_*_metrics.json; do
            if [ -f "$worker_file" ]; then
                if command -v jq &> /dev/null; then
                    local w_index=$(jq -r '.worker_index' "$worker_file" 2>/dev/null)
                    local w_instance=$(jq -r '.instance_id' "$worker_file" 2>/dev/null)
                    local w_files=$(jq -r '.files_processed' "$worker_file" 2>/dev/null)
                    local w_records=$(jq -r '.records_processed' "$worker_file" 2>/dev/null)
                    local w_rec_sec=$(jq -r '.records_per_second' "$worker_file" 2>/dev/null)
                    local w_mb_sec=$(jq -r '.mb_per_second' "$worker_file" 2>/dev/null)
                    local w_duration=$(jq -r '.duration' "$worker_file" 2>/dev/null)
                    echo "| $w_index | $w_instance | $w_files | $w_records | $w_rec_sec | $w_mb_sec | ${w_duration}s |" >> "$report_file"
                fi
            fi
        done
        echo "" >> "$report_file"
    fi

    cat >> "$report_file" <<EOF
## Files

- \`worker_metrics.json\` - Aggregated worker throughput metrics
- \`queue_metrics.jsonl\` - SQS queue metrics over time
- \`cpu_metrics.json\` - CloudWatch CPU metrics
- \`memory_metrics.json\` - CloudWatch memory metrics
- \`network_metrics.json\` - CloudWatch network metrics
- \`logs.json\` - CloudWatch log events
EOF

    # Add workers directory note if multiple workers
    if [ "$worker_count" -gt 1 ] 2>/dev/null; then
        echo "- \`workers/\` - Individual worker metrics" >> "$report_file"
    fi

    cat >> "$report_file" <<EOF

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
worker_count               = $WORKER_COUNT
worker_threads             = $WORKER_THREADS
batch_size                 = $BATCH_SIZE
max_files                  = $MAX_FILES
enable_detailed_monitoring = true
benchmark_mode             = true
bootstrap_timeout_seconds  = 900
EOF

    log_info "Benchmark configuration:"
    log_info "  - worker_instance_type: $INSTANCE_TYPE"
    log_info "  - worker_count: $WORKER_COUNT"
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
    # Get all worker instance IDs as a space-separated list
    WORKER_INSTANCE_IDS=$(terraform output -json worker_instance_ids 2>/dev/null | jq -r '.[]' 2>/dev/null | tr '\n' ' ' || echo "$WORKER_INSTANCE_ID")
    DEPLOYED_WORKER_COUNT=$(terraform output -raw worker_count 2>/dev/null || echo "1")
    local ecr_repository=$(terraform output -raw ecr_repository_url 2>/dev/null || echo "N/A")

    # CRITICAL: Get region from terraform - it may differ from AWS_REGION env var
    local tf_region
    tf_region=$(terraform output -raw aws_region 2>/dev/null || echo "")
    if [ -n "$tf_region" ] && [ "$tf_region" != "$AWS_REGION" ]; then
        log_warn "Terraform region ($tf_region) differs from AWS_REGION ($AWS_REGION)"
        log_info "Updating AWS_REGION to match terraform deployment region"
        export AWS_REGION="$tf_region"
        export AWS_DEFAULT_REGION="$tf_region"
    fi

    log_success "Infrastructure deployed successfully!"
    log_info "  Job ID: $JOB_ID"
    log_info "  Region: $AWS_REGION"
    log_info "  SQS Queue: $SQS_QUEUE_URL"
    log_info "  ECR Repository: $ecr_repository"
    log_info "  Discoverer Instance: $DISCOVERER_INSTANCE_ID"
    log_info "  Worker Count: $DEPLOYED_WORKER_COUNT"
    log_info "  Worker Instance(s): $WORKER_INSTANCE_IDS"

    # Verify instances exist before proceeding
    log_info "Verifying instances are running..."
    local max_verify_attempts=30
    local verify_attempt=0
    while [ $verify_attempt -lt $max_verify_attempts ]; do
        verify_attempt=$((verify_attempt + 1))
        local worker_state=$(get_instance_status "$WORKER_INSTANCE_ID")
        local discoverer_state=$(get_instance_status "$DISCOVERER_INSTANCE_ID")

        # Show current states for debugging
        if [ $verify_attempt -le 3 ] || [ $((verify_attempt % 5)) -eq 0 ]; then
            log_info "Instance states - worker: $worker_state, discoverer: $discoverer_state"
        fi

        if [[ "$worker_state" != ERROR:* ]] && [[ "$discoverer_state" != ERROR:* ]]; then
            log_success "Instances verified: worker=$worker_state, discoverer=$discoverer_state"
            break
        fi

        # Check for credential errors and fail fast
        if [[ "$worker_state" == ERROR:credentials* ]] || [[ "$discoverer_state" == ERROR:credentials* ]]; then
            log_error "AWS credentials error during instance verification!"
            log_error "Please ensure AWS_PROFILE is set correctly."
            return 1
        fi

        if [ $verify_attempt -eq $max_verify_attempts ]; then
            log_error "Instances failed to start after $max_verify_attempts attempts"
            log_error "Worker status: $worker_state"
            log_error "Discoverer status: $discoverer_state"
            return 1
        fi

        log_info "Waiting for instances to be ready (attempt $verify_attempt/$max_verify_attempts)..."
        sleep 5
    done

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

    # Fetch worker throughput metrics before instances terminate
    local worker_count="${DEPLOYED_WORKER_COUNT:-1}"
    local worker_ids="$WORKER_INSTANCE_IDS"

    # Fall back to single worker ID if WORKER_INSTANCE_IDS not set
    if [ -z "$worker_ids" ] || [ "$worker_ids" = " " ]; then
        worker_ids="$WORKER_INSTANCE_ID"
        if [ "$worker_ids" = "N/A" ] || [ -z "$worker_ids" ]; then
            worker_ids=$(get_instance_id "worker")
        fi
    fi

    log_info "Worker count: $worker_count"
    log_info "Worker instance IDs: $worker_ids"

    # Use multi-worker fetch if more than one worker
    if [ "$worker_count" -gt 1 ] 2>/dev/null; then
        log_info "Fetching and aggregating metrics from $worker_count workers..."
        fetch_all_worker_metrics "$run_dir" "$worker_ids" "$worker_count"
    else
        log_info "Fetching benchmark metrics from single worker instance..."
        local worker_id=$(echo "$worker_ids" | awk '{print $1}')
        fetch_benchmark_metrics "$worker_id" "$run_dir"
    fi

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
    log_info "  Worker Count:    $WORKER_COUNT"
    log_info "  Worker Threads:  $([ "$WORKER_THREADS" = "0" ] && echo "auto (nproc)" || echo "$WORKER_THREADS")"
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
    "worker_count": $WORKER_COUNT,
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
    local config_desc="$INSTANCE_TYPE"
    if [ "$WORKER_COUNT" -gt 1 ] 2>/dev/null; then
        config_desc="${WORKER_COUNT}× $INSTANCE_TYPE"
    fi
    log_info "BENCHMARK SUMMARY:"
    log_info "  Configuration:      $config_desc"
    log_info "  Instance Type:      $INSTANCE_TYPE"
    log_info "  Worker Count:       $WORKER_COUNT"
    log_info "  Total Duration:     ${total_time}s"
    log_info "  Files Processed:    ${WORKER_FILES_PROCESSED:-0}"
    log_info "  Records Processed:  ${WORKER_RECORDS_PROCESSED:-0}"
    log_info "  Files/sec:          ${WORKER_FILES_PER_SEC:-0}"
    log_info "  Records/sec:        ${WORKER_RECORDS_PER_SEC:-0}"
    log_info "  Throughput:         ${WORKER_MB_PER_SEC:-0} MB/s"

    # Format: instance_type,worker_count,duration,files_processed,records_processed,files_per_sec,records_per_sec,mb_per_sec
    echo ""
    echo "BENCHMARK_RESULT:$INSTANCE_TYPE,$WORKER_COUNT,$total_time,${WORKER_FILES_PROCESSED:-0},${WORKER_RECORDS_PROCESSED:-0},${WORKER_FILES_PER_SEC:-0},${WORKER_RECORDS_PER_SEC:-0},${WORKER_MB_PER_SEC:-0}"

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
    --worker-count NUM      Number of worker instances (default: 1)
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
    WORKER_COUNT        Number of worker instances (for horizontal scaling)
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
            --worker-count)
                WORKER_COUNT="$2"
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
