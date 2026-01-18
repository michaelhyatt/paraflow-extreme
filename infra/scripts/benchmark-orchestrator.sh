#!/bin/bash
# ============================================================================
# Paraflow Benchmark Orchestrator
# Runs benchmarks across multiple ARM64 instance types and generates comparison
# ============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARK_RUNNER="$SCRIPT_DIR/benchmark-runner.sh"
RESULTS_DIR="${RESULTS_DIR:-/tmp/paraflow-benchmarks}"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
ORCHESTRATOR_DIR="$RESULTS_DIR/orchestrator-$TIMESTAMP"

# Instance types to benchmark (ARM64)
# Organized by category for clear comparison
T4G_INSTANCES=("t4g.small" "t4g.medium" "t4g.large" "t4g.xlarge")
C7G_INSTANCES=("c7g.medium" "c7g.large" "c7g.xlarge")
M7G_INSTANCES=("m7g.medium" "m7g.large" "m7g.xlarge")

# Default to all instances unless specified
INSTANCE_TYPES=()

# Benchmark configuration
WORKER_THREADS="${WORKER_THREADS:-4}"
BATCH_SIZE="${BATCH_SIZE:-10000}"
MAX_FILES="${MAX_FILES:-100}"
PARALLEL="${PARALLEL:-false}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# ============================================================================
# Logging Functions
# ============================================================================

log_info() {
    echo -e "${BLUE}[ORCHESTRATOR]${NC} $(date -u +"%Y-%m-%dT%H:%M:%SZ") $1"
}

log_success() {
    echo -e "${GREEN}[ORCHESTRATOR]${NC} $(date -u +"%Y-%m-%dT%H:%M:%SZ") $1"
}

log_error() {
    echo -e "${RED}[ORCHESTRATOR]${NC} $(date -u +"%Y-%m-%dT%H:%M:%SZ") $1"
}

log_warn() {
    echo -e "${YELLOW}[ORCHESTRATOR]${NC} $(date -u +"%Y-%m-%dT%H:%M:%SZ") $1"
}

log_header() {
    echo -e "${CYAN}============================================================${NC}"
    echo -e "${CYAN}$1${NC}"
    echo -e "${CYAN}============================================================${NC}"
}

# ============================================================================
# Instance Type Pricing (us-east-1, on-demand, as of 2024)
# ============================================================================

get_hourly_price() {
    local instance_type="$1"
    case "$instance_type" in
        # T4g - Burstable (Graviton2)
        t4g.small)   echo "0.0168" ;;
        t4g.medium)  echo "0.0336" ;;
        t4g.large)   echo "0.0672" ;;
        t4g.xlarge)  echo "0.1344" ;;
        # C7g - Compute Optimized (Graviton3)
        c7g.medium)  echo "0.0363" ;;
        c7g.large)   echo "0.0725" ;;
        c7g.xlarge)  echo "0.145" ;;
        # M7g - General Purpose (Graviton3)
        m7g.medium)  echo "0.0408" ;;
        m7g.large)   echo "0.0816" ;;
        m7g.xlarge)  echo "0.163" ;;
        *)           echo "0.05" ;;
    esac
}

get_vcpu_count() {
    local instance_type="$1"
    case "$instance_type" in
        *.small)   echo "2" ;;
        *.medium)  echo "2" ;;
        *.large)   echo "2" ;;
        *.xlarge)  echo "4" ;;
        *)         echo "2" ;;
    esac
}

get_memory_gb() {
    local instance_type="$1"
    case "$instance_type" in
        t4g.small)   echo "2" ;;
        t4g.medium)  echo "4" ;;
        t4g.large)   echo "8" ;;
        t4g.xlarge)  echo "16" ;;
        c7g.medium)  echo "4" ;;
        c7g.large)   echo "8" ;;
        c7g.xlarge)  echo "16" ;;
        m7g.medium)  echo "4" ;;
        m7g.large)   echo "8" ;;
        m7g.xlarge)  echo "16" ;;
        *)           echo "4" ;;
    esac
}

# ============================================================================
# Benchmark Functions
# ============================================================================

run_single_benchmark() {
    local instance_type="$1"
    local run_number="$2"
    local total_runs="$3"

    log_header "Benchmark $run_number/$total_runs: $instance_type"

    local start_time=$(date +%s)

    # Run the benchmark and capture output for result parsing
    local benchmark_output
    benchmark_output=$(INSTANCE_TYPE="$instance_type" \
        WORKER_THREADS="$WORKER_THREADS" \
        BATCH_SIZE="$BATCH_SIZE" \
        MAX_FILES="$MAX_FILES" \
        RESULTS_DIR="$ORCHESTRATOR_DIR" \
        "$BENCHMARK_RUNNER" run 2>&1 | tee /dev/stderr)

    local end_time=$(date +%s)
    local duration=$((end_time - start_time))

    log_success "Completed benchmark for $instance_type in ${duration}s"

    # Parse the BENCHMARK_RESULT line from output
    # Format: BENCHMARK_RESULT:instance_type,duration,files_processed,records_processed,files_per_sec,records_per_sec,mb_per_sec
    local result_line=$(echo "$benchmark_output" | grep "^BENCHMARK_RESULT:" | tail -1)
    if [ -n "$result_line" ]; then
        # Strip prefix and save to CSV
        local csv_data="${result_line#BENCHMARK_RESULT:}"
        echo "$csv_data" >> "$ORCHESTRATOR_DIR/benchmark_results.csv"
        log_info "Recorded result: $csv_data"
    else
        # Fallback to just duration if no result line
        echo "$instance_type,$duration,0,0,0,0,0" >> "$ORCHESTRATOR_DIR/benchmark_results.csv"
        log_warn "No throughput metrics found, recorded duration only"
    fi
}

# ============================================================================
# Report Generation
# ============================================================================

generate_comparison_report() {
    log_info "Generating comparison report..."

    local report_file="$ORCHESTRATOR_DIR/comparison-report.md"
    local results_file="$ORCHESTRATOR_DIR/benchmark_results.csv"

    cat > "$report_file" <<EOF
# Paraflow ARM64 Instance Benchmark Comparison

**Generated:** $(date -u +"%Y-%m-%dT%H:%M:%SZ")
**Region:** us-east-1

## Test Configuration

| Parameter | Value |
|-----------|-------|
| Worker Threads | $WORKER_THREADS |
| Batch Size | $BATCH_SIZE |
| Max Files | $MAX_FILES |
| Benchmark Mode | Enabled |
| Detailed Monitoring | Enabled |

## Instance Type Specifications

| Instance Type | vCPUs | Memory (GB) | Processor | Hourly Cost |
|---------------|-------|-------------|-----------|-------------|
EOF

    for instance_type in "${INSTANCE_TYPES[@]}"; do
        local vcpus=$(get_vcpu_count "$instance_type")
        local memory=$(get_memory_gb "$instance_type")
        local price=$(get_hourly_price "$instance_type")
        local processor="Graviton2"
        if [[ "$instance_type" == c7g.* ]] || [[ "$instance_type" == m7g.* ]]; then
            processor="Graviton3"
        fi
        echo "| $instance_type | $vcpus | $memory | $processor | \$$price |" >> "$report_file"
    done

    cat >> "$report_file" <<EOF

## Throughput Results

| Instance Type | Files Processed | Records Processed | Files/sec | Records/sec | MB/s Read |
|---------------|-----------------|-------------------|-----------|-------------|-----------|
EOF

    # Read results and output throughput metrics (skip header line)
    if [ -f "$results_file" ]; then
        tail -n +2 "$results_file" | while IFS=, read -r instance_type duration files_proc records_proc files_sec records_sec mb_sec; do
            echo "| $instance_type | $files_proc | $records_proc | $files_sec | $records_sec | $mb_sec |" >> "$report_file"
        done
    fi

    cat >> "$report_file" <<EOF

## Cost Analysis

| Instance Type | Duration (s) | Hourly Cost | Job Cost | Cost per 1M Records |
|---------------|--------------|-------------|----------|---------------------|
EOF

    # Read results and calculate costs
    if [ -f "$results_file" ]; then
        tail -n +2 "$results_file" | while IFS=, read -r instance_type duration files_proc records_proc files_sec records_sec mb_sec; do
            local hourly_cost=$(get_hourly_price "$instance_type")
            local hours=$(echo "scale=4; $duration / 3600" | bc)
            local job_cost=$(echo "scale=6; $hours * $hourly_cost" | bc)

            # Calculate cost per 1M records using actual records processed
            local cost_per_million="N/A"
            if [ "$records_proc" -gt 0 ] 2>/dev/null; then
                cost_per_million=$(echo "scale=4; ($job_cost / $records_proc) * 1000000" | bc)
            fi

            echo "| $instance_type | $duration | \$$hourly_cost | \$$job_cost | \$$cost_per_million |" >> "$report_file"
        done
    fi

    cat >> "$report_file" <<EOF

## Performance Rankings

### Throughput Rankings (Records/sec)
EOF

    # Sort by records per second (highest first)
    if [ -f "$results_file" ]; then
        echo "" >> "$report_file"
        echo "Highest throughput first:" >> "$report_file"
        echo "" >> "$report_file"
        tail -n +2 "$results_file" | sort -t, -k5 -rn | while IFS=, read -r instance_type duration files_proc records_proc files_sec records_sec mb_sec; do
            echo "1. **$instance_type** - $records_sec records/sec ($mb_sec MB/s)" >> "$report_file"
        done
    fi

    cat >> "$report_file" <<EOF

### Speed Rankings (Total Time)
EOF

    # Sort by execution time
    if [ -f "$results_file" ]; then
        echo "" >> "$report_file"
        echo "Fastest to slowest:" >> "$report_file"
        echo "" >> "$report_file"
        tail -n +2 "$results_file" | sort -t, -k2 -n | while IFS=, read -r instance_type duration files_proc records_proc files_sec records_sec mb_sec; do
            echo "1. **$instance_type** - ${duration}s" >> "$report_file"
        done
    fi

    cat >> "$report_file" <<EOF

### Cost-Efficiency Rankings
EOF

    # Calculate and sort by cost efficiency (cost per 1M records)
    if [ -f "$results_file" ]; then
        echo "" >> "$report_file"
        echo "Best value (lowest cost per 1M records):" >> "$report_file"
        echo "" >> "$report_file"

        # Create temp file with cost calculations
        local temp_file=$(mktemp)
        tail -n +2 "$results_file" | while IFS=, read -r instance_type duration files_proc records_proc files_sec records_sec mb_sec; do
            local hourly_cost=$(get_hourly_price "$instance_type")
            local hours=$(echo "scale=4; $duration / 3600" | bc)
            local job_cost=$(echo "scale=6; $hours * $hourly_cost" | bc)

            if [ "$records_proc" -gt 0 ] 2>/dev/null; then
                local cost_per_million=$(echo "scale=6; ($job_cost / $records_proc) * 1000000" | bc)
                echo "$cost_per_million,$instance_type,$job_cost,$records_sec" >> "$temp_file"
            fi
        done

        sort -t, -k1 -n "$temp_file" | while IFS=, read -r cost_per_mil instance_type job_cost records_sec; do
            echo "1. **$instance_type** - \$$cost_per_mil per 1M records (job: \$$job_cost, throughput: $records_sec rec/s)" >> "$report_file"
        done

        rm -f "$temp_file"
    fi

    cat >> "$report_file" <<EOF

## Recommendations

### For Small Workloads (<1M records)
- **Recommended:** t4g.small
- Lowest absolute cost
- Suitable for development and testing

### For Medium Workloads (1-10M records)
- **Recommended:** t4g.medium or c7g.medium
- Good balance of speed and cost
- c7g provides better sustained performance

### For Large Workloads (10-100M records)
- **Recommended:** c7g.large
- Graviton3 provides better compute efficiency
- Non-burstable for consistent performance

### For Very Large Workloads (>100M records)
- **Recommended:** c7g.xlarge
- Maximum compute throughput
- Consider using multiple workers

## Raw Data

Results are stored in:
- \`benchmark_results.csv\` - Full benchmark results with throughput metrics
- \`*/report.md\` - Individual benchmark reports
- \`*/worker_metrics.json\` - Worker throughput metrics
- \`*/queue_metrics.jsonl\` - SQS queue metrics over time
- \`*/cpu_metrics.json\` - CloudWatch CPU metrics
- \`*/memory_metrics.json\` - CloudWatch memory metrics

---
Generated by Paraflow Benchmark Orchestrator
EOF

    log_success "Comparison report generated: $report_file"
}

# ============================================================================
# Main Functions
# ============================================================================

setup() {
    mkdir -p "$ORCHESTRATOR_DIR"

    # Initialize results CSV with headers
    echo "instance_type,duration_seconds,files_processed,records_processed,files_per_sec,records_per_sec,mb_per_sec" > "$ORCHESTRATOR_DIR/benchmark_results.csv"

    log_info "Results will be saved to: $ORCHESTRATOR_DIR"
}

run_all_benchmarks() {
    local total=${#INSTANCE_TYPES[@]}
    local current=0

    for instance_type in "${INSTANCE_TYPES[@]}"; do
        current=$((current + 1))
        run_single_benchmark "$instance_type" "$current" "$total"

        # Small delay between benchmarks to ensure cleanup
        if [ "$current" -lt "$total" ]; then
            log_info "Waiting 30s before next benchmark..."
            sleep 30
        fi
    done
}

# ============================================================================
# CLI
# ============================================================================

usage() {
    cat <<EOF
Paraflow Benchmark Orchestrator

Runs benchmarks across multiple ARM64 instance types and generates comparison.

Usage: $0 [options]

Options:
    --instances TYPES   Comma-separated list of instance types to benchmark
                        (default: all T4g, C7g, M7g instances)
    --t4g-only          Benchmark only T4g instances
    --c7g-only          Benchmark only C7g instances
    --m7g-only          Benchmark only M7g instances
    --quick             Quick benchmark with only medium instances
    --threads NUM       Worker threads (default: 4)
    --batch-size NUM    Batch size (default: 10000)
    --max-files NUM     Max files to process (default: 100)
    --results-dir DIR   Results directory (default: /tmp/paraflow-benchmarks)
    --help              Show this help message

Instance Types Available:
    T4g (Burstable, Graviton2): t4g.small, t4g.medium, t4g.large, t4g.xlarge
    C7g (Compute, Graviton3):   c7g.medium, c7g.large, c7g.xlarge
    M7g (General, Graviton3):   m7g.medium, m7g.large, m7g.xlarge

Examples:
    # Run all benchmarks
    $0

    # Quick comparison of medium instances only
    $0 --quick

    # Benchmark specific instances
    $0 --instances "t4g.medium,c7g.large,m7g.large"

    # Benchmark only C7g instances with custom config
    $0 --c7g-only --threads 8 --max-files 500

Environment Variables:
    WORKER_THREADS      Number of worker threads
    BATCH_SIZE          Batch size for processing
    MAX_FILES           Maximum files to process
    RESULTS_DIR         Results directory
EOF
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --instances)
                IFS=',' read -ra INSTANCE_TYPES <<< "$2"
                shift 2
                ;;
            --t4g-only)
                INSTANCE_TYPES=("${T4G_INSTANCES[@]}")
                shift
                ;;
            --c7g-only)
                INSTANCE_TYPES=("${C7G_INSTANCES[@]}")
                shift
                ;;
            --m7g-only)
                INSTANCE_TYPES=("${M7G_INSTANCES[@]}")
                shift
                ;;
            --quick)
                INSTANCE_TYPES=("t4g.medium" "c7g.medium" "m7g.medium")
                shift
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
            --results-dir)
                RESULTS_DIR="$2"
                ORCHESTRATOR_DIR="$RESULTS_DIR/orchestrator-$TIMESTAMP"
                shift 2
                ;;
            --help|-h)
                usage
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done

    # Default to all instances if not specified
    if [ ${#INSTANCE_TYPES[@]} -eq 0 ]; then
        INSTANCE_TYPES=("${T4G_INSTANCES[@]}" "${C7G_INSTANCES[@]}" "${M7G_INSTANCES[@]}")
    fi
}

main() {
    parse_args "$@"

    log_header "Paraflow Benchmark Orchestrator"
    log_info "Instance types to benchmark: ${INSTANCE_TYPES[*]}"
    log_info "Worker threads: $WORKER_THREADS"
    log_info "Batch size: $BATCH_SIZE"
    log_info "Max files: $MAX_FILES"

    setup
    run_all_benchmarks
    generate_comparison_report

    log_header "Benchmark Orchestration Complete"
    log_success "Results saved to: $ORCHESTRATOR_DIR"
    log_success "View comparison report: $ORCHESTRATOR_DIR/comparison-report.md"
}

main "$@"
