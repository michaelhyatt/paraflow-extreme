#!/bin/bash
# ============================================================================
# Paraflow Instance Health Check Script
# Checks bootstrap status and component readiness
# ============================================================================

set -e

BOOTSTRAP_STATUS_FILE="/var/log/paraflow-bootstrap-status"
BENCHMARK_METRICS_FILE="/var/log/benchmark-metrics.json"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# ============================================================================
# Health Check Functions
# ============================================================================

check_bootstrap_status() {
    echo "=== Bootstrap Status ==="
    if [ -f "$BOOTSTRAP_STATUS_FILE" ]; then
        cat "$BOOTSTRAP_STATUS_FILE"
        echo ""

        # Parse status
        local status=$(grep -o '"status":[^,]*' "$BOOTSTRAP_STATUS_FILE" | cut -d'"' -f4)
        local stage=$(grep -o '"stage":[^,]*' "$BOOTSTRAP_STATUS_FILE" | cut -d'"' -f4)

        if [ "$status" = "SUCCESS" ]; then
            echo -e "${GREEN}Bootstrap completed successfully${NC}"
            return 0
        elif [ "$status" = "FAILED" ]; then
            echo -e "${RED}Bootstrap failed at stage: $stage${NC}"
            return 1
        else
            echo -e "${YELLOW}Bootstrap in progress: $stage${NC}"
            return 2
        fi
    else
        echo -e "${RED}Bootstrap status file not found${NC}"
        return 1
    fi
}

check_docker() {
    echo "=== Docker Status ==="
    if command -v docker &> /dev/null; then
        if docker info &> /dev/null; then
            echo -e "${GREEN}Docker daemon is running${NC}"
            docker version --format 'Docker version: {{.Server.Version}}'
            return 0
        else
            echo -e "${RED}Docker daemon is not running${NC}"
            return 1
        fi
    else
        echo -e "${RED}Docker is not installed${NC}"
        return 1
    fi
}

check_containers() {
    echo "=== Container Status ==="
    if docker ps -a --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}' 2>/dev/null; then
        return 0
    else
        echo "No containers found"
        return 0
    fi
}

check_cloudwatch_agent() {
    echo "=== CloudWatch Agent Status ==="
    if systemctl is-active --quiet amazon-cloudwatch-agent 2>/dev/null; then
        echo -e "${GREEN}CloudWatch agent is running${NC}"
        return 0
    else
        echo -e "${YELLOW}CloudWatch agent is not running (may be disabled)${NC}"
        return 0
    fi
}

check_logs() {
    echo "=== Recent Log Entries ==="
    if [ -f "/var/log/user-data.log" ]; then
        echo "Last 10 lines of user-data.log:"
        tail -10 /var/log/user-data.log
    else
        echo "No user-data.log found"
    fi
}

check_benchmark_metrics() {
    echo "=== Benchmark Metrics ==="
    if [ -f "$BENCHMARK_METRICS_FILE" ]; then
        cat "$BENCHMARK_METRICS_FILE"
        return 0
    else
        echo "No benchmark metrics available (benchmark_mode may be disabled)"
        return 0
    fi
}

check_system_resources() {
    echo "=== System Resources ==="
    echo "CPU cores: $(nproc)"
    echo "Memory:"
    free -h
    echo ""
    echo "Disk usage:"
    df -h /
}

check_instance_metadata() {
    echo "=== Instance Metadata ==="
    echo "Instance ID: $(curl -s --connect-timeout 2 http://169.254.169.254/latest/meta-data/instance-id || echo 'N/A')"
    echo "Instance Type: $(curl -s --connect-timeout 2 http://169.254.169.254/latest/meta-data/instance-type || echo 'N/A')"
    echo "Availability Zone: $(curl -s --connect-timeout 2 http://169.254.169.254/latest/meta-data/placement/availability-zone || echo 'N/A')"
    echo "Public IP: $(curl -s --connect-timeout 2 http://169.254.169.254/latest/meta-data/public-ipv4 || echo 'N/A')"
}

# ============================================================================
# Main
# ============================================================================

main() {
    local check_type="${1:-all}"

    case "$check_type" in
        bootstrap)
            check_bootstrap_status
            ;;
        docker)
            check_docker
            ;;
        containers)
            check_containers
            ;;
        cloudwatch)
            check_cloudwatch_agent
            ;;
        logs)
            check_logs
            ;;
        benchmark)
            check_benchmark_metrics
            ;;
        resources)
            check_system_resources
            ;;
        metadata)
            check_instance_metadata
            ;;
        all|*)
            echo "=============================================="
            echo "Paraflow Instance Health Check"
            echo "Timestamp: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
            echo "=============================================="
            echo ""

            check_instance_metadata
            echo ""

            check_bootstrap_status
            echo ""

            check_docker
            echo ""

            check_containers
            echo ""

            check_cloudwatch_agent
            echo ""

            check_system_resources
            echo ""

            check_benchmark_metrics
            echo ""

            echo "=============================================="
            echo "Health Check Complete"
            echo "=============================================="
            ;;
    esac
}

main "$@"
