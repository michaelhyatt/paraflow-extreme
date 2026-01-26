#!/bin/bash
# Run paraflow-extreme with Docker Compose
#
# Usage: ./docker/run.sh [num_workers]
#
# Examples:
#   ./docker/run.sh          # Run with default 3 workers
#   ./docker/run.sh 5        # Run with 5 workers
#   ./docker/run.sh 10       # Run with 10 workers

set -e

cd "$(dirname "$0")/.."

NUM_WORKERS=${1:-3}

echo "Starting paraflow-extreme with $NUM_WORKERS workers..."
echo ""

WORKER_REPLICAS=$NUM_WORKERS docker compose up --scale worker=$NUM_WORKERS "$@"
