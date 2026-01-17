#!/bin/bash
# Build Docker images for paraflow-extreme
#
# Usage: ./docker/build.sh [options]
#
# Options are passed directly to docker compose build

set -e

cd "$(dirname "$0")/.."

echo "Building paraflow-extreme Docker images..."
docker compose build "$@"

echo ""
echo "Build complete!"
echo "Run './docker/run.sh' to start the services"
