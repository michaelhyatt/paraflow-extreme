#!/usr/bin/env bash
set -euo pipefail

# Get version from Cargo.toml or git tag
VERSION="${1:-$(grep '^version' Cargo.toml | head -1 | cut -d'"' -f2)}"
IMAGE_NAME="paraflow-extreme"

echo "Building $IMAGE_NAME:$VERSION"

docker build -t "$IMAGE_NAME:$VERSION" \
             -t "$IMAGE_NAME:latest" \
             --build-arg VERSION="$VERSION" .

echo ""
echo "Built images:"
echo "  - $IMAGE_NAME:$VERSION"
echo "  - $IMAGE_NAME:latest"
echo ""
echo "To push to ECR:"
echo "  aws ecr get-login-password | docker login --username AWS --password-stdin <account>.dkr.ecr.us-east-1.amazonaws.com"
echo "  docker tag $IMAGE_NAME:$VERSION <account>.dkr.ecr.us-east-1.amazonaws.com/$IMAGE_NAME:$VERSION"
echo "  docker push <account>.dkr.ecr.us-east-1.amazonaws.com/$IMAGE_NAME:$VERSION"
