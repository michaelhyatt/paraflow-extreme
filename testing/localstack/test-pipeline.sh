#!/bin/bash
# End-to-end integration test for paraflow-extreme pipeline
# Requires: LocalStack running, test files uploaded

set -e

# Change to repo root directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

ENDPOINT="http://localhost:4566"
REGION="us-east-1"
BUCKET="test-bucket"
PREFIX="test-data/"
SQS_QUEUE_URL="http://localhost:4566/000000000000/work-queue"

# Set dummy AWS credentials for LocalStack
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=$REGION

echo "=================================="
echo "ParaFlow Extreme Integration Test"
echo "=================================="
echo ""

# Check LocalStack is running
echo "1. Checking LocalStack health..."
if ! curl -s "${ENDPOINT}/_localstack/health" | grep -q "running"; then
    echo "ERROR: LocalStack is not running. Start it with: docker compose up -d"
    exit 1
fi
echo "   LocalStack is healthy"
echo ""

# Create test data directory
echo "2. Creating test data..."

# Create bucket using curl (avoids AWS CLI v2 compatibility issues with LocalStack)
curl -s -X PUT "${ENDPOINT}/${BUCKET}" > /dev/null 2>&1 || true

# Create a simple test NDJSON file
cat > /tmp/test-data.ndjson << 'EOF'
{"id": 1, "name": "Alice", "score": 95}
{"id": 2, "name": "Bob", "score": 87}
{"id": 3, "name": "Charlie", "score": 92}
{"id": 4, "name": "Diana", "score": 88}
{"id": 5, "name": "Eve", "score": 91}
EOF

# Upload using curl (avoids AWS CLI v2 x-amz-trailer header issues)
curl -s -X PUT "${ENDPOINT}/${BUCKET}/${PREFIX}test-data.ndjson" \
    --data-binary @/tmp/test-data.ndjson \
    -H "Content-Type: application/x-ndjson" > /dev/null
echo "   Uploaded test NDJSON file"
echo ""

# Build the project
echo "3. Building project..."
cargo build --release --package pf-discoverer --package pf-worker-cli 2>/dev/null
echo "   Build complete"
echo ""

# Test stdin pipeline
echo "4. Testing stdin pipeline (discoverer -> worker)..."
echo "   Running: pf-discoverer | pf-worker (stats destination)"
echo ""

RESULT=$(./target/release/pf-discoverer \
    --bucket $BUCKET \
    --prefix $PREFIX \
    --s3-endpoint $ENDPOINT \
    --region $REGION \
  | ./target/release/pf-worker \
      --input stdin \
      --destination stats \
      --s3-endpoint $ENDPOINT \
      --region $REGION \
      --threads 2 \
      --log-level warn 2>&1)

echo "$RESULT"
echo ""

# Check results (use regex to handle variable spacing in formatted output)
if echo "$RESULT" | grep -qE "Files processed:\s+1"; then
    echo "   PASS: Pipeline processed 1 file"
else
    echo "   FAIL: Expected 1 file processed"
    exit 1
fi

if echo "$RESULT" | grep -qE "Records processed:\s+5"; then
    echo "   PASS: Processed 5 records"
else
    echo "   FAIL: Expected 5 records processed"
    exit 1
fi

echo ""

# Test SQS pipeline
echo "5. Testing SQS pipeline (discoverer -> SQS -> worker)..."

# Delete and recreate queue to ensure clean state
echo "   Recreating SQS queue..."
curl -s -X POST "${ENDPOINT}" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "Action=DeleteQueue&QueueUrl=${SQS_QUEUE_URL}" > /dev/null 2>&1 || true

sleep 1  # Wait for delete to complete

curl -s -X POST "${ENDPOINT}" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "Action=CreateQueue&QueueName=work-queue" > /dev/null 2>&1 || true

sleep 1  # Wait for create to complete

# Run discoverer to send messages to SQS
echo "   Running pf-discoverer -> SQS..."
DISCOVER_RESULT=$(./target/release/pf-discoverer \
    --bucket $BUCKET \
    --prefix $PREFIX \
    --s3-endpoint $ENDPOINT \
    --sqs-endpoint $ENDPOINT \
    --region $REGION \
    --destination sqs \
    --sqs-queue-url $SQS_QUEUE_URL \
    --log-level warn 2>&1)

echo "$DISCOVER_RESULT"

if echo "$DISCOVER_RESULT" | grep -qE "Files output:\s+1"; then
    echo "   PASS: Discoverer sent 1 file to SQS"
else
    echo "   FAIL: Expected 1 file output to SQS"
    exit 1
fi

# Run worker to process from SQS
echo ""
echo "   Running pf-worker <- SQS..."
WORKER_RESULT=$(./target/release/pf-worker \
    --input sqs \
    --sqs-queue-url $SQS_QUEUE_URL \
    --sqs-endpoint $ENDPOINT \
    --destination stats \
    --s3-endpoint $ENDPOINT \
    --region $REGION \
    --threads 2 \
    --sqs-drain \
    --sqs-wait-time 1 \
    --log-level warn 2>&1)

echo "$WORKER_RESULT"
echo ""

# Check worker results (use regex to handle variable spacing)
if echo "$WORKER_RESULT" | grep -qE "Files processed:\s+1"; then
    echo "   PASS: Worker processed 1 file from SQS"
else
    echo "   FAIL: Expected 1 file processed from SQS"
    exit 1
fi

if echo "$WORKER_RESULT" | grep -qE "Records processed:\s+5"; then
    echo "   PASS: Processed 5 records from SQS"
else
    echo "   FAIL: Expected 5 records processed from SQS"
    exit 1
fi

echo ""
echo "=================================="
echo "All integration tests PASSED!"
echo "=================================="
