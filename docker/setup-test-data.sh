#!/bin/bash
# Upload sample test files to LocalStack S3 for testing
#
# Usage: ./docker/setup-test-data.sh
#
# Prerequisites:
#   - LocalStack must be running: docker compose up -d localstack
#   - AWS CLI must be installed

set -e

cd "$(dirname "$0")/.."

ENDPOINT_URL="http://localhost:4566"
BUCKET="test-bucket"

# Set AWS credentials for LocalStack
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

echo "Setting up test data in LocalStack S3..."
echo ""

# Check if LocalStack is running
if ! curl -sf "$ENDPOINT_URL/_localstack/health" > /dev/null 2>&1; then
    echo "Error: LocalStack is not running."
    echo "Start it with: docker compose up -d localstack"
    exit 1
fi

# Create sample NDJSON test files
echo "Creating sample NDJSON files..."

# Create temp directory for test files
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

# Generate sample data
for i in {1..5}; do
    FILE="$TEMP_DIR/sample-$i.ndjson"
    for j in {1..100}; do
        echo "{\"id\": $((i * 100 + j)), \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\", \"message\": \"Test message $j from file $i\", \"level\": \"info\"}" >> "$FILE"
    done
    echo "Created: sample-$i.ndjson (100 records)"
done

# Upload to S3
echo ""
echo "Uploading files to s3://$BUCKET/..."
for file in "$TEMP_DIR"/*.ndjson; do
    filename=$(basename "$file")
    aws --endpoint-url="$ENDPOINT_URL" s3 cp "$file" "s3://$BUCKET/$filename" --quiet
    echo "Uploaded: $filename"
done

echo ""
echo "Test data setup complete!"
echo ""
echo "Files in bucket:"
aws --endpoint-url="$ENDPOINT_URL" s3 ls "s3://$BUCKET/"
echo ""
echo "Run 'docker compose up' to process these files"
