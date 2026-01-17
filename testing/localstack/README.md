# LocalStack Testing Environment

This directory contains the LocalStack setup for local testing of paraflow-extreme.

## Quick Start

1. **Start LocalStack:**
   ```bash
   cd testing/localstack
   docker-compose up -d
   ```

2. **Wait for LocalStack to be ready:**
   ```bash
   # Check health
   curl http://localhost:4566/_localstack/health
   ```

3. **Upload test files:**
   ```bash
   # Upload a Parquet file
   aws --endpoint-url=http://localhost:4566 s3 cp test.parquet s3://test-bucket/data/

   # Upload NDJSON files
   aws --endpoint-url=http://localhost:4566 s3 cp test.ndjson s3://test-bucket/data/
   ```

## Testing pf-discoverer + pf-worker Pipeline

### Discover files and send to worker:

```bash
# Run discoverer to find files and pipe to worker
cargo run -p pf-discoverer-cli -- \
    --bucket test-bucket \
    --prefix data/ \
    --s3-endpoint http://localhost:4566 \
    --region us-east-1 \
  | cargo run -p pf-worker-cli -- \
      --input stdin \
      --destination stats \
      --s3-endpoint http://localhost:4566 \
      --region us-east-1 \
      --threads 4
```

### Test with SQS input:

```bash
# First, run discoverer to send messages to SQS
cargo run -p pf-discoverer-cli -- \
    --bucket test-bucket \
    --prefix data/ \
    --s3-endpoint http://localhost:4566 \
    --region us-east-1 \
    --destination sqs \
    --sqs-queue-url http://localhost:4566/000000000000/work-queue \
    --sqs-endpoint http://localhost:4566

# Then run worker to process from SQS
cargo run -p pf-worker-cli -- \
    --input sqs \
    --sqs-queue-url http://localhost:4566/000000000000/work-queue \
    --sqs-endpoint http://localhost:4566 \
    --destination stats \
    --s3-endpoint http://localhost:4566 \
    --region us-east-1 \
    --threads 4
```

## Created Resources

The init script creates:

| Resource | URL/Name |
|----------|----------|
| S3 Bucket | `s3://test-bucket` |
| SQS Work Queue | `http://localhost:4566/000000000000/work-queue` |
| SQS Dead Letter Queue | `http://localhost:4566/000000000000/work-queue-dlq` |

## Useful Commands

```bash
# List S3 buckets
aws --endpoint-url=http://localhost:4566 s3 ls

# List files in bucket
aws --endpoint-url=http://localhost:4566 s3 ls s3://test-bucket/data/

# List SQS queues
aws --endpoint-url=http://localhost:4566 sqs list-queues

# View messages in queue (without consuming)
aws --endpoint-url=http://localhost:4566 sqs receive-message \
    --queue-url http://localhost:4566/000000000000/work-queue \
    --visibility-timeout 0

# Purge queue
aws --endpoint-url=http://localhost:4566 sqs purge-queue \
    --queue-url http://localhost:4566/000000000000/work-queue
```

## Cleanup

```bash
cd testing/localstack
docker-compose down -v  # -v removes volumes
```

## Environment Variables for CLI

For convenience, you can set these environment variables:

```bash
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
```
