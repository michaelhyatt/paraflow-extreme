# Docker Compose Deployment

This directory contains Docker configuration for running paraflow-extreme locally with LocalStack for S3 and SQS emulation.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Docker Compose Network                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────────┐ │
│  │  LocalStack  │     │  Discoverer  │     │  Worker (1..N)   │ │
│  │  (S3 + SQS)  │◄────│  Scans S3    │────►│  Consumes SQS    │ │
│  └──────────────┘     └──────────────┘     └──────────────────┘ │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Quick Start

```bash
# 1. Build the Docker images
./docker/build.sh

# 2. Start LocalStack and wait for initialization
docker compose up -d localstack
sleep 10

# 3. Upload test data
./docker/setup-test-data.sh

# 4. Run the full pipeline
docker compose up discoverer worker
```

## Step-by-Step Guide

### 1. Build Docker Images

```bash
# From the repository root
./docker/build.sh

# Or directly with docker compose
docker compose build
```

This builds a multi-stage Docker image containing both `pf-discoverer` and `pf-worker` binaries.

### 2. Start LocalStack

```bash
docker compose up -d localstack
```

LocalStack automatically initializes:
- S3 bucket: `test-bucket`
- SQS queue: `work-queue`
- SQS dead-letter queue: `work-queue-dlq`

Verify it's ready:
```bash
curl http://localhost:4566/_localstack/health
```

### 3. Upload Test Data

```bash
./docker/setup-test-data.sh
```

This creates 5 sample NDJSON files (100 records each) and uploads them to LocalStack S3.

### 4. Run the Pipeline

```bash
# Run with default 3 workers
docker compose up discoverer worker

# Or run with custom worker count
docker compose up --scale worker=5 discoverer worker
```

The discoverer scans S3, sends file references to SQS, and workers process the files.

## Scaling Workers

Three methods to configure worker replicas:

| Method | Command |
|--------|---------|
| `--scale` flag | `docker compose up --scale worker=5` |
| Environment variable | `WORKER_REPLICAS=5 docker compose up` |
| `.env` file | Add `WORKER_REPLICAS=5` to `.env` |

## Configuration

Copy `.env.example` to `.env` and customize:

| Variable | Description | Default |
|----------|-------------|---------|
| `WORKER_REPLICAS` | Number of worker containers | `3` |
| `WORKER_THREADS` | Processing threads per worker | `0` (auto: 2× CPU) |
| `WORKER_BATCH_SIZE` | Records per batch | `50000` |
| `WORKER_COLUMNS` | Column projection (comma-separated) | (all columns) |
| `WORKER_FILTER` | Row filter predicate (e.g., `status=active`) | (none) |
| `WORKER_SQS_CONCURRENT_POLLS` | Concurrent SQS receive requests | `2` |
| `WORKER_PREFETCH_COUNT` | Files to prefetch per thread | `6` |
| `WORKER_PREFETCH_MEMORY_MB` | Memory budget per thread for prefetch | `200` |
| `S3_BUCKET` | S3 bucket name | `test-bucket` |
| `S3_PREFIX` | S3 prefix filter | (empty) |
| `DISCOVERY_PATTERN` | Glob pattern for files | `*` |
| `DISCOVERY_CONCURRENCY` | Parallel S3 list operations | `10` |
| `LOG_LEVEL` | Logging verbosity | `info` |
| `LOCALSTACK_DEBUG` | Enable LocalStack debug logs | `0` |

## Helper Scripts

| Script | Purpose |
|--------|---------|
| `build.sh` | Build Docker images |
| `run.sh [N]` | Run with N workers (default: 3) |
| `setup-test-data.sh` | Upload sample files to LocalStack |

## Common Operations

### View Logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f worker
```

### Check Queue Status

```bash
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  aws --endpoint-url=http://localhost:4566 sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/work-queue \
  --attribute-names ApproximateNumberOfMessages
```

### List S3 Files

```bash
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  aws --endpoint-url=http://localhost:4566 s3 ls s3://test-bucket/
```

### Clean Up

```bash
# Stop containers
docker compose down

# Stop and remove volumes (full reset)
docker compose down -v
```

## Troubleshooting

### LocalStack not ready

```bash
# Check health endpoint
curl http://localhost:4566/_localstack/health

# View logs
docker compose logs localstack
```

### Workers not receiving messages

```bash
# Check if discoverer sent messages
docker compose logs discoverer

# Check queue depth
AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
  aws --endpoint-url=http://localhost:4566 sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/work-queue \
  --attribute-names All
```

### Build failures

```bash
# Rebuild without cache
docker compose build --no-cache

# Check build output
docker compose build --progress=plain
```

## File Structure

```
docker/
├── README.md                 # This file
├── build.sh                  # Build helper
├── run.sh                    # Run helper with scaling
├── setup-test-data.sh        # Test data setup
└── init-localstack/
    └── 01-create-resources.sh  # LocalStack initialization
```
