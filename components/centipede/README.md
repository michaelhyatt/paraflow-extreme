# Centipede Component

High-throughput S3-to-Elasticsearch data pipeline component for Paraflow Extreme.

## Architecture

```
S3 Bucket --> Discoverer --> SQS Queue --> Worker(s) --> Elasticsearch
                  |                            |
                  v                            v
            Step Functions              Transform Scripts
            (orchestration)                 (Rhai)
```

## Services

### Discoverer
Scans S3 buckets for new files and queues work items to SQS.

- Supports glob patterns for file filtering
- Partitions large file lists across multiple SQS messages
- Integrates with Step Functions for job orchestration

### Worker
High-throughput data processing worker that:
- Pulls work items from SQS
- Reads files (Parquet, NDJSON) via streaming
- Applies Rhai transforms with enrichment
- Indexes to Elasticsearch in bulk

## Building

```bash
# Build all centipede binaries
cargo build -p pf-discoverer-cli -p pf-worker-cli -p pf-transform-cli

# Build release binaries
cargo build --release -p pf-discoverer-cli -p pf-worker-cli
```

## Testing

```bash
# Run unit tests
cargo test -p pf-discoverer -p pf-worker

# Run integration tests (requires LocalStack)
docker compose -f docker/docker-compose.dev.yml up -d
cargo test -p integration-tests
docker compose -f docker/docker-compose.dev.yml down
```

## Docker

```bash
# Build image
docker build -f docker/Dockerfile -t centipede .

# Run discoverer
docker run centipede pf-discoverer --help

# Run worker
docker run centipede pf-worker --help
```

## Configuration

See the main project README for configuration options and environment variables.
