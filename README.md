# Paraflow Extreme

High-throughput data ingestion engine built for maximum performance.

## Overview

Paraflow Extreme is a greenfield implementation designed for TB-scale ingestion at 1+ GB/s per worker. It follows a per-thread pipeline architecture:

```
Reader → Transform+Enrich → Indexer
```

### Key Features

- **Zero-copy Arrow data path** - No serialization overhead between stages
- **Per-thread pipelines** - Eliminate synchronization overhead
- **Streaming memory model** - Process 10GB files with ~50MB RAM per thread
- **SQS-based work distribution** - Durable, scalable queue with built-in DLQ
- **Rhai transforms** - Flexible scripting with built-in enrichment functions
- **Partial failure handling** - Continue processing with record-level error tracking

### Target Performance

- **1+ GB/s** per worker throughput
- **TB-scale** ingestion in minutes
- **<100MB RAM** per thread regardless of file size

## Quick Start

### Local Development with LocalStack

```bash
# Start LocalStack (S3 + SQS)
cd testing/localstack
docker-compose up -d

# Discover files and pipe to worker for processing
cargo run --package pf-discoverer-cli -- \
    --bucket test-bucket \
    --prefix data/ \
    --s3-endpoint http://localhost:4566 \
    --region us-east-1 \
  | cargo run --package pf-worker-cli -- \
      --input stdin \
      --destination stats \
      --s3-endpoint http://localhost:4566 \
      --region us-east-1 \
      --threads 4

# Or discover to stdout only (for debugging)
cargo run --package pf-discoverer-cli -- \
    --bucket test-bucket \
    --s3-endpoint http://localhost:4566 \
    --pattern "*.parquet"
```

### With SQS Queue (Production-like)

```bash
# First, run discoverer to send messages to SQS
cargo run --package pf-discoverer-cli -- \
    --bucket test-bucket \
    --prefix data/ \
    --s3-endpoint http://localhost:4566 \
    --destination sqs \
    --sqs-queue-url http://localhost:4566/000000000000/work-queue \
    --sqs-endpoint http://localhost:4566

# Then run worker to process from SQS
cargo run --package pf-worker-cli -- \
    --input sqs \
    --sqs-queue-url http://localhost:4566/000000000000/work-queue \
    --sqs-endpoint http://localhost:4566 \
    --destination stats \
    --s3-endpoint http://localhost:4566 \
    --threads 4
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Worker Node                               │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ Thread Pool (rayon)                                         │ │
│  │  ┌────────────────────────────────────────────────────────┐ │ │
│  │  │ Thread 1: Reader ──▶ Transform+Enrich ──▶ Indexer ─▶ ack│ │ │
│  │  └────────────────────────────────────────────────────────┘ │ │
│  │  ┌────────────────────────────────────────────────────────┐ │ │
│  │  │ Thread N: Reader ──▶ Transform+Enrich ──▶ Indexer ─▶ ack│ │ │
│  │  └────────────────────────────────────────────────────────┘ │ │
│  └────────────────────────────────────────────────────────────┘ │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ Enrichment Tables (Arc-shared, loaded at startup)        │   │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐         │   │
│  │  │ geo_ip     │  │ users      │  │ threats    │         │   │
│  │  │ (CIDR)     │  │ (Exact)    │  │ (CIDR)     │         │   │
│  │  └────────────┘  └────────────┘  └────────────┘         │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
paraflow-extreme/
├── crates/
│   ├── core/
│   │   ├── pf-types/       # Core types (WorkItem, Batch, Checkpoint)
│   │   ├── pf-error/       # Error types and classification
│   │   └── pf-traits/      # Core traits (WorkQueue, Reader, Indexer)
│   ├── queue/
│   │   ├── pf-queue-memory/  # In-memory queue (dev/test)
│   │   └── pf-queue-sqs/     # AWS SQS queue (production)
│   ├── reader/
│   │   ├── pf-reader-parquet/  # Streaming Parquet reader
│   │   └── pf-reader-ndjson/   # Streaming NDJSON reader
│   ├── indexer/
│   │   ├── pf-indexer-es/      # Elasticsearch bulk indexer
│   │   └── pf-indexer-stdout/  # Stdout for debugging
│   ├── transform/
│   │   ├── pf-transform/     # Rhai transform engine
│   │   └── pf-enrichment/    # Enrichment tables
│   ├── orchestration/
│   │   ├── pf-file-processor/  # Per-file processing
│   │   ├── pf-worker/          # Worker pool management
│   │   └── pf-discoverer/      # S3 file discovery
│   └── support/
│       ├── pf-accumulator/   # Batch accumulator
│       ├── pf-metrics/       # Prometheus metrics
│       ├── pf-arrow-utils/   # Arrow helpers
│       └── pf-dlq/           # DLQ processors
├── cli/                      # CLI binary
└── tests/                    # Integration tests
```

## Building

```bash
# Build all crates
cargo build --release

# Run tests
cargo test

# Run benchmarks
cargo bench
```

## Configuration

Configuration via YAML or CLI arguments:

```yaml
# worker.yaml
worker:
  id: ${HOSTNAME:-worker-1}
  threads: 8

queue:
  type: sqs
  sqs:
    queue_url: ${SQS_QUEUE_URL}
    visibility_timeout_secs: 300

reader:
  type: parquet
  batch_size: 2000

indexer:
  type: elasticsearch
  elasticsearch:
    endpoint: ${ES_ENDPOINT}
    index: ${ES_INDEX}
    bulk_size_mb: 10
```

## License

Apache-2.0
