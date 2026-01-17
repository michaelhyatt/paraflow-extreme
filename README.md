# Paraflow Extreme

High-throughput data ingestion engine built for maximum performance.

## Overview

Paraflow Extreme is a greenfield implementation designed for TB-scale ingestion at 1+ GB/s per worker. It follows a per-thread pipeline architecture:

```
Reader → Transform+Enrich → Indexer
```

### Key Features

**Implemented:**
- **Zero-copy Arrow data path** - No serialization overhead between stages
- **Per-thread pipelines** - Eliminate synchronization overhead
- **Streaming memory model** - Process 10GB files with ~50MB RAM per thread
- **Streaming readers** - Parquet and NDJSON with gzip/zstd decompression
- **S3 file discovery** - Partitioning, filtering, parallel listing
- **Dual input modes** - SQS for production, stdin for local testing

**Planned:**
- Rhai transforms with enrichment functions
- Elasticsearch bulk indexing
- SQS queue integration (currently stub)

### Target Performance

- **1+ GB/s** per worker throughput
- **TB-scale** ingestion in minutes
- **<100MB RAM** per thread regardless of file size

## Prerequisites

For local development, you need LocalStack running. See [testing/localstack/README.md](testing/localstack/README.md) for setup:

```bash
cd testing/localstack
docker-compose up -d
```

## Quick Start

### Local Development with LocalStack

```bash
# Discover files and pipe to worker for processing
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

# Or discover to stdout only (for debugging)
cargo run -p pf-discoverer-cli -- \
    --bucket test-bucket \
    --s3-endpoint http://localhost:4566 \
    --pattern "*.parquet"
```

### With SQS Queue (Production-like)

```bash
# First, run discoverer to send messages to SQS
cargo run -p pf-discoverer-cli -- \
    --bucket test-bucket \
    --prefix data/ \
    --s3-endpoint http://localhost:4566 \
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
    --threads 4
```

### CLI Options

Run `pf-discoverer --help` and `pf-worker --help` for full options. Key features include:
- Partitioning expressions: `--partitioning 'data/YEAR=${_time:%Y}/'`
- Time-based filtering: `--filter "_time=2024-01-01..2024-01-31"`
- Glob patterns: `--pattern "*.parquet"`

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

Configuration is via CLI arguments. Environment variables are also supported for sensitive values:

```bash
# Worker configuration via CLI arguments
pf-worker \
    --input sqs \
    --sqs-queue-url $SQS_QUEUE_URL \
    --destination stats \
    --threads 8 \
    --batch-size 10000

# Or via environment variables
export PF_SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123/queue
export PF_S3_ENDPOINT=http://localhost:4566
pf-worker --input sqs --destination stats --threads 8
```

### Key Worker Options

| Option | Description | Default |
|--------|-------------|---------|
| `--input` | Input source: `stdin` or `sqs` | `stdin` |
| `--destination` | Output: `stdout` or `stats` | `stdout` |
| `--threads` | Processing threads | CPU count |
| `--batch-size` | Records per batch | 10000 |
| `--s3-endpoint` | Custom S3 endpoint | AWS default |
| `--sqs-endpoint` | Custom SQS endpoint | AWS default |

## Testing

```bash
# Unit tests
cargo test --lib

# Integration tests (requires LocalStack)
cd testing/localstack && docker-compose up -d
cargo test --test '*'
```

## License

Apache-2.0
