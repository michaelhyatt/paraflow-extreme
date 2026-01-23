# Example Terraform variables for Paraflow deployment
# Based on NOAA GHCN dataset processing

# Prepopulate queue before starting workers
prepopulate_queue = true

# Job Configuration
job_id      = "noaa-ghcn-job"
environment = "dev"

# Network Configuration (LogStream VPC with proper internet connectivity)
vpc_id = "vpc-0a99600ed2fa7d002"

# Source Data (NOAA Global Historical Climatology Network)
source_bucket = "noaa-ghcn-pds"
# source_prefix = "parquet/by_year/YEAR=2014/"
file_pattern = "*.parquet"
max_files    = 20000

# Partitioning (for date-based discovery)
partitioning = "parquet/by_year/YEAR=$${_time:%Y}/"
filter       = "_time=1900-01-01..2026-01-05"

# Docker Image
image_tag = "latest"

# EC2 Instance Types (ARM-based for cost efficiency)
discoverer_instance_type = "t4g.small"  # 2 vCPU, 2GB RAM
worker_instance_type     = "t4g.medium" # 2 vCPU, 4GB RAM

# Worker Configuration
worker_count   = 1 # Number of worker instances (for horizontal scaling)
worker_threads = 0 # 0 = auto-detect from CPU cores
batch_size     = 50000

# Prefetch Configuration (for hiding S3 latency)
# Deeper prefetch buffers reduce convoy stalls when multiple threads hit slow S3 reads
prefetch_count     = 12  # Files to prefetch per thread (default: 6)
prefetch_memory_mb = 400 # Memory budget per thread in MB (default: 200)

# SQS Configuration
# Visibility timeout must accommodate prefetch buffer depth during drain mode:
# With prefetch_count=12 and 16 threads, up to 192 messages can be buffered.
# At worst-case 12s/file processing (network delays), we need ~10 minutes headroom.
sqs_visibility_timeout = 600 # 10 minutes (prevents redeliveries during drain)
sqs_max_receive_count  = 3   # Retries before DLQ

# CloudWatch Logs
log_retention_days = 30

# SSH Access (optional, for debugging)
# enable_ssh      = true
# ssh_cidr_blocks = ["10.0.0.0/8"]
# key_name        = "my-key-pair"

# ============================================================================
# Monitoring and Benchmark Configuration
# ============================================================================

# Enable detailed CloudWatch metrics (CPU, memory, disk, network at 10s intervals)
# Installs CloudWatch agent on instances - recommended for production and benchmarking
enable_detailed_monitoring = true

# Bootstrap timeout - increase for larger instances or slow networks
bootstrap_timeout_seconds = 600 # 10 minutes

# Enable benchmark mode to collect performance metrics
# Records to /var/log/benchmark-metrics.json on each instance
benchmark_mode = false

# ============================================================================
# Profiling Configuration
# ============================================================================

# Enable profiling artifact collection and S3 upload
# Captures tokio runtime metrics (JSONL) and optional CPU flamegraphs (SVG)
enable_profiling = true

# S3 bucket for profiling artifacts (required if enable_profiling=true)
# Artifacts are uploaded to s3://{bucket}/profiling/{job_id}/
artifacts_bucket = "mh-paraflow-terraform-state"

# ============================================================================
# Recommended Instance Types for Different Workloads
# ============================================================================
#
# Cost-Efficient (Small datasets, <1M records):
#   discoverer_instance_type = "t4g.small"   # $0.0168/hr
#   worker_instance_type     = "t4g.small"   # $0.0168/hr
#   worker_count             = 1
#
# Balanced (Medium datasets, 1-10M records):
#   discoverer_instance_type = "t4g.small"   # $0.0168/hr
#   worker_instance_type     = "t4g.medium"  # $0.0336/hr
#   worker_count             = 1
#
# Performance (Large datasets, >10M records):
#   discoverer_instance_type = "t4g.medium"  # $0.0336/hr
#   worker_instance_type     = "t4g.2xlarge" # $0.2688/hr
#   worker_count             = 1
#
# High-Performance (Very large datasets, >100M records):
#   discoverer_instance_type = "t4g.medium"  # $0.0336/hr
#   worker_instance_type     = "c7g.4xlarge" # $0.58/hr (best single-instance throughput)
#   worker_count             = 1
#
# Maximum Cost Efficiency (Best $/record):
#   discoverer_instance_type = "t4g.small"   # $0.0168/hr
#   worker_instance_type     = "t4g.2xlarge" # $0.2688/hr each
#   worker_count             = 2             # 2x t4g.2xlarge is 53% cheaper than 1x c7g.4xlarge
#
# See research/arm64-benchmark-results-2026-01-18.md for detailed comparisons
