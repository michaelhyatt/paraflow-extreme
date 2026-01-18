# Example Terraform variables for Paraflow deployment
# Based on NOAA GHCN dataset processing

# Job Configuration
job_id      = "noaa-ghcn-job"
environment = "dev"

# Network Configuration (LogStream VPC with proper internet connectivity)
vpc_id = "vpc-0a99600ed2fa7d002"

# Source Data (NOAA Global Historical Climatology Network)
source_bucket = "noaa-ghcn-pds"
# source_prefix = "parquet/by_year/YEAR=2014/"
file_pattern  = "*.parquet"
max_files     = 20000

# Partitioning (for date-based discovery)
partitioning = "parquet/by_year/YEAR=$${_time:%Y}/"
filter       = "_time=2000-01-01..2026-01-05"

# Docker Image
image_tag = "latest"

# EC2 Instance Types (ARM-based for cost efficiency)
discoverer_instance_type = "t4g.small"  # 2 vCPU, 2GB RAM
worker_instance_type     = "t4g.medium" # 2 vCPU, 4GB RAM

# Worker Configuration
worker_threads = 0  # 0 = auto-detect from CPU cores
batch_size     = 10000

# SQS Configuration
sqs_visibility_timeout = 300 # 5 minutes
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
bootstrap_timeout_seconds = 600  # 10 minutes

# Enable benchmark mode to collect performance metrics
# Records to /var/log/benchmark-metrics.json on each instance
benchmark_mode = false

# ============================================================================
# Recommended Instance Types for Different Workloads
# ============================================================================
#
# Cost-Efficient (Small datasets, <1M records):
#   discoverer_instance_type = "t4g.small"   # $0.0168/hr
#   worker_instance_type     = "t4g.small"   # $0.0168/hr
#
# Balanced (Medium datasets, 1-10M records):
#   discoverer_instance_type = "t4g.small"   # $0.0168/hr
#   worker_instance_type     = "t4g.medium"  # $0.0336/hr
#
# Performance (Large datasets, >10M records):
#   discoverer_instance_type = "t4g.medium"  # $0.0336/hr
#   worker_instance_type     = "c7g.large"   # $0.0725/hr
#
# High-Performance (Very large datasets, >100M records):
#   discoverer_instance_type = "t4g.medium"  # $0.0336/hr
#   worker_instance_type     = "c7g.xlarge"  # $0.145/hr
#
# See benchmark results in docs/benchmarks/ for detailed comparisons
