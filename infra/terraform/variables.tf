# ============================================================================
# General Configuration
# ============================================================================

variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "job_id" {
  description = "Unique identifier for the job"
  type        = string
}

# ============================================================================
# ECR Configuration
# ============================================================================

variable "ecr_repository_name" {
  description = "Name of the existing ECR repository"
  type        = string
  default     = "paraflow-extreme"
}

variable "image_tag" {
  description = "Docker image tag to deploy"
  type        = string
  default     = "latest"
}

# ============================================================================
# Network Configuration
# ============================================================================

variable "vpc_id" {
  description = "VPC ID (leave empty to use default VPC)"
  type        = string
  default     = ""
}

variable "enable_ssh" {
  description = "Enable SSH access to EC2 instances"
  type        = bool
  default     = false
}

variable "ssh_cidr_blocks" {
  description = "CIDR blocks allowed for SSH access"
  type        = list(string)
  default     = []
}

variable "key_name" {
  description = "EC2 key pair name for SSH access"
  type        = string
  default     = null
}

# ============================================================================
# EC2 Instance Configuration
# ============================================================================

variable "discoverer_instance_type" {
  description = "EC2 instance type for discoverer"
  type        = string
  default     = "t4g.small" # ARM-based, 2 vCPU, 2GB RAM
}

variable "worker_instance_type" {
  description = "EC2 instance type for worker"
  type        = string
  default     = "t4g.medium" # ARM-based, 2 vCPU, 4GB RAM
}

# ============================================================================
# Source Data Configuration
# ============================================================================

variable "source_bucket" {
  description = "S3 bucket containing source data"
  type        = string
}

variable "source_prefix" {
  description = "S3 prefix to scan for files"
  type        = string
  default     = ""
}

variable "file_pattern" {
  description = "File pattern to match (e.g., *.parquet)"
  type        = string
  default     = "*"
}

variable "max_files" {
  description = "Maximum number of files to discover (0 = unlimited)"
  type        = number
  default     = 0
}

variable "partitioning" {
  description = "Partitioning pattern for date-based file discovery (e.g., 'parquet/by_year/YEAR=$${_time:%Y}/')"
  type        = string
  default     = ""
}

variable "filter" {
  description = "Time-based filter for partitioned data (e.g., '_time=2012-01-01..2026-01-05')"
  type        = string
  default     = ""
}

# ============================================================================
# SQS Configuration
# ============================================================================

variable "sqs_visibility_timeout" {
  description = "SQS visibility timeout in seconds"
  type        = number
  default     = 300 # 5 minutes
}

variable "sqs_message_retention_seconds" {
  description = "SQS message retention period in seconds"
  type        = number
  default     = 345600 # 4 days
}

variable "sqs_receive_wait_time_seconds" {
  description = "SQS long-polling wait time in seconds (1-20)"
  type        = number
  default     = 20
}

variable "sqs_max_receive_count" {
  description = "Number of times a message can be received before DLQ"
  type        = number
  default     = 3
}

# ============================================================================
# Worker Configuration
# ============================================================================

variable "worker_threads" {
  description = "Number of processing threads for worker (0 = auto-detect from CPU cores)"
  type        = number
  default     = 0
}

variable "batch_size" {
  description = "Batch size for record processing"
  type        = number
  default     = 10000
}

# ============================================================================
# CloudWatch Configuration
# ============================================================================

variable "log_retention_days" {
  description = "CloudWatch log retention in days"
  type        = number
  default     = 30
}

# ============================================================================
# Monitoring and Benchmark Configuration
# ============================================================================

variable "enable_detailed_monitoring" {
  description = "Enable CloudWatch agent for detailed CPU, memory, disk, and network metrics. Installs CloudWatch agent on instances."
  type        = bool
  default     = true
}

variable "bootstrap_timeout_seconds" {
  description = "Maximum time allowed for EC2 instance bootstrap before timeout. Increase for larger instances or slower networks."
  type        = number
  default     = 600 # 10 minutes
}

variable "benchmark_mode" {
  description = "Enable benchmark mode for collecting performance metrics. Records timing and throughput data to /var/log/benchmark-metrics.json"
  type        = bool
  default     = false
}
