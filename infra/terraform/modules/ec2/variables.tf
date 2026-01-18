variable "job_id" {
  description = "Unique identifier for the job"
  type        = string
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID for the EC2 instances"
  type        = string
}

variable "subnet_id" {
  description = "Subnet ID for the EC2 instances"
  type        = string
}

variable "key_name" {
  description = "EC2 key pair name for SSH access (optional)"
  type        = string
  default     = null
}

variable "enable_ssh" {
  description = "Enable SSH access to instances"
  type        = bool
  default     = false
}

variable "ssh_cidr_blocks" {
  description = "CIDR blocks allowed for SSH access"
  type        = list(string)
  default     = []
}

# ECR Configuration
variable "ecr_repository" {
  description = "Full ECR repository URL (e.g., 123456789.dkr.ecr.us-east-1.amazonaws.com/paraflow-extreme)"
  type        = string
}

variable "image_tag" {
  description = "Docker image tag to deploy"
  type        = string
  default     = "latest"
}

# Instance Types
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

# Source Configuration
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

# SQS Configuration
variable "sqs_queue_url" {
  description = "URL of the SQS work queue"
  type        = string
}

variable "sqs_queue_arn" {
  description = "ARN of the SQS work queue (for IAM permissions)"
  type        = string
}

# Worker Configuration
variable "worker_threads" {
  description = "Number of processing threads for worker"
  type        = number
  default     = 2
}

variable "batch_size" {
  description = "Batch size for record processing"
  type        = number
  default     = 10000
}

# Logging
variable "log_group_name" {
  description = "CloudWatch log group name"
  type        = string
}

variable "tags" {
  description = "Additional tags for resources"
  type        = map(string)
  default     = {}
}

# ============================================================================
# Monitoring and Benchmark Configuration
# ============================================================================

variable "enable_detailed_monitoring" {
  description = "Enable CloudWatch agent for detailed CPU, memory, disk, and network metrics"
  type        = bool
  default     = false
}

variable "bootstrap_timeout_seconds" {
  description = "Maximum time allowed for EC2 instance bootstrap before timeout"
  type        = number
  default     = 600 # 10 minutes
}

variable "benchmark_mode" {
  description = "Enable benchmark mode for collecting performance metrics"
  type        = bool
  default     = false
}
