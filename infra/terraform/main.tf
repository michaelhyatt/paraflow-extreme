# Paraflow Infrastructure
# Deploys Paraflow discoverer and worker as Docker containers on EC2 with SQS queue

# ============================================================================
# Data Sources
# ============================================================================

# Get default VPC (or specify vpc_id variable to use a specific VPC)
data "aws_vpc" "selected" {
  id      = var.vpc_id != "" ? var.vpc_id : null
  default = var.vpc_id == ""
}

# Get subnets in the VPC
data "aws_subnets" "selected" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.selected.id]
  }
}

# ============================================================================
# Shared Infrastructure
# ============================================================================

module "shared" {
  source = "./modules/shared"

  aws_region          = var.aws_region
  environment         = var.environment
  ecr_repository_name = var.ecr_repository_name
}

# ============================================================================
# Job Infrastructure
# ============================================================================

# CloudWatch Log Group
module "job_logs" {
  source = "./modules/job"

  job_id             = var.job_id
  environment        = var.environment
  log_retention_days = var.log_retention_days
}

# SQS Queues
module "sqs" {
  source = "./modules/sqs"

  job_id                    = var.job_id
  environment               = var.environment
  visibility_timeout        = var.sqs_visibility_timeout
  message_retention_seconds = var.sqs_message_retention_seconds
  receive_wait_time_seconds = var.sqs_receive_wait_time_seconds
  max_receive_count         = var.sqs_max_receive_count
}

# EC2 Instances (using default EC2 IAM role)
module "ec2" {
  source = "./modules/ec2"

  job_id      = var.job_id
  environment = var.environment
  aws_region  = var.aws_region

  # Network
  vpc_id    = data.aws_vpc.selected.id
  subnet_id = data.aws_subnets.selected.ids[0]

  # SSH (optional)
  enable_ssh      = var.enable_ssh
  ssh_cidr_blocks = var.ssh_cidr_blocks
  key_name        = var.key_name

  # ECR
  ecr_repository = module.shared.ecr_repository_url
  image_tag      = var.image_tag

  # Instance types
  discoverer_instance_type = var.discoverer_instance_type
  worker_instance_type     = var.worker_instance_type

  # Source configuration
  source_bucket = var.source_bucket
  source_prefix = var.source_prefix
  file_pattern  = var.file_pattern
  max_files     = var.max_files

  # SQS
  sqs_queue_url = module.sqs.queue_url

  # Worker configuration
  worker_threads = var.worker_threads
  batch_size     = var.batch_size

  # Logging
  log_group_name = module.job_logs.log_group_name

  depends_on = [module.sqs, module.job_logs]
}
