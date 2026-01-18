# ============================================================================
# Job Configuration Outputs
# ============================================================================

output "job_id" {
  description = "Job ID used for resource tagging"
  value       = var.job_id
}

output "aws_region" {
  description = "AWS region where resources are deployed"
  value       = var.aws_region
}

# ============================================================================
# ECR Outputs
# ============================================================================

output "ecr_repository_url" {
  description = "URL of the ECR repository"
  value       = module.shared.ecr_repository_url
}

# ============================================================================
# SQS Outputs
# ============================================================================

output "sqs_queue_url" {
  description = "URL of the SQS work queue"
  value       = module.sqs.queue_url
}

output "sqs_queue_arn" {
  description = "ARN of the SQS work queue"
  value       = module.sqs.queue_arn
}

output "sqs_dlq_url" {
  description = "URL of the SQS dead letter queue"
  value       = module.sqs.dlq_url
}

# ============================================================================
# EC2 Outputs
# ============================================================================

output "discoverer_instance_id" {
  description = "Instance ID of the discoverer EC2 instance"
  value       = module.ec2.discoverer_instance_id
}

output "discoverer_private_ip" {
  description = "Private IP of the discoverer EC2 instance"
  value       = module.ec2.discoverer_private_ip
}

output "worker_instance_id" {
  description = "Instance ID of the worker EC2 instance"
  value       = module.ec2.worker_instance_id
}

output "worker_private_ip" {
  description = "Private IP of the worker EC2 instance"
  value       = module.ec2.worker_private_ip
}

# ============================================================================
# CloudWatch Outputs
# ============================================================================

output "log_group_name" {
  description = "Name of the CloudWatch log group"
  value       = module.job_logs.log_group_name
}
