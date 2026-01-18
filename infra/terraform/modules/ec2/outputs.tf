output "discoverer_instance_id" {
  description = "Instance ID of the discoverer EC2 instance"
  value       = aws_instance.discoverer.id
}

output "discoverer_private_ip" {
  description = "Private IP of the discoverer EC2 instance"
  value       = aws_instance.discoverer.private_ip
}

output "discoverer_public_ip" {
  description = "Public IP of the discoverer EC2 instance (if assigned)"
  value       = aws_instance.discoverer.public_ip
}

output "worker_instance_ids" {
  description = "Instance IDs of the worker EC2 instances"
  value       = aws_instance.worker[*].id
}

output "worker_private_ips" {
  description = "Private IPs of the worker EC2 instances"
  value       = aws_instance.worker[*].private_ip
}

output "worker_public_ips" {
  description = "Public IPs of the worker EC2 instances (if assigned)"
  value       = aws_instance.worker[*].public_ip
}

# Backwards compatibility - return first worker's info (empty string if no workers)
output "worker_instance_id" {
  description = "Instance ID of the first worker EC2 instance (for backwards compatibility)"
  value       = length(aws_instance.worker) > 0 ? aws_instance.worker[0].id : ""
}

output "worker_private_ip" {
  description = "Private IP of the first worker EC2 instance (for backwards compatibility)"
  value       = length(aws_instance.worker) > 0 ? aws_instance.worker[0].private_ip : ""
}

output "worker_public_ip" {
  description = "Public IP of the first worker EC2 instance (for backwards compatibility)"
  value       = length(aws_instance.worker) > 0 ? aws_instance.worker[0].public_ip : ""
}

output "worker_count" {
  description = "Number of worker instances deployed"
  value       = var.worker_count
}

output "security_group_id" {
  description = "ID of the security group"
  value       = aws_security_group.paraflow.id
}

output "iam_role_arn" {
  description = "ARN of the IAM role for EC2 instances"
  value       = aws_iam_role.paraflow.arn
}

output "iam_instance_profile_name" {
  description = "Name of the IAM instance profile"
  value       = aws_iam_instance_profile.paraflow.name
}
