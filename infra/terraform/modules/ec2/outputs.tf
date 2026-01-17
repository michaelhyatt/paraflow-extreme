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

output "worker_instance_id" {
  description = "Instance ID of the worker EC2 instance"
  value       = aws_instance.worker.id
}

output "worker_private_ip" {
  description = "Private IP of the worker EC2 instance"
  value       = aws_instance.worker.private_ip
}

output "worker_public_ip" {
  description = "Public IP of the worker EC2 instance (if assigned)"
  value       = aws_instance.worker.public_ip
}

output "security_group_id" {
  description = "ID of the security group"
  value       = aws_security_group.paraflow.id
}
