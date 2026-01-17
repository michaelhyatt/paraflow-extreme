output "ecr_repository_url" {
  description = "URL of the ECR repository"
  value       = data.aws_ecr_repository.paraflow.repository_url
}

output "ecr_repository_arn" {
  description = "ARN of the ECR repository"
  value       = data.aws_ecr_repository.paraflow.arn
}
