output "log_group_name" {
  description = "Name of the CloudWatch log group for this job"
  value       = aws_cloudwatch_log_group.job.name
}

output "log_group_arn" {
  description = "ARN of the CloudWatch log group for this job"
  value       = aws_cloudwatch_log_group.job.arn
}
