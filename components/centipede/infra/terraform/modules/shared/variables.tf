variable "aws_region" {
  description = "AWS region for resources"
  type        = string
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
}

variable "ecr_repository_name" {
  description = "Name of the existing ECR repository"
  type        = string
}
