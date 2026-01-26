# Shared Infrastructure Module
# References existing resources (ECR) rather than creating new ones.
# VPC configuration is deferred to Phase 6 (Dedicated VPC).

# Reference the existing ECR repository (created manually or via release workflow)
data "aws_ecr_repository" "paraflow" {
  name = var.ecr_repository_name
}
