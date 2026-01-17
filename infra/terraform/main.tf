# Paraflow Infrastructure
# This configuration uses the default VPC and references the existing ECR repository.

module "shared" {
  source = "./modules/shared"

  aws_region          = var.aws_region
  environment         = var.environment
  ecr_repository_name = var.ecr_repository_name
}
