#!/bin/bash
set -ex

# Log output to CloudWatch
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

echo "Starting Paraflow Worker setup..."

# Install Docker
dnf install -y docker
systemctl enable docker
systemctl start docker

# Login to ECR
aws ecr get-login-password --region ${aws_region} | docker login --username AWS --password-stdin ${ecr_repository}

# Pull the worker image
docker pull ${ecr_repository}:${image_tag}

# Run the worker
echo "Starting pf-worker..."
docker run --rm \
  --name pf-worker \
  -e AWS_REGION=${aws_region} \
  ${ecr_repository}:${image_tag} \
  pf-worker \
  --input sqs \
  --sqs-queue-url ${sqs_queue_url} \
  --destination stats \
  --threads ${worker_threads} \
  --batch-size ${batch_size} \
  --sqs-drain \
  --region ${aws_region} \
  --progress \
  --log-level info

echo "Worker completed successfully"
