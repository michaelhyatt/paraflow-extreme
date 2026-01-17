#!/bin/bash
set -ex

# Log output to CloudWatch
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

echo "Starting Paraflow Discoverer setup..."

# Install Docker
dnf install -y docker
systemctl enable docker
systemctl start docker

# Login to ECR
aws ecr get-login-password --region ${aws_region} | docker login --username AWS --password-stdin ${ecr_repository}

# Pull the discoverer image
docker pull ${ecr_repository}:${image_tag}

# Run the discoverer
echo "Starting pf-discoverer..."
docker run --rm \
  --name pf-discoverer \
  -e AWS_REGION=${aws_region} \
  ${ecr_repository}:${image_tag} \
  pf-discoverer \
  --bucket ${source_bucket} \
  --prefix "${source_prefix}" \
  --destination sqs \
  --sqs-queue-url ${sqs_queue_url} \
  --pattern "${file_pattern}" \
%{ if max_files > 0 ~}
  --max-files ${max_files} \
%{ endif ~}
  --region ${aws_region} \
  --progress \
  --log-level info

echo "Discoverer completed successfully"
