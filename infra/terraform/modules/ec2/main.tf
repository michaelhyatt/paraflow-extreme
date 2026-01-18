# EC2 Module
# Creates EC2 instances for Paraflow discoverer and worker with Docker

# ============================================================================
# IAM Role and Instance Profile
# ============================================================================

data "aws_caller_identity" "current" {}

resource "aws_iam_role" "paraflow" {
  name = "paraflow-${var.job_id}-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })

  tags = merge(
    {
      Name        = "paraflow-${var.job_id}-ec2-role"
      JobId       = var.job_id
      Environment = var.environment
    },
    var.tags
  )
}

resource "aws_iam_role_policy" "paraflow" {
  name = "paraflow-${var.job_id}-policy"
  role = aws_iam_role.paraflow.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # ECR - pull images
      {
        Effect = "Allow"
        Action = [
          "ecr:GetAuthorizationToken"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "ecr:BatchCheckLayerAvailability",
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage"
        ]
        Resource = "arn:aws:ecr:${var.aws_region}:${data.aws_caller_identity.current.account_id}:repository/*"
      },
      # S3 - read source data
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.source_bucket}",
          "arn:aws:s3:::${var.source_bucket}/*"
        ]
      },
      # SQS - send and receive messages
      {
        Effect = "Allow"
        Action = [
          "sqs:SendMessage",
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:GetQueueUrl"
        ]
        Resource = var.sqs_queue_arn
      },
      # CloudWatch Logs - write logs
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "logs:DescribeLogStreams"
        ]
        Resource = "arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:log-group:${var.log_group_name}:*"
      },
      # CloudWatch Metrics - publish custom metrics
      {
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData"
        ]
        Resource = "*"
        Condition = {
          StringEquals = {
            "cloudwatch:namespace" = "Paraflow/${var.job_id}"
          }
        }
      },
      # SSM - for troubleshooting access
      {
        Effect = "Allow"
        Action = [
          "ssm:UpdateInstanceInformation",
          "ssmmessages:CreateControlChannel",
          "ssmmessages:CreateDataChannel",
          "ssmmessages:OpenControlChannel",
          "ssmmessages:OpenDataChannel"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_instance_profile" "paraflow" {
  name = "paraflow-${var.job_id}-profile"
  role = aws_iam_role.paraflow.name

  tags = merge(
    {
      Name        = "paraflow-${var.job_id}-profile"
      JobId       = var.job_id
      Environment = var.environment
    },
    var.tags
  )
}

# ============================================================================
# AMI Data Source - ECS-Optimized AL2023 (has Docker pre-installed)
# ============================================================================

data "aws_ami" "ecs_optimized" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-ecs-hvm-*-arm64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  filter {
    name   = "architecture"
    values = ["arm64"]
  }
}

# ============================================================================
# Security Group
# ============================================================================

resource "aws_security_group" "paraflow" {
  name        = "paraflow-${var.job_id}"
  description = "Security group for Paraflow EC2 instances"
  vpc_id      = var.vpc_id

  # Allow all outbound traffic (for S3, SQS, ECR access)
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # SSH access (optional, for debugging)
  dynamic "ingress" {
    for_each = var.enable_ssh ? [1] : []
    content {
      from_port   = 22
      to_port     = 22
      protocol    = "tcp"
      cidr_blocks = var.ssh_cidr_blocks
    }
  }

  tags = merge(
    {
      Name        = "paraflow-${var.job_id}"
      JobId       = var.job_id
      Environment = var.environment
    },
    var.tags
  )
}

# ============================================================================
# Discoverer Instance
# ============================================================================

resource "aws_instance" "discoverer" {
  ami                    = data.aws_ami.ecs_optimized.id
  instance_type          = var.discoverer_instance_type
  subnet_id              = var.subnet_id
  vpc_security_group_ids = [aws_security_group.paraflow.id]
  iam_instance_profile   = aws_iam_instance_profile.paraflow.name
  key_name               = var.key_name

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
    encrypted   = true
  }

  user_data = base64encode(templatefile("${path.module}/user_data_discoverer.sh.tpl", {
    aws_region                  = var.aws_region
    ecr_repository              = var.ecr_repository
    image_tag                   = var.image_tag
    source_bucket               = var.source_bucket
    source_prefix               = var.source_prefix
    sqs_queue_url               = var.sqs_queue_url
    file_pattern                = var.file_pattern
    max_files                   = var.max_files
    log_group_name              = var.log_group_name
    job_id                      = var.job_id
    enable_detailed_monitoring  = var.enable_detailed_monitoring
    bootstrap_timeout_seconds   = var.bootstrap_timeout_seconds
    benchmark_mode              = var.benchmark_mode
  }))

  tags = merge(
    {
      Name        = "paraflow-${var.job_id}-discoverer"
      JobId       = var.job_id
      Environment = var.environment
      Component   = "discoverer"
    },
    var.tags
  )
}

# ============================================================================
# Worker Instance
# ============================================================================

resource "aws_instance" "worker" {
  ami                    = data.aws_ami.ecs_optimized.id
  instance_type          = var.worker_instance_type
  subnet_id              = var.subnet_id
  vpc_security_group_ids = [aws_security_group.paraflow.id]
  iam_instance_profile   = aws_iam_instance_profile.paraflow.name
  key_name               = var.key_name

  root_block_device {
    volume_size = 50
    volume_type = "gp3"
    encrypted   = true
  }

  user_data = base64encode(templatefile("${path.module}/user_data_worker.sh.tpl", {
    aws_region                  = var.aws_region
    ecr_repository              = var.ecr_repository
    image_tag                   = var.image_tag
    sqs_queue_url               = var.sqs_queue_url
    worker_threads              = var.worker_threads
    batch_size                  = var.batch_size
    log_group_name              = var.log_group_name
    job_id                      = var.job_id
    enable_detailed_monitoring  = var.enable_detailed_monitoring
    bootstrap_timeout_seconds   = var.bootstrap_timeout_seconds
    benchmark_mode              = var.benchmark_mode
  }))

  tags = merge(
    {
      Name        = "paraflow-${var.job_id}-worker"
      JobId       = var.job_id
      Environment = var.environment
      Component   = "worker"
    },
    var.tags
  )
}
