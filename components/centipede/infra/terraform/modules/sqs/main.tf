# SQS Queue Module
# Creates main work queue and dead letter queue for Paraflow job processing
# Both queues are created in parallel, then redrive policy is attached

resource "aws_sqs_queue" "dlq" {
  name                       = "paraflow-${var.job_id}-dlq"
  message_retention_seconds  = 1209600 # 14 days
  visibility_timeout_seconds = 600

  tags = merge(
    {
      Name        = "paraflow-${var.job_id}-dlq"
      JobId       = var.job_id
      Environment = var.environment
      Purpose     = "dead-letter-queue"
    },
    var.tags
  )
}

resource "aws_sqs_queue" "main" {
  name                       = "paraflow-${var.job_id}-queue"
  visibility_timeout_seconds = var.visibility_timeout
  message_retention_seconds  = var.message_retention_seconds
  receive_wait_time_seconds  = var.receive_wait_time_seconds

  # Note: redrive_policy moved to separate resource to allow parallel queue creation

  tags = merge(
    {
      Name        = "paraflow-${var.job_id}-queue"
      JobId       = var.job_id
      Environment = var.environment
      Purpose     = "work-queue"
    },
    var.tags
  )
}

# Attach redrive policy after both queues are created
resource "aws_sqs_queue_redrive_policy" "main" {
  queue_url = aws_sqs_queue.main.id
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = var.max_receive_count
  })
}
