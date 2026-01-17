# SQS Queue Module
# Creates main work queue and dead letter queue for Paraflow job processing

resource "aws_sqs_queue" "dlq" {
  name                       = "paraflow-${var.job_id}-dlq"
  message_retention_seconds  = 1209600 # 14 days
  visibility_timeout_seconds = 300

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
  name                       = "paraflow-${var.job_id}"
  visibility_timeout_seconds = var.visibility_timeout
  message_retention_seconds  = var.message_retention_seconds
  receive_wait_time_seconds  = var.receive_wait_time_seconds

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = var.max_receive_count
  })

  tags = merge(
    {
      Name        = "paraflow-${var.job_id}"
      JobId       = var.job_id
      Environment = var.environment
      Purpose     = "work-queue"
    },
    var.tags
  )
}
