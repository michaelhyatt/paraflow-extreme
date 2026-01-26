# CloudWatch Log Group for Per-Job Logging
# Creates a dedicated log group for each job with configurable retention.

resource "aws_cloudwatch_log_group" "job" {
  name              = "/paraflow/jobs/${var.job_id}"
  retention_in_days = var.log_retention_days

  tags = merge(
    {
      Name        = "paraflow-job-${var.job_id}"
      JobId       = var.job_id
      Environment = var.environment
    },
    var.tags
  )
}
