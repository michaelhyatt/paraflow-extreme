variable "job_id" {
  description = "Unique identifier for the job"
  type        = string
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "visibility_timeout" {
  description = "Visibility timeout in seconds (time for worker to process message)"
  type        = number
  default     = 300 # 5 minutes
}

variable "message_retention_seconds" {
  description = "Message retention period in seconds"
  type        = number
  default     = 345600 # 4 days
}

variable "receive_wait_time_seconds" {
  description = "Long-polling wait time in seconds (1-20)"
  type        = number
  default     = 20
}

variable "max_receive_count" {
  description = "Number of times a message can be received before moving to DLQ"
  type        = number
  default     = 3
}

variable "tags" {
  description = "Additional tags for resources"
  type        = map(string)
  default     = {}
}
