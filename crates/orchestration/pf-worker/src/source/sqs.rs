//! SQS work source implementation.

use super::parse_work_item;
use async_trait::async_trait;
use aws_sdk_sqs::Client;
use aws_sdk_sqs::types::QueueAttributeName;
use chrono::Utc;
use pf_error::{PfError, QueueError, Result};
use pf_traits::{FailureContext, QueueMessage, WorkQueue};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use tracing::{debug, error, info, warn};

/// Configuration for the SQS source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqsSourceConfig {
    /// SQS queue URL
    pub queue_url: String,

    /// DLQ URL (if separate from main queue's redrive policy)
    pub dlq_url: Option<String>,

    /// Long-polling wait time in seconds (1-20)
    pub wait_time_seconds: i32,

    /// Visibility timeout in seconds
    pub visibility_timeout: i32,

    /// Maximum number of messages to receive per batch (1-10)
    pub max_batch_size: i32,

    /// Drain mode: exit when queue is empty (for batch processing).
    /// When enabled, the source will signal completion after receiving
    /// no messages for one full polling cycle.
    pub drain_mode: bool,
}

impl SqsSourceConfig {
    /// Create a new SQS source configuration.
    pub fn new(queue_url: impl Into<String>) -> Self {
        Self {
            queue_url: queue_url.into(),
            dlq_url: None,
            wait_time_seconds: 20,
            visibility_timeout: 300, // 5 minutes
            max_batch_size: 10,
            drain_mode: false,
        }
    }

    /// Set the DLQ URL.
    pub fn with_dlq_url(mut self, url: impl Into<String>) -> Self {
        self.dlq_url = Some(url.into());
        self
    }

    /// Set the long-polling wait time.
    pub fn with_wait_time(mut self, seconds: i32) -> Self {
        self.wait_time_seconds = seconds.clamp(1, 20);
        self
    }

    /// Set the visibility timeout.
    pub fn with_visibility_timeout(mut self, seconds: i32) -> Self {
        self.visibility_timeout = seconds;
        self
    }

    /// Set the maximum batch size.
    pub fn with_max_batch_size(mut self, size: i32) -> Self {
        self.max_batch_size = size.clamp(1, 10);
        self
    }

    /// Enable drain mode (exit when queue is empty).
    pub fn with_drain_mode(mut self, enabled: bool) -> Self {
        self.drain_mode = enabled;
        self
    }
}

/// Work source that receives WorkItems from an AWS SQS queue.
///
/// Supports two message formats:
///
/// 1. **Full WorkItem format** (for production use with complete configuration):
///    ```json
///    {"job_id":"job-1","file_uri":"s3://bucket/file.parquet","file_size_bytes":1024,...}
///    ```
///
/// 2. **DiscoveredFile format** (from pf-discoverer):
///    ```json
///    {"uri":"s3://bucket/file.parquet","size_bytes":1024,"last_modified":"2024-01-01T00:00:00Z"}
///    ```
pub struct SqsSource {
    /// SQS client
    client: Client,

    /// Configuration
    config: SqsSourceConfig,

    /// Whether to stop receiving (for graceful shutdown)
    stopped: AtomicBool,

    /// Counter for consecutive empty receives (for drain mode)
    empty_receive_count: AtomicU32,
}

impl SqsSource {
    /// Create a new SQS source.
    pub fn new(client: Client, config: SqsSourceConfig) -> Self {
        Self {
            client,
            config,
            stopped: AtomicBool::new(false),
            empty_receive_count: AtomicU32::new(0),
        }
    }

    /// Create an SQS source with default AWS configuration.
    pub async fn from_config(config: SqsSourceConfig) -> Result<Self> {
        let aws_config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = Client::new(&aws_config);
        Ok(Self::new(client, config))
    }

    /// Create an SQS source with a custom endpoint (for LocalStack).
    pub async fn from_config_with_endpoint(
        config: SqsSourceConfig,
        endpoint_url: &str,
        region: &str,
    ) -> Result<Self> {
        let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_sdk_sqs::config::Region::new(region.to_string()))
            .endpoint_url(endpoint_url)
            .load()
            .await;
        let client = Client::new(&aws_config);
        Ok(Self::new(client, config))
    }

    /// Signal the source to stop receiving.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Relaxed);
    }

    /// Check if queue is truly empty (no visible + no in-flight messages).
    ///
    /// This is used in drain mode to verify the queue is actually empty before
    /// signaling completion. This prevents premature termination when multiple
    /// workers are processing and one worker receives an empty batch while
    /// messages are still in-flight with other workers.
    async fn is_queue_empty(&self) -> Result<bool> {
        let response = self
            .client
            .get_queue_attributes()
            .queue_url(&self.config.queue_url)
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessagesNotVisible)
            .send()
            .await
            .map_err(|e| {
                PfError::Queue(QueueError::Receive(format!(
                    "GetQueueAttributes failed: {}",
                    e
                )))
            })?;

        let attrs = response.attributes.unwrap_or_default();
        let visible: i64 = attrs
            .get(&QueueAttributeName::ApproximateNumberOfMessages)
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let in_flight: i64 = attrs
            .get(&QueueAttributeName::ApproximateNumberOfMessagesNotVisible)
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);

        debug!(
            visible = visible,
            in_flight = in_flight,
            "Queue status check"
        );
        Ok(visible == 0 && in_flight == 0)
    }
}

#[async_trait]
impl WorkQueue for SqsSource {
    async fn receive_batch(&self, max: usize) -> Result<Option<Vec<QueueMessage>>> {
        if self.stopped.load(Ordering::Relaxed) {
            return Ok(None);
        }

        let batch_size = (max as i32).min(self.config.max_batch_size);

        let response = self
            .client
            .receive_message()
            .queue_url(&self.config.queue_url)
            .max_number_of_messages(batch_size)
            .wait_time_seconds(self.config.wait_time_seconds)
            .visibility_timeout(self.config.visibility_timeout)
            .message_system_attribute_names(
                aws_sdk_sqs::types::MessageSystemAttributeName::ApproximateReceiveCount,
            )
            .send()
            .await
            .map_err(|e| {
                PfError::Queue(QueueError::Receive(format!("SQS receive failed: {}", e)))
            })?;

        let sqs_messages = response.messages.unwrap_or_default();
        debug!("Received {} messages from SQS", sqs_messages.len());

        // Handle drain mode: exit when queue is truly empty
        if sqs_messages.is_empty() {
            if self.config.drain_mode {
                let count = self.empty_receive_count.fetch_add(1, Ordering::Relaxed) + 1;
                debug!("Empty receive #{} in drain mode", count);

                // After empty long-poll, verify queue is truly empty
                // This prevents premature termination when multiple workers are active
                // and one gets an empty batch while messages are in-flight with others
                match self.is_queue_empty().await {
                    Ok(true) => {
                        info!("Queue drained (verified empty), signaling completion");
                        self.stopped.store(true, Ordering::Relaxed);
                        return Ok(None);
                    }
                    Ok(false) => {
                        // Messages are in-flight with other workers. However, if we've had
                        // many consecutive empty receives, those workers may have crashed
                        // and their messages will become visible after visibility timeout.
                        // With 20s long-poll and 300s visibility timeout, 15+ empty receives
                        // means we've waited long enough for orphaned messages to reappear.
                        let max_empty_receives =
                            (self.config.visibility_timeout / self.config.wait_time_seconds) + 1;
                        if count >= max_empty_receives as u32 {
                            warn!(
                                "Exceeded {} empty receives with in-flight messages, assuming orphaned - exiting",
                                max_empty_receives
                            );
                            self.stopped.store(true, Ordering::Relaxed);
                            return Ok(None);
                        }
                        debug!(
                            "Queue has in-flight messages, continuing to poll ({}/{})",
                            count, max_empty_receives
                        );
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to check queue status, continuing to poll");
                    }
                }
            }
            return Ok(Some(vec![]));
        }

        // Reset empty receive counter when we get messages
        self.empty_receive_count.store(0, Ordering::Relaxed);

        let mut messages = Vec::with_capacity(sqs_messages.len());

        for msg in sqs_messages {
            let receipt_handle = msg.receipt_handle.unwrap_or_default();
            let body = msg.body.unwrap_or_default();

            // Parse receive count from attributes
            let receive_count = msg
                .attributes
                .as_ref()
                .and_then(|attrs| {
                    attrs.get(
                        &aws_sdk_sqs::types::MessageSystemAttributeName::ApproximateReceiveCount,
                    )
                })
                .and_then(|v| v.parse().ok())
                .unwrap_or(1);

            // Try to parse as WorkItem or DiscoveredFile
            match parse_work_item(&body) {
                Some(work_item) => {
                    messages.push(QueueMessage {
                        receipt_handle,
                        work_item,
                        receive_count,
                        first_received_at: Utc::now(),
                    });
                }
                None => {
                    error!("Failed to parse work item from SQS message: {}", body);
                    // Delete the malformed message to prevent infinite redelivery
                    if let Err(del_err) = self
                        .client
                        .delete_message()
                        .queue_url(&self.config.queue_url)
                        .receipt_handle(&receipt_handle)
                        .send()
                        .await
                    {
                        warn!("Failed to delete malformed message: {}", del_err);
                    }
                }
            }
        }

        Ok(Some(messages))
    }

    async fn ack(&self, receipt: &str) -> Result<()> {
        self.client
            .delete_message()
            .queue_url(&self.config.queue_url)
            .receipt_handle(receipt)
            .send()
            .await
            .map_err(|e| PfError::Queue(QueueError::Ack(format!("SQS delete failed: {}", e))))?;

        debug!("Acknowledged message: {}", receipt);
        Ok(())
    }

    async fn nack(&self, receipt: &str) -> Result<()> {
        // Return to queue for retry - change visibility to 0
        self.client
            .change_message_visibility()
            .queue_url(&self.config.queue_url)
            .receipt_handle(receipt)
            .visibility_timeout(0)
            .send()
            .await
            .map_err(|e| {
                PfError::Queue(QueueError::Nack(format!(
                    "Failed to change visibility: {}",
                    e
                )))
            })?;

        debug!("Returned message {} to queue for retry", receipt);
        Ok(())
    }

    async fn move_to_dlq(&self, receipt: &str, failure: &FailureContext) -> Result<()> {
        // Move to DLQ if configured
        if let Some(dlq_url) = &self.config.dlq_url {
            // Send failure context to DLQ
            let body = serde_json::to_string(failure).map_err(|e| {
                PfError::Queue(QueueError::Serialize(format!(
                    "Failed to serialize failure context: {}",
                    e
                )))
            })?;

            self.client
                .send_message()
                .queue_url(dlq_url)
                .message_body(body)
                .send()
                .await
                .map_err(|e| {
                    PfError::Queue(QueueError::DlqMove(format!("Failed to send to DLQ: {}", e)))
                })?;

            // Delete from main queue
            self.client
                .delete_message()
                .queue_url(&self.config.queue_url)
                .receipt_handle(receipt)
                .send()
                .await
                .map_err(|e| {
                    PfError::Queue(QueueError::Ack(format!("Failed to delete message: {}", e)))
                })?;

            info!(
                "Moved message {} to DLQ: {}",
                receipt, failure.error_message
            );
        } else {
            // No explicit DLQ - let SQS redrive policy handle it
            warn!(
                "Message {} should go to DLQ but no DLQ configured, will be redriven by SQS policy",
                receipt
            );
        }

        Ok(())
    }

    fn has_more(&self) -> bool {
        !self.stopped.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqs_source_config_defaults() {
        let config = SqsSourceConfig::new("https://sqs.us-east-1.amazonaws.com/123/queue");

        assert_eq!(config.wait_time_seconds, 20);
        assert_eq!(config.visibility_timeout, 300);
        assert_eq!(config.max_batch_size, 10);
        assert!(config.dlq_url.is_none());
    }

    #[test]
    fn test_sqs_source_config_builder() {
        let config = SqsSourceConfig::new("https://sqs.us-east-1.amazonaws.com/123/queue")
            .with_dlq_url("https://sqs.us-east-1.amazonaws.com/123/dlq")
            .with_wait_time(10)
            .with_visibility_timeout(600)
            .with_max_batch_size(5);

        assert_eq!(config.wait_time_seconds, 10);
        assert_eq!(config.visibility_timeout, 600);
        assert_eq!(config.max_batch_size, 5);
        assert_eq!(
            config.dlq_url,
            Some("https://sqs.us-east-1.amazonaws.com/123/dlq".to_string())
        );
    }

    #[test]
    fn test_sqs_source_config_clamps() {
        let config = SqsSourceConfig::new("url")
            .with_wait_time(100) // Should clamp to 20
            .with_max_batch_size(50); // Should clamp to 10

        assert_eq!(config.wait_time_seconds, 20);
        assert_eq!(config.max_batch_size, 10);
    }
}
