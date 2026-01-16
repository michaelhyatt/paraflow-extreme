//! SQS work source implementation.

use super::{WorkAck, WorkMessage, WorkNack, WorkSource};
use async_trait::async_trait;
use aws_sdk_sqs::Client;
use pf_error::{PfError, QueueError, Result};
use pf_types::WorkItem;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
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
}

/// Work source that receives WorkItems from an AWS SQS queue.
pub struct SqsSource {
    /// SQS client
    client: Client,

    /// Configuration
    config: SqsSourceConfig,

    /// Whether to stop receiving (for graceful shutdown)
    stopped: AtomicBool,
}

impl SqsSource {
    /// Create a new SQS source.
    pub fn new(client: Client, config: SqsSourceConfig) -> Self {
        Self {
            client,
            config,
            stopped: AtomicBool::new(false),
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
}

#[async_trait]
impl WorkSource for SqsSource {
    async fn receive(&self, max: usize) -> Result<Option<Vec<WorkMessage>>> {
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
            .message_system_attribute_names(aws_sdk_sqs::types::MessageSystemAttributeName::ApproximateReceiveCount)
            .send()
            .await
            .map_err(|e| PfError::Queue(QueueError::Receive(format!("SQS receive failed: {}", e))))?;

        let sqs_messages = response.messages.unwrap_or_default();
        debug!("Received {} messages from SQS", sqs_messages.len());

        let mut messages = Vec::with_capacity(sqs_messages.len());

        for msg in sqs_messages {
            let receipt_handle = msg.receipt_handle.unwrap_or_default();
            let body = msg.body.unwrap_or_default();

            // Parse receive count from attributes
            let receive_count = msg
                .attributes
                .as_ref()
                .and_then(|attrs| attrs.get(&aws_sdk_sqs::types::MessageSystemAttributeName::ApproximateReceiveCount))
                .and_then(|v| v.parse().ok())
                .unwrap_or(1);

            match serde_json::from_str::<WorkItem>(&body) {
                Ok(work_item) => {
                    messages.push(WorkMessage {
                        id: receipt_handle,
                        work_item,
                        receive_count,
                    });
                }
                Err(e) => {
                    error!("Failed to parse work item from SQS message: {}", e);
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

    async fn ack(&self, items: &[WorkAck]) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }

        // SQS supports batch delete up to 10 messages
        for chunk in items.chunks(10) {
            let entries: Vec<_> = chunk
                .iter()
                .enumerate()
                .map(|(i, item)| {
                    aws_sdk_sqs::types::DeleteMessageBatchRequestEntry::builder()
                        .id(i.to_string())
                        .receipt_handle(&item.id)
                        .build()
                        .unwrap()
                })
                .collect();

            let result = self
                .client
                .delete_message_batch()
                .queue_url(&self.config.queue_url)
                .set_entries(Some(entries))
                .send()
                .await
                .map_err(|e| PfError::Queue(QueueError::Ack(format!("SQS batch delete failed: {}", e))))?;

            for f in &result.failed {
                warn!("Failed to delete message {}: {}", f.id, f.message.as_deref().unwrap_or("unknown"));
            }
        }

        debug!("Acknowledged {} messages", items.len());
        Ok(())
    }

    async fn nack(&self, items: &[WorkNack]) -> Result<()> {
        for item in items {
            if item.should_dlq {
                // Move to DLQ if configured
                if let Some(dlq_url) = &self.config.dlq_url {
                    // Send failure context to DLQ
                    let body = serde_json::to_string(&item.failure).map_err(|e| {
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
                        .receipt_handle(&item.id)
                        .send()
                        .await
                        .map_err(|e| {
                            PfError::Queue(QueueError::Ack(format!("Failed to delete message: {}", e)))
                        })?;

                    info!(
                        "Moved message {} to DLQ: {}",
                        item.id, item.failure.error_message
                    );
                } else {
                    // No explicit DLQ - let SQS redrive policy handle it
                    warn!(
                        "Message {} should go to DLQ but no DLQ configured, will be redriven by SQS policy",
                        item.id
                    );
                }
            } else {
                // Return to queue for retry - change visibility to 0
                self.client
                    .change_message_visibility()
                    .queue_url(&self.config.queue_url)
                    .receipt_handle(&item.id)
                    .visibility_timeout(0)
                    .send()
                    .await
                    .map_err(|e| {
                        PfError::Queue(QueueError::Nack(format!(
                            "Failed to change visibility: {}",
                            e
                        )))
                    })?;

                debug!("Returned message {} to queue for retry", item.id);
            }
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
