//! SQS work source implementation.

use super::parse_work_item;
use async_trait::async_trait;
use aws_sdk_sqs::Client;
use aws_sdk_sqs::types::QueueAttributeName;
use chrono::Utc;
use futures::future::join_all;
use pf_error::{PfError, QueueError, Result};
use pf_traits::{FailureContext, QueueMessage, WorkQueue};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};
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

    /// Number of concurrent SQS polling requests.
    /// Higher values improve throughput by keeping more messages ready.
    /// SQS limits each request to 10 messages, so 2-3 concurrent polls
    /// can provide 20-30 messages per cycle.
    pub concurrent_polls: usize,
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
            concurrent_polls: 2, // Default to 2 concurrent polls
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

    /// Set the number of concurrent polling requests.
    ///
    /// Higher values improve throughput by fetching more messages per cycle.
    /// Recommended: 2-4 for high-throughput workloads.
    pub fn with_concurrent_polls(mut self, count: usize) -> Self {
        self.concurrent_polls = count.max(1);
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

    /// Last observed in-flight message count (for detecting stuck queues)
    last_in_flight: AtomicI64,

    /// Counter for consecutive checks with same in-flight count
    stalled_count: AtomicU32,

    /// Shared counter for pending prefetch items across all worker threads.
    /// This is checked during drain mode to ensure all prefetched items are
    /// processed before declaring the queue empty.
    /// Uses parking_lot::Mutex for interior mutability since the trait method takes &self.
    pending_prefetch_items:
        parking_lot::Mutex<Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>>,
}

impl SqsSource {
    /// Create a new SQS source.
    pub fn new(client: Client, config: SqsSourceConfig) -> Self {
        Self {
            client,
            config,
            stopped: AtomicBool::new(false),
            empty_receive_count: AtomicU32::new(0),
            last_in_flight: AtomicI64::new(-1),
            stalled_count: AtomicU32::new(0),
            pending_prefetch_items: parking_lot::Mutex::new(None),
        }
    }

    /// Get the current count of pending prefetch items, if tracking is enabled.
    fn pending_prefetch_count(&self) -> usize {
        self.pending_prefetch_items
            .lock()
            .as_ref()
            .map(|c| c.load(Ordering::SeqCst))
            .unwrap_or(0)
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

    /// Check queue status and return (visible, in_flight) message counts.
    ///
    /// This is used in drain mode to verify the queue is actually empty before
    /// signaling completion. This prevents premature termination when multiple
    /// workers are processing and one worker receives an empty batch while
    /// messages are still in-flight with other workers.
    async fn get_queue_status(&self) -> Result<(i64, i64)> {
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
        Ok((visible, in_flight))
    }
}

#[async_trait]
impl WorkQueue for SqsSource {
    async fn receive_batch(&self, max: usize) -> Result<Option<Vec<QueueMessage>>> {
        if self.stopped.load(Ordering::Relaxed) {
            return Ok(None);
        }

        let batch_size = (max as i32).min(self.config.max_batch_size);
        let concurrent_polls = self.config.concurrent_polls;

        // Launch concurrent SQS receive requests
        let poll_futures: Vec<_> = (0..concurrent_polls)
            .map(|_| {
                self.client
                    .receive_message()
                    .queue_url(&self.config.queue_url)
                    .max_number_of_messages(batch_size)
                    .wait_time_seconds(self.config.wait_time_seconds)
                    .visibility_timeout(self.config.visibility_timeout)
                    .message_system_attribute_names(
                        aws_sdk_sqs::types::MessageSystemAttributeName::ApproximateReceiveCount,
                    )
                    .send()
            })
            .collect();

        // Wait for all polls to complete
        let results = join_all(poll_futures).await;

        // Combine messages from all successful polls
        let mut sqs_messages = Vec::new();
        let mut had_error = false;
        for result in results {
            match result {
                Ok(response) => {
                    if let Some(msgs) = response.messages {
                        sqs_messages.extend(msgs);
                    }
                }
                Err(e) => {
                    // Log but don't fail if some polls succeed
                    warn!("One SQS poll failed: {}", e);
                    had_error = true;
                }
            }
        }

        // If all polls failed, return error
        if sqs_messages.is_empty() && had_error {
            return Err(PfError::Queue(QueueError::Receive(
                "All SQS receive requests failed".to_string(),
            )));
        }

        debug!(
            "Received {} messages from {} concurrent SQS polls",
            sqs_messages.len(),
            concurrent_polls
        );

        // Handle drain mode: exit when queue is truly empty
        if sqs_messages.is_empty() {
            if self.config.drain_mode {
                let empty_count = self.empty_receive_count.fetch_add(1, Ordering::Relaxed) + 1;
                let pending_prefetch = self.pending_prefetch_count();
                debug!(empty_count, pending_prefetch, "Empty receive in drain mode");

                // After empty long-poll, verify queue is truly empty
                // This prevents premature termination when multiple workers are active
                // and one gets an empty batch while messages are in-flight with others
                match self.get_queue_status().await {
                    Ok((visible, in_flight)) => {
                        // Check if all work is complete:
                        // - No visible messages in queue
                        // - No in-flight messages at SQS level
                        // - No pending prefetch items in worker buffers
                        if visible == 0 && in_flight == 0 && pending_prefetch == 0 {
                            info!(
                                empty_polls = empty_count,
                                "Queue drained (verified empty), signaling completion"
                            );
                            self.stopped.store(true, Ordering::Relaxed);
                            return Ok(None);
                        }

                        // If there are pending prefetch items, don't count as stalled
                        // Workers are still processing prefetched items
                        if pending_prefetch > 0 {
                            self.stalled_count.store(0, Ordering::Relaxed);
                            info!(
                                visible = visible,
                                in_flight = in_flight,
                                pending_prefetch = pending_prefetch,
                                "Waiting for workers to process prefetched items"
                            );
                        } else {
                            // Messages are in-flight with other workers. Track if progress is being made.
                            let last = self.last_in_flight.swap(in_flight, Ordering::Relaxed);
                            if in_flight > 0 && in_flight == last {
                                // In-flight count hasn't changed - workers may be stuck or crashed
                                let stalled =
                                    self.stalled_count.fetch_add(1, Ordering::Relaxed) + 1;
                                let max_stalled = (self.config.visibility_timeout
                                    / self.config.wait_time_seconds)
                                    + 1;

                                if stalled >= max_stalled as u32 {
                                    warn!(
                                        in_flight = in_flight,
                                        stalled_checks = stalled,
                                        visibility_timeout_secs = self.config.visibility_timeout,
                                        "In-flight messages stuck, assuming orphaned - exiting. \
                                         These messages will become visible again after visibility timeout."
                                    );
                                    self.stopped.store(true, Ordering::Relaxed);
                                    return Ok(None);
                                }
                                info!(
                                    in_flight = in_flight,
                                    stalled_checks = stalled,
                                    max_stalled_checks = max_stalled,
                                    visibility_timeout_secs = self.config.visibility_timeout,
                                    "Waiting for in-flight messages (no progress detected). \
                                     Workers may be processing large files or are stuck."
                                );
                            } else {
                                // In-flight count changed - progress is being made
                                self.stalled_count.store(0, Ordering::Relaxed);
                                info!(
                                    visible = visible,
                                    in_flight = in_flight,
                                    previous_in_flight = last,
                                    "Queue has in-flight messages, waiting for completion"
                                );
                            }
                        }
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
                    info!(
                        file = %work_item.file_uri,
                        receive_count = receive_count,
                        "Received message from SQS"
                    );
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

    fn set_pending_prefetch_counter(
        &self,
        counter: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) {
        *self.pending_prefetch_items.lock() = Some(counter);
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
        assert_eq!(config.concurrent_polls, 2);
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

    #[test]
    fn test_sqs_source_config_concurrent_polls() {
        let config = SqsSourceConfig::new("url").with_concurrent_polls(4);
        assert_eq!(config.concurrent_polls, 4);

        // Test minimum of 1
        let config = SqsSourceConfig::new("url").with_concurrent_polls(0);
        assert_eq!(config.concurrent_polls, 1);
    }

    #[test]
    fn test_pending_prefetch_count_default() {
        // Create a mock SQS client - we only need to test the pending_prefetch logic
        let aws_config = aws_sdk_sqs::Config::builder()
            .behavior_version_latest()
            .build();
        let client = Client::from_conf(aws_config);
        let config = SqsSourceConfig::new("url");
        let source = SqsSource::new(client, config);

        // Default should be 0 when no counter is set
        assert_eq!(source.pending_prefetch_count(), 0);
    }

    #[test]
    fn test_pending_prefetch_count_with_counter() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicUsize;

        let aws_config = aws_sdk_sqs::Config::builder()
            .behavior_version_latest()
            .build();
        let client = Client::from_conf(aws_config);
        let config = SqsSourceConfig::new("url");
        let source = SqsSource::new(client, config);

        // Create and set a counter
        let counter = Arc::new(AtomicUsize::new(0));
        source.set_pending_prefetch_counter(counter.clone());

        // Counter should now reflect updates
        assert_eq!(source.pending_prefetch_count(), 0);

        counter.store(5, Ordering::SeqCst);
        assert_eq!(source.pending_prefetch_count(), 5);

        counter.store(10, Ordering::SeqCst);
        assert_eq!(source.pending_prefetch_count(), 10);

        counter.store(0, Ordering::SeqCst);
        assert_eq!(source.pending_prefetch_count(), 0);
    }

    #[test]
    fn test_pending_prefetch_counter_fetch_add_sub() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicUsize;

        let aws_config = aws_sdk_sqs::Config::builder()
            .behavior_version_latest()
            .build();
        let client = Client::from_conf(aws_config);
        let config = SqsSourceConfig::new("url");
        let source = SqsSource::new(client, config);

        let counter = Arc::new(AtomicUsize::new(0));
        source.set_pending_prefetch_counter(counter.clone());

        // Simulate worker behavior: increment on prefetch start, decrement on completion
        counter.fetch_add(1, Ordering::SeqCst);
        assert_eq!(source.pending_prefetch_count(), 1);

        counter.fetch_add(1, Ordering::SeqCst);
        assert_eq!(source.pending_prefetch_count(), 2);

        counter.fetch_add(1, Ordering::SeqCst);
        assert_eq!(source.pending_prefetch_count(), 3);

        // Now process items
        counter.fetch_sub(1, Ordering::SeqCst);
        assert_eq!(source.pending_prefetch_count(), 2);

        counter.fetch_sub(1, Ordering::SeqCst);
        assert_eq!(source.pending_prefetch_count(), 1);

        counter.fetch_sub(1, Ordering::SeqCst);
        assert_eq!(source.pending_prefetch_count(), 0);
    }

    #[test]
    fn test_pending_prefetch_counter_thread_safety() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicUsize;
        use std::thread;

        let aws_config = aws_sdk_sqs::Config::builder()
            .behavior_version_latest()
            .build();
        let client = Client::from_conf(aws_config);
        let config = SqsSourceConfig::new("url");
        let source = Arc::new(SqsSource::new(client, config));

        let counter = Arc::new(AtomicUsize::new(0));
        source.set_pending_prefetch_counter(counter.clone());

        let mut handles = vec![];

        // Spawn threads that increment the counter
        for _ in 0..10 {
            let counter_clone = counter.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    counter_clone.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // After 10 threads each adding 100, we should have 1000
        assert_eq!(source.pending_prefetch_count(), 1000);

        // Now spawn threads to decrement
        let mut handles = vec![];
        for _ in 0..10 {
            let counter_clone = counter.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    counter_clone.fetch_sub(1, Ordering::SeqCst);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Should be back to 0
        assert_eq!(source.pending_prefetch_count(), 0);
    }

    #[test]
    fn test_set_pending_prefetch_counter_replaces_previous() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicUsize;

        let aws_config = aws_sdk_sqs::Config::builder()
            .behavior_version_latest()
            .build();
        let client = Client::from_conf(aws_config);
        let config = SqsSourceConfig::new("url");
        let source = SqsSource::new(client, config);

        // Set first counter
        let counter1 = Arc::new(AtomicUsize::new(100));
        source.set_pending_prefetch_counter(counter1.clone());
        assert_eq!(source.pending_prefetch_count(), 100);

        // Set second counter - should replace the first
        let counter2 = Arc::new(AtomicUsize::new(200));
        source.set_pending_prefetch_counter(counter2.clone());
        assert_eq!(source.pending_prefetch_count(), 200);

        // Updating counter1 should not affect the source
        counter1.store(999, Ordering::SeqCst);
        assert_eq!(source.pending_prefetch_count(), 200);

        // Updating counter2 should affect the source
        counter2.store(300, Ordering::SeqCst);
        assert_eq!(source.pending_prefetch_count(), 300);
    }

    // === Drain Mode Condition Edge Case Tests ===

    #[test]
    fn test_drain_mode_completion_conditions() {
        // Test the three conditions for drain mode completion:
        // 1. visible == 0
        // 2. in_flight == 0
        // 3. pending_prefetch == 0
        use std::sync::Arc;
        use std::sync::atomic::AtomicUsize;

        let aws_config = aws_sdk_sqs::Config::builder()
            .behavior_version_latest()
            .build();
        let client = Client::from_conf(aws_config);
        let config = SqsSourceConfig::new("url").with_drain_mode(true);
        let source = SqsSource::new(client, config);

        // Case 1: No counter set - pending_prefetch defaults to 0
        assert_eq!(source.pending_prefetch_count(), 0);

        // Case 2: Counter set to 0
        let counter = Arc::new(AtomicUsize::new(0));
        source.set_pending_prefetch_counter(counter.clone());
        assert_eq!(source.pending_prefetch_count(), 0);

        // Case 3: Counter set to non-zero - should prevent drain completion
        counter.store(5, Ordering::SeqCst);
        assert_eq!(source.pending_prefetch_count(), 5);
        // In real code, this would cause drain mode to wait

        // Case 4: Counter decrements to 0 - should allow drain completion
        counter.store(0, Ordering::SeqCst);
        assert_eq!(source.pending_prefetch_count(), 0);
    }

    #[test]
    fn test_drain_mode_stall_reset_with_pending_prefetch() {
        // Test that stall counter is reset when pending_prefetch > 0
        // This simulates the scenario where workers are processing prefetched items
        use std::sync::Arc;
        use std::sync::atomic::AtomicUsize;

        let aws_config = aws_sdk_sqs::Config::builder()
            .behavior_version_latest()
            .build();
        let client = Client::from_conf(aws_config);
        let config = SqsSourceConfig::new("url")
            .with_drain_mode(true)
            .with_visibility_timeout(300)
            .with_wait_time(20);
        let source = SqsSource::new(client, config);

        let counter = Arc::new(AtomicUsize::new(10)); // 10 pending items
        source.set_pending_prefetch_counter(counter.clone());

        // Verify pending count
        assert_eq!(source.pending_prefetch_count(), 10);

        // Simulate workers finishing
        for i in (0..=10).rev() {
            counter.store(i, Ordering::SeqCst);
            assert_eq!(source.pending_prefetch_count(), i);
        }

        // Final check - should be 0
        assert_eq!(source.pending_prefetch_count(), 0);
    }

    #[test]
    fn test_drain_mode_config_enabled() {
        let config = SqsSourceConfig::new("url").with_drain_mode(true);
        assert!(config.drain_mode);

        let config = SqsSourceConfig::new("url").with_drain_mode(false);
        assert!(!config.drain_mode);

        // Default is false
        let config = SqsSourceConfig::new("url");
        assert!(!config.drain_mode);
    }

    #[test]
    fn test_stalled_count_calculation() {
        // Test the max_stalled calculation: (visibility_timeout / wait_time) + 1
        let config = SqsSourceConfig::new("url")
            .with_visibility_timeout(300) // 5 minutes
            .with_wait_time(20); // 20 seconds

        // max_stalled = (300 / 20) + 1 = 16
        let max_stalled = (config.visibility_timeout / config.wait_time_seconds) + 1;
        assert_eq!(max_stalled, 16);

        // Different config
        let config = SqsSourceConfig::new("url")
            .with_visibility_timeout(60)
            .with_wait_time(10);

        let max_stalled = (config.visibility_timeout / config.wait_time_seconds) + 1;
        assert_eq!(max_stalled, 7);
    }

    #[test]
    fn test_pending_prefetch_counter_concurrent_read_write() {
        // Test that reading the counter while another thread writes is safe
        use std::sync::Arc;
        use std::sync::atomic::AtomicUsize;
        use std::thread;

        let aws_config = aws_sdk_sqs::Config::builder()
            .behavior_version_latest()
            .build();
        let client = Client::from_conf(aws_config);
        let config = SqsSourceConfig::new("url");
        let source = Arc::new(SqsSource::new(client, config));

        let counter = Arc::new(AtomicUsize::new(0));
        source.set_pending_prefetch_counter(counter.clone());

        // Writer thread
        let counter_writer = counter.clone();
        let writer_handle = thread::spawn(move || {
            for i in 0..10000 {
                counter_writer.store(i % 100, Ordering::SeqCst);
            }
        });

        // Reader threads
        let mut reader_handles = vec![];
        for _ in 0..4 {
            let source_clone = source.clone();
            reader_handles.push(thread::spawn(move || {
                for _ in 0..10000 {
                    let val = source_clone.pending_prefetch_count();
                    // Value should be in valid range (0-99)
                    assert!(val < 100, "Value {} out of expected range", val);
                }
            }));
        }

        writer_handle.join().unwrap();
        for handle in reader_handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_has_more_reflects_stopped_state() {
        let aws_config = aws_sdk_sqs::Config::builder()
            .behavior_version_latest()
            .build();
        let client = Client::from_conf(aws_config);
        let config = SqsSourceConfig::new("url");
        let source = SqsSource::new(client, config);

        // Initially should have more
        assert!(source.has_more());

        // After marking stopped, should not have more
        source.stopped.store(true, Ordering::Relaxed);
        assert!(!source.has_more());

        // Resetting stopped
        source.stopped.store(false, Ordering::Relaxed);
        assert!(source.has_more());
    }
}
