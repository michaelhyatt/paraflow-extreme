//! Work queue trait and related types.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use pf_error::Result;
use pf_types::WorkItem;
use serde::{Deserialize, Serialize};

/// Trait for work queue backends.
///
/// Implementations include:
/// - In-memory queue (for testing/development)
/// - AWS SQS queue (production)
///
/// # Message Flow
///
/// 1. Discoverer calls [`enqueue`](WorkQueue::enqueue) to add work items
/// 2. Workers call [`receive_batch`](WorkQueue::receive_batch) to get messages
/// 3. After processing, workers call:
///    - [`ack`](WorkQueue::ack) on success
///    - [`nack`](WorkQueue::nack) on transient failure (retry)
///    - [`move_to_dlq`](WorkQueue::move_to_dlq) on permanent failure
#[async_trait]
pub trait WorkQueue: Send + Sync {
    /// Enqueues a work item for processing.
    async fn enqueue(&self, item: WorkItem) -> Result<()>;

    /// Receives a batch of messages (long-polling).
    ///
    /// # Arguments
    ///
    /// * `max` - Maximum number of messages to receive (capped by implementation)
    ///
    /// # Returns
    ///
    /// Vector of messages, may be empty if queue is empty
    async fn receive_batch(&self, max: usize) -> Result<Vec<QueueMessage>>;

    /// Acknowledges successful processing (deletes message).
    async fn ack(&self, receipt: &str) -> Result<()>;

    /// Negative acknowledges (returns message to queue for retry).
    async fn nack(&self, receipt: &str) -> Result<()>;

    /// Moves message to DLQ with failure context.
    async fn move_to_dlq(&self, receipt: &str, failure: &FailureContext) -> Result<()>;

    /// Gets approximate queue depth (for monitoring).
    async fn depth(&self) -> Result<QueueDepth>;

    /// Checks if the queue is empty.
    async fn is_empty(&self) -> Result<bool> {
        let depth = self.depth().await?;
        Ok(depth.visible == 0 && depth.in_flight == 0)
    }
}

/// A message received from the queue.
#[derive(Debug, Clone)]
pub struct QueueMessage {
    /// Handle used for ack/nack operations
    pub receipt_handle: String,

    /// The work item payload
    pub work_item: WorkItem,

    /// Number of times this message has been received
    pub receive_count: u32,

    /// When this message was first received
    pub first_received_at: DateTime<Utc>,
}

/// Queue depth information for monitoring.
#[derive(Debug, Clone, Default)]
pub struct QueueDepth {
    /// Messages available for processing
    pub visible: u64,

    /// Messages currently being processed
    pub in_flight: u64,

    /// Messages in dead letter queue
    pub dlq: u64,
}

/// Context for failed messages sent to DLQ.
///
/// Provides full details for investigation and potential replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureContext {
    /// Original work item
    pub work_item: WorkItem,

    /// Error type classification
    pub error_type: String,

    /// Human-readable error message
    pub error_message: String,

    /// Full error chain for debugging
    pub error_chain: Vec<String>,

    /// Processing stage where failure occurred
    pub stage: String,

    /// Worker ID that processed this message
    pub worker_id: String,

    /// Thread ID within the worker
    pub thread_id: u32,

    /// When this message was first attempted
    pub first_attempt_at: DateTime<Utc>,

    /// When the final failure occurred
    pub failed_at: DateTime<Utc>,

    /// Total number of processing attempts
    pub total_attempts: u32,

    /// Records successfully processed before failure
    pub records_processed: u64,

    /// Records that failed to process
    pub records_failed: u64,
}

impl FailureContext {
    /// Creates a new failure context.
    pub fn new(
        work_item: WorkItem,
        error_type: impl Into<String>,
        error_message: impl Into<String>,
        stage: impl Into<String>,
    ) -> Self {
        Self {
            work_item,
            error_type: error_type.into(),
            error_message: error_message.into(),
            error_chain: Vec::new(),
            stage: stage.into(),
            worker_id: std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string()),
            thread_id: 0,
            first_attempt_at: Utc::now(),
            failed_at: Utc::now(),
            total_attempts: 1,
            records_processed: 0,
            records_failed: 0,
        }
    }

    /// Adds an error to the error chain.
    pub fn with_error_chain(mut self, chain: Vec<String>) -> Self {
        self.error_chain = chain;
        self
    }

    /// Sets the thread ID.
    pub fn with_thread_id(mut self, id: u32) -> Self {
        self.thread_id = id;
        self
    }

    /// Sets the first attempt timestamp.
    pub fn with_first_attempt(mut self, at: DateTime<Utc>) -> Self {
        self.first_attempt_at = at;
        self
    }

    /// Sets the total attempts count.
    pub fn with_attempts(mut self, count: u32) -> Self {
        self.total_attempts = count;
        self
    }

    /// Sets record processing counts.
    pub fn with_record_counts(mut self, processed: u64, failed: u64) -> Self {
        self.records_processed = processed;
        self.records_failed = failed;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pf_types::{DestinationConfig, FileFormat};

    fn create_test_work_item() -> WorkItem {
        WorkItem {
            job_id: "test-job".to_string(),
            file_uri: "s3://bucket/file.parquet".to_string(),
            file_size_bytes: 1024,
            format: FileFormat::Parquet,
            destination: DestinationConfig {
                endpoint: "http://localhost:9200".to_string(),
                index: "test".to_string(),
                credentials: None,
            },
            transform: None,
            attempt: 0,
            enqueued_at: Utc::now(),
        }
    }

    #[test]
    fn test_failure_context_creation() {
        let item = create_test_work_item();
        let ctx = FailureContext::new(item, "Permanent", "File not found", "S3Download");

        assert_eq!(ctx.error_type, "Permanent");
        assert_eq!(ctx.stage, "S3Download");
        assert_eq!(ctx.total_attempts, 1);
    }

    #[test]
    fn test_failure_context_builder() {
        let item = create_test_work_item();
        let ctx = FailureContext::new(item, "Transient", "Connection timeout", "EsIndexing")
            .with_thread_id(3)
            .with_attempts(5)
            .with_record_counts(1000, 50);

        assert_eq!(ctx.thread_id, 3);
        assert_eq!(ctx.total_attempts, 5);
        assert_eq!(ctx.records_processed, 1000);
        assert_eq!(ctx.records_failed, 50);
    }

    #[test]
    fn test_queue_depth_default() {
        let depth = QueueDepth::default();
        assert_eq!(depth.visible, 0);
        assert_eq!(depth.in_flight, 0);
        assert_eq!(depth.dlq, 0);
    }
}
