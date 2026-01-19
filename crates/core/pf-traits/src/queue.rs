//! Work queue trait and related types.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use pf_error::Result;
use pf_types::WorkItem;
use serde::{Deserialize, Serialize};

/// Trait for work queue backends.
///
/// This is the unified abstraction for work queue operations used by both:
/// - Discoverer: enqueues work items to the queue
/// - Worker: receives and processes work items from the queue
///
/// Implementations include:
/// - In-memory queue (for testing/development)
/// - AWS SQS queue (production)
/// - Stdin source (for local testing/piping)
///
/// # Message Flow
///
/// 1. Discoverer calls [`enqueue`](WorkQueue::enqueue) to add work items
/// 2. Workers call [`receive_batch`](WorkQueue::receive_batch) to get messages
/// 3. After processing, workers call:
///    - [`ack`](WorkQueue::ack) on success
///    - [`nack`](WorkQueue::nack) on transient failure (retry)
///    - [`move_to_dlq`](WorkQueue::move_to_dlq) on permanent failure
///
/// # Optional Methods
///
/// Some methods have default implementations that return errors, allowing
/// implementations to only implement what they support:
/// - `enqueue` is only needed for discoverer (not needed for stdin source)
/// - `depth` is only needed for monitoring (not critical for basic operation)
#[async_trait]
pub trait WorkQueue: Send + Sync {
    /// Enqueues a work item for processing.
    ///
    /// # Default Implementation
    ///
    /// Returns an error indicating the operation is not supported.
    /// Implementations that only receive (like stdin) can use this default.
    async fn enqueue(&self, _item: WorkItem) -> Result<()> {
        Err(pf_error::PfError::Queue(pf_error::QueueError::Enqueue(
            "enqueue not supported by this implementation".to_string(),
        )))
    }

    /// Receives a batch of messages (long-polling).
    ///
    /// # Arguments
    ///
    /// * `max` - Maximum number of messages to receive (capped by implementation)
    ///
    /// # Returns
    ///
    /// - `Ok(Some(vec))` - Messages received (may be empty if no messages available)
    /// - `Ok(None)` - Source is exhausted (EOF reached, drain complete)
    async fn receive_batch(&self, max: usize) -> Result<Option<Vec<QueueMessage>>>;

    /// Acknowledges successful processing (deletes message).
    async fn ack(&self, receipt: &str) -> Result<()>;

    /// Negative acknowledges (returns message to queue for retry).
    async fn nack(&self, receipt: &str) -> Result<()>;

    /// Moves message to DLQ with failure context.
    async fn move_to_dlq(&self, receipt: &str, failure: &FailureContext) -> Result<()>;

    /// Gets approximate queue depth (for monitoring).
    ///
    /// # Default Implementation
    ///
    /// Returns zeros for implementations that don't support depth queries.
    async fn depth(&self) -> Result<QueueDepth> {
        Ok(QueueDepth::default())
    }

    /// Checks if the queue is empty.
    async fn is_empty(&self) -> Result<bool> {
        let depth = self.depth().await?;
        Ok(depth.visible == 0 && depth.in_flight == 0)
    }

    /// Returns true if more work may be available.
    ///
    /// Used for drain mode support - when this returns false, the worker
    /// knows it can shut down gracefully.
    ///
    /// # Default Implementation
    ///
    /// Always returns true (continuous queue operation).
    fn has_more(&self) -> bool {
        true
    }

    /// Sets a shared counter for tracking pending prefetch items.
    ///
    /// This is used during drain mode to ensure all prefetched items in worker
    /// buffers are processed before declaring the queue empty. The counter is
    /// updated by worker threads as they prefetch and process items.
    ///
    /// # Default Implementation
    ///
    /// No-op for implementations that don't support drain mode coordination.
    fn set_pending_prefetch_counter(
        &self,
        _counter: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) {
        // Default: no-op for implementations that don't need this
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

    // Test the default implementation of set_pending_prefetch_counter
    // by creating a minimal mock implementation
    mod mock_queue {
        use super::*;

        /// A minimal mock queue for testing trait default methods
        pub struct MockQueue;

        #[async_trait]
        impl WorkQueue for MockQueue {
            async fn receive_batch(&self, _max: usize) -> Result<Option<Vec<QueueMessage>>> {
                Ok(Some(vec![]))
            }

            async fn ack(&self, _receipt: &str) -> Result<()> {
                Ok(())
            }

            async fn nack(&self, _receipt: &str) -> Result<()> {
                Ok(())
            }

            async fn move_to_dlq(&self, _receipt: &str, _failure: &FailureContext) -> Result<()> {
                Ok(())
            }

            // Note: We don't override set_pending_prefetch_counter,
            // so it uses the default no-op implementation
        }
    }

    #[test]
    fn test_set_pending_prefetch_counter_default_is_noop() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicUsize;

        let queue = mock_queue::MockQueue;
        let counter = Arc::new(AtomicUsize::new(42));

        // Call the default implementation - it should be a no-op
        // This should not panic and should complete normally
        queue.set_pending_prefetch_counter(counter.clone());

        // Counter should still have its original value (default impl doesn't use it)
        assert_eq!(counter.load(std::sync::atomic::Ordering::SeqCst), 42);
    }

    #[test]
    fn test_has_more_default_returns_true() {
        let queue = mock_queue::MockQueue;
        // Default implementation should return true
        assert!(queue.has_more());
    }
}
