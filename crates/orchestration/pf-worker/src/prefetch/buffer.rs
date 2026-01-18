//! Prefetch buffer for holding prefetched streams.

use pf_error::PfError;
use pf_traits::{BatchStream, QueueMessage};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Mutex;
use tracing::{debug, trace};

/// A prefetched item ready for processing.
pub struct PrefetchedItem {
    /// The original queue message.
    pub message: QueueMessage,

    /// The pre-opened batch stream.
    pub stream: BatchStream,

    /// Estimated size of this item in bytes.
    pub estimated_bytes: usize,
}

impl std::fmt::Debug for PrefetchedItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrefetchedItem")
            .field("message", &self.message.receipt_handle)
            .field("estimated_bytes", &self.estimated_bytes)
            .finish()
    }
}

/// Error that occurred during prefetching.
#[derive(Debug)]
pub struct PrefetchError {
    /// The original queue message that failed.
    pub message: QueueMessage,

    /// The error that occurred.
    pub error: PfError,
}

/// Bounded buffer holding prefetched streams.
///
/// Tracks memory usage and limits concurrent prefetch operations
/// based on configured thresholds.
pub struct PrefetchBuffer {
    /// Thread ID for logging.
    thread_id: u32,

    /// Maximum number of prefetched items.
    max_count: usize,

    /// Maximum memory budget in bytes.
    max_memory_bytes: usize,

    /// Current estimated memory usage in bytes.
    current_memory_bytes: AtomicUsize,

    /// Number of items currently being prefetched (in flight).
    in_flight_count: AtomicUsize,

    /// Queue of ready items.
    ready_queue: Mutex<VecDeque<PrefetchedItem>>,

    /// Queue of errors.
    error_queue: Mutex<VecDeque<PrefetchError>>,
}

impl PrefetchBuffer {
    /// Create a new prefetch buffer with the given limits.
    pub fn new(thread_id: u32, max_count: usize, max_memory_bytes: usize) -> Self {
        Self {
            thread_id,
            max_count,
            max_memory_bytes,
            current_memory_bytes: AtomicUsize::new(0),
            in_flight_count: AtomicUsize::new(0),
            ready_queue: Mutex::new(VecDeque::with_capacity(max_count)),
            error_queue: Mutex::new(VecDeque::new()),
        }
    }

    /// Check if we can start another prefetch operation.
    ///
    /// Returns true if both count and memory limits allow.
    pub fn can_prefetch(&self) -> bool {
        let ready_count = {
            // Use try_lock to avoid blocking - if we can't get the lock,
            // be conservative and say we can't prefetch
            match self.ready_queue.try_lock() {
                Ok(queue) => queue.len(),
                Err(_) => self.max_count, // Conservative: assume full
            }
        };

        let in_flight = self.in_flight_count.load(Ordering::SeqCst);
        let total_count = ready_count + in_flight;

        if total_count >= self.max_count {
            trace!(
                thread = self.thread_id,
                ready_count,
                in_flight,
                max_count = self.max_count,
                "Cannot prefetch: count limit reached"
            );
            return false;
        }

        let current_memory = self.current_memory_bytes.load(Ordering::SeqCst);
        if current_memory >= self.max_memory_bytes {
            trace!(
                thread = self.thread_id,
                current_memory,
                max_memory = self.max_memory_bytes,
                "Cannot prefetch: memory limit reached"
            );
            return false;
        }

        true
    }

    /// Mark a prefetch operation as starting.
    ///
    /// Call this before spawning a prefetch task.
    pub fn mark_prefetch_started(&self) {
        self.in_flight_count.fetch_add(1, Ordering::SeqCst);
    }

    /// Mark a prefetch operation as completed (successfully or not).
    ///
    /// This decrements the in-flight count. If successful, also call
    /// `push_ready` to add the item to the queue.
    pub fn mark_prefetch_completed(&self) {
        self.in_flight_count.fetch_sub(1, Ordering::SeqCst);
    }

    /// Add a prefetched item to the ready queue.
    pub async fn push_ready(&self, item: PrefetchedItem) {
        let bytes = item.estimated_bytes;
        self.current_memory_bytes.fetch_add(bytes, Ordering::SeqCst);

        let mut queue = self.ready_queue.lock().await;
        queue.push_back(item);

        debug!(
            thread = self.thread_id,
            queue_len = queue.len(),
            memory_bytes = self.current_memory_bytes.load(Ordering::SeqCst),
            "Prefetched item added to ready queue"
        );
    }

    /// Get the next ready item, if any.
    ///
    /// Returns immediately with None if no items are ready.
    pub async fn pop_ready(&self) -> Option<PrefetchedItem> {
        let mut queue = self.ready_queue.lock().await;
        let item = queue.pop_front()?;

        // Subtract memory
        self.current_memory_bytes
            .fetch_sub(item.estimated_bytes, Ordering::SeqCst);

        trace!(
            thread = self.thread_id,
            queue_len = queue.len(),
            memory_bytes = self.current_memory_bytes.load(Ordering::SeqCst),
            "Prefetched item popped from ready queue"
        );

        Some(item)
    }

    /// Try to pop a ready item without blocking.
    ///
    /// Returns None if no items are ready or if the lock is held.
    pub fn try_pop_ready(&self) -> Option<PrefetchedItem> {
        let mut queue = match self.ready_queue.try_lock() {
            Ok(q) => q,
            Err(_) => return None,
        };

        let item = queue.pop_front()?;

        // Subtract memory
        self.current_memory_bytes
            .fetch_sub(item.estimated_bytes, Ordering::SeqCst);

        Some(item)
    }

    /// Push a prefetch error to the error queue.
    pub async fn push_error(&self, error: PrefetchError) {
        let mut queue = self.error_queue.lock().await;
        queue.push_back(error);
    }

    /// Pop a prefetch error, if any.
    pub async fn pop_error(&self) -> Option<PrefetchError> {
        let mut queue = self.error_queue.lock().await;
        queue.pop_front()
    }

    /// Get the number of ready items.
    pub async fn ready_count(&self) -> usize {
        self.ready_queue.lock().await.len()
    }

    /// Get the number of pending errors.
    pub async fn error_count(&self) -> usize {
        self.error_queue.lock().await.len()
    }

    /// Get the number of in-flight prefetch operations.
    pub fn in_flight_count(&self) -> usize {
        self.in_flight_count.load(Ordering::SeqCst)
    }

    /// Get the current estimated memory usage in bytes.
    pub fn current_memory_bytes(&self) -> usize {
        self.current_memory_bytes.load(Ordering::SeqCst)
    }

    /// Check if the buffer has any ready items or errors.
    pub async fn has_items(&self) -> bool {
        let ready = self.ready_queue.lock().await.len();
        let errors = self.error_queue.lock().await.len();
        ready > 0 || errors > 0
    }

    /// Check if there are any in-flight prefetches or ready items.
    pub async fn has_pending_work(&self) -> bool {
        let ready = self.ready_queue.lock().await.len();
        let in_flight = self.in_flight_count.load(Ordering::SeqCst);
        ready > 0 || in_flight > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use chrono::Utc;
    use futures::stream;
    use pf_types::{Batch, DestinationConfig, FileFormat, WorkItem};
    use std::sync::Arc;

    fn create_test_message(uri: &str) -> QueueMessage {
        QueueMessage {
            receipt_handle: format!("receipt-{}", uri),
            work_item: WorkItem {
                job_id: "test-job".to_string(),
                file_uri: uri.to_string(),
                file_size_bytes: 1024,
                format: FileFormat::Parquet,
                destination: DestinationConfig {
                    endpoint: "http://localhost".to_string(),
                    index: "test".to_string(),
                    credentials: None,
                },
                transform: None,
                attempt: 0,
                enqueued_at: Utc::now(),
            },
            receive_count: 1,
            first_received_at: Utc::now(),
        }
    }

    fn create_test_stream() -> BatchStream {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let ids: Vec<i64> = (0..10).collect();
        let record_batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(ids))]).unwrap();
        let batch = Batch::new(record_batch, "test.parquet", 0);
        Box::pin(stream::iter(vec![Ok(batch)]))
    }

    #[tokio::test]
    async fn test_buffer_creation() {
        let buffer = PrefetchBuffer::new(0, 2, 30 * 1024 * 1024);
        assert!(buffer.can_prefetch());
        assert_eq!(buffer.in_flight_count(), 0);
        assert_eq!(buffer.current_memory_bytes(), 0);
    }

    #[tokio::test]
    async fn test_buffer_count_limit() {
        let buffer = PrefetchBuffer::new(0, 2, 100 * 1024 * 1024);

        // Add two items
        for i in 0..2 {
            let item = PrefetchedItem {
                message: create_test_message(&format!("s3://bucket/file{}.parquet", i)),
                stream: create_test_stream(),
                estimated_bytes: 1024,
            };
            buffer.push_ready(item).await;
        }

        // Should not be able to prefetch more
        assert!(!buffer.can_prefetch());

        // Pop one item
        let _ = buffer.pop_ready().await;

        // Should be able to prefetch now
        assert!(buffer.can_prefetch());
    }

    #[tokio::test]
    async fn test_buffer_memory_limit() {
        let buffer = PrefetchBuffer::new(0, 10, 2048); // 2KB limit

        // Add item that uses all memory
        let item = PrefetchedItem {
            message: create_test_message("s3://bucket/file.parquet"),
            stream: create_test_stream(),
            estimated_bytes: 2048,
        };
        buffer.push_ready(item).await;

        // Should not be able to prefetch more
        assert!(!buffer.can_prefetch());

        // Pop the item
        let _ = buffer.pop_ready().await;

        // Should be able to prefetch now
        assert!(buffer.can_prefetch());
    }

    #[tokio::test]
    async fn test_buffer_in_flight_tracking() {
        let buffer = PrefetchBuffer::new(0, 2, 100 * 1024 * 1024);

        assert_eq!(buffer.in_flight_count(), 0);

        buffer.mark_prefetch_started();
        assert_eq!(buffer.in_flight_count(), 1);

        buffer.mark_prefetch_started();
        assert_eq!(buffer.in_flight_count(), 2);

        // Should not be able to prefetch with 2 in flight (max_count = 2)
        assert!(!buffer.can_prefetch());

        buffer.mark_prefetch_completed();
        assert_eq!(buffer.in_flight_count(), 1);
        assert!(buffer.can_prefetch());
    }

    #[tokio::test]
    async fn test_buffer_error_handling() {
        let buffer = PrefetchBuffer::new(0, 2, 100 * 1024 * 1024);

        let error = PrefetchError {
            message: create_test_message("s3://bucket/missing.parquet"),
            error: pf_error::PfError::Reader(pf_error::ReaderError::NotFound(
                "test file".to_string(),
            )),
        };

        buffer.push_error(error).await;
        assert_eq!(buffer.error_count().await, 1);

        let popped = buffer.pop_error().await;
        assert!(popped.is_some());
        assert_eq!(buffer.error_count().await, 0);
    }

    #[tokio::test]
    async fn test_buffer_fifo_order() {
        let buffer = PrefetchBuffer::new(0, 10, 100 * 1024 * 1024);

        // Add items in order
        for i in 0..3 {
            let item = PrefetchedItem {
                message: create_test_message(&format!("s3://bucket/file{}.parquet", i)),
                stream: create_test_stream(),
                estimated_bytes: 1024,
            };
            buffer.push_ready(item).await;
        }

        // Pop items - should be FIFO
        for i in 0..3 {
            let item = buffer.pop_ready().await.unwrap();
            assert!(
                item.message
                    .work_item
                    .file_uri
                    .contains(&format!("file{}", i))
            );
        }

        // Should be empty now
        assert!(buffer.pop_ready().await.is_none());
    }
}
