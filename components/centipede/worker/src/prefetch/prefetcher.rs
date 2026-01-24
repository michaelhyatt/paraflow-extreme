//! Prefetcher for async I/O prefetching.

use super::buffer::{PrefetchBuffer, PrefetchError, PrefetchedItem};
use super::config::PrefetchConfig;
use pf_traits::{QueueMessage, StreamingReader};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{debug, error, trace, warn};

/// Async I/O prefetcher that overlaps S3 downloads with processing.
///
/// The prefetcher spawns async tasks to download files ahead of time,
/// storing them in a bounded buffer. This allows the worker thread to
/// immediately start processing prefetched files without waiting for
/// S3 round trips.
pub struct Prefetcher {
    /// Thread ID for logging.
    thread_id: u32,

    /// Configuration.
    config: PrefetchConfig,

    /// Shared reader for opening streams.
    reader: Arc<dyn StreamingReader>,

    /// Buffer for prefetched items.
    buffer: Arc<PrefetchBuffer>,
}

impl Prefetcher {
    /// Create a new prefetcher.
    pub fn new(thread_id: u32, config: PrefetchConfig, reader: Arc<dyn StreamingReader>) -> Self {
        let buffer = Arc::new(PrefetchBuffer::new(
            thread_id,
            config.max_prefetch_count,
            config.max_memory_bytes,
        ));

        Self {
            thread_id,
            config,
            reader,
            buffer,
        }
    }

    /// Get a reference to the prefetch buffer.
    pub fn buffer(&self) -> Arc<PrefetchBuffer> {
        self.buffer.clone()
    }

    /// Check if prefetching is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Check if we can start another prefetch operation.
    pub fn can_prefetch(&self) -> bool {
        self.config.enabled && self.buffer.can_prefetch()
    }

    /// Spawn a prefetch task for the given message.
    ///
    /// Returns a JoinHandle that can be used to wait for completion,
    /// though this is typically not needed as results go to the buffer.
    ///
    /// The prefetch task will:
    /// 1. Optionally fetch metadata (HEAD request) to estimate size
    /// 2. Open the stream (initiates S3 GET)
    /// 3. Push the result to the ready queue or error queue
    pub fn spawn_prefetch(&self, message: QueueMessage) -> JoinHandle<()> {
        // Mark prefetch as started before spawning
        self.buffer.mark_prefetch_started();

        let reader = self.reader.clone();
        let buffer = self.buffer.clone();
        let thread_id = self.thread_id;
        let prefetch_metadata = self.config.prefetch_metadata;
        let file_uri = message.work_item.file_uri.clone();

        tokio::spawn(async move {
            trace!(
                thread = thread_id,
                file = %file_uri,
                "Starting prefetch"
            );

            // Estimate size from metadata or use file_size_bytes from work item
            let estimated_bytes = if prefetch_metadata {
                match reader.file_metadata(&file_uri).await {
                    Ok(metadata) => metadata.size_bytes as usize,
                    Err(e) => {
                        // Log but continue with work item's size estimate
                        warn!(
                            thread = thread_id,
                            file = %file_uri,
                            error = %e,
                            "Failed to fetch metadata for prefetch, using work item size"
                        );
                        message.work_item.file_size_bytes as usize
                    }
                }
            } else {
                message.work_item.file_size_bytes as usize
            };

            // Open the stream
            match reader.read_stream(&file_uri).await {
                Ok(stream) => {
                    let item = PrefetchedItem {
                        message,
                        stream,
                        estimated_bytes,
                    };
                    buffer.push_ready(item).await;
                    debug!(
                        thread = thread_id,
                        file = %file_uri,
                        estimated_bytes,
                        "Prefetch completed successfully"
                    );
                }
                Err(error) => {
                    error!(
                        thread = thread_id,
                        file = %file_uri,
                        error = %error,
                        "Prefetch failed"
                    );
                    let prefetch_error = PrefetchError { message, error };
                    buffer.push_error(prefetch_error).await;
                }
            }

            // Mark prefetch as completed
            buffer.mark_prefetch_completed();
        })
    }

    /// Get statistics about the prefetcher state.
    pub async fn stats(&self) -> PrefetcherStats {
        PrefetcherStats {
            ready_count: self.buffer.ready_count().await,
            error_count: self.buffer.error_count().await,
            in_flight_count: self.buffer.in_flight_count(),
            current_memory_bytes: self.buffer.current_memory_bytes(),
            max_prefetch_count: self.config.max_prefetch_count,
            max_memory_bytes: self.config.max_memory_bytes,
        }
    }
}

/// Statistics about the prefetcher state.
#[derive(Debug, Clone)]
pub struct PrefetcherStats {
    /// Number of ready items in the buffer.
    pub ready_count: usize,

    /// Number of errors in the buffer.
    pub error_count: usize,

    /// Number of in-flight prefetch operations.
    pub in_flight_count: usize,

    /// Current estimated memory usage in bytes.
    pub current_memory_bytes: usize,

    /// Maximum prefetch count configuration.
    pub max_prefetch_count: usize,

    /// Maximum memory bytes configuration.
    pub max_memory_bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use async_trait::async_trait;
    use chrono::Utc;
    use futures::stream;
    use pf_error::{PfError, ReaderError, Result};
    use pf_traits::{BatchStream, FileMetadata};
    use pf_types::{Batch, DestinationConfig, FileFormat, WorkItem};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::time::{Duration, sleep};

    struct MockReader {
        call_count: AtomicUsize,
        should_fail: AtomicBool,
        delay_ms: AtomicUsize,
    }

    impl MockReader {
        fn new() -> Self {
            Self {
                call_count: AtomicUsize::new(0),
                should_fail: AtomicBool::new(false),
                delay_ms: AtomicUsize::new(0),
            }
        }

        fn with_delay(delay_ms: usize) -> Self {
            Self {
                call_count: AtomicUsize::new(0),
                should_fail: AtomicBool::new(false),
                delay_ms: AtomicUsize::new(delay_ms),
            }
        }

        fn failing() -> Self {
            Self {
                call_count: AtomicUsize::new(0),
                should_fail: AtomicBool::new(true),
                delay_ms: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait]
    impl StreamingReader for MockReader {
        async fn read_stream(&self, _uri: &str) -> Result<BatchStream> {
            self.call_count.fetch_add(1, Ordering::SeqCst);

            let delay = self.delay_ms.load(Ordering::SeqCst);
            if delay > 0 {
                sleep(Duration::from_millis(delay as u64)).await;
            }

            if self.should_fail.load(Ordering::SeqCst) {
                return Err(PfError::Reader(ReaderError::NotFound(
                    "test file".to_string(),
                )));
            }

            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
            let ids: Vec<i64> = (0..10).collect();
            let record_batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(ids))])
                    .unwrap();
            let batch = Batch::new(record_batch, "test.parquet", 0);

            Ok(Box::pin(stream::iter(vec![Ok(batch)])))
        }

        async fn file_metadata(&self, _uri: &str) -> Result<FileMetadata> {
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
            Ok(FileMetadata::new(1024, schema))
        }
    }

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

    #[tokio::test]
    async fn test_prefetcher_creation() {
        let reader = Arc::new(MockReader::new());
        let config = PrefetchConfig::new();
        let prefetcher = Prefetcher::new(0, config, reader);

        assert!(prefetcher.is_enabled());
        assert!(prefetcher.can_prefetch());
    }

    #[tokio::test]
    async fn test_prefetcher_disabled() {
        let reader = Arc::new(MockReader::new());
        let config = PrefetchConfig::disabled();
        let prefetcher = Prefetcher::new(0, config, reader);

        assert!(!prefetcher.is_enabled());
        assert!(!prefetcher.can_prefetch());
    }

    #[tokio::test]
    async fn test_prefetcher_spawn_success() {
        let reader = Arc::new(MockReader::new());
        let config = PrefetchConfig::new();
        let prefetcher = Prefetcher::new(0, config, reader.clone());
        let buffer = prefetcher.buffer();

        let message = create_test_message("s3://bucket/file.parquet");
        let handle = prefetcher.spawn_prefetch(message);

        // Wait for prefetch to complete
        handle.await.unwrap();

        // Check buffer has the item
        assert_eq!(buffer.ready_count().await, 1);
        assert_eq!(buffer.error_count().await, 0);
        assert_eq!(reader.call_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_prefetcher_spawn_failure() {
        let reader = Arc::new(MockReader::failing());
        let config = PrefetchConfig::new();
        let prefetcher = Prefetcher::new(0, config, reader);
        let buffer = prefetcher.buffer();

        let message = create_test_message("s3://bucket/missing.parquet");
        let handle = prefetcher.spawn_prefetch(message);

        // Wait for prefetch to complete
        handle.await.unwrap();

        // Check buffer has the error
        assert_eq!(buffer.ready_count().await, 0);
        assert_eq!(buffer.error_count().await, 1);
    }

    #[tokio::test]
    async fn test_prefetcher_multiple_files() {
        let reader = Arc::new(MockReader::with_delay(10));
        let config = PrefetchConfig::new().with_max_prefetch_count(3);
        let prefetcher = Prefetcher::new(0, config, reader.clone());
        let buffer = prefetcher.buffer();

        // Spawn multiple prefetches
        let mut handles = vec![];
        for i in 0..3 {
            let message = create_test_message(&format!("s3://bucket/file{}.parquet", i));
            handles.push(prefetcher.spawn_prefetch(message));
        }

        // Wait for all to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Check all items are ready
        assert_eq!(buffer.ready_count().await, 3);
        assert_eq!(reader.call_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_prefetcher_stats() {
        let reader = Arc::new(MockReader::new());
        let config = PrefetchConfig::new()
            .with_max_prefetch_count(3)
            .with_max_memory_bytes(50 * 1024 * 1024);
        let prefetcher = Prefetcher::new(0, config, reader);

        let message = create_test_message("s3://bucket/file.parquet");
        let handle = prefetcher.spawn_prefetch(message);
        handle.await.unwrap();

        let stats = prefetcher.stats().await;
        assert_eq!(stats.ready_count, 1);
        assert_eq!(stats.error_count, 0);
        assert_eq!(stats.in_flight_count, 0);
        assert_eq!(stats.max_prefetch_count, 3);
        assert_eq!(stats.max_memory_bytes, 50 * 1024 * 1024);
    }

    #[tokio::test]
    async fn test_prefetcher_respects_limits() {
        let reader = Arc::new(MockReader::with_delay(50)); // Add delay so prefetches overlap
        let config = PrefetchConfig::new().with_max_prefetch_count(2);
        let prefetcher = Prefetcher::new(0, config, reader);

        // Start 2 prefetches (at the limit)
        let m1 = create_test_message("s3://bucket/file1.parquet");
        let m2 = create_test_message("s3://bucket/file2.parquet");

        let _h1 = prefetcher.spawn_prefetch(m1);
        let _h2 = prefetcher.spawn_prefetch(m2);

        // Small delay to let spawns register
        sleep(Duration::from_millis(5)).await;

        // Should not be able to prefetch more while at limit
        // Note: can_prefetch checks in_flight + ready count
        assert!(!prefetcher.can_prefetch());
    }
}
