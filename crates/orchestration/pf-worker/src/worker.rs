//! Main worker orchestration.

use crate::config::WorkerConfig;
use crate::pipeline::{Pipeline, ProcessingResult};
use crate::router::WorkRouter;
use crate::source::{QueueMessage, WorkQueue};
use crate::stats::{StatsSnapshot, WorkerStats};
use pf_error::{ErrorCategory, Result};
use pf_traits::{BatchIndexer, FailureContext, StreamingReader};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

/// High-throughput data processing worker.
///
/// The worker receives file locations from a work source, processes files
/// through a configurable pipeline, and writes to a destination backend.
pub struct Worker<S: WorkQueue, R: StreamingReader + 'static> {
    /// Worker configuration
    config: WorkerConfig,

    /// Work source (SQS or stdin)
    source: Arc<S>,

    /// File reader
    reader: Arc<R>,

    /// Destination for processed data
    destination: Arc<dyn BatchIndexer>,

    /// Global statistics
    stats: Arc<WorkerStats>,
}

impl<S: WorkQueue + 'static, R: StreamingReader + 'static> Worker<S, R> {
    /// Create a new worker.
    pub fn new(
        config: WorkerConfig,
        source: S,
        reader: R,
        destination: Arc<dyn BatchIndexer>,
    ) -> Self {
        Self {
            config,
            source: Arc::new(source),
            reader: Arc::new(reader),
            destination,
            stats: Arc::new(WorkerStats::new()),
        }
    }

    /// Get a reference to the worker statistics.
    pub fn stats(&self) -> &Arc<WorkerStats> {
        &self.stats
    }

    /// Run the worker until the source is exhausted or shutdown is signaled.
    pub async fn run(&self) -> Result<StatsSnapshot> {
        info!(
            threads = self.config.thread_count,
            batch_size = self.config.batch_size,
            "Starting worker"
        );

        // Create the work router
        let (router, receivers) = WorkRouter::new(
            self.config.thread_count,
            self.config.channel_buffer,
        );
        let router = Arc::new(router);

        // Spawn worker threads
        let mut worker_handles: Vec<JoinHandle<()>> = Vec::new();
        for (thread_id, rx) in receivers.into_iter().enumerate() {
            let pipeline = Pipeline::new(
                thread_id as u32,
                self.reader.clone(),
                self.destination.clone(),
                self.stats.clone(),
            );
            let source = self.source.clone();
            let max_retries = self.config.max_retries;

            let handle = tokio::spawn(async move {
                worker_thread(thread_id as u32, rx, pipeline, source, max_retries).await;
            });
            worker_handles.push(handle);
        }

        // Run the receiver loop that fetches from source and routes to workers
        let receiver_result = self.run_receiver(router.clone()).await;

        // Signal shutdown to the router
        router.shutdown();

        // Wait for all workers to complete with timeout
        let shutdown_result = tokio::time::timeout(
            self.config.shutdown_timeout,
            futures::future::join_all(worker_handles),
        )
        .await;

        match shutdown_result {
            Ok(results) => {
                for (i, result) in results.into_iter().enumerate() {
                    if let Err(e) = result {
                        error!(thread = i, error = %e, "Worker thread panicked");
                    }
                }
            }
            Err(_) => {
                warn!("Shutdown timeout exceeded, some workers may not have completed");
            }
        }

        // Flush the destination
        if let Err(e) = self.destination.flush().await {
            warn!(error = %e, "Failed to flush destination");
        }

        // Complete statistics
        let mut stats = self.stats.snapshot();
        stats.completed_at = Some(chrono::Utc::now());

        info!(
            files = stats.files_processed,
            records = stats.records_processed,
            bytes = stats.bytes_read,
            "Worker completed"
        );

        receiver_result?;
        Ok(stats)
    }

    /// Run the receiver loop that fetches from source and routes to workers.
    async fn run_receiver(&self, router: Arc<WorkRouter>) -> Result<()> {
        loop {
            // Check if source has more work
            if !self.source.has_more() {
                debug!("Source exhausted, stopping receiver");
                break;
            }

            // Receive a batch of work items
            let messages = match self.source.receive_batch(self.config.channel_buffer).await {
                Ok(Some(msgs)) => msgs,
                Ok(None) => {
                    debug!("Source returned None, stopping receiver");
                    break;
                }
                Err(e) => {
                    error!(error = %e, "Failed to receive from source");
                    self.stats.record_transient_error();
                    // Continue trying
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            };

            if messages.is_empty() {
                // No messages available, wait a bit before polling again
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }

            debug!(count = messages.len(), "Received messages from source");

            // Route messages to workers
            let failed = router.route_batch(messages).await;

            // If routing failed, something is wrong (shutdown or all workers dead)
            if !failed.is_empty() {
                warn!(count = failed.len(), "Failed to route messages to workers");
                // Nack the failed messages for retry
                for m in failed {
                    let failure = FailureContext::new(
                        m.work_item.clone(),
                        "RoutingFailed",
                        "Failed to route to worker",
                        "Routing",
                    );
                    if let Err(e) = self.source.nack(&m.receipt_handle).await {
                        error!(error = %e, "Failed to nack unrouted message");
                    }
                    // Also try to move to DLQ if routing continues to fail
                    drop(failure);
                }
            }
        }

        Ok(())
    }
}

/// Worker thread that processes messages from a channel.
async fn worker_thread<S: WorkQueue>(
    thread_id: u32,
    mut rx: mpsc::Receiver<QueueMessage>,
    pipeline: Pipeline,
    source: Arc<S>,
    max_retries: u32,
) {
    debug!(thread = thread_id, "Worker thread started");

    while let Some(message) = rx.recv().await {
        let receipt_handle = message.receipt_handle.clone();
        let _receive_count = message.receive_count;
        let _work_item = message.work_item.clone();

        // Process the message
        let result = pipeline.process(&message).await;

        // Handle the result
        match handle_result(&result, &message, max_retries) {
            AckAction::Ack => {
                if let Err(e) = source.ack(&receipt_handle).await {
                    error!(
                        thread = thread_id,
                        message = %receipt_handle,
                        error = %e,
                        "Failed to ack message"
                    );
                }
            }
            AckAction::Retry(failure) => {
                if let Err(e) = source.nack(&receipt_handle).await {
                    error!(
                        thread = thread_id,
                        message = %receipt_handle,
                        error = %e,
                        "Failed to nack message for retry"
                    );
                }
                drop(failure); // Failure context logged but not needed for simple nack
            }
            AckAction::Dlq(failure) => {
                if let Err(e) = source.move_to_dlq(&receipt_handle, &failure).await {
                    error!(
                        thread = thread_id,
                        message = %receipt_handle,
                        error = %e,
                        "Failed to move message to DLQ"
                    );
                }
            }
        }
    }

    debug!(thread = thread_id, "Worker thread stopped");
}

/// Action to take after processing a message.
enum AckAction {
    /// Acknowledge success - delete the message
    Ack,
    /// Retry - return to queue
    Retry(FailureContext),
    /// Move to DLQ - permanent failure
    Dlq(FailureContext),
}

/// Determine what action to take based on the processing result.
fn handle_result(result: &ProcessingResult, message: &QueueMessage, max_retries: u32) -> AckAction {
    if result.success {
        return AckAction::Ack;
    }

    let error_msg = result
        .error
        .as_ref()
        .map(|e| e.to_string())
        .unwrap_or_else(|| "Unknown error".to_string());

    let failure = FailureContext::new(
        message.work_item.clone(),
        result
            .error_category
            .map(|c| format!("{:?}", c))
            .unwrap_or_else(|| "Unknown".to_string()),
        &error_msg,
        "Processing",
    )
    .with_attempts(message.receive_count)
    .with_record_counts(result.records_processed, result.records_failed);

    match result.error_category {
        Some(ErrorCategory::Permanent) => AckAction::Dlq(failure),
        Some(ErrorCategory::Transient) => {
            if message.receive_count >= max_retries {
                AckAction::Dlq(failure)
            } else {
                AckAction::Retry(failure)
            }
        }
        Some(ErrorCategory::Partial) => {
            // Partial success - ack the message (records were partially processed)
            AckAction::Ack
        }
        None => {
            // Unknown error category - treat as transient
            if message.receive_count >= max_retries {
                AckAction::Dlq(failure)
            } else {
                AckAction::Retry(failure)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::destination::StatsDestination;
    use crate::source::StdinSource;
    use async_trait::async_trait;
    use chrono::Utc;
    use futures::stream;
    use pf_traits::{BatchStream, FileMetadata};
    use pf_types::{Batch, DestinationConfig, FileFormat, WorkItem};
    use std::io::Cursor;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    struct MockReader;

    #[async_trait]
    impl StreamingReader for MockReader {
        async fn read_stream(&self, _uri: &str) -> Result<BatchStream> {
            // Return a single batch with 10 records
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
            let ids: Vec<i64> = (0..10).collect();
            let record_batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(ids))],
            )
            .unwrap();

            let batch = Batch::new(record_batch, "test.parquet", 0);

            Ok(Box::pin(stream::iter(vec![Ok(batch)])))
        }

        async fn file_metadata(&self, _uri: &str) -> Result<FileMetadata> {
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
            Ok(FileMetadata::new(1024, schema))
        }
    }

    fn create_test_work_item_json() -> String {
        let item = WorkItem {
            job_id: "test-job".to_string(),
            file_uri: "s3://bucket/file.parquet".to_string(),
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
        };
        serde_json::to_string(&item).unwrap()
    }

    #[tokio::test]
    async fn test_worker_with_stdin() {
        let json = create_test_work_item_json();
        let input = Cursor::new(json);
        let source = StdinSource::with_reader(Box::new(input));
        let reader = MockReader;
        let destination = Arc::new(StatsDestination::new());

        let config = WorkerConfig::new()
            .with_thread_count(2)
            .with_channel_buffer(10);

        let worker = Worker::new(config, source, reader, destination.clone());
        let stats = worker.run().await.unwrap();

        assert_eq!(stats.files_processed, 1);
        assert_eq!(stats.records_processed, 10);
        assert_eq!(destination.get_stats().records, 10);
    }

    #[tokio::test]
    async fn test_worker_empty_source() {
        let input = Cursor::new("");
        let source = StdinSource::with_reader(Box::new(input));
        let reader = MockReader;
        let destination = Arc::new(StatsDestination::new());

        let config = WorkerConfig::new().with_thread_count(1);

        let worker = Worker::new(config, source, reader, destination);
        let stats = worker.run().await.unwrap();

        assert_eq!(stats.files_processed, 0);
    }

    #[test]
    fn test_handle_result_success() {
        let result = ProcessingResult::success(100, 1024, 2048);
        let message = create_test_message();

        match handle_result(&result, &message, 3) {
            AckAction::Ack => {}
            _ => panic!("Expected Ack"),
        }
    }

    #[test]
    fn test_handle_result_permanent_failure() {
        let result = ProcessingResult::failure(
            pf_error::PfError::Reader(pf_error::ReaderError::NotFound("test".to_string())),
            pf_error::ProcessingStage::S3Download,
        );
        let message = create_test_message();

        match handle_result(&result, &message, 3) {
            AckAction::Dlq(_) => {}
            _ => panic!("Expected Dlq"),
        }
    }

    #[test]
    fn test_handle_result_transient_retry() {
        let mut result = ProcessingResult::success(0, 0, 0);
        result.success = false;
        result.error = Some(pf_error::PfError::Queue(pf_error::QueueError::Connection(
            "timeout".to_string(),
        )));
        result.error_category = Some(ErrorCategory::Transient);

        let mut message = create_test_message();
        message.receive_count = 1;

        match handle_result(&result, &message, 3) {
            AckAction::Retry(_) => {}
            _ => panic!("Expected Retry"),
        }
    }

    #[test]
    fn test_handle_result_transient_max_retries() {
        let mut result = ProcessingResult::success(0, 0, 0);
        result.success = false;
        result.error_category = Some(ErrorCategory::Transient);

        let mut message = create_test_message();
        message.receive_count = 3; // At max retries

        match handle_result(&result, &message, 3) {
            AckAction::Dlq(_) => {}
            _ => panic!("Expected Dlq after max retries"),
        }
    }

    fn create_test_message() -> QueueMessage {
        QueueMessage {
            receipt_handle: "test-msg".to_string(),
            work_item: WorkItem {
                job_id: "test-job".to_string(),
                file_uri: "s3://bucket/file.parquet".to_string(),
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
}
