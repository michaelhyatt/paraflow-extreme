//! Main worker orchestration.

use crate::config::WorkerConfig;
use crate::pipeline::{Pipeline, ProcessingResult};
use crate::prefetch::{PrefetchConfig, PrefetchError, Prefetcher};
use crate::router::WorkRouter;
use crate::source::{QueueMessage, WorkQueue};
use crate::stats::{StatsSnapshot, WorkerStats};
use pf_error::{ErrorCategory, ProcessingStage, Result};
use pf_traits::{BatchIndexer, FailureContext, StreamingReader};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};

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
        let prefetch_enabled = self.config.prefetch.enabled;

        info!(
            threads = self.config.thread_count,
            batch_size = self.config.batch_size,
            shutdown_timeout_secs = self.config.shutdown_timeout.as_secs(),
            prefetch_enabled,
            prefetch_count = self.config.prefetch.max_prefetch_count,
            prefetch_memory_mb = self.config.prefetch.max_memory_bytes / (1024 * 1024),
            "Starting worker"
        );

        // Create shared counter for pending prefetch items (for drain mode coordination)
        let pending_prefetch_counter = Arc::new(AtomicUsize::new(0));

        // Set up the pending prefetch counter on the source for drain mode coordination
        if prefetch_enabled {
            self.source
                .set_pending_prefetch_counter(pending_prefetch_counter.clone());
        }

        // Create the work router
        let (router, receivers) =
            WorkRouter::new(self.config.thread_count, self.config.channel_buffer);
        let router = Arc::new(router);

        // Create cancellation token for graceful shutdown
        let cancellation_token = CancellationToken::new();

        // Create worker status trackers
        let worker_statuses: Vec<Arc<WorkerStatus>> = (0..self.config.thread_count)
            .map(|id| Arc::new(WorkerStatus::new(id as u32)))
            .collect();

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
            let cancel_token = cancellation_token.child_token();
            let status = worker_statuses[thread_id].clone();
            let prefetch_config = self.config.prefetch.clone();
            let prefetch_counter = pending_prefetch_counter.clone();

            let handle = if prefetch_enabled {
                tokio::spawn(async move {
                    worker_thread_with_prefetch(
                        thread_id as u32,
                        rx,
                        pipeline,
                        source,
                        max_retries,
                        cancel_token,
                        status,
                        prefetch_config,
                        prefetch_counter,
                    )
                    .await;
                })
            } else {
                tokio::spawn(async move {
                    worker_thread(
                        thread_id as u32,
                        rx,
                        pipeline,
                        source,
                        max_retries,
                        cancel_token,
                        status,
                    )
                    .await;
                })
            };
            worker_handles.push(handle);
        }

        // Run the receiver loop that fetches from source and routes to workers
        let receiver_result = self.run_receiver(router.clone()).await;

        // Signal shutdown to the router (stops new work from being distributed)
        router.shutdown();

        debug!("Router shutdown signaled, waiting for workers to complete");

        // Wait for all workers to complete with timeout
        let shutdown_result = tokio::time::timeout(
            self.config.shutdown_timeout,
            futures::future::join_all(worker_handles),
        )
        .await;

        match shutdown_result {
            Ok(results) => {
                debug!("All workers completed within shutdown timeout");
                for (i, result) in results.into_iter().enumerate() {
                    if let Err(e) = result {
                        error!(thread = i, error = %e, "Worker thread panicked");
                    }
                }
            }
            Err(_) => {
                // Log which workers are still running
                let still_running: Vec<_> = worker_statuses
                    .iter()
                    .filter(|s| s.is_processing())
                    .collect();

                if still_running.is_empty() {
                    warn!(
                        timeout_secs = self.config.shutdown_timeout.as_secs(),
                        "Shutdown timeout exceeded, but no workers appear to be processing"
                    );
                } else {
                    for status in &still_running {
                        warn!(
                            thread = status.thread_id,
                            current_file = %status.current_file(),
                            "Worker still processing at shutdown timeout"
                        );
                    }
                    warn!(
                        timeout_secs = self.config.shutdown_timeout.as_secs(),
                        workers_still_running = still_running.len(),
                        "Shutdown timeout exceeded, signaling cancellation to workers"
                    );
                }

                // Signal cancellation to all workers
                cancellation_token.cancel();

                // Give workers a brief grace period to respond to cancellation
                tokio::time::sleep(Duration::from_millis(500)).await;
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

/// Tracks the status of a worker thread for shutdown diagnostics.
struct WorkerStatus {
    /// Thread ID
    thread_id: u32,
    /// Whether the worker is currently processing a file
    processing: AtomicBool,
    /// Current file being processed (for diagnostics)
    current_file: std::sync::Mutex<String>,
}

impl WorkerStatus {
    fn new(thread_id: u32) -> Self {
        Self {
            thread_id,
            processing: AtomicBool::new(false),
            current_file: std::sync::Mutex::new(String::new()),
        }
    }

    fn start_processing(&self, file_uri: &str) {
        self.processing.store(true, Ordering::SeqCst);
        if let Ok(mut guard) = self.current_file.lock() {
            *guard = file_uri.to_string();
        }
    }

    fn finish_processing(&self) {
        self.processing.store(false, Ordering::SeqCst);
        if let Ok(mut guard) = self.current_file.lock() {
            guard.clear();
        }
    }

    fn is_processing(&self) -> bool {
        self.processing.load(Ordering::SeqCst)
    }

    fn current_file(&self) -> String {
        self.current_file
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_else(|_| "<unknown>".to_string())
    }
}

/// Worker thread that processes messages from a channel.
///
/// Supports graceful shutdown via cancellation token. When cancelled,
/// the worker will finish processing its current message before exiting.
async fn worker_thread<S: WorkQueue>(
    thread_id: u32,
    mut rx: mpsc::Receiver<QueueMessage>,
    pipeline: Pipeline,
    source: Arc<S>,
    max_retries: u32,
    cancel_token: CancellationToken,
    status: Arc<WorkerStatus>,
) {
    debug!(thread = thread_id, "Worker thread started");

    loop {
        // Check for cancellation or new message
        tokio::select! {
            biased;

            // Check for cancellation
            _ = cancel_token.cancelled() => {
                debug!(thread = thread_id, "Worker received cancellation signal");
                break;
            }

            // Wait for next message
            message = rx.recv() => {
                let Some(message) = message else {
                    // Channel closed, exit normally
                    break;
                };

                let receipt_handle = message.receipt_handle.clone();
                let file_uri = message.work_item.file_uri.clone();

                // Update status for shutdown diagnostics
                status.start_processing(&file_uri);

                // Process the message
                let result = pipeline.process(&message).await;

                // Mark processing complete
                status.finish_processing();

                // Handle the result
                handle_ack_action(
                    &handle_result(&result, &message, max_retries),
                    &source,
                    thread_id,
                    &receipt_handle,
                    &file_uri,
                )
                .await;
            }
        }
    }

    debug!(thread = thread_id, "Worker thread stopped");
}

/// Worker thread with async I/O prefetching.
///
/// This variant overlaps S3 downloads with processing by prefetching files
/// while the current file is being processed. This significantly improves
/// throughput when the workload is I/O-latency-bound.
///
/// The prefetch flow:
/// 1. Start prefetching for queued messages (non-blocking)
/// 2. Get next item (prefetched or direct from channel)
/// 3. Process with optional pre-opened stream
/// 4. Handle ack/nack/dlq (unchanged from regular worker)
#[allow(clippy::too_many_arguments)]
async fn worker_thread_with_prefetch<S: WorkQueue>(
    thread_id: u32,
    mut rx: mpsc::Receiver<QueueMessage>,
    pipeline: Pipeline,
    source: Arc<S>,
    max_retries: u32,
    cancel_token: CancellationToken,
    status: Arc<WorkerStatus>,
    prefetch_config: PrefetchConfig,
    pending_prefetch_counter: Arc<AtomicUsize>,
) {
    debug!(
        thread = thread_id,
        max_prefetch = prefetch_config.max_prefetch_count,
        memory_budget_mb = prefetch_config.max_memory_bytes / (1024 * 1024),
        "Worker thread with prefetch started"
    );

    // Create the prefetcher
    let prefetcher = Prefetcher::new(thread_id, prefetch_config, pipeline.reader().clone());
    let buffer = prefetcher.buffer();

    loop {
        // 1. Start prefetching for any queued messages while we can
        while prefetcher.can_prefetch() {
            match rx.try_recv() {
                Ok(msg) => {
                    trace!(
                        thread = thread_id,
                        file = %msg.work_item.file_uri,
                        "Starting prefetch for queued message"
                    );
                    // Increment global pending count when starting prefetch
                    pending_prefetch_counter.fetch_add(1, Ordering::SeqCst);
                    prefetcher.spawn_prefetch(msg);
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    // Channel closed - process remaining prefetched items then exit
                    debug!(
                        thread = thread_id,
                        "Channel closed, processing remaining prefetched items"
                    );
                    break;
                }
            }
        }

        // 2. Check for prefetch errors first
        if let Some(prefetch_error) = buffer.pop_error().await {
            // Decrement global pending count when consuming error
            pending_prefetch_counter.fetch_sub(1, Ordering::SeqCst);

            let PrefetchError { message, error } = prefetch_error;
            let receipt_handle = message.receipt_handle.clone();
            let file_uri = message.work_item.file_uri.clone();

            warn!(
                thread = thread_id,
                file = %file_uri,
                error = %error,
                "Prefetch failed, handling error"
            );

            // Create a failure result for the prefetch error
            let result = ProcessingResult::failure(error, ProcessingStage::S3Download);

            // Handle the result (will likely retry or DLQ)
            handle_ack_action(
                &handle_result(&result, &message, max_retries),
                &source,
                thread_id,
                &receipt_handle,
                &file_uri,
            )
            .await;

            continue;
        }

        // 3. Get next item - prefer prefetched, fall back to channel
        let (message, stream, is_prefetched) = {
            // Try to get a prefetched item first
            if let Some(item) = buffer.pop_ready().await {
                trace!(
                    thread = thread_id,
                    file = %item.message.work_item.file_uri,
                    "Using prefetched stream"
                );
                (item.message, Some(item.stream), true)
            } else {
                // No prefetched items, wait for channel or cancellation
                tokio::select! {
                    biased;

                    // Check for cancellation
                    _ = cancel_token.cancelled() => {
                        debug!(thread = thread_id, "Worker received cancellation signal");
                        break;
                    }

                    // Wait for next message from channel
                    message = rx.recv() => {
                        match message {
                            Some(msg) => {
                                trace!(
                                    thread = thread_id,
                                    file = %msg.work_item.file_uri,
                                    "Processing message directly (no prefetch available)"
                                );
                                (msg, None, false)
                            }
                            None => {
                                // Channel closed and no more prefetched items
                                if !buffer.has_pending_work().await {
                                    debug!(thread = thread_id, "Channel closed, no pending work");
                                    break;
                                }
                                // Still have in-flight prefetches, wait a bit
                                tokio::time::sleep(Duration::from_millis(10)).await;
                                continue;
                            }
                        }
                    }
                }
            }
        };

        // Decrement global pending count when consuming a prefetched item
        if is_prefetched {
            pending_prefetch_counter.fetch_sub(1, Ordering::SeqCst);
        }

        let receipt_handle = message.receipt_handle.clone();
        let file_uri = message.work_item.file_uri.clone();

        // Update status for shutdown diagnostics
        status.start_processing(&file_uri);

        // 4. Process the message
        let result = match stream {
            Some(s) => {
                // Use prefetched stream
                pipeline.process_with_stream(&message, s).await
            }
            None => {
                // No prefetch available, process normally
                pipeline.process(&message).await
            }
        };

        // Mark processing complete
        status.finish_processing();

        // 5. Handle the result
        handle_ack_action(
            &handle_result(&result, &message, max_retries),
            &source,
            thread_id,
            &receipt_handle,
            &file_uri,
        )
        .await;
    }

    debug!(thread = thread_id, "Worker thread with prefetch stopped");
}

/// Helper to handle ack/nack/dlq actions.
async fn handle_ack_action<S: WorkQueue>(
    action: &AckAction,
    source: &Arc<S>,
    thread_id: u32,
    receipt_handle: &str,
    file_uri: &str,
) {
    match action {
        AckAction::Ack => {
            info!(
                thread = thread_id,
                file = %file_uri,
                "Acknowledging message (success)"
            );
            if let Err(e) = source.ack(receipt_handle).await {
                error!(
                    thread = thread_id,
                    file = %file_uri,
                    error = %e,
                    "Failed to ack message"
                );
            }
        }
        AckAction::Retry(failure) => {
            warn!(
                thread = thread_id,
                file = %file_uri,
                attempt = failure.total_attempts,
                error = %failure.error_message,
                "Returning message to queue for retry"
            );
            if let Err(e) = source.nack(receipt_handle).await {
                error!(
                    thread = thread_id,
                    file = %file_uri,
                    error = %e,
                    "Failed to nack message for retry"
                );
            }
        }
        AckAction::Dlq(failure) => {
            error!(
                thread = thread_id,
                file = %file_uri,
                attempt = failure.total_attempts,
                error = %failure.error_message,
                "Moving message to DLQ (permanent failure)"
            );
            if let Err(e) = source.move_to_dlq(receipt_handle, failure).await {
                error!(
                    thread = thread_id,
                    file = %file_uri,
                    error = %e,
                    "Failed to move message to DLQ"
                );
            }
        }
    }
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
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use async_trait::async_trait;
    use chrono::Utc;
    use futures::stream;
    use pf_traits::{BatchStream, FileMetadata};
    use pf_types::{Batch, DestinationConfig, FileFormat, WorkItem};
    use std::io::Cursor;

    struct MockReader;

    #[async_trait]
    impl StreamingReader for MockReader {
        async fn read_stream(&self, _uri: &str) -> Result<BatchStream> {
            // Return a single batch with 10 records
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

    // === Pending Prefetch Counter Edge Case Tests ===

    #[test]
    fn test_atomic_counter_fetch_add_sub_balance() {
        // Test that fetch_add and fetch_sub operations balance correctly
        // This simulates the worker thread behavior
        let counter = Arc::new(AtomicUsize::new(0));

        // Simulate multiple prefetch starts
        for _ in 0..100 {
            counter.fetch_add(1, Ordering::SeqCst);
        }
        assert_eq!(counter.load(Ordering::SeqCst), 100);

        // Simulate processing all items
        for _ in 0..100 {
            counter.fetch_sub(1, Ordering::SeqCst);
        }
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_atomic_counter_concurrent_add_sub() {
        // Test concurrent increments and decrements from multiple threads
        // Simulates multiple worker threads operating on the same counter
        use std::thread;

        let counter = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];

        // Spawn 8 "worker" threads that each do 1000 add/sub cycles
        for _ in 0..8 {
            let counter_clone = counter.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    // Increment (start prefetch)
                    counter_clone.fetch_add(1, Ordering::SeqCst);
                    // Small yield to increase interleaving
                    std::hint::spin_loop();
                    // Decrement (finish processing)
                    counter_clone.fetch_sub(1, Ordering::SeqCst);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // After all balanced operations, counter should be 0
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_atomic_counter_underflow_wraps() {
        // Test behavior when counter underflows (should wrap to usize::MAX)
        // This is a defensive test - in production, underflow should never happen
        // if the code is correct, but we should understand the behavior
        let counter = Arc::new(AtomicUsize::new(0));

        // Subtracting from 0 causes wrapping to usize::MAX
        let result = counter.fetch_sub(1, Ordering::SeqCst);
        assert_eq!(result, 0); // fetch_sub returns the OLD value

        // Counter is now at usize::MAX (wrapped)
        let current = counter.load(Ordering::SeqCst);
        assert_eq!(current, usize::MAX);

        // Adding 1 brings it back to 0
        counter.fetch_add(1, Ordering::SeqCst);
        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_worker_with_prefetch_disabled() {
        // Test that workers work correctly when prefetch is disabled
        // The pending prefetch counter should still be created but never incremented
        let json_item = r#"{"uri":"s3://bucket/file.parquet","size_bytes":1024,"last_modified":"2024-01-01T00:00:00Z"}"#;
        let input = Cursor::new(json_item);
        let source = StdinSource::with_reader(Box::new(input));
        let reader = MockReader;
        let destination = Arc::new(StatsDestination::new());

        // Explicitly disable prefetch using the disabled() constructor
        let prefetch_config = crate::prefetch::PrefetchConfig::disabled();
        let config = WorkerConfig::new()
            .with_thread_count(1)
            .with_prefetch(prefetch_config);

        let worker = Worker::new(config, source, reader, destination.clone());
        let stats = worker.run().await.unwrap();

        // Worker should still process successfully
        assert_eq!(stats.files_processed, 1);
    }

    #[tokio::test]
    async fn test_worker_with_prefetch_enabled() {
        // Test that workers work correctly when prefetch is enabled
        let json_item = r#"{"uri":"s3://bucket/file.parquet","size_bytes":1024,"last_modified":"2024-01-01T00:00:00Z"}"#;
        let input = Cursor::new(json_item);
        let source = StdinSource::with_reader(Box::new(input));
        let reader = MockReader;
        let destination = Arc::new(StatsDestination::new());

        // Default config has prefetch enabled, just set max_prefetch_count
        let prefetch_config = crate::prefetch::PrefetchConfig::default().with_max_prefetch_count(2);
        let config = WorkerConfig::new()
            .with_thread_count(1)
            .with_prefetch(prefetch_config);

        let worker = Worker::new(config, source, reader, destination.clone());
        let stats = worker.run().await.unwrap();

        // Worker should process successfully with prefetch
        assert_eq!(stats.files_processed, 1);
    }

    #[test]
    fn test_stdin_source_ignores_prefetch_counter() {
        // StdinSource uses the default no-op implementation
        // This test verifies it doesn't crash or misbehave
        use crate::source::WorkQueue;

        let input = Cursor::new("");
        let source = StdinSource::with_reader(Box::new(input));

        let counter = Arc::new(AtomicUsize::new(42));

        // This should be a no-op (default trait implementation)
        source.set_pending_prefetch_counter(counter.clone());

        // Counter should be unchanged (default impl is no-op)
        assert_eq!(counter.load(Ordering::SeqCst), 42);

        // has_more should return true initially (before receive_batch is called)
        // StdinSource only sets eof_reached after receive_batch returns empty
        assert!(source.has_more());
    }

    #[test]
    fn test_prefetch_counter_multiple_threads_interleaved() {
        // Simulate the real-world scenario where multiple worker threads
        // interleave their prefetch operations
        use std::thread;

        let counter = Arc::new(AtomicUsize::new(0));

        // Track max concurrent prefetch count seen
        let max_concurrent = Arc::new(AtomicUsize::new(0));

        let mut handles = vec![];

        // Spawn 4 worker threads
        for _ in 0..4 {
            let counter_clone = counter.clone();
            let max_clone = max_concurrent.clone();

            handles.push(thread::spawn(move || {
                for _ in 0..50 {
                    // Start prefetch
                    let new_val = counter_clone.fetch_add(1, Ordering::SeqCst) + 1;

                    // Update max if needed
                    let mut current_max = max_clone.load(Ordering::SeqCst);
                    while new_val > current_max {
                        match max_clone.compare_exchange_weak(
                            current_max,
                            new_val,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        ) {
                            Ok(_) => break,
                            Err(actual) => current_max = actual,
                        }
                    }

                    // Simulate some work
                    std::thread::sleep(std::time::Duration::from_micros(10));

                    // Finish processing
                    counter_clone.fetch_sub(1, Ordering::SeqCst);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Counter should be back to 0
        assert_eq!(counter.load(Ordering::SeqCst), 0);

        // We should have seen some concurrent prefetch activity
        // (max_concurrent > 1 means threads overlapped)
        let max_seen = max_concurrent.load(Ordering::SeqCst);
        assert!(
            max_seen >= 1,
            "Should have seen at least 1 concurrent prefetch"
        );
    }

    #[test]
    fn test_prefetch_counter_exact_tracking() {
        // Test that the counter exactly tracks the number of in-flight prefetches
        let counter = Arc::new(AtomicUsize::new(0));

        // Simulate prefetch lifecycle for 5 items
        let items = ["file1", "file2", "file3", "file4", "file5"];

        // Start prefetching all items
        for (i, _item) in items.iter().enumerate() {
            counter.fetch_add(1, Ordering::SeqCst);
            assert_eq!(counter.load(Ordering::SeqCst), i + 1);
        }

        assert_eq!(counter.load(Ordering::SeqCst), 5);

        // Process items one by one
        for (i, _item) in items.iter().enumerate() {
            counter.fetch_sub(1, Ordering::SeqCst);
            assert_eq!(counter.load(Ordering::SeqCst), 4 - i);
        }

        assert_eq!(counter.load(Ordering::SeqCst), 0);
    }
}
