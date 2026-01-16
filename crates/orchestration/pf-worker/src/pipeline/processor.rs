//! File processing pipeline.

use super::ThreadStats;
use crate::source::WorkMessage;
use crate::stats::WorkerStats;
use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use pf_error::{classify_error, ErrorCategory, PfError, ProcessingStage};
use pf_traits::{BatchIndexer, BatchStream, StreamingReader};
use pf_types::WorkItem;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, trace, warn};

/// Processing result for a single file.
#[derive(Debug)]
pub struct ProcessingResult {
    /// Number of records successfully processed
    pub records_processed: u64,

    /// Number of records that failed
    pub records_failed: u64,

    /// Bytes read from the source file
    pub bytes_read: u64,

    /// Bytes written to the destination
    pub bytes_written: u64,

    /// Whether the processing completed successfully
    pub success: bool,

    /// Error if processing failed
    pub error: Option<PfError>,

    /// Error category for retry decisions
    pub error_category: Option<ErrorCategory>,
}

impl ProcessingResult {
    /// Create a successful result.
    pub fn success(records: u64, bytes_read: u64, bytes_written: u64) -> Self {
        Self {
            records_processed: records,
            records_failed: 0,
            bytes_read,
            bytes_written,
            success: true,
            error: None,
            error_category: None,
        }
    }

    /// Create a failed result.
    pub fn failure(error: PfError, stage: ProcessingStage) -> Self {
        let category = classify_error(&error, stage);
        Self {
            records_processed: 0,
            records_failed: 0,
            bytes_read: 0,
            bytes_written: 0,
            success: false,
            error: Some(error),
            error_category: Some(category),
        }
    }

    /// Create a partial success result.
    pub fn partial(
        records_processed: u64,
        records_failed: u64,
        bytes_read: u64,
        bytes_written: u64,
    ) -> Self {
        Self {
            records_processed,
            records_failed,
            bytes_read,
            bytes_written,
            success: records_failed == 0,
            error: None,
            error_category: if records_failed > 0 {
                Some(ErrorCategory::Partial)
            } else {
                None
            },
        }
    }
}

/// Per-thread processing pipeline.
///
/// Each thread has its own pipeline instance that processes files
/// through Reader → Transform → Destination stages.
pub struct Pipeline {
    /// Thread identifier
    thread_id: u32,

    /// Reader for opening and streaming files
    reader: Arc<dyn StreamingReader>,

    /// Destination for writing processed batches
    destination: Arc<dyn BatchIndexer>,

    /// Per-thread statistics
    stats: Arc<ThreadStats>,

    /// Global worker statistics
    global_stats: Arc<WorkerStats>,
}

impl Pipeline {
    /// Create a new pipeline.
    pub fn new(
        thread_id: u32,
        reader: Arc<dyn StreamingReader>,
        destination: Arc<dyn BatchIndexer>,
        global_stats: Arc<WorkerStats>,
    ) -> Self {
        Self {
            thread_id,
            reader,
            destination,
            stats: Arc::new(ThreadStats::new(thread_id)),
            global_stats,
        }
    }

    /// Get the thread statistics.
    pub fn stats(&self) -> &Arc<ThreadStats> {
        &self.stats
    }

    /// Process a single work item.
    pub async fn process(&self, message: &WorkMessage) -> ProcessingResult {
        let start = Instant::now();
        let work_item = &message.work_item;

        debug!(
            thread = self.thread_id,
            file = %work_item.file_uri,
            "Processing file"
        );

        // Open the file and get a batch stream
        let stream = match self.reader.read_stream(&work_item.file_uri).await {
            Ok(stream) => stream,
            Err(e) => {
                error!(
                    thread = self.thread_id,
                    file = %work_item.file_uri,
                    error = %e,
                    "Failed to open file"
                );
                self.stats.record_file_failure();
                self.global_stats.record_file_failure();
                return ProcessingResult::failure(e, ProcessingStage::S3Download);
            }
        };

        // Process the batch stream
        let result = self.process_stream(stream, work_item).await;
        let duration = start.elapsed();

        if result.success {
            self.stats.record_file_success(
                result.records_processed,
                result.bytes_read,
                result.bytes_written,
                duration,
            );
            self.global_stats.record_file_success(
                result.records_processed,
                result.bytes_read,
                result.bytes_written,
            );
            info!(
                thread = self.thread_id,
                file = %work_item.file_uri,
                records = result.records_processed,
                duration_ms = duration.as_millis(),
                "File processed successfully"
            );
        } else {
            self.stats.record_file_failure();
            self.global_stats.record_file_failure();
            if let Some(ref error) = result.error {
                warn!(
                    thread = self.thread_id,
                    file = %work_item.file_uri,
                    error = %error,
                    "File processing failed"
                );
            }
        }

        result
    }

    /// Process a batch stream.
    async fn process_stream(
        &self,
        mut stream: BatchStream,
        work_item: &WorkItem,
    ) -> ProcessingResult {
        let mut total_records = 0u64;
        let mut total_bytes_read = 0u64;
        let mut total_bytes_written = 0u64;
        let mut failed_records = 0u64;
        let mut batch_buffer: Vec<Arc<RecordBatch>> = Vec::new();

        while let Some(batch_result) = stream.next().await {
            match batch_result {
                Ok(batch) => {
                    let batch_records = batch.num_rows() as u64;
                    let batch_bytes = batch.metadata().size_bytes as u64;
                    total_bytes_read += batch_bytes;

                    // Get the inner RecordBatch
                    let record_batch = batch.into_arc();
                    batch_buffer.push(record_batch);

                    trace!(
                        thread = self.thread_id,
                        records = batch_records,
                        "Received batch"
                    );

                    // Send batches to destination when buffer is full
                    // For now, send each batch immediately
                    if !batch_buffer.is_empty() {
                        match self.destination.index_batches(&batch_buffer).await {
                            Ok(result) => {
                                total_records += result.success_count;
                                total_bytes_written += result.bytes_sent;
                                failed_records += result.failed_records.len() as u64;

                                self.stats.record_batch(result.success_count, result.bytes_sent);
                                self.global_stats.record_batch();

                                if !result.failed_records.is_empty() {
                                    self.stats.record_failed_records(result.failed_records.len() as u64);
                                    self.global_stats.record_failed_records(result.failed_records.len() as u64);
                                }
                            }
                            Err(e) => {
                                error!(
                                    thread = self.thread_id,
                                    error = %e,
                                    "Failed to index batch"
                                );
                                return ProcessingResult::failure(e, ProcessingStage::EsIndexing);
                            }
                        }
                        batch_buffer.clear();
                    }
                }
                Err(e) => {
                    error!(
                        thread = self.thread_id,
                        error = %e,
                        "Failed to read batch"
                    );
                    // Determine the processing stage based on file format
                    let stage = match work_item.format {
                        pf_types::FileFormat::Parquet => ProcessingStage::ParquetDecode,
                        pf_types::FileFormat::NdJson => ProcessingStage::NdjsonParse,
                    };
                    return ProcessingResult::failure(e, stage);
                }
            }
        }

        // Flush the destination
        if let Err(e) = self.destination.flush().await {
            warn!(
                thread = self.thread_id,
                error = %e,
                "Failed to flush destination"
            );
        }

        if failed_records > 0 {
            ProcessingResult::partial(total_records, failed_records, total_bytes_read, total_bytes_written)
        } else {
            ProcessingResult::success(total_records, total_bytes_read, total_bytes_written)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::destination::StatsDestination;
    use async_trait::async_trait;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use chrono::Utc;
    use futures::stream;
    use pf_error::Result;
    use pf_traits::FileMetadata;
    use pf_types::{Batch, DestinationConfig, FileFormat};

    struct MockReader {
        batches: Vec<Batch>,
        should_fail: bool,
    }

    impl MockReader {
        fn new(batches: Vec<Batch>) -> Self {
            Self {
                batches,
                should_fail: false,
            }
        }

        fn failing() -> Self {
            Self {
                batches: vec![],
                should_fail: true,
            }
        }
    }

    #[async_trait]
    impl StreamingReader for MockReader {
        async fn read_stream(&self, _uri: &str) -> Result<BatchStream> {
            if self.should_fail {
                return Err(PfError::Reader(pf_error::ReaderError::NotFound(
                    "test file".to_string(),
                )));
            }

            let batches: Vec<Result<Batch>> = self.batches.iter().cloned().map(Ok).collect();
            Ok(Box::pin(stream::iter(batches)))
        }

        async fn file_metadata(&self, _uri: &str) -> Result<FileMetadata> {
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
            Ok(FileMetadata::new(1024, schema))
        }
    }

    fn create_test_batch(num_rows: usize) -> Batch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let ids: Vec<i64> = (0..num_rows as i64).collect();
        let id_array = Int64Array::from(ids);
        let record_batch =
            RecordBatch::try_new(schema, vec![Arc::new(id_array)]).unwrap();

        Batch::new(record_batch, "test.parquet", 0)
    }

    fn create_test_message(file_uri: &str) -> WorkMessage {
        WorkMessage {
            id: "test-msg".to_string(),
            work_item: WorkItem {
                job_id: "test-job".to_string(),
                file_uri: file_uri.to_string(),
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
        }
    }

    #[tokio::test]
    async fn test_pipeline_success() {
        let batch = create_test_batch(100);
        let reader = Arc::new(MockReader::new(vec![batch]));
        let destination = Arc::new(StatsDestination::new());
        let global_stats = Arc::new(WorkerStats::new());

        let pipeline = Pipeline::new(0, reader, destination.clone(), global_stats.clone());
        let message = create_test_message("s3://bucket/test.parquet");

        let result = pipeline.process(&message).await;

        assert!(result.success);
        assert_eq!(result.records_processed, 100);
        assert!(result.error.is_none());

        assert_eq!(global_stats.files_processed(), 1);
        assert_eq!(destination.get_stats().records, 100);
    }

    #[tokio::test]
    async fn test_pipeline_file_not_found() {
        let reader = Arc::new(MockReader::failing());
        let destination = Arc::new(StatsDestination::new());
        let global_stats = Arc::new(WorkerStats::new());

        let pipeline = Pipeline::new(0, reader, destination, global_stats.clone());
        let message = create_test_message("s3://bucket/missing.parquet");

        let result = pipeline.process(&message).await;

        assert!(!result.success);
        assert!(result.error.is_some());
        assert_eq!(result.error_category, Some(ErrorCategory::Permanent));

        assert_eq!(global_stats.files_failed(), 1);
    }

    #[tokio::test]
    async fn test_pipeline_multiple_batches() {
        let batches = vec![create_test_batch(50), create_test_batch(75), create_test_batch(25)];
        let reader = Arc::new(MockReader::new(batches));
        let destination = Arc::new(StatsDestination::new());
        let global_stats = Arc::new(WorkerStats::new());

        let pipeline = Pipeline::new(0, reader, destination.clone(), global_stats);
        let message = create_test_message("s3://bucket/test.parquet");

        let result = pipeline.process(&message).await;

        assert!(result.success);
        assert_eq!(result.records_processed, 150);
        assert_eq!(destination.get_stats().records, 150);
        assert_eq!(destination.get_stats().batches, 3);
    }

    #[tokio::test]
    async fn test_processing_result_types() {
        let success = ProcessingResult::success(100, 1024, 2048);
        assert!(success.success);
        assert!(success.error.is_none());

        let failure = ProcessingResult::failure(
            PfError::Reader(pf_error::ReaderError::NotFound("test".to_string())),
            ProcessingStage::S3Download,
        );
        assert!(!failure.success);
        assert!(failure.error.is_some());
        assert_eq!(failure.error_category, Some(ErrorCategory::Permanent));

        let partial = ProcessingResult::partial(90, 10, 1024, 2048);
        assert!(!partial.success);
        assert_eq!(partial.error_category, Some(ErrorCategory::Partial));
    }
}
