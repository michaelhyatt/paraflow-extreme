//! Checkpoint types for progress tracking and failure reporting.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Tracks progress through a file for recovery and reporting.
///
/// Checkpoints enable:
/// - Progress reporting to users
/// - Failure tracking with record-level granularity
/// - Decision making for ack/nack/DLQ based on failure rates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingCheckpoint {
    /// URI of the file being processed
    pub file_uri: String,

    /// Total size of the file in bytes
    pub file_size_bytes: u64,

    /// Bytes read so far
    pub bytes_read: u64,

    /// Records read from the file
    pub records_read: u64,

    /// Records successfully transformed
    pub records_transformed: u64,

    /// Records successfully indexed
    pub records_indexed: u64,

    /// Records that failed to index
    pub records_failed: u64,

    /// Number of batches sent to indexer
    pub batches_sent: u64,

    /// Batches where all records succeeded
    pub batches_fully_succeeded: u64,

    /// Batches with at least one failed record
    pub batches_partially_failed: u64,

    /// Details of failed records (capped to prevent memory issues)
    pub failed_records: Vec<FailedRecord>,

    /// True if failed_records was truncated due to cap
    pub failed_records_truncated: bool,

    /// When processing started
    pub started_at: DateTime<Utc>,

    /// Last progress update time
    pub last_progress_at: DateTime<Utc>,
}

impl ProcessingCheckpoint {
    /// Maximum number of failed records to track in detail.
    pub const MAX_FAILED_RECORDS: usize = 1000;

    /// Creates a new checkpoint for the given file.
    pub fn new(file_uri: impl Into<String>, file_size_bytes: u64) -> Self {
        let now = Utc::now();
        Self {
            file_uri: file_uri.into(),
            file_size_bytes,
            bytes_read: 0,
            records_read: 0,
            records_transformed: 0,
            records_indexed: 0,
            records_failed: 0,
            batches_sent: 0,
            batches_fully_succeeded: 0,
            batches_partially_failed: 0,
            failed_records: Vec::new(),
            failed_records_truncated: false,
            started_at: now,
            last_progress_at: now,
        }
    }

    /// Adds a failed record to the checkpoint.
    ///
    /// Returns true if the record was added, false if truncated.
    pub fn add_failed_record(&mut self, record: FailedRecord) -> bool {
        if self.failed_records.len() < Self::MAX_FAILED_RECORDS {
            self.failed_records.push(record);
            true
        } else {
            self.failed_records_truncated = true;
            false
        }
    }

    /// Updates the last progress timestamp.
    pub fn touch(&mut self) {
        self.last_progress_at = Utc::now();
    }

    /// Returns the failure rate (0.0 to 1.0).
    pub fn failure_rate(&self) -> f64 {
        let total = self.records_indexed + self.records_failed;
        if total == 0 {
            0.0
        } else {
            self.records_failed as f64 / total as f64
        }
    }

    /// Returns the processing duration.
    pub fn duration(&self) -> chrono::Duration {
        self.last_progress_at - self.started_at
    }

    /// Returns progress as a percentage (0.0 to 100.0).
    pub fn progress_percent(&self) -> f64 {
        if self.file_size_bytes == 0 {
            100.0
        } else {
            (self.bytes_read as f64 / self.file_size_bytes as f64) * 100.0
        }
    }
}

/// Details about a single failed record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailedRecord {
    /// Batch index within the file
    pub batch_index: u64,

    /// Record offset within the file (for replay)
    pub record_offset_in_file: u64,

    /// Document ID if available (e.g., from ES response)
    pub record_id: Option<String>,

    /// Type of error (e.g., "mapper_parsing_exception")
    pub error_type: String,

    /// Human-readable error message
    pub error_message: String,
}

impl FailedRecord {
    /// Creates a new failed record entry.
    pub fn new(
        batch_index: u64,
        record_offset_in_file: u64,
        error_type: impl Into<String>,
        error_message: impl Into<String>,
    ) -> Self {
        Self {
            batch_index,
            record_offset_in_file,
            record_id: None,
            error_type: error_type.into(),
            error_message: error_message.into(),
        }
    }

    /// Sets the record ID.
    pub fn with_record_id(mut self, id: impl Into<String>) -> Self {
        self.record_id = Some(id.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_creation() {
        let cp = ProcessingCheckpoint::new("s3://bucket/file.parquet", 1024 * 1024);

        assert_eq!(cp.file_uri, "s3://bucket/file.parquet");
        assert_eq!(cp.file_size_bytes, 1024 * 1024);
        assert_eq!(cp.records_read, 0);
        assert_eq!(cp.failure_rate(), 0.0);
    }

    #[test]
    fn test_failure_rate() {
        let mut cp = ProcessingCheckpoint::new("file.parquet", 1000);
        cp.records_indexed = 90;
        cp.records_failed = 10;

        assert!((cp.failure_rate() - 0.1).abs() < 0.001);
    }

    #[test]
    fn test_failed_record_truncation() {
        let mut cp = ProcessingCheckpoint::new("file.parquet", 1000);

        for i in 0..ProcessingCheckpoint::MAX_FAILED_RECORDS + 100 {
            let record = FailedRecord::new(0, i as u64, "error", "message");
            cp.add_failed_record(record);
        }

        assert_eq!(
            cp.failed_records.len(),
            ProcessingCheckpoint::MAX_FAILED_RECORDS
        );
        assert!(cp.failed_records_truncated);
    }
}
