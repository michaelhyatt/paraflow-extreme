//! Batch indexer trait and related types.

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use pf_error::Result;
use std::sync::Arc;
use std::time::Duration;

/// Trait for batch indexers.
///
/// Indexers receive batches of Arrow RecordBatches and write them to
/// a destination (Elasticsearch, stdout, etc.).
///
/// # Implementations
///
/// - Elasticsearch indexer: Bulk API with partial failure handling
/// - Stdout indexer: JSON output for testing/debugging
#[async_trait]
pub trait BatchIndexer: Send + Sync {
    /// Indexes a slice of batches.
    ///
    /// # Arguments
    ///
    /// * `batches` - Batches to index
    ///
    /// # Returns
    ///
    /// Result with success/failure counts
    async fn index_batches(&self, batches: &[Arc<RecordBatch>]) -> Result<IndexResult>;

    /// Flushes any buffered data.
    async fn flush(&self) -> Result<()>;

    /// Checks if the indexer is healthy and can accept data.
    async fn health_check(&self) -> Result<bool>;
}

/// Result of indexing a batch of records.
#[derive(Debug, Clone, Default)]
pub struct IndexResult {
    /// Number of records successfully indexed
    pub success_count: u64,

    /// Details of failed records
    pub failed_records: Vec<RecordFailure>,

    /// Total bytes sent to the indexer
    pub bytes_sent: u64,

    /// Time taken for the indexing operation
    pub duration: Duration,
}

impl IndexResult {
    /// Creates a new successful result.
    pub fn success(count: u64, bytes: u64, duration: Duration) -> Self {
        Self {
            success_count: count,
            failed_records: Vec::new(),
            bytes_sent: bytes,
            duration,
        }
    }

    /// Creates a result with failures.
    pub fn with_failures(mut self, failures: Vec<RecordFailure>) -> Self {
        self.failed_records = failures;
        self
    }

    /// Returns the total number of records (success + failed).
    pub fn total_count(&self) -> u64 {
        self.success_count + self.failed_records.len() as u64
    }

    /// Returns true if all records succeeded.
    pub fn is_complete_success(&self) -> bool {
        self.failed_records.is_empty()
    }

    /// Returns the failure rate (0.0 to 1.0).
    pub fn failure_rate(&self) -> f64 {
        let total = self.total_count();
        if total == 0 {
            0.0
        } else {
            self.failed_records.len() as f64 / total as f64
        }
    }
}

/// Details about a single failed record.
#[derive(Debug, Clone)]
pub struct RecordFailure {
    /// Offset of the record within the batch
    pub offset_in_batch: u64,

    /// Document ID if available (from ES response)
    pub doc_id: Option<String>,

    /// Error type (e.g., "mapper_parsing_exception")
    pub error_type: String,

    /// Human-readable error message
    pub error_message: String,

    /// Whether this error is retryable
    pub retryable: bool,
}

impl RecordFailure {
    /// Creates a new record failure.
    pub fn new(
        offset_in_batch: u64,
        error_type: impl Into<String>,
        error_message: impl Into<String>,
    ) -> Self {
        Self {
            offset_in_batch,
            doc_id: None,
            error_type: error_type.into(),
            error_message: error_message.into(),
            retryable: false,
        }
    }

    /// Sets the document ID.
    pub fn with_doc_id(mut self, id: impl Into<String>) -> Self {
        self.doc_id = Some(id.into());
        self
    }

    /// Marks this error as retryable.
    pub fn retryable(mut self) -> Self {
        self.retryable = true;
        self
    }
}

/// Checks if an Elasticsearch error type is retryable.
pub fn is_retryable_es_error(error_type: &str) -> bool {
    matches!(
        error_type,
        "es_rejected_execution_exception"  // Queue full
            | "cluster_block_exception"    // Cluster overwhelmed
            | "timeout_exception"          // Network timeout
            | "circuit_breaking_exception" // Memory pressure
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_result_success() {
        let result = IndexResult::success(100, 1024, Duration::from_millis(50));

        assert_eq!(result.success_count, 100);
        assert_eq!(result.total_count(), 100);
        assert!(result.is_complete_success());
        assert_eq!(result.failure_rate(), 0.0);
    }

    #[test]
    fn test_index_result_with_failures() {
        let failures = vec![
            RecordFailure::new(5, "mapper_parsing_exception", "failed to parse"),
            RecordFailure::new(10, "strict_dynamic_mapping", "field not allowed"),
        ];

        let result =
            IndexResult::success(98, 1024, Duration::from_millis(50)).with_failures(failures);

        assert_eq!(result.success_count, 98);
        assert_eq!(result.total_count(), 100);
        assert!(!result.is_complete_success());
        assert!((result.failure_rate() - 0.02).abs() < 0.001);
    }

    #[test]
    fn test_retryable_errors() {
        assert!(is_retryable_es_error("es_rejected_execution_exception"));
        assert!(is_retryable_es_error("timeout_exception"));
        assert!(!is_retryable_es_error("mapper_parsing_exception"));
        assert!(!is_retryable_es_error("strict_dynamic_mapping"));
    }
}
