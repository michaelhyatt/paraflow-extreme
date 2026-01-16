//! Error types and classification for Paraflow Extreme.
//!
//! This crate provides:
//! - [`PfError`] - Top-level error enum for all pipeline errors
//! - Domain-specific errors ([`QueueError`], [`ReaderError`], [`IndexerError`], [`TransformError`])
//! - [`ErrorCategory`] for retry/DLQ decision making
//! - Error classification logic based on error type and processing stage

use thiserror::Error;

/// Top-level error type for Paraflow Extreme.
#[derive(Error, Debug)]
pub enum PfError {
    /// Queue-related errors (enqueue, dequeue, ack/nack)
    #[error("Queue error: {0}")]
    Queue(#[from] QueueError),

    /// Reader errors (file access, parsing)
    #[error("Reader error: {0}")]
    Reader(#[from] ReaderError),

    /// Indexer errors (connection, bulk API)
    #[error("Indexer error: {0}")]
    Indexer(#[from] IndexerError),

    /// Transform errors (script execution, enrichment)
    #[error("Transform error: {0}")]
    Transform(#[from] TransformError),

    /// Configuration errors
    #[error("Configuration error: {0}")]
    Config(String),

    /// Generic errors (wrapped anyhow)
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Queue-related errors.
#[derive(Error, Debug)]
pub enum QueueError {
    /// Failed to connect to queue backend
    #[error("Connection failed: {0}")]
    Connection(String),

    /// Failed to enqueue a message
    #[error("Enqueue failed: {0}")]
    Enqueue(String),

    /// Failed to receive messages
    #[error("Receive failed: {0}")]
    Receive(String),

    /// Failed to acknowledge a message
    #[error("Ack failed: {0}")]
    Ack(String),

    /// Failed to negative-acknowledge a message
    #[error("Nack failed: {0}")]
    Nack(String),

    /// Failed to move message to DLQ
    #[error("DLQ move failed: {0}")]
    DlqMove(String),

    /// Message deserialization failed
    #[error("Deserialization failed: {0}")]
    Deserialize(String),

    /// Message serialization failed
    #[error("Serialization failed: {0}")]
    Serialize(String),
}

/// Reader-related errors.
#[derive(Error, Debug)]
pub enum ReaderError {
    /// File not found
    #[error("File not found: {0}")]
    NotFound(String),

    /// Access denied
    #[error("Access denied: {0}")]
    AccessDenied(String),

    /// File is corrupted or invalid
    #[error("Invalid file format: {0}")]
    InvalidFormat(String),

    /// I/O error during read
    #[error("I/O error: {0}")]
    Io(String),

    /// Schema mismatch or unsupported type
    #[error("Schema error: {0}")]
    Schema(String),

    /// Decompression failed
    #[error("Decompression failed: {0}")]
    Decompression(String),
}

/// Indexer-related errors.
#[derive(Error, Debug)]
pub enum IndexerError {
    /// Failed to connect to indexer
    #[error("Connection failed: {0}")]
    Connection(String),

    /// Bulk indexing failed
    #[error("Bulk index failed: {0}")]
    BulkFailed(String),

    /// Partial bulk failure (some docs rejected)
    #[error("Partial failure: {success} succeeded, {failed} failed")]
    PartialFailure { success: u64, failed: u64 },

    /// Rate limited / throttled
    #[error("Rate limited: {0}")]
    RateLimited(String),

    /// Cluster unavailable
    #[error("Cluster unavailable: {0}")]
    Unavailable(String),

    /// Index mapping error
    #[error("Mapping error: {0}")]
    MappingError(String),
}

/// Transform-related errors.
#[derive(Error, Debug)]
pub enum TransformError {
    /// Script compilation failed
    #[error("Script compilation failed: {0}")]
    Compilation(String),

    /// Script execution failed
    #[error("Script execution failed: {0}")]
    Execution(String),

    /// Enrichment table not found
    #[error("Enrichment table not found: {0}")]
    TableNotFound(String),

    /// Enrichment lookup failed
    #[error("Enrichment lookup failed: {0}")]
    LookupFailed(String),
}

/// Error classification for retry decisions.
///
/// Used to determine whether to retry, send to DLQ, or continue with partial success.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    /// Transient error - retry with exponential backoff
    ///
    /// Examples: network timeout, ES 503, S3 throttling
    Transient,

    /// Permanent error - never retry, send directly to DLQ
    ///
    /// Examples: file not found, access denied, corrupt file
    Permanent,

    /// Partial failure - some records failed, continue processing
    ///
    /// Examples: ES bulk partially rejected, transform errors on some records
    Partial,
}

/// Processing stage for error context.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessingStage {
    /// Downloading from S3 or reading from filesystem
    S3Download,

    /// Decoding Parquet file
    ParquetDecode,

    /// Parsing NDJSON
    NdjsonParse,

    /// Executing Rhai transform
    Transform,

    /// Performing enrichment lookups
    Enrichment,

    /// Indexing to Elasticsearch
    EsIndexing,
}

impl std::fmt::Display for ProcessingStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::S3Download => write!(f, "S3Download"),
            Self::ParquetDecode => write!(f, "ParquetDecode"),
            Self::NdjsonParse => write!(f, "NdjsonParse"),
            Self::Transform => write!(f, "Transform"),
            Self::Enrichment => write!(f, "Enrichment"),
            Self::EsIndexing => write!(f, "EsIndexing"),
        }
    }
}

/// Classifies an error to determine retry behavior.
///
/// # Arguments
///
/// * `error` - The error to classify
/// * `stage` - The processing stage where the error occurred
///
/// # Returns
///
/// The appropriate [`ErrorCategory`] for retry decisions
pub fn classify_error(error: &PfError, stage: ProcessingStage) -> ErrorCategory {
    match error {
        PfError::Queue(e) => classify_queue_error(e),
        PfError::Reader(e) => classify_reader_error(e, stage),
        PfError::Indexer(e) => classify_indexer_error(e),
        PfError::Transform(e) => classify_transform_error(e),
        PfError::Config(_) => ErrorCategory::Permanent,
        PfError::Other(e) => classify_anyhow_error(e, stage),
    }
}

fn classify_queue_error(error: &QueueError) -> ErrorCategory {
    match error {
        QueueError::Connection(_) => ErrorCategory::Transient,
        QueueError::Enqueue(_) => ErrorCategory::Transient,
        QueueError::Receive(_) => ErrorCategory::Transient,
        QueueError::Ack(_) => ErrorCategory::Transient,
        QueueError::Nack(_) => ErrorCategory::Transient,
        QueueError::DlqMove(_) => ErrorCategory::Transient,
        QueueError::Deserialize(_) => ErrorCategory::Permanent,
        QueueError::Serialize(_) => ErrorCategory::Permanent,
    }
}

fn classify_reader_error(error: &ReaderError, stage: ProcessingStage) -> ErrorCategory {
    match error {
        ReaderError::NotFound(_) => ErrorCategory::Permanent,
        ReaderError::AccessDenied(_) => ErrorCategory::Permanent,
        ReaderError::InvalidFormat(_) => ErrorCategory::Permanent,
        ReaderError::Io(_) => {
            // I/O errors during S3 download might be transient
            if matches!(stage, ProcessingStage::S3Download) {
                ErrorCategory::Transient
            } else {
                ErrorCategory::Permanent
            }
        }
        ReaderError::Schema(_) => ErrorCategory::Permanent,
        ReaderError::Decompression(_) => ErrorCategory::Permanent,
    }
}

fn classify_indexer_error(error: &IndexerError) -> ErrorCategory {
    match error {
        IndexerError::Connection(_) => ErrorCategory::Transient,
        IndexerError::BulkFailed(_) => ErrorCategory::Transient,
        IndexerError::PartialFailure { .. } => ErrorCategory::Partial,
        IndexerError::RateLimited(_) => ErrorCategory::Transient,
        IndexerError::Unavailable(_) => ErrorCategory::Transient,
        IndexerError::MappingError(_) => ErrorCategory::Permanent,
    }
}

fn classify_transform_error(error: &TransformError) -> ErrorCategory {
    match error {
        TransformError::Compilation(_) => ErrorCategory::Permanent,
        TransformError::Execution(_) => ErrorCategory::Permanent,
        TransformError::TableNotFound(_) => ErrorCategory::Permanent,
        TransformError::LookupFailed(_) => ErrorCategory::Partial,
    }
}

fn classify_anyhow_error(error: &anyhow::Error, stage: ProcessingStage) -> ErrorCategory {
    let err_string = error.to_string().to_lowercase();

    match stage {
        ProcessingStage::S3Download => {
            if err_string.contains("nosuchkey")
                || err_string.contains("404")
                || err_string.contains("accessdenied")
                || err_string.contains("403")
            {
                ErrorCategory::Permanent
            } else {
                ErrorCategory::Transient
            }
        }
        ProcessingStage::ParquetDecode | ProcessingStage::NdjsonParse => {
            if err_string.contains("corrupt") || err_string.contains("invalid") {
                ErrorCategory::Permanent
            } else {
                ErrorCategory::Transient
            }
        }
        ProcessingStage::Transform | ProcessingStage::Enrichment => ErrorCategory::Permanent,
        ProcessingStage::EsIndexing => {
            if err_string.contains("429") || err_string.contains("503") {
                ErrorCategory::Transient
            } else if err_string.contains("400") || err_string.contains("mapper_parsing") {
                ErrorCategory::Permanent
            } else {
                ErrorCategory::Transient
            }
        }
    }
}

/// Result type alias using PfError.
pub type Result<T> = std::result::Result<T, PfError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_classification_reader_not_found() {
        let error = PfError::Reader(ReaderError::NotFound("file.parquet".to_string()));
        assert_eq!(
            classify_error(&error, ProcessingStage::S3Download),
            ErrorCategory::Permanent
        );
    }

    #[test]
    fn test_error_classification_indexer_rate_limited() {
        let error = PfError::Indexer(IndexerError::RateLimited(
            "429 Too Many Requests".to_string(),
        ));
        assert_eq!(
            classify_error(&error, ProcessingStage::EsIndexing),
            ErrorCategory::Transient
        );
    }

    #[test]
    fn test_error_classification_partial_failure() {
        let error = PfError::Indexer(IndexerError::PartialFailure {
            success: 900,
            failed: 100,
        });
        assert_eq!(
            classify_error(&error, ProcessingStage::EsIndexing),
            ErrorCategory::Partial
        );
    }

    #[test]
    fn test_error_display() {
        let error = PfError::Reader(ReaderError::NotFound(
            "s3://bucket/file.parquet".to_string(),
        ));
        assert!(error.to_string().contains("File not found"));
    }

    #[test]
    fn test_processing_stage_display() {
        assert_eq!(ProcessingStage::S3Download.to_string(), "S3Download");
        assert_eq!(ProcessingStage::EsIndexing.to_string(), "EsIndexing");
    }
}
