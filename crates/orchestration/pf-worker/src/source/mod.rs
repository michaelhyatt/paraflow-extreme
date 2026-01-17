//! Work source implementations.
//!
//! This module provides implementations of the [`WorkQueue`](pf_traits::WorkQueue) trait
//! for receiving work items from different sources:
//!
//! - [`StdinSource`]: Reads JSONL work items from stdin (for local testing)
//! - [`SqsSource`]: Receives work items from an AWS SQS queue (for production)
//!
//! Both implement the unified `WorkQueue` trait from pf-traits, providing
//! a consistent interface for the worker regardless of the underlying source.

mod sqs;
mod stdin;

pub use sqs::{SqsSource, SqsSourceConfig};
pub use stdin::StdinSource;

// Re-export WorkQueue and related types from pf-traits for convenience
pub use pf_traits::{FailureContext, QueueMessage, WorkQueue};

use chrono::{DateTime, Utc};
use pf_types::{DestinationConfig, FileFormat, WorkItem};
use serde::Deserialize;

/// A discovered file from pf-discoverer.
///
/// This is the simpler format output by `pf-discoverer` that can be
/// piped directly to `pf-worker` or sent via SQS.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct DiscoveredFile {
    /// The S3 URI of the file (e.g., "s3://bucket/path/file.parquet")
    uri: String,

    /// Size of the file in bytes
    size_bytes: u64,

    /// Last modified timestamp (if available)
    #[serde(default)]
    last_modified: Option<DateTime<Utc>>,
}

impl DiscoveredFile {
    /// Convert to a WorkItem with default configuration.
    ///
    /// Uses format detection based on file extension and placeholder
    /// destination configuration.
    pub(crate) fn into_work_item(self) -> WorkItem {
        // Detect format from file extension
        let format = guess_format_from_uri(&self.uri);

        WorkItem {
            job_id: "discovered".to_string(),
            file_uri: self.uri,
            file_size_bytes: self.size_bytes,
            format,
            destination: DestinationConfig {
                endpoint: "".to_string(),
                index: "".to_string(),
                credentials: None,
            },
            transform: None,
            attempt: 0,
            enqueued_at: self.last_modified.unwrap_or_else(Utc::now),
        }
    }
}

/// Guess file format from URI extension.
pub(crate) fn guess_format_from_uri(uri: &str) -> FileFormat {
    let uri_lower = uri.to_lowercase();
    if uri_lower.ends_with(".parquet") || uri_lower.ends_with(".pq") {
        FileFormat::Parquet
    } else if uri_lower.ends_with(".ndjson")
        || uri_lower.ends_with(".jsonl")
        || uri_lower.ends_with(".json")
    {
        FileFormat::NdJson
    } else {
        // Default to Parquet
        FileFormat::Parquet
    }
}

/// Parse a JSON string into a WorkItem.
///
/// Tries to parse as a full WorkItem first, then falls back to
/// DiscoveredFile format (from pf-discoverer).
pub(crate) fn parse_work_item(json: &str) -> Option<WorkItem> {
    // Try full WorkItem format first
    if let Ok(work_item) = serde_json::from_str::<WorkItem>(json) {
        return Some(work_item);
    }

    // Fall back to DiscoveredFile format (pf-discoverer output)
    if let Ok(discovered) = serde_json::from_str::<DiscoveredFile>(json) {
        return Some(discovered.into_work_item());
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
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
    fn test_guess_format_from_uri() {
        assert_eq!(
            guess_format_from_uri("s3://bucket/file.parquet"),
            FileFormat::Parquet
        );
        assert_eq!(
            guess_format_from_uri("s3://bucket/file.pq"),
            FileFormat::Parquet
        );
        assert_eq!(
            guess_format_from_uri("s3://bucket/file.ndjson"),
            FileFormat::NdJson
        );
        assert_eq!(
            guess_format_from_uri("s3://bucket/file.jsonl"),
            FileFormat::NdJson
        );
        assert_eq!(
            guess_format_from_uri("s3://bucket/file.json"),
            FileFormat::NdJson
        );
        // Unknown extension defaults to Parquet
        assert_eq!(
            guess_format_from_uri("s3://bucket/file.unknown"),
            FileFormat::Parquet
        );
    }

    #[test]
    fn test_parse_work_item_full_format() {
        let item = create_test_work_item();
        let json = serde_json::to_string(&item).unwrap();
        let parsed = parse_work_item(&json).unwrap();
        assert_eq!(parsed.job_id, "test-job");
    }

    #[test]
    fn test_parse_work_item_discovered_format() {
        let json = r#"{"uri":"s3://bucket/file.parquet","size_bytes":1024}"#;
        let item = parse_work_item(json).unwrap();
        assert_eq!(item.file_uri, "s3://bucket/file.parquet");
        assert_eq!(item.file_size_bytes, 1024);
        assert_eq!(item.job_id, "discovered");
    }

    #[test]
    fn test_parse_work_item_invalid() {
        assert!(parse_work_item("invalid json").is_none());
        assert!(parse_work_item(r#"{"foo": "bar"}"#).is_none());
    }
}
