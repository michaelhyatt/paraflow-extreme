//! Work item types representing queue message payloads.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A work item representing a file to be processed.
///
/// This is the payload of queue messages that workers receive.
/// Each work item describes a single file and its processing configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkItem {
    /// Unique identifier for the job this file belongs to
    pub job_id: String,

    /// URI of the file to process (e.g., `s3://bucket/key` or `file:///path`)
    pub file_uri: String,

    /// Size of the file in bytes (for progress tracking)
    pub file_size_bytes: u64,

    /// Format of the file
    pub format: FileFormat,

    /// Destination configuration for indexing
    pub destination: DestinationConfig,

    /// Optional transform configuration
    pub transform: Option<TransformConfig>,

    /// Current attempt number (0-indexed)
    pub attempt: u32,

    /// When this work item was enqueued
    pub enqueued_at: DateTime<Utc>,
}

/// Supported file formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FileFormat {
    /// Apache Parquet columnar format
    Parquet,

    /// Newline-delimited JSON
    NdJson,
}

impl FileFormat {
    /// Returns the typical file extensions for this format.
    pub fn extensions(&self) -> &[&str] {
        match self {
            FileFormat::Parquet => &["parquet", "pq"],
            FileFormat::NdJson => &["ndjson", "jsonl", "json"],
        }
    }
}

/// Configuration for the indexing destination.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DestinationConfig {
    /// Endpoint URL (e.g., Elasticsearch endpoint)
    pub endpoint: String,

    /// Target index name
    pub index: String,

    /// Optional credentials
    pub credentials: Option<Credentials>,
}

/// Credentials for destination authentication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Credentials {
    /// Username for basic auth
    pub username: Option<String>,

    /// Password for basic auth
    pub password: Option<String>,

    /// API key for token auth
    pub api_key: Option<String>,
}

/// Configuration for transforms applied during processing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformConfig {
    /// Inline Rhai script
    pub script: Option<String>,

    /// Path to Rhai script file (S3 or local)
    pub script_file: Option<String>,

    /// How to handle transform errors
    pub error_policy: ErrorPolicy,
}

/// Policy for handling transform errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ErrorPolicy {
    /// Drop records that error, continue processing
    #[default]
    Drop,

    /// Fail the entire batch if any record errors
    Fail,

    /// Pass records through unchanged if they error
    Passthrough,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_work_item_serialization() {
        let item = WorkItem {
            job_id: "job-123".to_string(),
            file_uri: "s3://bucket/file.parquet".to_string(),
            file_size_bytes: 1024 * 1024,
            format: FileFormat::Parquet,
            destination: DestinationConfig {
                endpoint: "http://localhost:9200".to_string(),
                index: "test-index".to_string(),
                credentials: None,
            },
            transform: None,
            attempt: 0,
            enqueued_at: Utc::now(),
        };

        let json = serde_json::to_string(&item).unwrap();
        let parsed: WorkItem = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.job_id, "job-123");
        assert_eq!(parsed.format, FileFormat::Parquet);
    }

    #[test]
    fn test_file_format_extensions() {
        assert!(FileFormat::Parquet.extensions().contains(&"parquet"));
        assert!(FileFormat::NdJson.extensions().contains(&"ndjson"));
    }
}
