//! pf-discoverer - S3 file discovery for paraflow-extreme.
//!
//! This crate provides functionality for discovering files in S3 buckets
//! and outputting file information. It supports:
//!
//! - S3 listing with pagination and LocalStack support
//! - Glob pattern filtering for file selection
//! - Output to stdout (JSONL/JSON) or SQS queues
//!
//! # Example
//!
//! ```ignore
//! use pf_discoverer::{Discoverer, PatternFilter, DiscoveryConfig};
//! use pf_discoverer::output::StdoutOutput;
//! use pf_discoverer::s3::{S3Config, create_s3_client};
//!
//! // Configure S3 access
//! let s3_config = S3Config::new("my-bucket")
//!     .with_prefix("data/")
//!     .with_endpoint("http://localhost:4566");
//!
//! let client = create_s3_client(&s3_config).await?;
//!
//! // Configure discovery
//! let config = DiscoveryConfig::new();
//! let filter = PatternFilter::new("*.parquet")?;
//! let output = StdoutOutput::default();
//!
//! // Run discovery
//! let discoverer = Discoverer::new(
//!     client,
//!     "my-bucket",
//!     Some("data/".to_string()),
//!     output,
//!     filter,
//!     config,
//! );
//!
//! let stats = discoverer.discover().await?;
//! eprintln!("Discovered {} files", stats.files_output);
//! ```

use chrono::{DateTime, Utc};
use pf_types::FileFormat;
use serde::{Deserialize, Serialize};

pub mod config;
pub mod discoverer;
pub mod filter;
pub mod output;
pub mod s3;
pub mod stats;

pub use config::DiscoveryConfig;
pub use discoverer::Discoverer;
pub use filter::{MatchAllFilter, PatternFilter};
pub use output::{Output, OutputFormat, SqsConfig, SqsOutput, StdoutOutput};
pub use s3::{S3Config, S3Object, create_s3_client, list_objects};
pub use stats::DiscoveryStats;

/// A discovered file from S3.
///
/// This is the output format for discovered files, containing
/// the essential information needed to process the file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredFile {
    /// The S3 URI of the file (e.g., "s3://bucket/path/file.parquet")
    pub uri: String,

    /// Size of the file in bytes
    pub size_bytes: u64,

    /// File format
    pub format: FileFormat,

    /// Last modified timestamp (if available)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_modified: Option<DateTime<Utc>>,
}
