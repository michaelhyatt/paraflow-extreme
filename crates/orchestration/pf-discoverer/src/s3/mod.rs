//! S3 client and listing functionality.
//!
//! This module provides S3 operations for discovering files:
//! - Client configuration with LocalStack support
//! - Paginated object listing with streaming
//! - Parallel listing across multiple prefixes
//! - Retry logic with exponential backoff

mod client;
mod list;
mod parallel;
mod retry;

pub use client::{S3Config, create_s3_client};
pub use list::{S3Object, list_objects};
pub use parallel::{ParallelConfig, ParallelLister};
pub use retry::{ErrorClassification, RetryConfig, classify_error, with_retry};
