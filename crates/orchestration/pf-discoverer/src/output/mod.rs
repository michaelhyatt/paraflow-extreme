//! Output implementations for discovered files.
//!
//! This module provides the [`Output`] trait and implementations for
//! outputting discovered files to various destinations:
//! - [`StdoutOutput`] - Outputs to stdout in JSON or JSONL format
//! - [`SqsOutput`] - Sends to an SQS queue (with optional batching)

mod sqs;
mod stdout;

pub use sqs::{SqsConfig, SqsOutput};
pub use stdout::{OutputFormat, StdoutOutput};

use async_trait::async_trait;
use pf_error::Result;

use crate::DiscoveredFile;

/// Trait for outputting discovered files.
///
/// Implementations handle the delivery of discovered files to their final destination,
/// whether that's stdout for piping to other tools, an SQS queue for distributed
/// processing, or other destinations.
///
/// The trait includes backpressure support through the `ready()` and `wait_ready()`
/// methods, allowing the discoverer to pause when the output cannot keep up.
#[async_trait]
pub trait Output: Send + Sync {
    /// Output a single discovered file.
    ///
    /// The implementation determines the serialization format and delivery mechanism.
    async fn output(&self, file: &DiscoveredFile) -> Result<()>;

    /// Flush any buffered output.
    ///
    /// Called after all files have been output to ensure all data is written.
    async fn flush(&self) -> Result<()>;

    /// Check if the output is ready to accept more files (for backpressure).
    ///
    /// Default implementation always returns `true` (no backpressure).
    fn ready(&self) -> bool {
        true
    }

    /// Wait until the output is ready to accept more files.
    ///
    /// Default implementation returns immediately.
    async fn wait_ready(&self) {
        // Default: no backpressure, always ready
    }
}
