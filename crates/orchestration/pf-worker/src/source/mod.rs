//! Work source implementations.
//!
//! This module provides the [`WorkSource`] trait and implementations
//! for receiving work items from different sources:
//!
//! - [`StdinSource`]: Reads JSONL work items from stdin (for local testing)
//! - [`SqsSource`]: Receives work items from an AWS SQS queue (for production)

mod stdin;
mod sqs;

pub use stdin::StdinSource;
pub use sqs::{SqsSource, SqsSourceConfig};

use async_trait::async_trait;
use pf_error::Result;
use pf_traits::FailureContext;
use pf_types::WorkItem;

/// A message received from a work source.
#[derive(Debug, Clone)]
pub struct WorkMessage {
    /// Unique message ID (receipt handle for SQS)
    pub id: String,

    /// The work item payload
    pub work_item: WorkItem,

    /// Number of times this message has been received
    pub receive_count: u32,
}

/// Acknowledgement for a successfully processed message.
#[derive(Debug, Clone)]
pub struct WorkAck {
    /// The message ID to acknowledge
    pub id: String,
}

impl WorkAck {
    /// Create a new acknowledgement.
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }
}

/// Negative acknowledgement for a failed message.
#[derive(Debug, Clone)]
pub struct WorkNack {
    /// The message ID
    pub id: String,

    /// Failure context for DLQ
    pub failure: FailureContext,

    /// If true, move to DLQ; otherwise retry
    pub should_dlq: bool,
}

impl WorkNack {
    /// Create a new negative acknowledgement for retry.
    pub fn retry(id: impl Into<String>, failure: FailureContext) -> Self {
        Self {
            id: id.into(),
            failure,
            should_dlq: false,
        }
    }

    /// Create a new negative acknowledgement for DLQ.
    pub fn dlq(id: impl Into<String>, failure: FailureContext) -> Self {
        Self {
            id: id.into(),
            failure,
            should_dlq: true,
        }
    }
}

/// Trait for work item sources.
///
/// Abstracts the source of work items whether from SQS, stdin, or other sources.
#[async_trait]
pub trait WorkSource: Send + Sync {
    /// Receives a batch of work items.
    ///
    /// Returns `Ok(None)` when the source is exhausted (e.g., stdin EOF).
    /// Returns `Ok(Some(vec![]))` when no messages are available but source is not exhausted.
    async fn receive(&self, max: usize) -> Result<Option<Vec<WorkMessage>>>;

    /// Acknowledges successful processing of work items.
    ///
    /// For SQS: deletes messages
    /// For stdin: no-op
    async fn ack(&self, items: &[WorkAck]) -> Result<()>;

    /// Negative acknowledges failed work items.
    ///
    /// For SQS: returns messages to queue or moves to DLQ
    /// For stdin: logs the failure
    async fn nack(&self, items: &[WorkNack]) -> Result<()>;

    /// Returns true if more work may be available.
    fn has_more(&self) -> bool;
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
    fn test_work_ack() {
        let ack = WorkAck::new("msg-123");
        assert_eq!(ack.id, "msg-123");
    }

    #[test]
    fn test_work_nack_retry() {
        let item = create_test_work_item();
        let failure = FailureContext::new(item, "Transient", "timeout", "S3Download");
        let nack = WorkNack::retry("msg-123", failure);

        assert_eq!(nack.id, "msg-123");
        assert!(!nack.should_dlq);
    }

    #[test]
    fn test_work_nack_dlq() {
        let item = create_test_work_item();
        let failure = FailureContext::new(item, "Permanent", "not found", "S3Download");
        let nack = WorkNack::dlq("msg-123", failure);

        assert_eq!(nack.id, "msg-123");
        assert!(nack.should_dlq);
    }
}
