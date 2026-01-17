//! Stdin work source implementation.

use super::parse_work_item;
use async_trait::async_trait;
use chrono::Utc;
use pf_error::{PfError, QueueError, Result};
use pf_traits::{FailureContext, QueueMessage, WorkQueue};
use std::io::{self, BufRead, BufReader};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tracing::{debug, trace, warn};
use uuid::Uuid;

/// Work source that reads JSONL work items from stdin.
///
/// Supports two input formats:
///
/// 1. **Full WorkItem format** (for production use with complete configuration):
///    ```jsonl
///    {"job_id":"job-1","file_uri":"s3://bucket/file.parquet","file_size_bytes":1024,...}
///    ```
///
/// 2. **DiscoveredFile format** (from pf-discoverer, for local testing):
///    ```jsonl
///    {"uri":"s3://bucket/file.parquet","size_bytes":1024,"last_modified":"2024-01-01T00:00:00Z"}
///    ```
///
/// Empty lines are skipped.
pub struct StdinSource {
    /// Reader for stdin (wrapped in Mutex for thread-safe access)
    reader: Mutex<Box<dyn BufRead + Send>>,

    /// Whether we've reached EOF
    eof_reached: AtomicBool,

    /// Counter for generating message IDs
    message_counter: AtomicU64,
}

impl StdinSource {
    /// Create a new stdin source.
    pub fn new() -> Self {
        Self {
            reader: Mutex::new(Box::new(BufReader::new(io::stdin()))),
            eof_reached: AtomicBool::new(false),
            message_counter: AtomicU64::new(0),
        }
    }

    /// Create a stdin source with a custom reader (for testing).
    pub fn with_reader(reader: Box<dyn BufRead + Send>) -> Self {
        Self {
            reader: Mutex::new(reader),
            eof_reached: AtomicBool::new(false),
            message_counter: AtomicU64::new(0),
        }
    }

    /// Generate a unique message ID.
    fn next_message_id(&self) -> String {
        let counter = self.message_counter.fetch_add(1, Ordering::Relaxed);
        format!("stdin-{}-{}", Uuid::new_v4(), counter)
    }
}

impl Default for StdinSource {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl WorkQueue for StdinSource {
    async fn receive_batch(&self, max: usize) -> Result<Option<Vec<QueueMessage>>> {
        if self.eof_reached.load(Ordering::Relaxed) {
            return Ok(None);
        }

        let mut messages = Vec::with_capacity(max);
        let mut reader = self.reader.lock().map_err(|e| {
            PfError::Queue(QueueError::Receive(format!(
                "Failed to lock stdin reader: {}",
                e
            )))
        })?;

        for _ in 0..max {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => {
                    // EOF reached
                    self.eof_reached.store(true, Ordering::Relaxed);
                    debug!("Stdin EOF reached");
                    break;
                }
                Ok(_) => {
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }

                    trace!("Read line from stdin: {}", line);

                    match parse_work_item(line) {
                        Some(work_item) => {
                            messages.push(QueueMessage {
                                receipt_handle: self.next_message_id(),
                                work_item,
                                receive_count: 1,
                                first_received_at: Utc::now(),
                            });
                        }
                        None => {
                            warn!("Failed to parse work item from stdin: {}", line);
                            // Skip invalid lines and continue
                        }
                    }
                }
                Err(e) => {
                    return Err(PfError::Queue(QueueError::Receive(format!(
                        "Failed to read from stdin: {}",
                        e
                    ))));
                }
            }
        }

        if messages.is_empty() && self.eof_reached.load(Ordering::Relaxed) {
            Ok(None)
        } else {
            Ok(Some(messages))
        }
    }

    async fn ack(&self, receipt: &str) -> Result<()> {
        // No-op for stdin - just log
        trace!("Ack for stdin message: {}", receipt);
        Ok(())
    }

    async fn nack(&self, receipt: &str) -> Result<()> {
        // Log failures for stdin
        warn!("Nack for stdin message: {}", receipt);
        Ok(())
    }

    async fn move_to_dlq(&self, receipt: &str, failure: &FailureContext) -> Result<()> {
        // Log DLQ moves for stdin
        warn!(
            "DLQ for stdin message {}: {}",
            receipt, failure.error_message
        );
        Ok(())
    }

    fn has_more(&self) -> bool {
        !self.eof_reached.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use pf_types::{DestinationConfig, FileFormat};
    use std::io::Cursor;

    fn create_test_work_item_json() -> String {
        let item = pf_types::WorkItem {
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
        };
        serde_json::to_string(&item).unwrap()
    }

    #[tokio::test]
    async fn test_stdin_source_single_item() {
        let json = create_test_work_item_json();
        // Add trailing newline so EOF is detected on second receive, not first
        let input = Cursor::new(format!("{}\n", json));
        let source = StdinSource::with_reader(Box::new(input));

        let messages = source.receive_batch(10).await.unwrap().unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].work_item.job_id, "test-job");

        // Next receive should return None (EOF)
        let next = source.receive_batch(10).await.unwrap();
        assert!(next.is_none() || next.unwrap().is_empty());
        assert!(!source.has_more()); // EOF should now be signaled
    }

    #[tokio::test]
    async fn test_stdin_source_multiple_items() {
        let json1 = create_test_work_item_json();
        let json2 = create_test_work_item_json();
        let input = Cursor::new(format!("{}\n{}\n", json1, json2));
        let source = StdinSource::with_reader(Box::new(input));

        let messages = source.receive_batch(10).await.unwrap().unwrap();
        assert_eq!(messages.len(), 2);
    }

    #[tokio::test]
    async fn test_stdin_source_empty_lines() {
        let json = create_test_work_item_json();
        let input = Cursor::new(format!("\n{}\n\n", json));
        let source = StdinSource::with_reader(Box::new(input));

        let messages = source.receive_batch(10).await.unwrap().unwrap();
        assert_eq!(messages.len(), 1);
    }

    #[tokio::test]
    async fn test_stdin_source_invalid_json() {
        let json = create_test_work_item_json();
        let input = Cursor::new(format!("invalid json\n{}\n", json));
        let source = StdinSource::with_reader(Box::new(input));

        let messages = source.receive_batch(10).await.unwrap().unwrap();
        // Should skip the invalid line and return the valid one
        assert_eq!(messages.len(), 1);
    }

    #[tokio::test]
    async fn test_stdin_source_eof() {
        let input = Cursor::new("");
        let source = StdinSource::with_reader(Box::new(input));

        let messages = source.receive_batch(10).await.unwrap();
        assert!(messages.is_none());
        assert!(!source.has_more());
    }

    #[tokio::test]
    async fn test_stdin_source_ack_nack() {
        let input = Cursor::new("");
        let source = StdinSource::with_reader(Box::new(input));

        // These should be no-ops
        source.ack("test-id").await.unwrap();
        source.nack("test-id").await.unwrap();

        let item = pf_types::WorkItem {
            job_id: "test".to_string(),
            file_uri: "s3://bucket/file".to_string(),
            file_size_bytes: 100,
            format: FileFormat::Parquet,
            destination: DestinationConfig {
                endpoint: "http://localhost".to_string(),
                index: "test".to_string(),
                credentials: None,
            },
            transform: None,
            attempt: 0,
            enqueued_at: Utc::now(),
        };
        let failure = FailureContext::new(item, "test", "test error", "test");
        source.move_to_dlq("test-id", &failure).await.unwrap();
    }

    #[tokio::test]
    async fn test_stdin_source_discovered_file_format() {
        // Format output by pf-discoverer
        let json = r#"{"uri":"s3://bucket/file.parquet","size_bytes":1024,"last_modified":"2024-01-01T00:00:00Z"}"#;
        let input = Cursor::new(format!("{}\n", json));
        let source = StdinSource::with_reader(Box::new(input));

        let messages = source.receive_batch(10).await.unwrap().unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].work_item.file_uri, "s3://bucket/file.parquet");
        assert_eq!(messages[0].work_item.file_size_bytes, 1024);
        assert_eq!(messages[0].work_item.format, FileFormat::Parquet);
        assert_eq!(messages[0].work_item.job_id, "discovered");
    }

    #[tokio::test]
    async fn test_stdin_source_discovered_file_ndjson_format() {
        // Format output by pf-discoverer for NDJSON file
        let json = r#"{"uri":"s3://bucket/data.ndjson","size_bytes":2048}"#;
        let input = Cursor::new(format!("{}\n", json));
        let source = StdinSource::with_reader(Box::new(input));

        let messages = source.receive_batch(10).await.unwrap().unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].work_item.file_uri, "s3://bucket/data.ndjson");
        assert_eq!(messages[0].work_item.format, FileFormat::NdJson);
    }

    #[tokio::test]
    async fn test_stdin_source_mixed_formats() {
        // Mix of full WorkItem and DiscoveredFile formats
        let work_item_json = create_test_work_item_json();
        let discovered_json = r#"{"uri":"s3://bucket/file2.parquet","size_bytes":512}"#;
        let input = Cursor::new(format!("{}\n{}\n", work_item_json, discovered_json));
        let source = StdinSource::with_reader(Box::new(input));

        let messages = source.receive_batch(10).await.unwrap().unwrap();
        assert_eq!(messages.len(), 2);
        // First is full WorkItem
        assert_eq!(messages[0].work_item.job_id, "test-job");
        // Second is converted DiscoveredFile
        assert_eq!(messages[1].work_item.job_id, "discovered");
        assert_eq!(messages[1].work_item.file_uri, "s3://bucket/file2.parquet");
    }
}
