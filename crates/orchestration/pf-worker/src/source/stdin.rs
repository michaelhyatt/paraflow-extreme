//! Stdin work source implementation.

use super::{WorkAck, WorkMessage, WorkNack, WorkSource};
use async_trait::async_trait;
use pf_error::{PfError, QueueError, Result};
use pf_types::WorkItem;
use std::io::{self, BufRead, BufReader};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use tracing::{debug, trace, warn};
use uuid::Uuid;

/// Work source that reads JSONL WorkItems from stdin.
///
/// Each line should be a valid JSON object representing a WorkItem.
/// Empty lines are skipped.
///
/// # Example Input
///
/// ```jsonl
/// {"job_id":"job-1","file_uri":"s3://bucket/file1.parquet",...}
/// {"job_id":"job-1","file_uri":"s3://bucket/file2.parquet",...}
/// ```
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
impl WorkSource for StdinSource {
    async fn receive(&self, max: usize) -> Result<Option<Vec<WorkMessage>>> {
        if self.eof_reached.load(Ordering::Relaxed) {
            return Ok(None);
        }

        let mut messages = Vec::with_capacity(max);
        let mut reader = self.reader.lock().map_err(|e| {
            PfError::Queue(QueueError::Receive(format!("Failed to lock stdin reader: {}", e)))
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

                    match serde_json::from_str::<WorkItem>(line) {
                        Ok(work_item) => {
                            messages.push(WorkMessage {
                                id: self.next_message_id(),
                                work_item,
                                receive_count: 1,
                            });
                        }
                        Err(e) => {
                            warn!("Failed to parse work item from stdin: {}", e);
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

    async fn ack(&self, items: &[WorkAck]) -> Result<()> {
        // No-op for stdin - just log
        for item in items {
            trace!("Ack for stdin message: {}", item.id);
        }
        Ok(())
    }

    async fn nack(&self, items: &[WorkNack]) -> Result<()> {
        // Log failures for stdin
        for item in items {
            warn!(
                "Nack for stdin message {}: {} (dlq={})",
                item.id, item.failure.error_message, item.should_dlq
            );
        }
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

        let messages = source.receive(10).await.unwrap().unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].work_item.job_id, "test-job");

        // Next receive should return None (EOF)
        let next = source.receive(10).await.unwrap();
        assert!(next.is_none() || next.unwrap().is_empty());
        assert!(!source.has_more()); // EOF should now be signaled
    }

    #[tokio::test]
    async fn test_stdin_source_multiple_items() {
        let json1 = create_test_work_item_json();
        let json2 = create_test_work_item_json();
        let input = Cursor::new(format!("{}\n{}\n", json1, json2));
        let source = StdinSource::with_reader(Box::new(input));

        let messages = source.receive(10).await.unwrap().unwrap();
        assert_eq!(messages.len(), 2);
    }

    #[tokio::test]
    async fn test_stdin_source_empty_lines() {
        let json = create_test_work_item_json();
        let input = Cursor::new(format!("\n{}\n\n", json));
        let source = StdinSource::with_reader(Box::new(input));

        let messages = source.receive(10).await.unwrap().unwrap();
        assert_eq!(messages.len(), 1);
    }

    #[tokio::test]
    async fn test_stdin_source_invalid_json() {
        let json = create_test_work_item_json();
        let input = Cursor::new(format!("invalid json\n{}\n", json));
        let source = StdinSource::with_reader(Box::new(input));

        let messages = source.receive(10).await.unwrap().unwrap();
        // Should skip the invalid line and return the valid one
        assert_eq!(messages.len(), 1);
    }

    #[tokio::test]
    async fn test_stdin_source_eof() {
        let input = Cursor::new("");
        let source = StdinSource::with_reader(Box::new(input));

        let messages = source.receive(10).await.unwrap();
        assert!(messages.is_none());
        assert!(!source.has_more());
    }

    #[tokio::test]
    async fn test_stdin_source_ack_nack() {
        let input = Cursor::new("");
        let source = StdinSource::with_reader(Box::new(input));

        // These should be no-ops
        source.ack(&[WorkAck::new("test-id")]).await.unwrap();

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
        let failure = pf_traits::FailureContext::new(item, "test", "test error", "test");
        source.nack(&[WorkNack::retry("test-id", failure)]).await.unwrap();
    }
}
