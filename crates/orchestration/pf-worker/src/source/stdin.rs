//! Stdin work source implementation.

use super::{WorkAck, WorkMessage, WorkNack, WorkSource};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use pf_error::{PfError, QueueError, Result};
use pf_types::{DestinationConfig, FileFormat, WorkItem};
use serde::Deserialize;
use std::io::{self, BufRead, BufReader};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use tracing::{debug, trace, warn};
use uuid::Uuid;

/// A discovered file from pf-discoverer.
///
/// This is the simpler format output by `pf-discoverer` that can be
/// piped directly to `pf-worker`.
#[derive(Debug, Clone, Deserialize)]
struct DiscoveredFile {
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
    fn into_work_item(self) -> WorkItem {
        // Detect format from file extension
        let format = guess_format_from_uri(&self.uri);

        WorkItem {
            job_id: "stdin".to_string(),
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
fn guess_format_from_uri(uri: &str) -> FileFormat {
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

/// Parse a JSON line into a WorkItem.
///
/// Tries to parse as a full WorkItem first, then falls back to
/// DiscoveredFile format (from pf-discoverer).
fn parse_work_item(line: &str) -> Option<WorkItem> {
    // Try full WorkItem format first
    if let Ok(work_item) = serde_json::from_str::<WorkItem>(line) {
        return Some(work_item);
    }

    // Fall back to DiscoveredFile format (pf-discoverer output)
    if let Ok(discovered) = serde_json::from_str::<DiscoveredFile>(line) {
        return Some(discovered.into_work_item());
    }

    None
}

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

                    match parse_work_item(line) {
                        Some(work_item) => {
                            messages.push(WorkMessage {
                                id: self.next_message_id(),
                                work_item,
                                receive_count: 1,
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

    #[tokio::test]
    async fn test_stdin_source_discovered_file_format() {
        // Format output by pf-discoverer
        let json = r#"{"uri":"s3://bucket/file.parquet","size_bytes":1024,"last_modified":"2024-01-01T00:00:00Z"}"#;
        let input = Cursor::new(format!("{}\n", json));
        let source = StdinSource::with_reader(Box::new(input));

        let messages = source.receive(10).await.unwrap().unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].work_item.file_uri, "s3://bucket/file.parquet");
        assert_eq!(messages[0].work_item.file_size_bytes, 1024);
        assert_eq!(messages[0].work_item.format, FileFormat::Parquet);
        assert_eq!(messages[0].work_item.job_id, "stdin");
    }

    #[tokio::test]
    async fn test_stdin_source_discovered_file_ndjson_format() {
        // Format output by pf-discoverer for NDJSON file
        let json = r#"{"uri":"s3://bucket/data.ndjson","size_bytes":2048}"#;
        let input = Cursor::new(format!("{}\n", json));
        let source = StdinSource::with_reader(Box::new(input));

        let messages = source.receive(10).await.unwrap().unwrap();
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

        let messages = source.receive(10).await.unwrap().unwrap();
        assert_eq!(messages.len(), 2);
        // First is full WorkItem
        assert_eq!(messages[0].work_item.job_id, "test-job");
        // Second is converted DiscoveredFile
        assert_eq!(messages[1].work_item.job_id, "stdin");
        assert_eq!(messages[1].work_item.file_uri, "s3://bucket/file2.parquet");
    }

    #[test]
    fn test_guess_format_from_uri() {
        assert_eq!(guess_format_from_uri("s3://bucket/file.parquet"), FileFormat::Parquet);
        assert_eq!(guess_format_from_uri("s3://bucket/file.pq"), FileFormat::Parquet);
        assert_eq!(guess_format_from_uri("s3://bucket/file.ndjson"), FileFormat::NdJson);
        assert_eq!(guess_format_from_uri("s3://bucket/file.jsonl"), FileFormat::NdJson);
        assert_eq!(guess_format_from_uri("s3://bucket/file.json"), FileFormat::NdJson);
        // Unknown extension defaults to Parquet
        assert_eq!(guess_format_from_uri("s3://bucket/file.unknown"), FileFormat::Parquet);
    }

    #[test]
    fn test_parse_work_item_full_format() {
        let json = create_test_work_item_json();
        let item = parse_work_item(&json).unwrap();
        assert_eq!(item.job_id, "test-job");
    }

    #[test]
    fn test_parse_work_item_discovered_format() {
        let json = r#"{"uri":"s3://bucket/file.parquet","size_bytes":1024}"#;
        let item = parse_work_item(json).unwrap();
        assert_eq!(item.file_uri, "s3://bucket/file.parquet");
        assert_eq!(item.file_size_bytes, 1024);
        assert_eq!(item.job_id, "stdin");
    }

    #[test]
    fn test_parse_work_item_invalid() {
        assert!(parse_work_item("invalid json").is_none());
        assert!(parse_work_item(r#"{"foo": "bar"}"#).is_none());
    }
}
