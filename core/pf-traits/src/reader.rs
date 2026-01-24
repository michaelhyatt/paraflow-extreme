//! Streaming reader trait and related types.

use async_trait::async_trait;
use futures::Stream;
use pf_error::Result;
use pf_types::Batch;
use std::pin::Pin;
use std::sync::Arc;

/// A stream of batches from a file.
pub type BatchStream = Pin<Box<dyn Stream<Item = Result<Batch>> + Send>>;

/// Trait for streaming file readers.
///
/// Readers implement streaming semantics with O(batch_size) memory usage,
/// enabling processing of files larger than available RAM.
///
/// # Implementations
///
/// - Parquet reader: Uses byte-range requests for S3, streaming decode
/// - NDJSON reader: Line-by-line streaming with optional decompression
#[async_trait]
pub trait StreamingReader: Send + Sync {
    /// Opens a file and returns a stream of batches.
    ///
    /// The stream yields [`Batch`] objects containing Arrow RecordBatches
    /// with metadata about the source file and batch position.
    ///
    /// # Arguments
    ///
    /// * `uri` - URI of the file to read (s3://, file://, etc.)
    ///
    /// # Returns
    ///
    /// A stream that yields batches as they are read
    async fn read_stream(&self, uri: &str) -> Result<BatchStream>;

    /// Gets metadata about a file without reading its contents.
    ///
    /// # Arguments
    ///
    /// * `uri` - URI of the file
    ///
    /// # Returns
    ///
    /// Metadata including size, row count (if available), and schema
    async fn file_metadata(&self, uri: &str) -> Result<FileMetadata>;
}

/// Metadata about a file.
#[derive(Debug, Clone)]
pub struct FileMetadata {
    /// File size in bytes
    pub size_bytes: u64,

    /// Row count (if known from file metadata)
    pub row_count: Option<u64>,

    /// Arrow schema of the file
    pub schema: Arc<arrow::datatypes::Schema>,
}

impl FileMetadata {
    /// Creates new file metadata.
    pub fn new(size_bytes: u64, schema: Arc<arrow::datatypes::Schema>) -> Self {
        Self {
            size_bytes,
            row_count: None,
            schema,
        }
    }

    /// Sets the row count.
    pub fn with_row_count(mut self, count: u64) -> Self {
        self.row_count = Some(count);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_file_metadata() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let meta = FileMetadata::new(1024 * 1024, schema.clone()).with_row_count(10000);

        assert_eq!(meta.size_bytes, 1024 * 1024);
        assert_eq!(meta.row_count, Some(10000));
        assert_eq!(meta.schema.fields().len(), 2);
    }
}
