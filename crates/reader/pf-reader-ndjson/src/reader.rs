//! NDJSON reader implementation.

use crate::s3::{parse_s3_uri, S3Client};
use arrow::datatypes::SchemaRef;
use arrow_json::ReaderBuilder;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;
use pf_error::{PfError, ReaderError, Result};
use pf_traits::{BatchStream, FileMetadata, StreamingReader};
use pf_types::Batch;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::sync::Arc;
use tracing::{debug, info, trace};

/// Configuration for the NDJSON reader.
#[derive(Debug, Clone)]
pub struct NdjsonReaderConfig {
    /// AWS region for S3 access
    pub region: String,

    /// Optional S3 endpoint URL (for LocalStack)
    pub endpoint: Option<String>,

    /// Batch size for reading (number of rows per batch)
    pub batch_size: usize,

    /// Maximum number of records to infer schema from
    pub schema_infer_max_records: usize,
}

impl NdjsonReaderConfig {
    /// Create a new configuration with default batch size.
    pub fn new(region: impl Into<String>) -> Self {
        Self {
            region: region.into(),
            endpoint: None,
            batch_size: 8192,
            schema_infer_max_records: 1000,
        }
    }

    /// Set the S3 endpoint URL.
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Set the batch size.
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Set the maximum records for schema inference.
    pub fn with_schema_infer_max_records(mut self, max: usize) -> Self {
        self.schema_infer_max_records = max;
        self
    }
}

/// Streaming NDJSON file reader.
///
/// Supports reading from both local files and S3 URIs.
pub struct NdjsonReader {
    config: NdjsonReaderConfig,
    s3_client: Option<S3Client>,
}

impl NdjsonReader {
    /// Create a new NDJSON reader with the given configuration.
    pub async fn new(config: NdjsonReaderConfig) -> Result<Self> {
        let s3_client = S3Client::new(&config.region, config.endpoint.as_deref()).await?;

        Ok(Self {
            config,
            s3_client: Some(s3_client),
        })
    }

    /// Create an NDJSON reader for local files only (no S3 support).
    pub fn local_only() -> Self {
        Self {
            config: NdjsonReaderConfig::new("local"),
            s3_client: None,
        }
    }

    /// Read file data from the given URI.
    async fn read_file_data(&self, uri: &str) -> Result<Bytes> {
        if uri.starts_with("s3://") {
            let s3_client = self.s3_client.as_ref().ok_or_else(|| {
                PfError::Reader(ReaderError::S3Error(
                    "S3 client not configured for S3 URIs".to_string(),
                ))
            })?;

            let (bucket, key) = parse_s3_uri(uri)?;
            s3_client.get_object(&bucket, &key).await
        } else if uri.starts_with("file://") {
            let path = uri.strip_prefix("file://").unwrap();
            self.read_local_file(path)
        } else {
            // Assume local file path
            self.read_local_file(uri)
        }
    }

    /// Read a local file into memory.
    fn read_local_file(&self, path: &str) -> Result<Bytes> {
        debug!(path = path, "Reading local NDJSON file");

        let mut file = File::open(path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                PfError::Reader(ReaderError::NotFound(path.to_string()))
            } else {
                PfError::Reader(ReaderError::Io(format!(
                    "Failed to open file '{}': {}",
                    path, e
                )))
            }
        })?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).map_err(|e| {
            PfError::Reader(ReaderError::Io(format!(
                "Failed to read file '{}': {}",
                path, e
            )))
        })?;

        Ok(Bytes::from(buffer))
    }

    /// Get the size of a file.
    async fn get_file_size(&self, uri: &str) -> Result<u64> {
        if uri.starts_with("s3://") {
            let s3_client = self.s3_client.as_ref().ok_or_else(|| {
                PfError::Reader(ReaderError::S3Error(
                    "S3 client not configured for S3 URIs".to_string(),
                ))
            })?;

            let (bucket, key) = parse_s3_uri(uri)?;
            s3_client.get_object_size(&bucket, &key).await
        } else {
            let path = if uri.starts_with("file://") {
                uri.strip_prefix("file://").unwrap()
            } else {
                uri
            };

            let metadata = std::fs::metadata(path).map_err(|e| {
                if e.kind() == std::io::ErrorKind::NotFound {
                    PfError::Reader(ReaderError::NotFound(path.to_string()))
                } else {
                    PfError::Reader(ReaderError::Io(format!(
                        "Failed to get metadata for '{}': {}",
                        path, e
                    )))
                }
            })?;

            Ok(metadata.len())
        }
    }

    /// Infer schema from NDJSON data.
    fn infer_schema(&self, data: &[u8]) -> Result<SchemaRef> {
        let cursor = std::io::Cursor::new(data);
        let reader = BufReader::new(cursor);

        // Collect up to max_records lines for schema inference
        let lines: Vec<String> = reader
            .lines()
            .take(self.config.schema_infer_max_records)
            .filter_map(|l| l.ok())
            .filter(|l| !l.trim().is_empty())
            .collect();

        if lines.is_empty() {
            return Err(PfError::Reader(ReaderError::ParseError(
                "Empty NDJSON file, cannot infer schema".to_string(),
            )));
        }

        // Use arrow-json to infer schema
        let infer_data = lines.join("\n");
        let cursor = std::io::Cursor::new(infer_data.as_bytes());

        let (inferred, _) = arrow_json::reader::infer_json_schema(cursor, None).map_err(|e| {
            PfError::Reader(ReaderError::ParseError(format!(
                "Failed to infer JSON schema: {}",
                e
            )))
        })?;

        Ok(Arc::new(inferred))
    }

    /// Count lines in NDJSON data.
    fn count_lines(&self, data: &[u8]) -> usize {
        data.iter().filter(|&&b| b == b'\n').count()
    }
}

#[async_trait]
impl StreamingReader for NdjsonReader {
    async fn read_stream(&self, uri: &str) -> Result<BatchStream> {
        info!(uri = uri, "Opening NDJSON file for streaming");

        // Download the file data
        let data = self.read_file_data(uri).await?;
        let file_size = data.len();

        debug!(
            uri = uri,
            size = file_size,
            "Downloaded NDJSON file, creating reader"
        );

        // Infer schema from the data
        let schema = self.infer_schema(&data)?;

        debug!(
            uri = uri,
            fields = schema.fields().len(),
            "Inferred schema"
        );

        // Create JSON reader
        let cursor = std::io::Cursor::new(data);
        let reader = ReaderBuilder::new(schema.clone())
            .with_batch_size(self.config.batch_size)
            .build(BufReader::new(cursor))
            .map_err(|e| {
                PfError::Reader(ReaderError::ParseError(format!(
                    "Failed to create JSON reader for '{}': {}",
                    uri, e
                )))
            })?;

        let uri_clone = uri.to_string();

        // Convert to batches
        let batches: Vec<_> = reader
            .enumerate()
            .map(|(batch_index, result)| {
                result
                    .map(|record_batch| {
                        trace!(
                            batch_index = batch_index,
                            rows = record_batch.num_rows(),
                            "Read NDJSON batch"
                        );
                        Batch::new(record_batch, uri_clone.clone(), batch_index)
                    })
                    .map_err(|e| {
                        PfError::Reader(ReaderError::ParseError(format!(
                            "Failed to read batch {}: {}",
                            batch_index, e
                        )))
                    })
            })
            .collect();

        debug!(
            uri = uri,
            batch_count = batches.len(),
            "Created batch stream"
        );

        Ok(Box::pin(stream::iter(batches)))
    }

    async fn file_metadata(&self, uri: &str) -> Result<FileMetadata> {
        debug!(uri = uri, "Getting NDJSON file metadata");

        let size = self.get_file_size(uri).await?;
        let data = self.read_file_data(uri).await?;

        let schema = self.infer_schema(&data)?;
        let row_count = self.count_lines(&data);

        Ok(FileMetadata::new(size, schema).with_row_count(row_count as u64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_test_ndjson_file(num_rows: usize) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();

        for i in 0..num_rows {
            let name = if i % 10 == 0 {
                "null".to_string()
            } else {
                format!("\"name_{}\"", i)
            };
            writeln!(file, r#"{{"id": {}, "name": {}, "value": {}}}"#, i, name, i * 10).unwrap();
        }

        file
    }

    #[tokio::test]
    async fn test_read_local_ndjson() {
        let file = create_test_ndjson_file(100);
        let path = file.path().to_str().unwrap();

        let reader = NdjsonReader::local_only();
        let mut stream = reader.read_stream(path).await.unwrap();

        let mut total_rows = 0;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.unwrap();
            total_rows += batch.num_rows();
        }

        assert_eq!(total_rows, 100);
    }

    #[tokio::test]
    async fn test_file_metadata_local() {
        let file = create_test_ndjson_file(250);
        let path = file.path().to_str().unwrap();

        let reader = NdjsonReader::local_only();
        let metadata = reader.file_metadata(path).await.unwrap();

        assert_eq!(metadata.row_count, Some(250));
        assert_eq!(metadata.schema.fields().len(), 3); // id, name, value
        assert!(metadata.size_bytes > 0);
    }

    #[tokio::test]
    async fn test_file_not_found() {
        let reader = NdjsonReader::local_only();
        let result = reader.read_stream("/nonexistent/file.ndjson").await;

        assert!(result.is_err());
        match result.err().unwrap() {
            PfError::Reader(ReaderError::NotFound(_)) => {}
            e => panic!("Expected NotFound error, got: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_batch_size_config() {
        let file = create_test_ndjson_file(1000);
        let path = file.path().to_str().unwrap();

        // Create reader with small batch size
        let config = NdjsonReaderConfig::new("local").with_batch_size(100);
        let reader = NdjsonReader {
            config,
            s3_client: None,
        };

        let mut stream = reader.read_stream(path).await.unwrap();

        let mut batch_count = 0;
        let mut total_rows = 0;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.unwrap();
            total_rows += batch.num_rows();
            batch_count += 1;
        }

        assert_eq!(total_rows, 1000);
        // With batch size 100 and 1000 rows, we should get multiple batches
        assert!(batch_count >= 10);
    }

    #[tokio::test]
    async fn test_file_uri_scheme() {
        let file = create_test_ndjson_file(50);
        let path = file.path().to_str().unwrap();
        let file_uri = format!("file://{}", path);

        let reader = NdjsonReader::local_only();
        let mut stream = reader.read_stream(&file_uri).await.unwrap();

        let mut total_rows = 0;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.unwrap();
            total_rows += batch.num_rows();
        }

        assert_eq!(total_rows, 50);
    }

    #[tokio::test]
    async fn test_schema_inference() {
        let file = create_test_ndjson_file(10);
        let path = file.path().to_str().unwrap();

        let reader = NdjsonReader::local_only();
        let metadata = reader.file_metadata(path).await.unwrap();

        // Should have id, name, value fields
        let schema = metadata.schema;
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("name").is_ok());
        assert!(schema.field_with_name("value").is_ok());
    }
}
