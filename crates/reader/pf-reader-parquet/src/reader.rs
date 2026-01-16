//! Parquet reader implementation.

use crate::s3::{parse_s3_uri, S3Client};
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use pf_error::{PfError, ReaderError, Result};
use pf_traits::{BatchStream, FileMetadata, StreamingReader};
use pf_types::Batch;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;
use tracing::{debug, info, trace};

/// Configuration for the Parquet reader.
#[derive(Debug, Clone)]
pub struct ParquetReaderConfig {
    /// AWS region for S3 access
    pub region: String,

    /// Optional S3 endpoint URL (for LocalStack)
    pub endpoint: Option<String>,

    /// Batch size for reading (number of rows per batch)
    pub batch_size: usize,
}

impl ParquetReaderConfig {
    /// Create a new configuration with default batch size.
    pub fn new(region: impl Into<String>) -> Self {
        Self {
            region: region.into(),
            endpoint: None,
            batch_size: 8192,
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
}

/// Streaming Parquet file reader.
///
/// Supports reading from both local files and S3 URIs.
pub struct ParquetReader {
    config: ParquetReaderConfig,
    s3_client: Option<S3Client>,
}

impl ParquetReader {
    /// Create a new Parquet reader with the given configuration.
    pub async fn new(config: ParquetReaderConfig) -> Result<Self> {
        let s3_client = S3Client::new(&config.region, config.endpoint.as_deref()).await?;

        Ok(Self {
            config,
            s3_client: Some(s3_client),
        })
    }

    /// Create a Parquet reader for local files only (no S3 support).
    pub fn local_only() -> Self {
        Self {
            config: ParquetReaderConfig::new("local"),
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
        debug!(path = path, "Reading local Parquet file");

        let mut file = File::open(path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                PfError::Reader(ReaderError::NotFound(path.to_string()))
            } else {
                PfError::Reader(ReaderError::IoError(format!(
                    "Failed to open file '{}': {}",
                    path, e
                )))
            }
        })?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).map_err(|e| {
            PfError::Reader(ReaderError::IoError(format!(
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
                    PfError::Reader(ReaderError::IoError(format!(
                        "Failed to get metadata for '{}': {}",
                        path, e
                    )))
                }
            })?;

            Ok(metadata.len())
        }
    }
}

#[async_trait]
impl StreamingReader for ParquetReader {
    async fn read_stream(&self, uri: &str) -> Result<BatchStream> {
        info!(uri = uri, "Opening Parquet file for streaming");

        // Download the file data
        let data = self.read_file_data(uri).await?;
        let file_size = data.len();

        debug!(
            uri = uri,
            size = file_size,
            "Downloaded Parquet file, creating reader"
        );

        // Create a Parquet reader from the bytes
        let builder = ParquetRecordBatchReaderBuilder::try_new(data).map_err(|e| {
            PfError::Reader(ReaderError::ParseError(format!(
                "Failed to create Parquet reader for '{}': {}",
                uri, e
            )))
        })?;

        let schema = builder.schema().clone();
        let batch_size = self.config.batch_size;
        let uri_clone = uri.to_string();

        // Build the reader with configured batch size
        let reader = builder.with_batch_size(batch_size).build().map_err(|e| {
            PfError::Reader(ReaderError::ParseError(format!(
                "Failed to build Parquet reader for '{}': {}",
                uri, e
            )))
        })?;

        // Convert the synchronous iterator to an async stream
        let batches: Vec<_> = reader
            .enumerate()
            .map(|(batch_index, result)| {
                result
                    .map(|record_batch| {
                        trace!(
                            batch_index = batch_index,
                            rows = record_batch.num_rows(),
                            "Read Parquet batch"
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
        debug!(uri = uri, "Getting Parquet file metadata");

        let size = self.get_file_size(uri).await?;
        let data = self.read_file_data(uri).await?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(data).map_err(|e| {
            PfError::Reader(ReaderError::ParseError(format!(
                "Failed to read Parquet metadata for '{}': {}",
                uri, e
            )))
        })?;

        let parquet_metadata = builder.metadata();
        let row_count: i64 = parquet_metadata
            .row_groups()
            .iter()
            .map(|rg| rg.num_rows())
            .sum();

        let schema = builder.schema().clone();

        Ok(FileMetadata::new(size, schema).with_row_count(row_count as u64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use futures::StreamExt;
    use parquet::arrow::ArrowWriter;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_test_parquet_file(num_rows: usize) -> NamedTempFile {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let ids: Vec<i32> = (0..num_rows as i32).collect();
        let names: Vec<Option<&str>> = (0..num_rows)
            .map(|i| {
                if i % 10 == 0 {
                    None
                } else {
                    Some("test_name")
                }
            })
            .collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap();

        let mut file = NamedTempFile::new().unwrap();
        {
            let mut writer = ArrowWriter::try_new(&mut file, schema, None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        file
    }

    #[tokio::test]
    async fn test_read_local_parquet() {
        let file = create_test_parquet_file(100);
        let path = file.path().to_str().unwrap();

        let reader = ParquetReader::local_only();
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
        let file = create_test_parquet_file(250);
        let path = file.path().to_str().unwrap();

        let reader = ParquetReader::local_only();
        let metadata = reader.file_metadata(path).await.unwrap();

        assert_eq!(metadata.row_count, Some(250));
        assert_eq!(metadata.schema.fields().len(), 2);
        assert!(metadata.size_bytes > 0);
    }

    #[tokio::test]
    async fn test_file_not_found() {
        let reader = ParquetReader::local_only();
        let result = reader.read_stream("/nonexistent/file.parquet").await;

        assert!(result.is_err());
        match result.err().unwrap() {
            PfError::Reader(ReaderError::NotFound(_)) => {}
            e => panic!("Expected NotFound error, got: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_batch_size_config() {
        let file = create_test_parquet_file(1000);
        let path = file.path().to_str().unwrap();

        // Create reader with small batch size
        let config = ParquetReaderConfig::new("local").with_batch_size(100);
        let reader = ParquetReader {
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
        assert!(batch_count >= 1);
    }

    #[tokio::test]
    async fn test_file_uri_scheme() {
        let file = create_test_parquet_file(50);
        let path = file.path().to_str().unwrap();
        let file_uri = format!("file://{}", path);

        let reader = ParquetReader::local_only();
        let mut stream = reader.read_stream(&file_uri).await.unwrap();

        let mut total_rows = 0;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.unwrap();
            total_rows += batch.num_rows();
        }

        assert_eq!(total_rows, 50);
    }
}
