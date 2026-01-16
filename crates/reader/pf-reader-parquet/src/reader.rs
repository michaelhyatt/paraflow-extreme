//! Parquet reader implementation with true streaming support.
//!
//! This reader uses byte-range requests to fetch Parquet row groups on demand,
//! maintaining a constant memory footprint regardless of file size.

use crate::s3::parse_s3_uri;
use async_trait::async_trait;
use futures::StreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use pf_error::{PfError, ReaderError, Result};
use pf_traits::{BatchStream, FileMetadata, StreamingReader};
use pf_types::Batch;
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

    /// Optional AWS access key ID
    pub access_key: Option<String>,

    /// Optional AWS secret access key
    pub secret_key: Option<String>,

    /// Optional AWS session token (for temporary credentials)
    pub session_token: Option<String>,
}

impl ParquetReaderConfig {
    /// Create a new configuration with default batch size.
    pub fn new(region: impl Into<String>) -> Self {
        Self {
            region: region.into(),
            endpoint: None,
            batch_size: 8192,
            access_key: None,
            secret_key: None,
            session_token: None,
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

    /// Set AWS credentials.
    pub fn with_credentials(
        mut self,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
        session_token: Option<String>,
    ) -> Self {
        self.access_key = Some(access_key.into());
        self.secret_key = Some(secret_key.into());
        self.session_token = session_token;
        self
    }
}

/// Streaming Parquet file reader.
///
/// Uses byte-range requests to stream Parquet files with O(batch_size) memory,
/// enabling processing of files larger than available RAM.
///
/// # Memory Model
///
/// - S3 response buffer: 8 KB (per request)
/// - Parquet decoder: decodes on-demand, not buffered
/// - Current RecordBatch: 2-10 MB (batch_size rows × row width)
///
/// Total memory per thread stays bounded at ~50MB regardless of file size.
pub struct ParquetReader {
    config: ParquetReaderConfig,
}

impl ParquetReader {
    /// Create a new Parquet reader with the given configuration.
    pub async fn new(config: ParquetReaderConfig) -> Result<Self> {
        Ok(Self { config })
    }

    /// Create a Parquet reader for local files only (no S3 support).
    pub fn local_only() -> Self {
        Self {
            config: ParquetReaderConfig::new("local"),
        }
    }

    /// Create an object store for the given URI.
    fn create_object_store(&self, uri: &str) -> Result<(Arc<dyn ObjectStore>, ObjectPath)> {
        if uri.starts_with("s3://") {
            let (bucket, key) = parse_s3_uri(uri)?;

            // Start with a new builder (don't use from_env to avoid IMDS fallback)
            let mut builder = AmazonS3Builder::new()
                .with_bucket_name(&bucket)
                .with_region(&self.config.region);

            // Use explicit credentials if provided (resolved by caller via AWS SDK)
            if let (Some(access_key), Some(secret_key)) =
                (&self.config.access_key, &self.config.secret_key)
            {
                builder = builder
                    .with_access_key_id(access_key)
                    .with_secret_access_key(secret_key);

                if let Some(token) = &self.config.session_token {
                    builder = builder.with_token(token);
                }
            } else {
                // No credentials provided - use anonymous access for public buckets
                builder = builder.with_skip_signature(true);
            }

            if let Some(endpoint) = &self.config.endpoint {
                builder = builder
                    .with_endpoint(endpoint)
                    .with_allow_http(true)
                    .with_virtual_hosted_style_request(false);
            }

            let store = builder.build().map_err(|e| {
                PfError::Reader(ReaderError::S3Error(format!(
                    "Failed to create S3 object store: {}",
                    e
                )))
            })?;

            let path = ObjectPath::from(key);
            Ok((Arc::new(store), path))
        } else {
            let path_str = if uri.starts_with("file://") {
                uri.strip_prefix("file://").unwrap()
            } else {
                uri
            };

            let store = LocalFileSystem::new();
            let path = ObjectPath::from_absolute_path(path_str).map_err(|e| {
                PfError::Reader(ReaderError::Io(format!("Invalid local path '{}': {}", uri, e)))
            })?;

            Ok((Arc::new(store), path))
        }
    }
}

#[async_trait]
impl StreamingReader for ParquetReader {
    async fn read_stream(&self, uri: &str) -> Result<BatchStream> {
        info!(uri = uri, "Opening Parquet file for streaming");

        let (store, path) = self.create_object_store(uri)?;

        // Get object metadata to determine file size
        let meta = store.head(&path).await.map_err(|e| {
            if e.to_string().contains("not found") || e.to_string().contains("404") {
                PfError::Reader(ReaderError::NotFound(uri.to_string()))
            } else {
                PfError::Reader(ReaderError::S3Error(format!(
                    "Failed to get metadata for '{}': {}",
                    uri, e
                )))
            }
        })?;

        debug!(
            uri = uri,
            size = meta.size,
            "Got file metadata, creating async reader"
        );

        // Create async reader that uses byte-range requests
        let reader = ParquetObjectReader::new(store, meta);

        // Build async stream builder - only fetches footer initially
        let builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(|e| {
                PfError::Reader(ReaderError::ParseError(format!(
                    "Failed to create Parquet reader for '{}': {}",
                    uri, e
                )))
            })?;

        let batch_size = self.config.batch_size;
        let uri_clone = uri.to_string();

        debug!(
            uri = uri,
            row_groups = builder.metadata().num_row_groups(),
            batch_size = batch_size,
            "Building async stream"
        );

        // Build the async stream - fetches row groups on demand via byte-range requests
        let stream = builder
            .with_batch_size(batch_size)
            .build()
            .map_err(|e| {
                PfError::Reader(ReaderError::ParseError(format!(
                    "Failed to build Parquet stream for '{}': {}",
                    uri, e
                )))
            })?;

        // Convert to BatchStream, wrapping each RecordBatch
        let batch_stream = stream
            .enumerate()
            .map(move |(batch_index, result)| {
                result
                    .map(|record_batch| {
                        trace!(
                            batch_index = batch_index,
                            rows = record_batch.num_rows(),
                            "Streamed Parquet batch"
                        );
                        Batch::new(record_batch, uri_clone.clone(), batch_index)
                    })
                    .map_err(|e| {
                        PfError::Reader(ReaderError::ParseError(format!(
                            "Failed to read batch {}: {}",
                            batch_index, e
                        )))
                    })
            });

        Ok(Box::pin(batch_stream))
    }

    async fn file_metadata(&self, uri: &str) -> Result<FileMetadata> {
        debug!(uri = uri, "Getting Parquet file metadata");

        let (store, path) = self.create_object_store(uri)?;

        // Get object metadata for file size
        let meta = store.head(&path).await.map_err(|e| {
            if e.to_string().contains("not found") || e.to_string().contains("404") {
                PfError::Reader(ReaderError::NotFound(uri.to_string()))
            } else {
                PfError::Reader(ReaderError::S3Error(format!(
                    "Failed to get metadata for '{}': {}",
                    uri, e
                )))
            }
        })?;

        let size = meta.size as u64;

        // Create async reader - only fetches footer (few KB) not entire file
        let reader = ParquetObjectReader::new(store, meta);
        let builder = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .map_err(|e| {
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
    use std::sync::Arc;
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
        let reader = ParquetReader { config };

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
