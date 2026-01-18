//! NDJSON reader implementation with true streaming support.
//!
//! This reader streams NDJSON files line-by-line with bounded memory usage,
//! supporting both local files and S3 with optional gzip/zstd decompression.

use crate::s3::parse_s3_uri;
use arrow::datatypes::SchemaRef;
use arrow_json::ReaderBuilder;
use async_compression::tokio::bufread::{GzipDecoder, ZstdDecoder};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use object_store::ClientOptions;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use pf_error::{PfError, ReaderError, Result};
use pf_traits::{BatchStream, FileMetadata, StreamingReader};
use pf_types::Batch;
use std::collections::HashMap;
use std::io::BufRead;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, BufReader};
use tokio_util::io::StreamReader;
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

    /// Optional AWS access key ID
    pub access_key: Option<String>,

    /// Optional AWS secret access key
    pub secret_key: Option<String>,

    /// Optional AWS session token (for temporary credentials)
    pub session_token: Option<String>,
}

impl NdjsonReaderConfig {
    /// Create a new configuration with default batch size.
    pub fn new(region: impl Into<String>) -> Self {
        Self {
            region: region.into(),
            endpoint: None,
            batch_size: 8192,
            schema_infer_max_records: 1000,
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

    /// Set the maximum records for schema inference.
    pub fn with_schema_infer_max_records(mut self, max: usize) -> Self {
        self.schema_infer_max_records = max;
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

/// Compression type detected from file extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Gzip,
    Zstd,
}

impl Compression {
    /// Detect compression from URI/filename.
    pub fn from_uri(uri: &str) -> Self {
        let uri_lower = uri.to_lowercase();
        if uri_lower.ends_with(".gz") || uri_lower.ends_with(".gzip") {
            Compression::Gzip
        } else if uri_lower.ends_with(".zst") || uri_lower.ends_with(".zstd") {
            Compression::Zstd
        } else {
            Compression::None
        }
    }
}

/// Cache key for local filesystem object store.
const LOCAL_STORE_KEY: &str = "__local__";

/// Create optimized HTTP client options for S3 connection pooling.
///
/// Configures the HTTP client for high-throughput S3 access:
/// - Connection pool sized for concurrent requests
/// - Appropriate timeouts for bulk data transfer
/// - HTTP/2 keep-alive for persistent connections
fn create_s3_client_options() -> ClientOptions {
    ClientOptions::new()
        // Connection pool: allow many idle connections per host for parallel requests
        .with_pool_max_idle_per_host(100)
        // Keep idle connections alive for 90 seconds (match default)
        .with_pool_idle_timeout(Duration::from_secs(90))
        // Request timeout: 5 minutes for large file downloads
        .with_timeout(Duration::from_secs(300))
        // Connect timeout: 10 seconds
        .with_connect_timeout(Duration::from_secs(10))
        // HTTP/2 keep-alive to maintain persistent connections
        .with_http2_keep_alive_interval(Duration::from_secs(30))
        .with_http2_keep_alive_timeout(Duration::from_secs(20))
        .with_http2_keep_alive_while_idle()
}

/// Streaming NDJSON file reader.
///
/// Uses streaming I/O to read NDJSON files with O(batch_size) memory,
/// enabling processing of files larger than available RAM.
///
/// # Memory Model
///
/// - S3/file buffer: 8 KB (BufReader)
/// - Line buffer: ~1 KB per line (typical JSON record)
/// - Current RecordBatch: 2-10 MB (batch_size rows × row width)
///
/// Total memory per thread stays bounded at ~15MB regardless of file size.
///
/// # Compression Support
///
/// Automatically detects and handles:
/// - `.gz` / `.gzip` - Gzip compression
/// - `.zst` / `.zstd` - Zstd compression
///
/// # Object Store Caching
///
/// Object stores are cached by bucket name (for S3) or a sentinel key (for local files)
/// to avoid creating new clients for each file access. This reduces connection setup
/// overhead when processing many files.
pub struct NdjsonReader {
    config: NdjsonReaderConfig,
    /// Cache of object stores by bucket name (S3) or LOCAL_STORE_KEY (local files).
    /// Uses RwLock for thread-safe read-heavy access pattern.
    store_cache: RwLock<HashMap<String, Arc<dyn ObjectStore>>>,
}

impl NdjsonReader {
    /// Create a new NDJSON reader with the given configuration.
    pub async fn new(config: NdjsonReaderConfig) -> Result<Self> {
        Ok(Self {
            config,
            store_cache: RwLock::new(HashMap::new()),
        })
    }

    /// Create an NDJSON reader for local files only (no S3 support).
    pub fn local_only() -> Self {
        Self {
            config: NdjsonReaderConfig::new("local"),
            store_cache: RwLock::new(HashMap::new()),
        }
    }

    /// Get or create an object store for the given cache key.
    ///
    /// This method checks the cache first and returns a cached store if available.
    /// Otherwise, it creates a new store, caches it, and returns it.
    fn get_or_create_store(&self, cache_key: &str) -> Result<Arc<dyn ObjectStore>> {
        // Fast path: check if store exists in cache (read lock)
        {
            let cache = self.store_cache.read().unwrap();
            if let Some(store) = cache.get(cache_key) {
                debug!(cache_key = cache_key, "Using cached object store");
                return Ok(Arc::clone(store));
            }
        }

        // Slow path: create new store (write lock)
        let mut cache = self.store_cache.write().unwrap();

        // Double-check in case another thread created it while we waited
        if let Some(store) = cache.get(cache_key) {
            debug!(
                cache_key = cache_key,
                "Using cached object store (after lock)"
            );
            return Ok(Arc::clone(store));
        }

        // Create the appropriate store
        let store: Arc<dyn ObjectStore> = if cache_key == LOCAL_STORE_KEY {
            debug!("Creating new local filesystem object store");
            Arc::new(LocalFileSystem::new())
        } else {
            // cache_key is the bucket name for S3
            debug!(
                bucket = cache_key,
                "Creating new S3 object store with connection pooling"
            );
            let mut builder = AmazonS3Builder::new()
                .with_bucket_name(cache_key)
                .with_region(&self.config.region)
                // Use shared HTTP connection pool with optimized settings
                .with_client_options(create_s3_client_options());

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
            Arc::new(store)
        };

        cache.insert(cache_key.to_string(), Arc::clone(&store));
        Ok(store)
    }

    /// Create an object store for the given URI.
    ///
    /// Uses a cache to reuse object stores by bucket name (S3) or for local files.
    fn create_object_store(&self, uri: &str) -> Result<(Arc<dyn ObjectStore>, ObjectPath)> {
        if uri.starts_with("s3://") {
            let (bucket, key) = parse_s3_uri(uri)?;
            let store = self.get_or_create_store(&bucket)?;
            let path = ObjectPath::from(key);
            Ok((store, path))
        } else {
            let path_str = if uri.starts_with("file://") {
                uri.strip_prefix("file://").unwrap()
            } else {
                uri
            };

            let store = self.get_or_create_store(LOCAL_STORE_KEY)?;
            let path = ObjectPath::from_absolute_path(path_str).map_err(|e| {
                PfError::Reader(ReaderError::Io(format!(
                    "Invalid local path '{}': {}",
                    uri, e
                )))
            })?;

            Ok((store, path))
        }
    }

    /// Infer schema from the first N records of an NDJSON stream.
    ///
    /// This reads up to `schema_infer_max_records` lines to determine the schema,
    /// returning both the schema and the lines read (so they can be processed).
    async fn infer_schema_from_stream<R: AsyncBufRead + Unpin>(
        &self,
        reader: &mut R,
    ) -> Result<(SchemaRef, Vec<String>)> {
        let mut lines = Vec::with_capacity(self.config.schema_infer_max_records);
        let mut line = String::new();

        // Read up to max_records lines for schema inference
        while lines.len() < self.config.schema_infer_max_records {
            line.clear();
            let bytes_read = reader.read_line(&mut line).await.map_err(|e| {
                PfError::Reader(ReaderError::Io(format!("Failed to read line: {}", e)))
            })?;

            if bytes_read == 0 {
                break; // EOF
            }

            let trimmed = line.trim();
            if !trimmed.is_empty() {
                lines.push(trimmed.to_string());
            }
        }

        if lines.is_empty() {
            return Err(PfError::Reader(ReaderError::ParseError(
                "Empty NDJSON file, cannot infer schema".to_string(),
            )));
        }

        // Use arrow-json to infer schema from collected lines
        let infer_data = lines.join("\n");
        let cursor = std::io::Cursor::new(infer_data.as_bytes());

        let (inferred, _) = arrow_json::reader::infer_json_schema(cursor, None).map_err(|e| {
            PfError::Reader(ReaderError::ParseError(format!(
                "Failed to infer JSON schema: {}",
                e
            )))
        })?;

        Ok((Arc::new(inferred), lines))
    }

    /// Count lines in a small file (for metadata).
    /// Only used for local files where we can cheaply read the whole thing.
    fn count_lines_sync(&self, path: &str) -> Result<usize> {
        let file = std::fs::File::open(path)
            .map_err(|e| PfError::Reader(ReaderError::Io(format!("Failed to open file: {}", e))))?;
        let reader = std::io::BufReader::new(file);
        Ok(reader.lines().count())
    }
}

#[async_trait]
impl StreamingReader for NdjsonReader {
    async fn read_stream(&self, uri: &str) -> Result<BatchStream> {
        info!(uri = uri, "Opening NDJSON file for streaming");

        let (store, path) = self.create_object_store(uri)?;
        let compression = Compression::from_uri(uri);

        // Get object metadata
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
            compression = ?compression,
            "Got file metadata, creating streaming reader"
        );

        // Get the byte stream from object store
        let byte_stream = store.get(&path).await.map_err(|e| {
            PfError::Reader(ReaderError::S3Error(format!(
                "Failed to get object '{}': {}",
                uri, e
            )))
        })?;

        // Convert to tokio AsyncRead
        let bytes_stream = byte_stream
            .into_stream()
            .map(|result| result.map_err(std::io::Error::other));
        let async_read = StreamReader::new(bytes_stream);

        // Apply decompression if needed, then buffer
        let buf_reader: Pin<Box<dyn AsyncBufRead + Send>> = match compression {
            Compression::None => Box::pin(BufReader::with_capacity(8192, async_read)),
            Compression::Gzip => {
                let decoder = GzipDecoder::new(BufReader::with_capacity(8192, async_read));
                Box::pin(BufReader::with_capacity(8192, decoder))
            }
            Compression::Zstd => {
                let decoder = ZstdDecoder::new(BufReader::with_capacity(8192, async_read));
                Box::pin(BufReader::with_capacity(8192, decoder))
            }
        };

        // We need to infer schema first, which requires reading some data
        // For true streaming, we buffer the schema inference lines and process them first
        let mut buf_reader = buf_reader;
        let (schema, initial_lines) = self.infer_schema_from_stream(&mut buf_reader).await?;

        debug!(
            uri = uri,
            fields = schema.fields().len(),
            initial_lines = initial_lines.len(),
            "Inferred schema from stream"
        );

        let batch_size = self.config.batch_size;
        let uri_clone = uri.to_string();

        // Create a streaming batch producer
        let batch_stream =
            NdjsonBatchStream::new(buf_reader, schema, batch_size, uri_clone, initial_lines);

        Ok(Box::pin(batch_stream))
    }

    async fn file_metadata(&self, uri: &str) -> Result<FileMetadata> {
        debug!(uri = uri, "Getting NDJSON file metadata");

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

        // For schema inference, we need to read some data
        let byte_stream = store.get(&path).await.map_err(|e| {
            PfError::Reader(ReaderError::S3Error(format!(
                "Failed to get object '{}': {}",
                uri, e
            )))
        })?;

        let compression = Compression::from_uri(uri);
        let bytes_stream = byte_stream
            .into_stream()
            .map(|result| result.map_err(std::io::Error::other));
        let async_read = StreamReader::new(bytes_stream);

        let buf_reader: Pin<Box<dyn AsyncBufRead + Send>> = match compression {
            Compression::None => Box::pin(BufReader::with_capacity(8192, async_read)),
            Compression::Gzip => {
                let decoder = GzipDecoder::new(BufReader::with_capacity(8192, async_read));
                Box::pin(BufReader::with_capacity(8192, decoder))
            }
            Compression::Zstd => {
                let decoder = ZstdDecoder::new(BufReader::with_capacity(8192, async_read));
                Box::pin(BufReader::with_capacity(8192, decoder))
            }
        };

        let mut buf_reader = buf_reader;
        let (schema, _) = self.infer_schema_from_stream(&mut buf_reader).await?;

        // For row count, we can only provide it for local uncompressed files
        // For S3 or compressed files, we'd need to read the entire file
        let row_count = if !uri.starts_with("s3://") && compression == Compression::None {
            let path_str = if uri.starts_with("file://") {
                uri.strip_prefix("file://").unwrap()
            } else {
                uri
            };
            Some(self.count_lines_sync(path_str)? as u64)
        } else {
            None
        };

        let mut metadata = FileMetadata::new(size, schema);
        if let Some(count) = row_count {
            metadata = metadata.with_row_count(count);
        }

        Ok(metadata)
    }
}

/// Streaming batch producer for NDJSON files.
///
/// Reads lines from the async reader and produces Arrow RecordBatches
/// with bounded memory usage.
struct NdjsonBatchStream {
    /// Receiver for batches from the background task
    receiver: Option<tokio::sync::mpsc::Receiver<Result<Batch>>>,
}

impl NdjsonBatchStream {
    fn new(
        reader: Pin<Box<dyn AsyncBufRead + Send>>,
        schema: SchemaRef,
        batch_size: usize,
        uri: String,
        initial_lines: Vec<String>,
    ) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(2);

        // Spawn a task to read lines and produce batches
        tokio::spawn(async move {
            let result =
                Self::read_all_batches(reader, schema, batch_size, uri, initial_lines, tx).await;

            if let Err(e) = result {
                tracing::error!(error = %e, "Error in NDJSON batch stream");
            }
        });

        Self { receiver: Some(rx) }
    }

    async fn read_all_batches(
        mut reader: Pin<Box<dyn AsyncBufRead + Send>>,
        schema: SchemaRef,
        batch_size: usize,
        uri: String,
        initial_lines: Vec<String>,
        tx: tokio::sync::mpsc::Sender<Result<Batch>>,
    ) -> Result<()> {
        // Pre-allocate a single String buffer for accumulating JSON lines.
        // Estimate ~128 bytes per line as typical for JSON records.
        // This avoids per-line String allocations and the join() allocation.
        let estimated_capacity = batch_size * 128;
        let mut json_buffer = String::with_capacity(estimated_capacity);
        let mut line_count = 0usize;
        let mut batch_index = 0;
        let mut line = String::new();

        // Process initial lines - append them to our buffer
        for initial_line in initial_lines {
            if !json_buffer.is_empty() {
                json_buffer.push('\n');
            }
            json_buffer.push_str(&initial_line);
            line_count += 1;

            if line_count >= batch_size {
                let batch =
                    Self::buffer_to_batch(&schema, &json_buffer, line_count, &uri, batch_index)?;
                batch_index += 1;
                // Clear buffer but keep capacity for reuse
                json_buffer.clear();
                line_count = 0;
                if tx.send(Ok(batch)).await.is_err() {
                    return Ok(()); // Receiver dropped
                }
            }
        }

        // Read remaining lines from stream
        loop {
            line.clear();
            let bytes_read = reader.read_line(&mut line).await.map_err(|e| {
                PfError::Reader(ReaderError::Io(format!("Failed to read line: {}", e)))
            })?;

            if bytes_read == 0 {
                // EOF - flush remaining lines
                if line_count > 0 {
                    let batch = Self::buffer_to_batch(
                        &schema,
                        &json_buffer,
                        line_count,
                        &uri,
                        batch_index,
                    )?;
                    let _ = tx.send(Ok(batch)).await;
                }
                break;
            }

            let trimmed = line.trim();
            if !trimmed.is_empty() {
                // Append directly to buffer instead of creating intermediate Strings
                if !json_buffer.is_empty() {
                    json_buffer.push('\n');
                }
                json_buffer.push_str(trimmed);
                line_count += 1;

                if line_count >= batch_size {
                    let batch = Self::buffer_to_batch(
                        &schema,
                        &json_buffer,
                        line_count,
                        &uri,
                        batch_index,
                    )?;
                    batch_index += 1;
                    // Clear buffer but keep capacity for reuse
                    json_buffer.clear();
                    line_count = 0;
                    if tx.send(Ok(batch)).await.is_err() {
                        return Ok(()); // Receiver dropped
                    }
                }
            }
        }

        Ok(())
    }

    /// Convert pre-built JSON buffer to a RecordBatch.
    ///
    /// This method takes a pre-allocated buffer containing newline-separated JSON
    /// records, avoiding the double allocation of Vec<String> + join().
    fn buffer_to_batch(
        schema: &SchemaRef,
        json_buffer: &str,
        line_count: usize,
        uri: &str,
        batch_index: usize,
    ) -> Result<Batch> {
        if json_buffer.is_empty() || line_count == 0 {
            return Err(PfError::Reader(ReaderError::ParseError(
                "No lines to convert to batch".to_string(),
            )));
        }

        let cursor = std::io::Cursor::new(json_buffer.as_bytes());

        let reader = ReaderBuilder::new(schema.clone())
            .with_batch_size(line_count)
            .build(std::io::BufReader::new(cursor))
            .map_err(|e| {
                PfError::Reader(ReaderError::ParseError(format!(
                    "Failed to create JSON reader: {}",
                    e
                )))
            })?;

        // Read the single batch
        let batch = reader.into_iter().next();

        match batch {
            Some(Ok(record_batch)) => {
                trace!(
                    batch_index = batch_index,
                    rows = record_batch.num_rows(),
                    "Created NDJSON batch"
                );

                Ok(Batch::new(record_batch, uri.to_string(), batch_index))
            }
            Some(Err(e)) => Err(PfError::Reader(ReaderError::ParseError(format!(
                "Failed to parse JSON batch: {}",
                e
            )))),
            None => Err(PfError::Reader(ReaderError::ParseError(
                "No batch produced from JSON data".to_string(),
            ))),
        }
    }
}

impl Stream for NdjsonBatchStream {
    type Item = Result<Batch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(ref mut rx) = self.receiver {
            match Pin::new(rx).poll_recv(cx) {
                Poll::Ready(Some(batch)) => Poll::Ready(Some(batch)),
                Poll::Ready(None) => {
                    self.receiver = None;
                    Poll::Ready(None)
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            Poll::Ready(None)
        }
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
            writeln!(
                file,
                r#"{{"id": {}, "name": {}, "value": {}}}"#,
                i,
                name,
                i * 10
            )
            .unwrap();
        }

        file
    }

    fn create_gzipped_ndjson_file(num_rows: usize) -> NamedTempFile {
        use flate2::Compression;
        use flate2::write::GzEncoder;

        let mut file = NamedTempFile::with_suffix(".ndjson.gz").unwrap();

        {
            let mut encoder = GzEncoder::new(&mut file, Compression::default());

            for i in 0..num_rows {
                let name = if i % 10 == 0 {
                    "null".to_string()
                } else {
                    format!("\"name_{}\"", i)
                };
                writeln!(
                    encoder,
                    r#"{{"id": {}, "name": {}, "value": {}}}"#,
                    i,
                    name,
                    i * 10
                )
                .unwrap();
            }

            encoder.finish().unwrap();
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
            store_cache: std::sync::RwLock::new(std::collections::HashMap::new()),
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
        // With batch size 100 and 1000 rows, we should get 10 batches
        assert_eq!(batch_count, 10);
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

    #[tokio::test]
    async fn test_compression_detection() {
        assert_eq!(Compression::from_uri("file.ndjson"), Compression::None);
        assert_eq!(Compression::from_uri("file.ndjson.gz"), Compression::Gzip);
        assert_eq!(Compression::from_uri("file.ndjson.gzip"), Compression::Gzip);
        assert_eq!(Compression::from_uri("file.ndjson.zst"), Compression::Zstd);
        assert_eq!(Compression::from_uri("file.ndjson.zstd"), Compression::Zstd);
        assert_eq!(
            Compression::from_uri("s3://bucket/path/file.json.gz"),
            Compression::Gzip
        );
    }

    #[tokio::test]
    async fn test_read_gzipped_ndjson() {
        let file = create_gzipped_ndjson_file(100);
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
    async fn test_streaming_large_file() {
        // Create a larger file to verify streaming works
        let file = create_test_ndjson_file(10000);
        let path = file.path().to_str().unwrap();

        let config = NdjsonReaderConfig::new("local").with_batch_size(500);
        let reader = NdjsonReader {
            config,
            store_cache: std::sync::RwLock::new(std::collections::HashMap::new()),
        };

        let mut stream = reader.read_stream(path).await.unwrap();

        let mut total_rows = 0;
        let mut batch_count = 0;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.unwrap();
            total_rows += batch.num_rows();
            batch_count += 1;

            // Verify batches are roughly the expected size
            if batch_count < 20 {
                // Not the last batch
                assert!(batch.num_rows() <= 500);
            }
        }

        assert_eq!(total_rows, 10000);
        assert_eq!(batch_count, 20); // 10000 / 500 = 20 batches
    }

    #[tokio::test]
    async fn test_empty_lines_skipped() {
        let mut file = NamedTempFile::new().unwrap();

        // Write some records with empty lines interspersed
        writeln!(file, r#"{{"id": 1, "name": "one"}}"#).unwrap();
        writeln!(file).unwrap(); // empty line
        writeln!(file, r#"{{"id": 2, "name": "two"}}"#).unwrap();
        writeln!(file, "   ").unwrap(); // whitespace-only line
        writeln!(file, r#"{{"id": 3, "name": "three"}}"#).unwrap();

        let path = file.path().to_str().unwrap();
        let reader = NdjsonReader::local_only();
        let mut stream = reader.read_stream(path).await.unwrap();

        let mut total_rows = 0;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.unwrap();
            total_rows += batch.num_rows();
        }

        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_object_store_cache_reuse() {
        // Create two test files
        let file1 = create_test_ndjson_file(50);
        let file2 = create_test_ndjson_file(75);
        let path1 = file1.path().to_str().unwrap();
        let path2 = file2.path().to_str().unwrap();

        let reader = NdjsonReader::local_only();

        // Verify cache is initially empty
        {
            let cache = reader.store_cache.read().unwrap();
            assert!(cache.is_empty(), "Cache should be empty initially");
        }

        // Read first file
        let mut stream1 = reader.read_stream(path1).await.unwrap();
        while let Some(batch_result) = stream1.next().await {
            batch_result.unwrap();
        }

        // Verify cache has one entry for local store
        {
            let cache = reader.store_cache.read().unwrap();
            assert_eq!(
                cache.len(),
                1,
                "Cache should have one entry after first read"
            );
            assert!(
                cache.contains_key(super::LOCAL_STORE_KEY),
                "Cache should contain local store key"
            );
        }

        // Read second file - should reuse the same store
        let mut stream2 = reader.read_stream(path2).await.unwrap();
        while let Some(batch_result) = stream2.next().await {
            batch_result.unwrap();
        }

        // Verify cache still has only one entry (reused)
        {
            let cache = reader.store_cache.read().unwrap();
            assert_eq!(
                cache.len(),
                1,
                "Cache should still have one entry after second read (store reused)"
            );
        }
    }
}
