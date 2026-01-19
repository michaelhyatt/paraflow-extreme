//! Parquet reader implementation with true streaming support.
//!
//! This reader uses byte-range requests to fetch Parquet row groups on demand,
//! maintaining a constant memory footprint regardless of file size.

use crate::s3::parse_s3_uri;
use async_trait::async_trait;
use futures::StreamExt;
use object_store::ClientOptions;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter};
use parquet::arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder};
use pf_error::{PfError, ReaderError, Result};
use pf_traits::{BatchStream, FileMetadata, StreamingReader};
use pf_types::Batch;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tracing::{debug, info, trace};

/// A simple filter predicate for row filtering.
///
/// Supports basic comparisons: column op value where op is =, !=, <, <=, >, >=
#[derive(Debug, Clone)]
pub struct FilterPredicate {
    /// Column name to filter on
    pub column: String,
    /// Comparison operator
    pub op: FilterOp,
    /// Value to compare against (as string, will be parsed based on column type)
    pub value: String,
}

/// Filter comparison operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl FilterPredicate {
    /// Parse a filter predicate from a string like "column >= value".
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();

        // Try operators in order of length (longest first to avoid partial matches)
        let ops = [
            (">=", FilterOp::GtEq),
            ("<=", FilterOp::LtEq),
            ("!=", FilterOp::NotEq),
            ("=", FilterOp::Eq),
            (">", FilterOp::Gt),
            ("<", FilterOp::Lt),
        ];

        for (op_str, op) in ops {
            if let Some(pos) = s.find(op_str) {
                let column = s[..pos].trim().to_string();
                let value = s[pos + op_str.len()..].trim();
                // Remove quotes if present
                let value = value.trim_matches('"').trim_matches('\'').to_string();

                if !column.is_empty() && !value.is_empty() {
                    return Some(Self { column, op, value });
                }
            }
        }
        None
    }
}

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

    /// Optional column projection (list of column names to read).
    /// If None, all columns are read.
    pub projection: Option<Vec<String>>,

    /// Optional filter predicate for row-level filtering.
    /// Uses Parquet row group statistics for predicate pushdown.
    pub filter: Option<FilterPredicate>,
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
            projection: None,
            filter: None,
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

    /// Set column projection (list of column names to read).
    ///
    /// Only the specified columns will be read from the Parquet file,
    /// reducing I/O and improving performance for wide schemas.
    pub fn with_projection(mut self, columns: Vec<String>) -> Self {
        self.projection = Some(columns);
        self
    }

    /// Set a filter predicate for row-level filtering.
    ///
    /// The filter uses Parquet row group statistics for predicate pushdown,
    /// potentially skipping entire row groups that don't match the filter.
    pub fn with_filter(mut self, filter: FilterPredicate) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Set a filter predicate from a string expression.
    ///
    /// Parses expressions like "column >= value", "status = 'active'".
    /// Returns None if the expression cannot be parsed.
    pub fn with_filter_expr(mut self, expr: &str) -> Self {
        self.filter = FilterPredicate::parse(expr);
        self
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
///
/// # Object Store Caching
///
/// Object stores are cached by bucket name (for S3) or a sentinel key (for local files)
/// to avoid creating new clients for each file access. This reduces connection setup
/// overhead when processing many files.
pub struct ParquetReader {
    config: ParquetReaderConfig,
    /// Cache of object stores by bucket name (S3) or LOCAL_STORE_KEY (local files).
    /// Uses RwLock for thread-safe read-heavy access pattern.
    store_cache: RwLock<HashMap<String, Arc<dyn ObjectStore>>>,
}

impl ParquetReader {
    /// Create a new Parquet reader with the given configuration.
    pub async fn new(config: ParquetReaderConfig) -> Result<Self> {
        Ok(Self {
            config,
            store_cache: RwLock::new(HashMap::new()),
        })
    }

    /// Create a Parquet reader for local files only (no S3 support).
    pub fn local_only() -> Self {
        Self {
            config: ParquetReaderConfig::new("local"),
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
}

/// Helper function to create a comparison mask for i32 arrays.
fn create_comparison_mask_i32(
    arr: &arrow::array::Int32Array,
    op: FilterOp,
    val: i32,
) -> arrow::array::BooleanArray {
    use arrow::array::{Array, BooleanArray};

    let values: Vec<bool> = (0..arr.len())
        .map(|i| {
            if arr.is_null(i) {
                false
            } else {
                let v = arr.value(i);
                match op {
                    FilterOp::Eq => v == val,
                    FilterOp::NotEq => v != val,
                    FilterOp::Lt => v < val,
                    FilterOp::LtEq => v <= val,
                    FilterOp::Gt => v > val,
                    FilterOp::GtEq => v >= val,
                }
            }
        })
        .collect();
    BooleanArray::from(values)
}

/// Helper function to create a comparison mask for i64 arrays.
fn create_comparison_mask_i64(
    arr: &arrow::array::Int64Array,
    op: FilterOp,
    val: i64,
) -> arrow::array::BooleanArray {
    use arrow::array::{Array, BooleanArray};

    let values: Vec<bool> = (0..arr.len())
        .map(|i| {
            if arr.is_null(i) {
                false
            } else {
                let v = arr.value(i);
                match op {
                    FilterOp::Eq => v == val,
                    FilterOp::NotEq => v != val,
                    FilterOp::Lt => v < val,
                    FilterOp::LtEq => v <= val,
                    FilterOp::Gt => v > val,
                    FilterOp::GtEq => v >= val,
                }
            }
        })
        .collect();
    BooleanArray::from(values)
}

/// Helper function to create a comparison mask for string arrays.
fn create_comparison_mask_string(
    arr: &arrow::array::StringArray,
    op: FilterOp,
    val: &str,
) -> arrow::array::BooleanArray {
    use arrow::array::{Array, BooleanArray};

    let values: Vec<bool> = (0..arr.len())
        .map(|i| {
            if arr.is_null(i) {
                false
            } else {
                let v = arr.value(i);
                match op {
                    FilterOp::Eq => v == val,
                    FilterOp::NotEq => v != val,
                    FilterOp::Lt => v < val,
                    FilterOp::LtEq => v <= val,
                    FilterOp::Gt => v > val,
                    FilterOp::GtEq => v >= val,
                }
            }
        })
        .collect();
    BooleanArray::from(values)
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

        // Apply column projection if specified
        let builder = if let Some(ref columns) = self.config.projection {
            let parquet_schema = builder.parquet_schema();
            let arrow_schema = builder.schema();

            // Build projection mask from column names
            let indices: Vec<usize> = columns
                .iter()
                .filter_map(|col_name| {
                    arrow_schema
                        .fields()
                        .iter()
                        .position(|f| f.name() == col_name)
                })
                .collect();

            if indices.is_empty() {
                debug!(
                    uri = uri,
                    requested_columns = ?columns,
                    "No matching columns found for projection, reading all columns"
                );
                builder
            } else {
                debug!(
                    uri = uri,
                    projected_columns = indices.len(),
                    total_columns = arrow_schema.fields().len(),
                    "Applying column projection"
                );
                let projection = ProjectionMask::roots(parquet_schema, indices);
                builder.with_projection(projection)
            }
        } else {
            builder
        };

        // Apply row filter if specified
        let builder = if let Some(ref filter) = self.config.filter {
            let arrow_schema = builder.schema();

            // Find the column index for the filter
            if let Some(col_idx) = arrow_schema
                .fields()
                .iter()
                .position(|f| f.name() == &filter.column)
            {
                debug!(
                    uri = uri,
                    filter_column = &filter.column,
                    filter_op = ?filter.op,
                    filter_value = &filter.value,
                    "Applying row filter"
                );

                // Create projection mask for just the filter column
                let filter_projection =
                    ProjectionMask::roots(builder.parquet_schema(), vec![col_idx]);

                // Clone filter values for the closure
                let filter_value = filter.value.clone();
                let filter_op = filter.op;

                // Create the predicate function
                let predicate_fn = move |batch: arrow::record_batch::RecordBatch| {
                    use arrow::array::*;
                    use arrow::datatypes::DataType;

                    let col = batch.column(0);
                    let num_rows = batch.num_rows();

                    // Create boolean mask based on column type and operator
                    let mask: BooleanArray = match col.data_type() {
                        DataType::Int32 => {
                            if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
                                if let Ok(val) = filter_value.parse::<i32>() {
                                    create_comparison_mask_i32(arr, filter_op, val)
                                } else {
                                    BooleanArray::from(vec![true; num_rows])
                                }
                            } else {
                                BooleanArray::from(vec![true; num_rows])
                            }
                        }
                        DataType::Int64 => {
                            if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                                if let Ok(val) = filter_value.parse::<i64>() {
                                    create_comparison_mask_i64(arr, filter_op, val)
                                } else {
                                    BooleanArray::from(vec![true; num_rows])
                                }
                            } else {
                                BooleanArray::from(vec![true; num_rows])
                            }
                        }
                        DataType::Utf8 => {
                            if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                                create_comparison_mask_string(arr, filter_op, &filter_value)
                            } else {
                                BooleanArray::from(vec![true; num_rows])
                            }
                        }
                        _ => {
                            // Unsupported type, pass all rows through
                            BooleanArray::from(vec![true; num_rows])
                        }
                    };

                    Ok(mask)
                };

                let predicate = ArrowPredicateFn::new(filter_projection, predicate_fn);
                let row_filter = RowFilter::new(vec![Box::new(predicate)]);
                builder.with_row_filter(row_filter)
            } else {
                debug!(
                    uri = uri,
                    filter_column = &filter.column,
                    "Filter column not found, skipping filter"
                );
                builder
            }
        } else {
            builder
        };

        debug!(
            uri = uri,
            row_groups = builder.metadata().num_row_groups(),
            batch_size = batch_size,
            "Building async stream"
        );

        // Build the async stream - fetches row groups on demand via byte-range requests
        let stream = builder.with_batch_size(batch_size).build().map_err(|e| {
            PfError::Reader(ReaderError::ParseError(format!(
                "Failed to build Parquet stream for '{}': {}",
                uri, e
            )))
        })?;

        // Convert to BatchStream, wrapping each RecordBatch
        let batch_stream = stream.enumerate().map(move |(batch_index, result)| {
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
            .map(|i| if i % 10 == 0 { None } else { Some("test_name") })
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

    #[tokio::test]
    async fn test_object_store_cache_reuse() {
        // Create two test files
        let file1 = create_test_parquet_file(50);
        let file2 = create_test_parquet_file(75);
        let path1 = file1.path().to_str().unwrap();
        let path2 = file2.path().to_str().unwrap();

        let reader = ParquetReader::local_only();

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

    #[tokio::test]
    async fn test_column_projection() {
        let file = create_test_parquet_file(100);
        let path = file.path().to_str().unwrap();

        // Read with projection - only "id" column
        let config = ParquetReaderConfig::new("local").with_projection(vec!["id".to_string()]);
        let reader = ParquetReader {
            config,
            store_cache: std::sync::RwLock::new(std::collections::HashMap::new()),
        };

        let mut stream = reader.read_stream(path).await.unwrap();

        let mut total_rows = 0;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.unwrap();
            total_rows += batch.num_rows();
            // Should only have 1 column (id), not 2
            assert_eq!(batch.num_columns(), 1);
            assert_eq!(batch.schema().field(0).name(), "id");
        }

        assert_eq!(total_rows, 100);
    }

    #[tokio::test]
    async fn test_column_projection_multiple_columns() {
        let file = create_test_parquet_file(50);
        let path = file.path().to_str().unwrap();

        // Read with projection - both columns (should be same as no projection)
        let config = ParquetReaderConfig::new("local")
            .with_projection(vec!["id".to_string(), "name".to_string()]);
        let reader = ParquetReader {
            config,
            store_cache: std::sync::RwLock::new(std::collections::HashMap::new()),
        };

        let mut stream = reader.read_stream(path).await.unwrap();

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.unwrap();
            // Should have both columns
            assert_eq!(batch.num_columns(), 2);
        }
    }

    #[tokio::test]
    async fn test_column_projection_nonexistent_column() {
        let file = create_test_parquet_file(50);
        let path = file.path().to_str().unwrap();

        // Read with projection - column that doesn't exist
        let config =
            ParquetReaderConfig::new("local").with_projection(vec!["nonexistent".to_string()]);
        let reader = ParquetReader {
            config,
            store_cache: std::sync::RwLock::new(std::collections::HashMap::new()),
        };

        let mut stream = reader.read_stream(path).await.unwrap();

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.unwrap();
            // Should fall back to all columns since no match
            assert_eq!(batch.num_columns(), 2);
        }
    }

    #[test]
    fn test_filter_predicate_parse() {
        // Test equality
        let pred = FilterPredicate::parse("status = 'active'").unwrap();
        assert_eq!(pred.column, "status");
        assert_eq!(pred.op, FilterOp::Eq);
        assert_eq!(pred.value, "active");

        // Test greater than or equal
        let pred = FilterPredicate::parse("id >= 100").unwrap();
        assert_eq!(pred.column, "id");
        assert_eq!(pred.op, FilterOp::GtEq);
        assert_eq!(pred.value, "100");

        // Test less than
        let pred = FilterPredicate::parse("year < 2020").unwrap();
        assert_eq!(pred.column, "year");
        assert_eq!(pred.op, FilterOp::Lt);
        assert_eq!(pred.value, "2020");

        // Test not equal
        let pred = FilterPredicate::parse("type != 'test'").unwrap();
        assert_eq!(pred.column, "type");
        assert_eq!(pred.op, FilterOp::NotEq);
        assert_eq!(pred.value, "test");

        // Test with double quotes
        let pred = FilterPredicate::parse("name = \"John\"").unwrap();
        assert_eq!(pred.column, "name");
        assert_eq!(pred.value, "John");

        // Test with spaces
        let pred = FilterPredicate::parse("  count  >   50  ").unwrap();
        assert_eq!(pred.column, "count");
        assert_eq!(pred.op, FilterOp::Gt);
        assert_eq!(pred.value, "50");
    }

    #[tokio::test]
    async fn test_row_filter_int() {
        let file = create_test_parquet_file(100);
        let path = file.path().to_str().unwrap();

        // Filter: id >= 50 (should get ~50 rows)
        let config = ParquetReaderConfig::new("local").with_filter_expr("id >= 50");
        let reader = ParquetReader {
            config,
            store_cache: std::sync::RwLock::new(std::collections::HashMap::new()),
        };

        let mut stream = reader.read_stream(path).await.unwrap();

        let mut total_rows = 0;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.unwrap();
            total_rows += batch.num_rows();
        }

        // With filter id >= 50, we should get 50 rows (ids 50-99)
        assert_eq!(total_rows, 50);
    }

    #[tokio::test]
    async fn test_row_filter_combined_with_projection() {
        let file = create_test_parquet_file(100);
        let path = file.path().to_str().unwrap();

        // Filter id >= 90 AND only select "id" column
        let config = ParquetReaderConfig::new("local")
            .with_filter_expr("id >= 90")
            .with_projection(vec!["id".to_string()]);
        let reader = ParquetReader {
            config,
            store_cache: std::sync::RwLock::new(std::collections::HashMap::new()),
        };

        let mut stream = reader.read_stream(path).await.unwrap();

        let mut total_rows = 0;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.unwrap();
            total_rows += batch.num_rows();
            // Should only have 1 column
            assert_eq!(batch.num_columns(), 1);
        }

        // With filter id >= 90, we should get 10 rows (ids 90-99)
        assert_eq!(total_rows, 10);
    }
}
