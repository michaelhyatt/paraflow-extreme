//! Reader factory for creating file readers based on format.

use pf_error::{PfError, ReaderError, Result};
use pf_reader_ndjson::{NdjsonReader, NdjsonReaderConfig};
use pf_reader_parquet::{ParquetReader, ParquetReaderConfig};
use pf_traits::StreamingReader;
use pf_types::FileFormat;
use std::sync::Arc;
use tracing::debug;

/// Configuration for creating readers.
#[derive(Debug, Clone)]
pub struct ReaderFactoryConfig {
    /// AWS region for S3 access
    pub region: String,

    /// Optional S3 endpoint URL (for LocalStack)
    pub endpoint: Option<String>,

    /// Batch size for reading
    pub batch_size: usize,
}

impl ReaderFactoryConfig {
    /// Create a new reader factory configuration.
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

/// Factory for creating readers based on file format.
pub struct ReaderFactory {
    config: ReaderFactoryConfig,
}

impl ReaderFactory {
    /// Create a new reader factory.
    pub fn new(config: ReaderFactoryConfig) -> Self {
        Self { config }
    }

    /// Create a reader for the given file format.
    pub async fn create_reader(&self, format: FileFormat) -> Result<Arc<dyn StreamingReader>> {
        debug!(?format, "Creating reader for format");

        match format {
            FileFormat::Parquet => {
                let mut config = ParquetReaderConfig::new(&self.config.region)
                    .with_batch_size(self.config.batch_size);

                if let Some(ref endpoint) = self.config.endpoint {
                    config = config.with_endpoint(endpoint);
                }

                let reader = ParquetReader::new(config).await?;
                Ok(Arc::new(reader))
            }
            FileFormat::NdJson => {
                let mut config = NdjsonReaderConfig::new(&self.config.region)
                    .with_batch_size(self.config.batch_size);

                if let Some(ref endpoint) = self.config.endpoint {
                    config = config.with_endpoint(endpoint);
                }

                let reader = NdjsonReader::new(config).await?;
                Ok(Arc::new(reader))
            }
            // All format variants are handled above
        }
    }
}

/// Create a reader directly for a specific format.
pub async fn create_reader(
    format: FileFormat,
    region: &str,
    endpoint: Option<&str>,
) -> Result<Arc<dyn StreamingReader>> {
    let mut config = ReaderFactoryConfig::new(region);
    if let Some(ep) = endpoint {
        config = config.with_endpoint(ep);
    }

    let factory = ReaderFactory::new(config);
    factory.create_reader(format).await
}

/// A reader that dispatches to the appropriate format-specific reader.
///
/// This wraps the format detection logic, creating readers on-demand based on
/// the file format guessed from the URI extension.
pub struct FormatDispatchReader {
    parquet_reader: Arc<dyn StreamingReader>,
    ndjson_reader: Arc<dyn StreamingReader>,
}

impl FormatDispatchReader {
    /// Create a new format dispatch reader.
    pub async fn new(config: ReaderFactoryConfig) -> Result<Self> {
        let factory = ReaderFactory::new(config);

        let parquet_reader = factory.create_reader(FileFormat::Parquet).await?;
        let ndjson_reader = factory.create_reader(FileFormat::NdJson).await?;

        Ok(Self {
            parquet_reader,
            ndjson_reader,
        })
    }

    /// Get the reader for a given format.
    fn reader_for_format(&self, format: FileFormat) -> &dyn StreamingReader {
        match format {
            FileFormat::Parquet => self.parquet_reader.as_ref(),
            FileFormat::NdJson => self.ndjson_reader.as_ref(),
        }
    }

    /// Guess format from URI extension.
    pub fn guess_format_from_uri(uri: &str) -> FileFormat {
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
}

#[async_trait::async_trait]
impl StreamingReader for FormatDispatchReader {
    async fn read_stream(&self, uri: &str) -> Result<pf_traits::BatchStream> {
        let format = Self::guess_format_from_uri(uri);
        debug!(?format, uri = uri, "Dispatching read to format-specific reader");
        self.reader_for_format(format).read_stream(uri).await
    }

    async fn file_metadata(&self, uri: &str) -> Result<pf_traits::FileMetadata> {
        let format = Self::guess_format_from_uri(uri);
        self.reader_for_format(format).file_metadata(uri).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_parquet_reader() {
        let config = ReaderFactoryConfig::new("us-east-1");
        let factory = ReaderFactory::new(config);

        let result = factory.create_reader(FileFormat::Parquet).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_create_ndjson_reader() {
        let config = ReaderFactoryConfig::new("us-east-1");
        let factory = ReaderFactory::new(config);

        let result = factory.create_reader(FileFormat::NdJson).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_guess_format_parquet() {
        assert_eq!(
            FormatDispatchReader::guess_format_from_uri("s3://bucket/file.parquet"),
            FileFormat::Parquet
        );
        assert_eq!(
            FormatDispatchReader::guess_format_from_uri("s3://bucket/file.pq"),
            FileFormat::Parquet
        );
    }

    #[test]
    fn test_guess_format_ndjson() {
        assert_eq!(
            FormatDispatchReader::guess_format_from_uri("s3://bucket/file.ndjson"),
            FileFormat::NdJson
        );
        assert_eq!(
            FormatDispatchReader::guess_format_from_uri("s3://bucket/file.jsonl"),
            FileFormat::NdJson
        );
        assert_eq!(
            FormatDispatchReader::guess_format_from_uri("s3://bucket/file.json"),
            FileFormat::NdJson
        );
    }

    #[test]
    fn test_guess_format_default() {
        // Unknown extension defaults to Parquet
        assert_eq!(
            FormatDispatchReader::guess_format_from_uri("s3://bucket/file.unknown"),
            FileFormat::Parquet
        );
    }

    #[tokio::test]
    async fn test_format_dispatch_reader() {
        let config = ReaderFactoryConfig::new("us-east-1");
        let result = FormatDispatchReader::new(config).await;
        assert!(result.is_ok());
    }
}
