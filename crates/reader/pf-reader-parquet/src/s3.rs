//! S3 client wrapper for Parquet reader.

use aws_sdk_s3::Client;
use bytes::Bytes;
use pf_error::{PfError, ReaderError, Result};
use tracing::{debug, trace};

/// S3 client wrapper with optional endpoint override.
#[derive(Clone)]
pub struct S3Client {
    client: Client,
}

impl S3Client {
    /// Create a new S3 client with the given region and optional endpoint.
    pub async fn new(region: &str, endpoint: Option<&str>) -> Result<Self> {
        let config = if let Some(endpoint_url) = endpoint {
            aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(aws_sdk_s3::config::Region::new(region.to_string()))
                .endpoint_url(endpoint_url)
                .load()
                .await
        } else {
            aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(aws_sdk_s3::config::Region::new(region.to_string()))
                .load()
                .await
        };

        let client = Client::new(&config);
        Ok(Self { client })
    }

    /// Get the size of an object.
    pub async fn get_object_size(&self, bucket: &str, key: &str) -> Result<u64> {
        let result = self
            .client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| {
                PfError::Reader(ReaderError::S3Error(format!(
                    "Failed to get object metadata for s3://{}/{}: {}",
                    bucket, key, e
                )))
            })?;

        Ok(result.content_length().unwrap_or(0) as u64)
    }

    /// Download an entire object into memory.
    pub async fn get_object(&self, bucket: &str, key: &str) -> Result<Bytes> {
        debug!(bucket = bucket, key = key, "Downloading object from S3");

        let result = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| {
                PfError::Reader(ReaderError::S3Error(format!(
                    "Failed to download s3://{}/{}: {}",
                    bucket, key, e
                )))
            })?;

        let bytes = result.body.collect().await.map_err(|e| {
            PfError::Reader(ReaderError::S3Error(format!(
                "Failed to read body for s3://{}/{}: {}",
                bucket, key, e
            )))
        })?;

        let data = bytes.into_bytes();
        trace!(
            bucket = bucket,
            key = key,
            size = data.len(),
            "Downloaded object"
        );

        Ok(data)
    }

    /// Download a byte range of an object.
    pub async fn get_object_range(
        &self,
        bucket: &str,
        key: &str,
        start: u64,
        end: u64,
    ) -> Result<Bytes> {
        trace!(
            bucket = bucket,
            key = key,
            start = start,
            end = end,
            "Downloading byte range from S3"
        );

        let range = format!("bytes={}-{}", start, end);

        let result = self
            .client
            .get_object()
            .bucket(bucket)
            .key(key)
            .range(range)
            .send()
            .await
            .map_err(|e| {
                PfError::Reader(ReaderError::S3Error(format!(
                    "Failed to download range from s3://{}/{}: {}",
                    bucket, key, e
                )))
            })?;

        let bytes = result.body.collect().await.map_err(|e| {
            PfError::Reader(ReaderError::S3Error(format!(
                "Failed to read body for s3://{}/{}: {}",
                bucket, key, e
            )))
        })?;

        Ok(bytes.into_bytes())
    }
}

/// Parse an S3 URI into bucket and key.
pub fn parse_s3_uri(uri: &str) -> Result<(String, String)> {
    let url = url::Url::parse(uri).map_err(|e| {
        PfError::Reader(ReaderError::InvalidUri(format!(
            "Invalid S3 URI '{}': {}",
            uri, e
        )))
    })?;

    if url.scheme() != "s3" {
        return Err(PfError::Reader(ReaderError::InvalidUri(format!(
            "Expected s3:// URI, got: {}",
            uri
        ))));
    }

    let bucket = url.host_str().ok_or_else(|| {
        PfError::Reader(ReaderError::InvalidUri(format!(
            "Missing bucket in S3 URI: {}",
            uri
        )))
    })?;

    let key = url.path().trim_start_matches('/');
    if key.is_empty() {
        return Err(PfError::Reader(ReaderError::InvalidUri(format!(
            "Missing key in S3 URI: {}",
            uri
        ))));
    }

    Ok((bucket.to_string(), key.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s3_uri_valid() {
        let (bucket, key) = parse_s3_uri("s3://my-bucket/path/to/file.parquet").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "path/to/file.parquet");
    }

    #[test]
    fn test_parse_s3_uri_root_key() {
        let (bucket, key) = parse_s3_uri("s3://bucket/file.parquet").unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(key, "file.parquet");
    }

    #[test]
    fn test_parse_s3_uri_invalid_scheme() {
        let result = parse_s3_uri("http://bucket/key");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_s3_uri_missing_key() {
        let result = parse_s3_uri("s3://bucket/");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_s3_uri_missing_bucket() {
        let result = parse_s3_uri("s3:///key");
        assert!(result.is_err());
    }
}
