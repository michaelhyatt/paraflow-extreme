//! S3 utilities for NDJSON reader.

use pf_error::{PfError, ReaderError, Result};

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
        let (bucket, key) = parse_s3_uri("s3://my-bucket/path/to/file.ndjson").unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "path/to/file.ndjson");
    }

    #[test]
    fn test_parse_s3_uri_root_key() {
        let (bucket, key) = parse_s3_uri("s3://bucket/file.ndjson").unwrap();
        assert_eq!(bucket, "bucket");
        assert_eq!(key, "file.ndjson");
    }

    #[test]
    fn test_parse_s3_uri_invalid_scheme() {
        let result = parse_s3_uri("http://bucket/key");
        assert!(result.is_err());
    }
}
