//! S3 object listing with pagination support.

use async_stream::try_stream;
use aws_sdk_s3::Client;
use chrono::{DateTime, Utc};
use futures::Stream;
use pf_error::{PfError, Result};

/// Represents an S3 object discovered during listing.
#[derive(Debug, Clone)]
pub struct S3Object {
    /// The object key (full path within the bucket)
    pub key: String,

    /// Size of the object in bytes
    pub size: u64,

    /// Last modified timestamp
    pub last_modified: Option<DateTime<Utc>>,
}

/// List objects in an S3 bucket with optional prefix filtering.
///
/// Returns a stream of [`S3Object`] items, handling pagination automatically.
/// Directory markers (keys ending with `/`) are filtered out.
///
/// # Arguments
///
/// * `client` - The S3 client to use
/// * `bucket` - The bucket name to list
/// * `prefix` - Optional prefix to filter objects
///
/// # Example
///
/// ```ignore
/// use futures::StreamExt;
///
/// let stream = list_objects(&client, "my-bucket", Some("data/"));
/// pin_mut!(stream);
///
/// while let Some(result) = stream.next().await {
///     let obj = result?;
///     println!("Found: {} ({} bytes)", obj.key, obj.size);
/// }
/// ```
pub fn list_objects<'a>(
    client: &'a Client,
    bucket: &'a str,
    prefix: Option<&'a str>,
) -> impl Stream<Item = Result<S3Object>> + 'a {
    let bucket = bucket.to_string();
    let prefix = prefix.map(|s| s.to_string());

    try_stream! {
        let mut continuation_token: Option<String> = None;

        loop {
            let mut req = client.list_objects_v2().bucket(&bucket);

            if let Some(ref prefix) = prefix {
                req = req.prefix(prefix);
            }

            if let Some(ref token) = continuation_token {
                req = req.continuation_token(token);
            }

            let resp = req.send().await.map_err(|e| {
                PfError::Config(format!("S3 list objects failed: {e}"))
            })?;

            if let Some(contents) = resp.contents {
                for obj in contents {
                    let key = obj.key.unwrap_or_default();

                    // Skip directory markers
                    if key.ends_with('/') {
                        continue;
                    }

                    // Skip empty keys
                    if key.is_empty() {
                        continue;
                    }

                    let last_modified = obj.last_modified.and_then(|t| {
                        DateTime::from_timestamp(t.secs(), t.subsec_nanos())
                    });

                    yield S3Object {
                        key,
                        size: obj.size.unwrap_or(0) as u64,
                        last_modified,
                    };
                }
            }

            // Check if there are more results
            if resp.is_truncated == Some(true) {
                continuation_token = resp.next_continuation_token;
                if continuation_token.is_none() {
                    // No more pages
                    break;
                }
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_object_creation() {
        let obj = S3Object {
            key: "data/file.parquet".to_string(),
            size: 1024,
            last_modified: Some(Utc::now()),
        };

        assert_eq!(obj.key, "data/file.parquet");
        assert_eq!(obj.size, 1024);
        assert!(obj.last_modified.is_some());
    }

    #[test]
    fn test_s3_object_without_timestamp() {
        let obj = S3Object {
            key: "test.json".to_string(),
            size: 512,
            last_modified: None,
        };

        assert!(obj.last_modified.is_none());
    }
}
