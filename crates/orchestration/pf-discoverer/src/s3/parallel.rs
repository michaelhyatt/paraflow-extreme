//! Parallel S3 listing functionality.
//!
//! Provides concurrent listing of multiple S3 prefixes with controlled parallelism.

use aws_sdk_s3::Client;
use chrono::DateTime;
use futures::{Stream, StreamExt, stream};
use pf_error::{PfError, Result};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::debug;

use super::list::S3Object;
use super::retry::{RetryConfig, with_retry};

/// Configuration for parallel S3 operations.
#[derive(Debug, Clone)]
pub struct ParallelConfig {
    /// Maximum concurrent ListObjectsV2 operations.
    pub max_concurrent_lists: usize,
    /// Maximum parallel prefix discoveries.
    pub max_parallel_prefixes: usize,
    /// Continue on individual prefix errors (true) or fail fast (false).
    pub continue_on_error: bool,
    /// Request timeout in seconds.
    pub timeout_secs: u64,
    /// Retry configuration.
    pub retry: RetryConfig,
}

impl Default for ParallelConfig {
    fn default() -> Self {
        Self {
            max_concurrent_lists: 10,
            max_parallel_prefixes: 20,
            continue_on_error: true,
            timeout_secs: 300,
            retry: RetryConfig::default(),
        }
    }
}

impl ParallelConfig {
    /// Create a new parallel configuration with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the maximum concurrent list operations.
    pub fn with_max_concurrent_lists(mut self, max: usize) -> Self {
        self.max_concurrent_lists = max;
        self
    }

    /// Set the maximum parallel prefix discoveries.
    pub fn with_max_parallel_prefixes(mut self, max: usize) -> Self {
        self.max_parallel_prefixes = max;
        self
    }

    /// Set whether to continue on errors.
    pub fn with_continue_on_error(mut self, continue_on_error: bool) -> Self {
        self.continue_on_error = continue_on_error;
        self
    }

    /// Set the request timeout.
    pub fn with_timeout_secs(mut self, timeout_secs: u64) -> Self {
        self.timeout_secs = timeout_secs;
        self
    }

    /// Set the retry configuration.
    pub fn with_retry(mut self, retry: RetryConfig) -> Self {
        self.retry = retry;
        self
    }
}

/// Parallel S3 lister for efficient discovery across multiple prefixes.
pub struct ParallelLister {
    client: Client,
    bucket: String,
    config: ParallelConfig,
    semaphore: Arc<Semaphore>,
}

impl ParallelLister {
    /// Create a new parallel lister.
    pub fn new(client: Client, bucket: impl Into<String>, config: ParallelConfig) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent_lists));
        Self {
            client,
            bucket: bucket.into(),
            config,
            semaphore,
        }
    }

    /// List objects across multiple prefixes in parallel.
    ///
    /// Returns a stream of S3 objects discovered across all prefixes.
    /// The stream maintains backpressure through the semaphore.
    pub fn list_prefixes<'a>(
        &'a self,
        prefixes: Vec<String>,
    ) -> impl Stream<Item = Result<S3Object>> + 'a {
        let prefix_count = prefixes.len();
        debug!(prefix_count, "Starting parallel prefix listing");

        // Create a stream of prefix listing results
        let prefix_streams =
            prefixes.into_iter().map(|prefix| {
                let client = self.client.clone();
                let bucket = self.bucket.clone();
                let semaphore = self.semaphore.clone();
                let config = self.config.clone();

                async move {
                    list_prefix_with_semaphore(client, bucket, prefix, semaphore, config).await
                }
            });

        // Buffer the streams and flatten results
        stream::iter(prefix_streams)
            .buffer_unordered(self.config.max_parallel_prefixes)
            .flat_map(|result| match result {
                Ok(objects) => stream::iter(objects.into_iter().map(Ok)).boxed(),
                Err(e) => stream::iter(vec![Err(e)]).boxed(),
            })
    }

    /// Discover sub-prefixes under a given prefix.
    ///
    /// Uses ListObjectsV2 with delimiter="/" to find common prefixes.
    pub async fn discover_prefixes(&self, prefix: &str) -> Result<Vec<String>> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|e| PfError::Config(format!("Failed to acquire semaphore: {}", e)))?;

        debug!(prefix = prefix, "Discovering sub-prefixes");

        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let prefix_owned = prefix.to_string();
        let retry_config = self.config.retry.clone();

        with_retry(&retry_config, "discover_prefixes", || {
            let client = client.clone();
            let bucket = bucket.clone();
            let prefix = prefix_owned.clone();
            async move {
                let mut prefixes = Vec::new();
                let mut continuation_token: Option<String> = None;

                loop {
                    let mut req = client
                        .list_objects_v2()
                        .bucket(&bucket)
                        .prefix(&prefix)
                        .delimiter("/");

                    if let Some(ref token) = continuation_token {
                        req = req.continuation_token(token);
                    }

                    let resp = req.send().await.map_err(|e| {
                        PfError::Config(format!("S3 list with delimiter failed: {}", e))
                    })?;

                    // Extract common prefixes (sub-directories)
                    if let Some(common_prefixes) = resp.common_prefixes {
                        for cp in common_prefixes {
                            if let Some(p) = cp.prefix {
                                prefixes.push(p);
                            }
                        }
                    }

                    // Check for more results
                    if resp.is_truncated == Some(true) {
                        continuation_token = resp.next_continuation_token;
                        if continuation_token.is_none() {
                            break;
                        }
                    } else {
                        break;
                    }
                }

                debug!(
                    prefix = prefix,
                    discovered_count = prefixes.len(),
                    "Discovered sub-prefixes"
                );

                Ok(prefixes)
            }
        })
        .await
    }

    /// Get the bucket name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Get the configuration.
    pub fn config(&self) -> &ParallelConfig {
        &self.config
    }
}

/// List a single prefix with semaphore control.
async fn list_prefix_with_semaphore(
    client: Client,
    bucket: String,
    prefix: String,
    semaphore: Arc<Semaphore>,
    config: ParallelConfig,
) -> Result<Vec<S3Object>> {
    debug!(prefix = prefix, "Listing prefix");

    with_retry(&config.retry, "list_prefix", || {
        let client = client.clone();
        let bucket = bucket.clone();
        let prefix = prefix.clone();
        let semaphore = semaphore.clone();

        async move {
            let _permit = semaphore
                .acquire()
                .await
                .map_err(|e| PfError::Config(format!("Failed to acquire semaphore: {}", e)))?;

            let mut objects = Vec::new();
            let mut continuation_token: Option<String> = None;

            loop {
                let mut req = client.list_objects_v2().bucket(&bucket).prefix(&prefix);

                if let Some(ref token) = continuation_token {
                    req = req.continuation_token(token);
                }

                let resp = req
                    .send()
                    .await
                    .map_err(|e| PfError::Config(format!("S3 list objects failed: {}", e)))?;

                if let Some(contents) = resp.contents {
                    for obj in contents {
                        let key = obj.key.unwrap_or_default();

                        // Skip directory markers
                        if key.ends_with('/') || key.is_empty() {
                            continue;
                        }

                        let last_modified = obj
                            .last_modified
                            .and_then(|t| DateTime::from_timestamp(t.secs(), t.subsec_nanos()));

                        objects.push(S3Object {
                            key,
                            size: obj.size.unwrap_or(0) as u64,
                            last_modified,
                        });
                    }
                }

                // Check for more results
                if resp.is_truncated == Some(true) {
                    continuation_token = resp.next_continuation_token;
                    if continuation_token.is_none() {
                        break;
                    }
                } else {
                    break;
                }
            }

            debug!(
                prefix = prefix,
                object_count = objects.len(),
                "Listed prefix"
            );

            Ok(objects)
        }
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parallel_config_defaults() {
        let config = ParallelConfig::default();
        assert_eq!(config.max_concurrent_lists, 10);
        assert_eq!(config.max_parallel_prefixes, 20);
        assert!(config.continue_on_error);
        assert_eq!(config.timeout_secs, 300);
    }

    #[test]
    fn test_parallel_config_builder() {
        let config = ParallelConfig::new()
            .with_max_concurrent_lists(5)
            .with_max_parallel_prefixes(10)
            .with_continue_on_error(false)
            .with_timeout_secs(120);

        assert_eq!(config.max_concurrent_lists, 5);
        assert_eq!(config.max_parallel_prefixes, 10);
        assert!(!config.continue_on_error);
        assert_eq!(config.timeout_secs, 120);
    }
}
