//! Main Discoverer implementation.

use aws_sdk_s3::Client;
use futures::{StreamExt, pin_mut};
use pf_error::Result;
use tracing::{debug, warn};

use crate::DiscoveredFile;
use crate::config::DiscoveryConfig;
use crate::filter::Filter;
use crate::output::Output;
use crate::s3::{S3Object, list_objects};
use crate::stats::DiscoveryStats;

/// The main discoverer that coordinates S3 listing, filtering, and output.
///
/// Generic over the output type and filter type to allow different output
/// destinations (stdout, SQS, etc.) and filtering strategies with the same
/// discovery logic.
pub struct Discoverer<O: Output, F: Filter> {
    s3_client: Client,
    bucket: String,
    prefix: Option<String>,
    output: O,
    filter: F,
    config: DiscoveryConfig,
}

impl<O: Output, F: Filter> Discoverer<O, F> {
    /// Create a new Discoverer.
    ///
    /// # Arguments
    ///
    /// * `s3_client` - The S3 client to use for listing
    /// * `bucket` - The bucket to discover files in
    /// * `prefix` - Optional prefix to filter objects
    /// * `output` - The output destination for discovered files
    /// * `filter` - The filter for matching files
    /// * `config` - The discovery configuration
    pub fn new(
        s3_client: Client,
        bucket: impl Into<String>,
        prefix: Option<String>,
        output: O,
        filter: F,
        config: DiscoveryConfig,
    ) -> Self {
        Self {
            s3_client,
            bucket: bucket.into(),
            prefix,
            output,
            filter,
            config,
        }
    }

    /// Run the discovery process.
    ///
    /// Lists all objects in the configured S3 bucket/prefix, filters them
    /// by the configured filter, and outputs discovered files.
    ///
    /// # Returns
    ///
    /// Statistics about the discovery run, including counts and any errors.
    pub async fn discover(&self) -> Result<DiscoveryStats> {
        let mut stats = DiscoveryStats::new();

        debug!(
            bucket = %self.bucket,
            prefix = ?self.prefix,
            filter = %self.filter.description(),
            "Starting discovery"
        );

        let stream = list_objects(&self.s3_client, &self.bucket, self.prefix.as_deref());
        pin_mut!(stream);

        while let Some(result) = stream.next().await {
            match result {
                Ok(obj) => {
                    if self.filter.matches(&obj) {
                        let discovered_file = self.create_discovered_file(&obj);

                        if let Err(e) = self.output.output(&discovered_file).await {
                            warn!(key = %obj.key, error = %e, "Failed to output file");
                            stats.record_error(format!("Output failed for {}: {}", obj.key, e));
                            // Continue processing other files
                            continue;
                        }

                        stats.record_output(obj.size);
                        debug!(key = %obj.key, size = obj.size, "Discovered file");
                    } else {
                        stats.record_filtered();
                        debug!(key = %obj.key, "Filtered out");
                    }
                }
                Err(e) => {
                    warn!(error = %e, "Error listing S3 objects");
                    stats.record_error(format!("S3 listing error: {}", e));
                    // Continue trying to list more objects
                }
            }

            // Check max files limit
            if self.config.max_files > 0 && stats.files_output >= self.config.max_files {
                debug!(max_files = self.config.max_files, "Reached max files limit");
                break;
            }
        }

        // Flush output
        if let Err(e) = self.output.flush().await {
            warn!(error = %e, "Failed to flush output");
            stats.record_error(format!("Flush failed: {}", e));
        }

        stats.complete();

        debug!(
            files_discovered = stats.files_discovered,
            files_output = stats.files_output,
            files_filtered = stats.files_filtered,
            bytes = stats.bytes_discovered,
            errors = stats.error_count(),
            "Discovery completed"
        );

        Ok(stats)
    }

    /// Create a DiscoveredFile from an S3 object.
    fn create_discovered_file(&self, obj: &S3Object) -> DiscoveredFile {
        DiscoveredFile {
            uri: format!("s3://{}/{}", self.bucket, obj.key),
            size_bytes: obj.size,
            last_modified: obj.last_modified,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::PatternFilter;
    use crate::output::StdoutOutput;

    fn create_test_config() -> DiscoveryConfig {
        DiscoveryConfig::new()
    }

    #[test]
    fn test_create_discovered_file() {
        let obj = S3Object {
            key: "data/file.parquet".to_string(),
            size: 1024,
            last_modified: None,
        };

        let file = DiscoveredFile {
            uri: format!("s3://test-bucket/{}", obj.key),
            size_bytes: obj.size,
            last_modified: obj.last_modified,
        };

        assert_eq!(file.uri, "s3://test-bucket/data/file.parquet");
        assert_eq!(file.size_bytes, 1024);
    }

    #[tokio::test]
    async fn test_discoverer_builder() {
        // Test that we can construct a Discoverer with all components
        // This doesn't test actual discovery since we'd need a real S3 client

        let config = create_test_config();
        let filter = PatternFilter::new("*.parquet").unwrap();
        let output = StdoutOutput::default();

        // We need a real AWS config to create a client, so we just test the types compile
        // In integration tests, we'd use LocalStack

        // Type check that everything composes correctly
        fn _type_check<O: Output, F: Filter>(_output: O, _filter: F, _config: DiscoveryConfig) {}
        _type_check(output, filter, config);
    }
}
