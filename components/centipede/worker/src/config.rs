//! Configuration types for the worker.

use crate::prefetch::PrefetchConfig;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Default accumulator threshold for Elasticsearch (10MB).
pub const DEFAULT_ACCUMULATOR_THRESHOLD_BYTES: usize = 10 * 1024 * 1024;

/// Configuration for a worker instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    /// Number of processing threads
    pub thread_count: usize,

    /// Batch size for reading files
    pub batch_size: usize,

    /// Channel buffer size for work distribution
    pub channel_buffer: usize,

    /// Maximum retries before DLQ
    pub max_retries: u32,

    /// Shutdown timeout
    #[serde(with = "humantime_serde")]
    pub shutdown_timeout: Duration,

    /// AWS region for S3/SQS
    pub region: String,

    /// Custom S3 endpoint URL (for LocalStack)
    pub s3_endpoint: Option<String>,

    /// Batch accumulator threshold in bytes.
    ///
    /// Batches are accumulated until this threshold is exceeded, then flushed
    /// to the indexer. This improves Elasticsearch bulk API performance.
    /// Default: 10MB (optimal for Elasticsearch)
    pub accumulator_threshold_bytes: usize,

    /// Optional record count threshold for the accumulator.
    ///
    /// If set, flush is triggered when either byte or record threshold is exceeded.
    pub accumulator_threshold_records: Option<usize>,

    /// Prefetch configuration for async I/O.
    ///
    /// When enabled, the worker will prefetch files from S3 while processing
    /// other files, overlapping I/O latency with processing.
    #[serde(default)]
    pub prefetch: PrefetchConfig,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        let thread_count = num_cpus();
        Self {
            thread_count,
            batch_size: 10_000,
            // Adaptive channel buffer: 10 items per thread for optimal throughput
            channel_buffer: thread_count * 10,
            max_retries: 3,
            shutdown_timeout: Duration::from_secs(30),
            region: "us-east-1".to_string(),
            s3_endpoint: None,
            accumulator_threshold_bytes: DEFAULT_ACCUMULATOR_THRESHOLD_BYTES,
            accumulator_threshold_records: None,
            prefetch: PrefetchConfig::default(),
        }
    }
}

impl WorkerConfig {
    /// Create a new worker configuration with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the number of processing threads.
    pub fn with_thread_count(mut self, count: usize) -> Self {
        self.thread_count = count;
        self
    }

    /// Set the batch size for reading files.
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Set the channel buffer size.
    pub fn with_channel_buffer(mut self, size: usize) -> Self {
        self.channel_buffer = size;
        self
    }

    /// Set the maximum retries before DLQ.
    pub fn with_max_retries(mut self, retries: u32) -> Self {
        self.max_retries = retries;
        self
    }

    /// Set the shutdown timeout.
    pub fn with_shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    /// Set the AWS region.
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = region.into();
        self
    }

    /// Set a custom S3 endpoint URL.
    pub fn with_s3_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.s3_endpoint = Some(endpoint.into());
        self
    }

    /// Set the accumulator threshold in bytes.
    ///
    /// Batches are accumulated until this threshold is exceeded.
    /// Default: 10MB (optimal for Elasticsearch)
    pub fn with_accumulator_threshold_bytes(mut self, threshold: usize) -> Self {
        self.accumulator_threshold_bytes = threshold;
        self
    }

    /// Set the accumulator record count threshold.
    ///
    /// If set, flush is triggered when either byte or record threshold is exceeded.
    pub fn with_accumulator_threshold_records(mut self, threshold: usize) -> Self {
        self.accumulator_threshold_records = Some(threshold);
        self
    }

    /// Set the prefetch configuration.
    ///
    /// Prefetching overlaps S3 downloads with processing, improving throughput.
    pub fn with_prefetch(mut self, prefetch: PrefetchConfig) -> Self {
        self.prefetch = prefetch;
        self
    }

    /// Disable prefetching.
    ///
    /// This is a convenience method equivalent to
    /// `.with_prefetch(PrefetchConfig::disabled())`.
    pub fn with_prefetch_disabled(mut self) -> Self {
        self.prefetch = PrefetchConfig::disabled();
        self
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), String> {
        if self.thread_count == 0 {
            return Err("thread_count must be at least 1".to_string());
        }
        if self.batch_size == 0 {
            return Err("batch_size must be at least 1".to_string());
        }
        if self.channel_buffer == 0 {
            return Err("channel_buffer must be at least 1".to_string());
        }
        if self.accumulator_threshold_bytes == 0 {
            return Err("accumulator_threshold_bytes must be at least 1".to_string());
        }
        self.prefetch.validate()?;
        Ok(())
    }
}

/// Get the number of available CPUs.
fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// Serde helper for Duration serialization.
mod humantime_serde {
    use serde::{self, Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(duration.as_secs())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs = u64::deserialize(deserializer)?;
        Ok(Duration::from_secs(secs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_config_defaults() {
        let config = WorkerConfig::new();

        assert!(config.thread_count >= 1);
        assert_eq!(config.batch_size, 10_000);
        // Channel buffer is adaptive: thread_count * 10
        assert_eq!(config.channel_buffer, config.thread_count * 10);
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.shutdown_timeout, Duration::from_secs(30));
        assert_eq!(
            config.accumulator_threshold_bytes,
            DEFAULT_ACCUMULATOR_THRESHOLD_BYTES
        );
        assert!(config.accumulator_threshold_records.is_none());
        // Prefetch is enabled by default
        assert!(config.prefetch.enabled);
    }

    #[test]
    fn test_worker_config_builder() {
        let config = WorkerConfig::new()
            .with_thread_count(8)
            .with_batch_size(5000)
            .with_channel_buffer(50)
            .with_max_retries(5)
            .with_region("eu-west-1")
            .with_s3_endpoint("http://localhost:4566")
            .with_accumulator_threshold_bytes(15 * 1024 * 1024)
            .with_accumulator_threshold_records(100_000);

        assert_eq!(config.thread_count, 8);
        assert_eq!(config.batch_size, 5000);
        assert_eq!(config.channel_buffer, 50);
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.region, "eu-west-1");
        assert_eq!(
            config.s3_endpoint,
            Some("http://localhost:4566".to_string())
        );
        assert_eq!(config.accumulator_threshold_bytes, 15 * 1024 * 1024);
        assert_eq!(config.accumulator_threshold_records, Some(100_000));
    }

    #[test]
    fn test_worker_config_validation() {
        let config = WorkerConfig::new();
        assert!(config.validate().is_ok());

        let invalid = WorkerConfig::new().with_thread_count(0);
        assert!(invalid.validate().is_err());

        let invalid = WorkerConfig::new().with_batch_size(0);
        assert!(invalid.validate().is_err());

        let invalid = WorkerConfig::new().with_channel_buffer(0);
        assert!(invalid.validate().is_err());

        let invalid = WorkerConfig::new().with_accumulator_threshold_bytes(0);
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn test_worker_config_prefetch() {
        // Test with_prefetch_disabled
        let config = WorkerConfig::new().with_prefetch_disabled();
        assert!(!config.prefetch.enabled);
        assert!(config.validate().is_ok());

        // Test with_prefetch
        let custom_prefetch = PrefetchConfig::new()
            .with_max_prefetch_count(4)
            .with_max_memory_bytes(50 * 1024 * 1024);
        let config = WorkerConfig::new().with_prefetch(custom_prefetch);
        assert!(config.prefetch.enabled);
        assert_eq!(config.prefetch.max_prefetch_count, 4);
        assert_eq!(config.prefetch.max_memory_bytes, 50 * 1024 * 1024);
    }

    #[test]
    fn test_worker_config_prefetch_validation() {
        // Invalid prefetch config should cause validation to fail
        let mut config = WorkerConfig::new();
        config.prefetch.enabled = true;
        config.prefetch.max_prefetch_count = 0;
        assert!(config.validate().is_err());
    }
}
