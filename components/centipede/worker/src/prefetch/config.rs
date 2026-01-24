//! Prefetch configuration.

use serde::{Deserialize, Serialize};

/// Default max prefetch count per thread.
pub const DEFAULT_MAX_PREFETCH_COUNT: usize = 2;

/// Default max memory budget per thread in bytes (30MB).
pub const DEFAULT_MAX_MEMORY_BYTES: usize = 30 * 1024 * 1024;

/// Configuration for async I/O prefetching.
///
/// Prefetching allows overlapping S3 downloads with processing, improving
/// throughput when the workload is I/O-latency-bound.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrefetchConfig {
    /// Enable prefetching (default: true).
    ///
    /// When disabled, the worker falls back to sequential processing.
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Maximum files to prefetch per thread (default: 2).
    ///
    /// Higher values increase memory usage but can improve throughput
    /// by keeping more files ready for processing.
    #[serde(default = "default_max_prefetch_count")]
    pub max_prefetch_count: usize,

    /// Maximum memory budget per thread in bytes (default: 30MB).
    ///
    /// The prefetcher will not start new prefetches if it would exceed
    /// this memory limit.
    #[serde(default = "default_max_memory_bytes")]
    pub max_memory_bytes: usize,

    /// Prefetch file metadata for memory estimation (default: true).
    ///
    /// When enabled, the prefetcher issues HEAD requests to estimate
    /// file sizes before prefetching, allowing better memory management.
    #[serde(default = "default_prefetch_metadata")]
    pub prefetch_metadata: bool,
}

fn default_enabled() -> bool {
    true
}

fn default_max_prefetch_count() -> usize {
    DEFAULT_MAX_PREFETCH_COUNT
}

fn default_max_memory_bytes() -> usize {
    DEFAULT_MAX_MEMORY_BYTES
}

fn default_prefetch_metadata() -> bool {
    true
}

impl Default for PrefetchConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            max_prefetch_count: default_max_prefetch_count(),
            max_memory_bytes: default_max_memory_bytes(),
            prefetch_metadata: default_prefetch_metadata(),
        }
    }
}

impl PrefetchConfig {
    /// Create a new prefetch configuration with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Disable prefetching.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }

    /// Set the maximum prefetch count per thread.
    pub fn with_max_prefetch_count(mut self, count: usize) -> Self {
        self.max_prefetch_count = count;
        self
    }

    /// Set the maximum memory budget per thread.
    pub fn with_max_memory_bytes(mut self, bytes: usize) -> Self {
        self.max_memory_bytes = bytes;
        self
    }

    /// Set whether to prefetch file metadata.
    pub fn with_prefetch_metadata(mut self, prefetch: bool) -> Self {
        self.prefetch_metadata = prefetch;
        self
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), String> {
        if self.enabled && self.max_prefetch_count == 0 {
            return Err(
                "max_prefetch_count must be at least 1 when prefetching is enabled".to_string(),
            );
        }
        if self.enabled && self.max_memory_bytes == 0 {
            return Err(
                "max_memory_bytes must be at least 1 when prefetching is enabled".to_string(),
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefetch_config_defaults() {
        let config = PrefetchConfig::new();
        assert!(config.enabled);
        assert_eq!(config.max_prefetch_count, DEFAULT_MAX_PREFETCH_COUNT);
        assert_eq!(config.max_memory_bytes, DEFAULT_MAX_MEMORY_BYTES);
        assert!(config.prefetch_metadata);
    }

    #[test]
    fn test_prefetch_config_disabled() {
        let config = PrefetchConfig::disabled();
        assert!(!config.enabled);
    }

    #[test]
    fn test_prefetch_config_builder() {
        let config = PrefetchConfig::new()
            .with_max_prefetch_count(3)
            .with_max_memory_bytes(50 * 1024 * 1024)
            .with_prefetch_metadata(false);

        assert!(config.enabled);
        assert_eq!(config.max_prefetch_count, 3);
        assert_eq!(config.max_memory_bytes, 50 * 1024 * 1024);
        assert!(!config.prefetch_metadata);
    }

    #[test]
    fn test_prefetch_config_validation() {
        let config = PrefetchConfig::new();
        assert!(config.validate().is_ok());

        let invalid = PrefetchConfig {
            enabled: true,
            max_prefetch_count: 0,
            ..Default::default()
        };
        assert!(invalid.validate().is_err());

        let invalid = PrefetchConfig {
            enabled: true,
            max_memory_bytes: 0,
            ..Default::default()
        };
        assert!(invalid.validate().is_err());

        // Disabled config with zero values is OK
        let valid = PrefetchConfig {
            enabled: false,
            max_prefetch_count: 0,
            max_memory_bytes: 0,
            prefetch_metadata: false,
        };
        assert!(valid.validate().is_ok());
    }

    #[test]
    fn test_prefetch_config_serde() {
        let config = PrefetchConfig::new()
            .with_max_prefetch_count(4)
            .with_max_memory_bytes(40 * 1024 * 1024);

        let json = serde_json::to_string(&config).unwrap();
        let parsed: PrefetchConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.enabled, config.enabled);
        assert_eq!(parsed.max_prefetch_count, config.max_prefetch_count);
        assert_eq!(parsed.max_memory_bytes, config.max_memory_bytes);
        assert_eq!(parsed.prefetch_metadata, config.prefetch_metadata);
    }

    #[test]
    fn test_prefetch_config_serde_defaults() {
        // Empty JSON should use defaults
        let parsed: PrefetchConfig = serde_json::from_str("{}").unwrap();
        assert!(parsed.enabled);
        assert_eq!(parsed.max_prefetch_count, DEFAULT_MAX_PREFETCH_COUNT);
        assert_eq!(parsed.max_memory_bytes, DEFAULT_MAX_MEMORY_BYTES);
        assert!(parsed.prefetch_metadata);
    }
}
