//! Configuration types for the discoverer.

use pf_types::FileFormat;
use serde::{Deserialize, Serialize};

/// Configuration for a discovery run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryConfig {
    /// File format of discovered files
    pub file_format: FileFormat,

    /// Maximum number of files to output (0 = unlimited)
    pub max_files: usize,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            file_format: FileFormat::Parquet,
            max_files: 0,
        }
    }
}

impl DiscoveryConfig {
    /// Create a new discovery configuration with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the file format.
    pub fn with_format(mut self, format: FileFormat) -> Self {
        self.file_format = format;
        self
    }

    /// Set the maximum number of files to output.
    pub fn with_max_files(mut self, max_files: usize) -> Self {
        self.max_files = max_files;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_discovery_config_builder() {
        let config = DiscoveryConfig::new()
            .with_format(FileFormat::NdJson)
            .with_max_files(100);

        assert_eq!(config.file_format, FileFormat::NdJson);
        assert_eq!(config.max_files, 100);
    }

    #[test]
    fn test_discovery_config_defaults() {
        let config = DiscoveryConfig::new();

        assert_eq!(config.file_format, FileFormat::Parquet);
        assert_eq!(config.max_files, 0);
    }
}
