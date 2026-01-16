//! Configuration types for the discoverer.

use serde::{Deserialize, Serialize};

/// Configuration for a discovery run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveryConfig {
    /// Maximum number of files to output (0 = unlimited)
    pub max_files: usize,
}

impl Default for DiscoveryConfig {
    fn default() -> Self {
        Self {
            max_files: 0,
        }
    }
}

impl DiscoveryConfig {
    /// Create a new discovery configuration with defaults.
    pub fn new() -> Self {
        Self::default()
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
            .with_max_files(100);

        assert_eq!(config.max_files, 100);
    }

    #[test]
    fn test_discovery_config_defaults() {
        let config = DiscoveryConfig::new();

        assert_eq!(config.max_files, 0);
    }
}
