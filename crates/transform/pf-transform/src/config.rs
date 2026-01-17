//! Configuration types for Rhai transforms.

use serde::{Deserialize, Serialize};

/// Configuration for a Rhai transform.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformConfig {
    /// Inline Rhai script (mutually exclusive with script_file).
    #[serde(default)]
    pub script: Option<String>,

    /// Path to Rhai script file (S3 URI or local path).
    #[serde(default)]
    pub script_file: Option<String>,

    /// Error handling policy.
    #[serde(default)]
    pub error_policy: ErrorPolicy,
}

impl TransformConfig {
    /// Creates a new config with an inline script.
    pub fn with_script(script: impl Into<String>) -> Self {
        Self {
            script: Some(script.into()),
            script_file: None,
            error_policy: ErrorPolicy::default(),
        }
    }

    /// Creates a new config with a script file path.
    pub fn with_script_file(path: impl Into<String>) -> Self {
        Self {
            script: None,
            script_file: Some(path.into()),
            error_policy: ErrorPolicy::default(),
        }
    }

    /// Sets the error policy.
    pub fn with_error_policy(mut self, policy: ErrorPolicy) -> Self {
        self.error_policy = policy;
        self
    }
}

impl Default for TransformConfig {
    fn default() -> Self {
        Self {
            script: None,
            script_file: None,
            error_policy: ErrorPolicy::default(),
        }
    }
}

/// Error handling policies for transform failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ErrorPolicy {
    /// Drop records that fail transformation, continue processing.
    #[default]
    Drop,

    /// Fail the entire batch if any record fails.
    Fail,

    /// Pass through records unchanged if transformation fails.
    Passthrough,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_policy_default() {
        assert_eq!(ErrorPolicy::default(), ErrorPolicy::Drop);
    }

    #[test]
    fn test_config_with_script() {
        let config = TransformConfig::with_script("record.x = 1; record");
        assert!(config.script.is_some());
        assert!(config.script_file.is_none());
    }

    #[test]
    fn test_config_with_script_file() {
        let config = TransformConfig::with_script_file("s3://bucket/transform.rhai");
        assert!(config.script.is_none());
        assert!(config.script_file.is_some());
    }

    #[test]
    fn test_config_serde() {
        let config = TransformConfig::with_script("record")
            .with_error_policy(ErrorPolicy::Fail);

        let json = serde_json::to_string(&config).unwrap();
        let parsed: TransformConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.script, config.script);
        assert_eq!(parsed.error_policy, ErrorPolicy::Fail);
    }
}
