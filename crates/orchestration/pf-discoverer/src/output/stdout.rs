//! Stdout output implementation for discovered files.

use async_trait::async_trait;
use pf_error::{PfError, Result};
use serde::{Deserialize, Serialize};
use std::io::Write;

use super::Output;
use crate::DiscoveredFile;

/// Output format for stdout.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutputFormat {
    /// JSON Lines format - one JSON object per line (default)
    #[default]
    Jsonl,

    /// Pretty-printed JSON
    Json,
}

/// Stdout output implementation.
///
/// Outputs discovered files to stdout in either JSON or JSONL format.
/// JSONL (JSON Lines) outputs one JSON object per line, suitable for piping
/// to tools like `jq` or counting with `wc -l`.
pub struct StdoutOutput {
    format: OutputFormat,
}

impl StdoutOutput {
    /// Create a new StdoutOutput with the specified format.
    pub fn new(format: OutputFormat) -> Self {
        Self { format }
    }

    /// Create a new StdoutOutput with JSONL format (default).
    pub fn jsonl() -> Self {
        Self::new(OutputFormat::Jsonl)
    }

    /// Create a new StdoutOutput with pretty-printed JSON format.
    pub fn json() -> Self {
        Self::new(OutputFormat::Json)
    }
}

impl Default for StdoutOutput {
    fn default() -> Self {
        Self::jsonl()
    }
}

#[async_trait]
impl Output for StdoutOutput {
    async fn output(&self, file: &DiscoveredFile) -> Result<()> {
        let output = match self.format {
            OutputFormat::Json => serde_json::to_string_pretty(file)
                .map_err(|e| PfError::Config(format!("JSON serialization failed: {e}")))?,
            OutputFormat::Jsonl => serde_json::to_string(file)
                .map_err(|e| PfError::Config(format!("JSON serialization failed: {e}")))?,
        };

        println!("{output}");
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        std::io::stdout()
            .flush()
            .map_err(|e| PfError::Config(format!("Failed to flush stdout: {e}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use pf_types::FileFormat;

    fn create_test_file() -> DiscoveredFile {
        DiscoveredFile {
            uri: "s3://bucket/file.parquet".to_string(),
            size_bytes: 1024,
            format: FileFormat::Parquet,
            last_modified: Some(Utc::now()),
        }
    }

    #[test]
    fn test_output_format_default() {
        assert_eq!(OutputFormat::default(), OutputFormat::Jsonl);
    }

    #[test]
    fn test_stdout_output_default() {
        let output = StdoutOutput::default();
        assert_eq!(output.format, OutputFormat::Jsonl);
    }

    #[test]
    fn test_stdout_output_constructors() {
        let jsonl = StdoutOutput::jsonl();
        assert_eq!(jsonl.format, OutputFormat::Jsonl);

        let json = StdoutOutput::json();
        assert_eq!(json.format, OutputFormat::Json);
    }

    #[tokio::test]
    async fn test_jsonl_serialization() {
        let file = create_test_file();
        let json = serde_json::to_string(&file).unwrap();

        // JSONL should be a single line
        assert!(!json.contains('\n'));

        // Should be valid JSON
        let parsed: DiscoveredFile = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.uri, "s3://bucket/file.parquet");
    }

    #[tokio::test]
    async fn test_json_serialization() {
        let file = create_test_file();
        let json = serde_json::to_string_pretty(&file).unwrap();

        // Pretty JSON should contain newlines
        assert!(json.contains('\n'));

        // Should be valid JSON
        let parsed: DiscoveredFile = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.uri, "s3://bucket/file.parquet");
    }
}
