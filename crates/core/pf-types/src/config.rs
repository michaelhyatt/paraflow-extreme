//! Configuration types for worker and pipeline settings.

use serde::{Deserialize, Serialize};

/// Worker configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    /// Worker identifier (defaults to hostname)
    #[serde(default = "default_worker_id")]
    pub id: String,

    /// Number of threads in the rayon pool
    #[serde(default = "default_threads")]
    pub threads: usize,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            id: default_worker_id(),
            threads: default_threads(),
        }
    }
}

fn default_worker_id() -> String {
    std::env::var("HOSTNAME").unwrap_or_else(|_| "worker-1".to_string())
}

fn default_threads() -> usize {
    std::thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(4)
}

/// Queue configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum QueueConfig {
    /// In-memory queue (for testing/development)
    Memory {
        /// Visibility timeout in seconds
        #[serde(default = "default_visibility_timeout")]
        visibility_timeout_secs: u64,
    },

    /// AWS SQS queue
    Sqs {
        /// SQS queue URL
        queue_url: String,

        /// Optional DLQ URL
        dlq_url: Option<String>,

        /// Visibility timeout in seconds
        #[serde(default = "default_visibility_timeout")]
        visibility_timeout_secs: u64,

        /// Long polling wait time in seconds
        #[serde(default = "default_wait_time")]
        wait_time_secs: u64,

        /// Custom endpoint (for LocalStack)
        endpoint: Option<String>,
    },
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self::Memory {
            visibility_timeout_secs: default_visibility_timeout(),
        }
    }
}

fn default_visibility_timeout() -> u64 {
    300 // 5 minutes
}

fn default_wait_time() -> u64 {
    20 // SQS long polling
}

/// Reader configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ReaderConfig {
    /// Parquet reader
    Parquet {
        /// Batch size (rows per batch)
        #[serde(default = "default_batch_size")]
        batch_size: usize,
    },

    /// NDJSON reader
    NdJson {
        /// Batch size (rows per batch)
        #[serde(default = "default_batch_size")]
        batch_size: usize,
    },
}

impl Default for ReaderConfig {
    fn default() -> Self {
        Self::Parquet {
            batch_size: default_batch_size(),
        }
    }
}

fn default_batch_size() -> usize {
    2000
}

/// Indexer configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum IndexerConfig {
    /// Stdout indexer (for testing/debugging)
    Stdout {
        /// Output format
        #[serde(default)]
        format: OutputFormat,

        /// Only count records, don't output
        #[serde(default)]
        count_only: bool,
    },

    /// Elasticsearch/OpenSearch indexer
    Elasticsearch {
        /// ES endpoint URL
        endpoint: String,

        /// Target index name
        index: String,

        /// Username for authentication
        username: Option<String>,

        /// Password for authentication
        password: Option<String>,

        /// Bulk size in MB
        #[serde(default = "default_bulk_size_mb")]
        bulk_size_mb: usize,
    },
}

impl Default for IndexerConfig {
    fn default() -> Self {
        Self::Stdout {
            format: OutputFormat::default(),
            count_only: false,
        }
    }
}

fn default_bulk_size_mb() -> usize {
    10
}

/// Output format for stdout indexer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutputFormat {
    /// Pretty-printed JSON
    #[default]
    Pretty,

    /// Single-line compact JSON
    Compact,

    /// Just count, no output
    Count,
}

/// Metrics configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    /// Enable metrics endpoint
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Port for metrics HTTP server
    #[serde(default = "default_metrics_port")]
    pub port: u16,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: default_metrics_port(),
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_metrics_port() -> u16 {
    9090
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_config_defaults() {
        let config = WorkerConfig::default();
        assert!(config.threads > 0);
    }

    #[test]
    fn test_queue_config_yaml() {
        let yaml = r#"
type: sqs
queue_url: http://localhost:4566/000000000000/test-queue
visibility_timeout_secs: 300
"#;
        let config: QueueConfig = serde_yaml::from_str(yaml).unwrap();
        match config {
            QueueConfig::Sqs { queue_url, .. } => {
                assert!(queue_url.contains("test-queue"));
            }
            _ => panic!("Expected SQS config"),
        }
    }

    #[test]
    fn test_memory_queue_default() {
        let config = QueueConfig::default();
        match config {
            QueueConfig::Memory {
                visibility_timeout_secs,
            } => {
                assert_eq!(visibility_timeout_secs, 300);
            }
            _ => panic!("Expected Memory config"),
        }
    }
}
