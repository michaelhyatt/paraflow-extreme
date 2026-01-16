//! CLI argument definitions for pf-worker.

use clap::{Parser, ValueEnum};

/// High-throughput data processing worker for paraflow-extreme.
///
/// Receives file locations from input sources (SQS or stdin), processes files
/// through a configurable pipeline, and writes to pluggable destination backends.
///
/// ## Examples
///
/// Pipe from discoverer to worker (local testing):
///   pf-discoverer -b my-bucket --pattern "*.parquet" | pf-worker --destination stdout
///
/// Performance testing with stats destination:
///   pf-discoverer -b my-bucket | pf-worker --destination stats --threads 8
///
/// Production with SQS:
///   pf-worker --input sqs --sqs-queue-url https://sqs.us-east-1.amazonaws.com/123/queue
#[derive(Parser, Debug)]
#[command(name = "pf-worker")]
#[command(version, about, long_about = None)]
pub struct Cli {
    // === Input Source ===
    /// Input source type
    #[arg(long, value_enum, default_value = "stdin")]
    pub input: InputType,

    /// SQS queue URL (required when input=sqs)
    #[arg(long, env = "PF_SQS_QUEUE_URL")]
    pub sqs_queue_url: Option<String>,

    /// Custom SQS endpoint URL (for LocalStack)
    #[arg(long, env = "PF_SQS_ENDPOINT")]
    pub sqs_endpoint: Option<String>,

    /// SQS visibility timeout in seconds
    #[arg(long, default_value = "300")]
    pub sqs_visibility_timeout: i32,

    // === Destination ===
    /// Output destination type
    #[arg(long, value_enum, default_value = "stdout")]
    pub destination: DestinationType,

    /// Output format for stdout destination
    #[arg(long, value_enum, default_value = "jsonl")]
    pub output_format: OutputFormat,

    // === Processing ===
    /// Number of processing threads
    #[arg(long, default_value_t = num_cpus())]
    pub threads: usize,

    /// Batch size for reading files
    #[arg(long, default_value = "10000")]
    pub batch_size: usize,

    /// Maximum retries before moving to DLQ
    #[arg(long, default_value = "3")]
    pub max_retries: u32,

    /// Channel buffer size for work distribution
    #[arg(long, default_value = "100")]
    pub channel_buffer: usize,

    // === S3 Configuration ===
    /// AWS region
    #[arg(long, env = "AWS_REGION", default_value = "us-east-1")]
    pub region: String,

    /// Custom S3 endpoint URL (for LocalStack)
    #[arg(long, env = "PF_S3_ENDPOINT")]
    pub s3_endpoint: Option<String>,

    // === Logging ===
    /// Log level
    #[arg(long, value_enum, default_value = "info")]
    pub log_level: LogLevel,
}

/// Input source type.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum InputType {
    /// Read JSONL work items from stdin
    Stdin,
    /// Receive work items from SQS queue
    Sqs,
}

/// Destination type.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum DestinationType {
    /// Output to stdout as JSON/JSONL
    Stdout,
    /// Count records without output (for performance testing)
    Stats,
}

/// Output format for stdout destination.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum OutputFormat {
    /// JSON Lines (one JSON object per line)
    Jsonl,
    /// Pretty-printed JSON
    Json,
}

impl From<OutputFormat> for pf_worker::destination::OutputFormat {
    fn from(arg: OutputFormat) -> Self {
        match arg {
            OutputFormat::Jsonl => pf_worker::destination::OutputFormat::Jsonl,
            OutputFormat::Json => pf_worker::destination::OutputFormat::Json,
        }
    }
}

/// Log level argument.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum LogLevel {
    /// Trace level (most verbose)
    Trace,
    /// Debug level
    Debug,
    /// Info level (default)
    Info,
    /// Warning level
    Warn,
    /// Error level (least verbose)
    Error,
}

impl From<LogLevel> for tracing::Level {
    fn from(level: LogLevel) -> Self {
        match level {
            LogLevel::Trace => tracing::Level::TRACE,
            LogLevel::Debug => tracing::Level::DEBUG,
            LogLevel::Info => tracing::Level::INFO,
            LogLevel::Warn => tracing::Level::WARN,
            LogLevel::Error => tracing::Level::ERROR,
        }
    }
}

/// Get the number of available CPUs.
fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}
