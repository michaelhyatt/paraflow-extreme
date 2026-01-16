//! CLI argument definitions for pf-worker.

use clap::{Parser, ValueEnum};
pub use pf_cli_common::LogLevel;

mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

/// Get the version string with build metadata.
fn version_string() -> &'static str {
    // Use Box::leak to create a 'static string at runtime
    // This is acceptable as it's only called once for --version
    let version = env!("CARGO_PKG_VERSION");
    let commit = built_info::GIT_COMMIT_HASH_SHORT.unwrap_or("unknown");
    let date = built_info::BUILT_TIME_UTC;
    let s = format!("{version} ({commit} {date})");
    Box::leak(s.into_boxed_str())
}

/// High-throughput data processing worker for paraflow-extreme.
///
/// Receives file locations from input sources (SQS or stdin), processes files
/// through a configurable pipeline, and writes to pluggable destination backends.
///
/// ## Examples
///
/// Pipe from discoverer to worker (local testing):
///   pf-discoverer -b my-bucket -P "*.parquet" | pf-worker -d stdout
///
/// Performance testing with stats destination:
///   pf-discoverer -b my-bucket | pf-worker -d stats -t 8
///
/// Production with SQS:
///   pf-worker -i sqs --sqs-queue-url https://sqs.us-east-1.amazonaws.com/123/queue
#[derive(Parser, Debug)]
#[command(name = "pf-worker")]
#[command(version = version_string(), about, long_about = None)]
pub struct Cli {
    // === Input Source ===
    /// Input source type
    #[arg(short = 'i', long, value_enum, default_value = "stdin")]
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

    /// SQS long-poll wait time in seconds (1-20)
    #[arg(long, default_value = "20", value_parser = parse_sqs_wait_time)]
    pub sqs_wait_time: i32,

    /// Drain mode: exit when queue is empty (for batch processing)
    #[arg(long)]
    pub sqs_drain: bool,

    // === Destination ===
    /// Output destination type
    #[arg(short = 'd', long, value_enum, default_value = "stdout")]
    pub destination: DestinationType,

    /// Output format for stdout destination
    #[arg(long, value_enum, default_value = "jsonl")]
    pub output_format: OutputFormat,

    // === Processing ===
    /// Number of processing threads (must be >= 1)
    #[arg(short = 't', long, default_value_t = num_cpus(), value_parser = parse_positive_usize)]
    pub threads: usize,

    /// Batch size for reading files (must be >= 1)
    #[arg(long, default_value = "10000", value_parser = parse_positive_usize)]
    pub batch_size: usize,

    /// Maximum retries before moving to DLQ
    #[arg(long, default_value = "3")]
    pub max_retries: u32,

    /// Channel buffer size for work distribution (must be >= 1)
    #[arg(long, default_value = "100", value_parser = parse_positive_usize)]
    pub channel_buffer: usize,

    /// Shutdown timeout in seconds (time to wait for workers to complete)
    #[arg(long, default_value = "30", value_parser = clap::value_parser!(u64).range(1..))]
    pub shutdown_timeout: u64,

    // === AWS Configuration ===
    /// AWS region
    #[arg(long, env = "AWS_REGION", default_value = "us-east-1")]
    pub region: String,

    /// Custom S3 endpoint URL (for LocalStack)
    #[arg(long, env = "PF_S3_ENDPOINT")]
    pub s3_endpoint: Option<String>,

    /// Disable IMDS (EC2 Instance Metadata Service) credential/region provider.
    ///
    /// Use this flag when running outside AWS (local development, non-EC2 environments)
    /// to avoid 1+ second timeout delays and warning messages while the SDK attempts
    /// to reach the IMDS endpoint at 169.254.169.254.
    #[arg(long, env = "AWS_EC2_METADATA_DISABLED")]
    pub no_imds: bool,

    /// AWS access key ID
    #[arg(long, env = "AWS_ACCESS_KEY_ID")]
    pub access_key: Option<String>,

    /// AWS secret access key
    #[arg(long, env = "AWS_SECRET_ACCESS_KEY")]
    pub secret_key: Option<String>,

    /// AWS profile name
    #[arg(long, env = "AWS_PROFILE")]
    pub profile: Option<String>,

    // === Progress Options ===
    /// Enable progress reporting to stderr
    #[arg(long)]
    pub progress: bool,

    /// Progress reporting interval in seconds
    #[arg(long, default_value = "5", value_parser = clap::value_parser!(u64).range(1..))]
    pub progress_interval: u64,

    // === Logging ===
    /// Log level
    #[arg(short = 'l', long, value_enum, default_value = "info")]
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


/// Get the number of available CPUs.
fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// Parse a positive usize (>= 1).
fn parse_positive_usize(s: &str) -> Result<usize, String> {
    let value: usize = s
        .parse()
        .map_err(|_| format!("'{}' is not a valid number", s))?;
    if value < 1 {
        return Err(format!("{} is not in 1..", value));
    }
    Ok(value)
}

/// Parse SQS wait time (1-20 seconds).
fn parse_sqs_wait_time(s: &str) -> Result<i32, String> {
    let value: i32 = s
        .parse()
        .map_err(|_| format!("'{}' is not a valid number", s))?;
    if !(1..=20).contains(&value) {
        return Err(format!("{} is not in 1..=20", value));
    }
    Ok(value)
}
