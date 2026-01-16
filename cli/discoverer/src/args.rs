//! CLI argument definitions for pf-discoverer.

use clap::{Parser, ValueEnum};

/// S3 file discovery for paraflow-extreme.
///
/// Discovers files from S3 storage and outputs file information for processing.
/// By default, outputs to stdout in JSONL format (one JSON object per line).
///
/// ## Examples
///
/// Basic usage:
///   pf-discoverer -b my-bucket --pattern "*.parquet"
///
/// With partitioning:
///   pf-discoverer -b my-bucket --partitioning "logs/${index}/${year}/" \
///       -f "index=nginx" -f "year=2024,2025"
///
/// With time-based partitioning:
///   pf-discoverer -b my-bucket \
///       --partitioning "data/YEAR=${_time:%Y}/MONTH=${_time:%m}/${element}/" \
///       -f "_time=2022-01-01..2022-01-05" \
///       -f "element=cpu,memory"
///
/// With size and date filters:
///   pf-discoverer -b my-bucket --min-size 1024 --modified-after 2024-01-01
#[derive(Parser, Debug)]
#[command(name = "pf-discoverer")]
#[command(version, about, long_about = None)]
pub struct Cli {
    // === S3 Configuration ===
    /// S3 bucket name
    #[arg(short, long, env = "PF_S3_BUCKET")]
    pub bucket: String,

    /// S3 prefix to filter objects
    #[arg(short, long, env = "PF_S3_PREFIX")]
    pub prefix: Option<String>,

    /// Custom S3 endpoint URL (for LocalStack)
    #[arg(long, env = "PF_S3_ENDPOINT")]
    pub s3_endpoint: Option<String>,

    /// AWS region
    #[arg(long, env = "AWS_REGION", default_value = "us-east-1")]
    pub region: String,

    /// AWS access key ID
    #[arg(long, env = "AWS_ACCESS_KEY_ID")]
    pub access_key: Option<String>,

    /// AWS secret access key
    #[arg(long, env = "AWS_SECRET_ACCESS_KEY")]
    pub secret_key: Option<String>,

    /// AWS profile name
    #[arg(long, env = "AWS_PROFILE")]
    pub profile: Option<String>,

    // === Discovery Options ===
    /// Glob pattern to filter files (e.g., "*.parquet")
    #[arg(long, default_value = "*")]
    pub pattern: String,

    /// Maximum number of files to output (0 = unlimited)
    #[arg(long, default_value = "0")]
    pub max_files: usize,

    // === Partitioning Options ===
    /// Partitioning expression (e.g., "logs/${index}/${year}/")
    ///
    /// Supports time format specifiers: ${_time:%Y}, ${_time:%m}, ${_time:%d}
    #[arg(long)]
    pub partitioning: Option<String>,

    /// Partition filter (can be specified multiple times)
    ///
    /// Supports two formats:
    /// - Value filter: "field=value1,value2" (e.g., "index=nginx,apache")
    /// - Date range: "_time=YYYY-MM-DD..YYYY-MM-DD" (e.g., "_time=2022-01-01..2022-01-05")
    ///
    /// Date ranges work with ${_time:FORMAT} specifiers in partitioning expressions.
    /// Prefixes are automatically deduplicated when partition granularity is coarser
    /// than the date range.
    #[arg(long = "filter", short = 'f')]
    pub filters: Vec<String>,

    // === Size Filter Options ===
    /// Minimum file size in bytes
    #[arg(long)]
    pub min_size: Option<u64>,

    /// Maximum file size in bytes
    #[arg(long)]
    pub max_size: Option<u64>,

    // === Date Filter Options ===
    /// Only include files modified after this date (ISO 8601, date only, or relative like -24h)
    #[arg(long)]
    pub modified_after: Option<String>,

    /// Only include files modified before this date
    #[arg(long)]
    pub modified_before: Option<String>,

    // === Parallelism Options ===
    /// Maximum concurrent S3 list operations (must be >= 1)
    #[arg(long, default_value = "10", value_parser = parse_positive_usize)]
    pub concurrency: usize,

    /// Maximum parallel prefix discoveries (must be >= 1)
    #[arg(long, default_value = "20", value_parser = parse_positive_usize)]
    pub parallel_prefixes: usize,

    // === Destination Options ===
    /// Output destination type
    #[arg(long, value_enum, default_value = "stdout")]
    pub destination: DestinationType,

    /// Output format for stdout destination
    #[arg(long, value_enum, default_value = "jsonl")]
    pub output_format: OutputFormatArg,

    /// SQS queue URL (required when destination=sqs)
    #[arg(long, env = "PF_SQS_QUEUE_URL")]
    pub sqs_queue_url: Option<String>,

    /// Custom SQS endpoint URL (for LocalStack)
    #[arg(long, env = "PF_SQS_ENDPOINT")]
    pub sqs_endpoint: Option<String>,

    /// SQS batch size (1-10)
    #[arg(long, default_value = "10", value_parser = parse_sqs_batch_size)]
    pub sqs_batch_size: usize,

    // === Logging Options ===
    /// Log level
    #[arg(long, value_enum, default_value = "info")]
    pub log_level: LogLevel,
}

/// Destination type.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum DestinationType {
    /// Output to stdout
    Stdout,
    /// Output to SQS queue
    Sqs,
}

/// Output format argument.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum OutputFormatArg {
    /// JSON Lines (one JSON object per line)
    Jsonl,
    /// Pretty-printed JSON
    Json,
}

impl From<OutputFormatArg> for pf_discoverer::OutputFormat {
    fn from(arg: OutputFormatArg) -> Self {
        match arg {
            OutputFormatArg::Jsonl => pf_discoverer::OutputFormat::Jsonl,
            OutputFormatArg::Json => pf_discoverer::OutputFormat::Json,
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

/// Parse SQS batch size (1-10).
fn parse_sqs_batch_size(s: &str) -> Result<usize, String> {
    let value: usize = s
        .parse()
        .map_err(|_| format!("'{}' is not a valid number", s))?;
    if !(1..=10).contains(&value) {
        return Err(format!("{} is not in 1..=10", value));
    }
    Ok(value)
}
