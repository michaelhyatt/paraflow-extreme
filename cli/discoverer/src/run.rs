//! Main execution logic for pf-discoverer CLI.

use anyhow::Result;
use pf_discoverer::{
    Discoverer, DiscoveryConfig, DiscoveryStats, Output, PatternFilter, S3Config, SqsConfig,
    SqsOutput, StdoutOutput, create_s3_client,
};
use tracing::Level;
use tracing_subscriber::fmt;

use crate::args::{Cli, LogLevel, OutputType};

/// Initialize logging.
pub fn init_logging(level: LogLevel) -> Result<()> {
    let level: Level = level.into();

    let subscriber = fmt::Subscriber::builder()
        .with_max_level(level)
        .with_writer(std::io::stderr); // Log to stderr so stdout is clean for output

    subscriber.init();

    Ok(())
}

/// Execute the discoverer with the provided arguments.
pub async fn execute(args: Cli) -> Result<DiscoveryStats> {
    // Build S3 configuration
    let mut s3_config = S3Config::new(&args.bucket).with_region(&args.region);

    if let Some(prefix) = &args.prefix {
        s3_config = s3_config.with_prefix(prefix);
    }

    if let Some(endpoint) = &args.s3_endpoint {
        s3_config = s3_config.with_endpoint(endpoint);
    }

    if let (Some(access_key), Some(secret_key)) = (&args.access_key, &args.secret_key) {
        s3_config = s3_config.with_credentials(access_key, secret_key);
    }

    if let Some(profile) = &args.profile {
        s3_config = s3_config.with_profile(profile);
    }

    // Create S3 client
    let s3_client = create_s3_client(&s3_config).await?;

    // Build discovery configuration
    let config = DiscoveryConfig::new()
        .with_format(args.format.into())
        .with_max_files(args.max_files);

    // Build pattern filter
    let filter = PatternFilter::new(&args.pattern)?;

    // Execute based on output type
    let stats = match args.output {
        OutputType::Stdout => {
            let output = StdoutOutput::new(args.output_format.into());
            run_discovery(s3_client, &args, output, filter, config).await?
        }
        OutputType::Sqs => {
            let sqs_queue_url = args
                .sqs_queue_url
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("--sqs-queue-url is required when output=sqs"))?;

            let mut sqs_config = SqsConfig::new(sqs_queue_url).with_region(&args.region);

            if let Some(endpoint) = &args.sqs_endpoint {
                sqs_config = sqs_config.with_endpoint(endpoint);
            }

            let output = SqsOutput::new(sqs_config).await?;
            run_discovery(s3_client, &args, output, filter, config).await?
        }
    };

    Ok(stats)
}

/// Run discovery with a specific output type.
async fn run_discovery<O: Output>(
    s3_client: aws_sdk_s3::Client,
    args: &Cli,
    output: O,
    filter: PatternFilter,
    config: DiscoveryConfig,
) -> Result<DiscoveryStats> {
    let discoverer = Discoverer::new(
        s3_client,
        &args.bucket,
        args.prefix.clone(),
        output,
        filter,
        config,
    );

    let stats = discoverer.discover().await?;
    Ok(stats)
}
