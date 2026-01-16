//! Main execution logic for pf-discoverer CLI.

use anyhow::Result;
use pf_discoverer::{
    CompositeFilter, DateFilter, Discoverer, DiscoveryConfig, DiscoveryStats, Filter, Output,
    PatternFilter, S3Config, SizeFilter, SqsConfig, SqsOutput, StdoutOutput, create_s3_client,
};
use pf_discoverer::filter::parse_date;
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
    let mut s3_config = S3Config::new(&args.bucket)
        .with_region(&args.region)
        .with_concurrency(args.concurrency)
        .with_parallel_prefixes(args.parallel_prefixes);

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

    // Build composite filter
    let filter = build_filter(&args)?;

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

            let mut sqs_config = SqsConfig::new(sqs_queue_url)
                .with_region(&args.region)
                .with_batch_size(args.sqs_batch_size);

            if let Some(endpoint) = &args.sqs_endpoint {
                sqs_config = sqs_config.with_endpoint(endpoint);
            }

            let output = SqsOutput::new(sqs_config).await?;
            run_discovery(s3_client, &args, output, filter, config).await?
        }
    };

    Ok(stats)
}

/// Build the composite filter from CLI arguments.
fn build_filter(args: &Cli) -> Result<CompositeFilter> {
    let mut composite = CompositeFilter::new();

    // Add pattern filter
    let pattern_filter = PatternFilter::new(&args.pattern)
        .map_err(|e| anyhow::anyhow!("Invalid pattern: {}", e))?;
    composite.add_filter(Box::new(pattern_filter));

    // Add size filter if specified
    if args.min_size.is_some() || args.max_size.is_some() {
        let mut size_filter = SizeFilter::new();
        if let Some(min) = args.min_size {
            size_filter = size_filter.with_min_size(min);
        }
        if let Some(max) = args.max_size {
            size_filter = size_filter.with_max_size(max);
        }
        composite.add_filter(Box::new(size_filter));
    }

    // Add date filter if specified
    if args.modified_after.is_some() || args.modified_before.is_some() {
        let mut date_filter = DateFilter::new();
        if let Some(after) = &args.modified_after {
            let dt = parse_date(after)
                .map_err(|e| anyhow::anyhow!("Invalid --modified-after: {}", e))?;
            date_filter = date_filter.with_modified_after(dt);
        }
        if let Some(before) = &args.modified_before {
            let dt = parse_date(before)
                .map_err(|e| anyhow::anyhow!("Invalid --modified-before: {}", e))?;
            date_filter = date_filter.with_modified_before(dt);
        }
        composite.add_filter(Box::new(date_filter));
    }

    Ok(composite)
}

/// Run discovery with a specific output and filter type.
async fn run_discovery<O: Output, F: Filter>(
    s3_client: aws_sdk_s3::Client,
    args: &Cli,
    output: O,
    filter: F,
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
