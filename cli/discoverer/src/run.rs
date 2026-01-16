//! Main execution logic for pf-discoverer CLI.

use anyhow::Result;
use futures::{StreamExt, pin_mut};
use pf_discoverer::{
    CompositeFilter, DateFilter, DiscoveredFile, DiscoveryConfig, DiscoveryStats, Filter, Output,
    ParallelConfig, ParallelLister, PatternFilter, S3Config, SizeFilter, SqsConfig, SqsOutput,
    StdoutOutput, create_s3_client,
};
use pf_discoverer::filter::parse_date;
use pf_discoverer::partition::{PartitionFilters, PartitioningExpression, expand_all_prefixes};
use tracing::{debug, info, warn};

use crate::args::{Cli, DestinationType};
use crate::progress::ProgressReporter;

pub use pf_cli_common::init_logging;

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
        .with_max_files(args.max_files);

    // Build composite filter
    let filter = build_filter(&args)?;

    // Build partition configuration (prefixes to scan)
    let prefixes = build_partition_config(&args)?;

    // Execute based on destination type
    let stats = match args.destination {
        DestinationType::Stdout => {
            let output = StdoutOutput::new(args.output_format.into());
            run_discovery_with_prefixes(s3_client, &args, output, filter, config, prefixes).await?
        }
        DestinationType::Sqs => {
            let sqs_queue_url = args
                .sqs_queue_url
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("--sqs-queue-url is required when destination=sqs"))?;

            let mut sqs_config = SqsConfig::new(sqs_queue_url)
                .with_region(&args.region)
                .with_batch_size(args.sqs_batch_size);

            if let Some(endpoint) = &args.sqs_endpoint {
                sqs_config = sqs_config.with_endpoint(endpoint);
            }

            let output = SqsOutput::new(sqs_config).await?;
            run_discovery_with_prefixes(s3_client, &args, output, filter, config, prefixes).await?
        }
    };

    Ok(stats)
}

/// Build partition configuration and return the list of S3 prefixes to scan.
fn build_partition_config(args: &Cli) -> Result<Vec<String>> {
    let mut partition_filters = PartitionFilters::new();

    // Parse all filter arguments (handles both value filters and time range filters)
    for filter_str in &args.filters {
        partition_filters
            .parse_and_add(filter_str)
            .map_err(|e| anyhow::anyhow!("Invalid filter '{}': {}", filter_str, e))?;
    }

    // If no partitioning expression, use simple prefix
    let expr_str = match &args.partitioning {
        Some(e) => e,
        None => {
            // No partitioning - use single prefix (or empty)
            let prefix = args.prefix.clone().unwrap_or_default();
            return Ok(vec![prefix]);
        }
    };

    // Parse the partitioning expression
    let expression = PartitioningExpression::parse(expr_str)
        .map_err(|e| anyhow::anyhow!("Invalid partitioning expression: {}", e))?;

    // Expand to all prefixes
    let prefixes = expand_all_prefixes(&expression, &partition_filters)
        .map_err(|e| anyhow::anyhow!("Failed to expand partitioning: {}", e))?;

    info!(
        prefix_count = prefixes.len(),
        partitioning = expr_str,
        "Generated S3 prefixes from partitioning expression"
    );

    if prefixes.len() <= 10 {
        debug!(prefixes = ?prefixes, "Prefixes to scan");
    } else {
        debug!(
            first_10 = ?&prefixes[..10],
            total = prefixes.len(),
            "Prefixes to scan (showing first 10)"
        );
    }

    Ok(prefixes)
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

/// Run discovery with multiple prefixes using ParallelLister.
async fn run_discovery_with_prefixes<O: Output, F: Filter>(
    s3_client: aws_sdk_s3::Client,
    args: &Cli,
    output: O,
    filter: F,
    config: DiscoveryConfig,
    prefixes: Vec<String>,
) -> Result<DiscoveryStats> {
    let parallel_config = ParallelConfig::new()
        .with_max_concurrent_lists(args.concurrency)
        .with_max_parallel_prefixes(args.parallel_prefixes);

    let lister = ParallelLister::new(s3_client, &args.bucket, parallel_config);

    let mut stats = DiscoveryStats::new();
    let max_files = config.max_files;

    // Start progress reporter if enabled
    let mut progress = ProgressReporter::new(args.progress, args.progress_interval);
    progress.start();

    debug!(
        bucket = %args.bucket,
        prefix_count = prefixes.len(),
        "Starting multi-prefix discovery"
    );

    let stream = lister.list_prefixes(prefixes);
    pin_mut!(stream);

    while let Some(result) = stream.next().await {
        // Check max files limit
        if max_files > 0 && stats.files_output >= max_files {
            debug!(max_files = max_files, "Reached max files limit");
            break;
        }

        match result {
            Ok(obj) => {
                if filter.matches(&obj) {
                    let discovered_file = DiscoveredFile {
                        uri: format!("s3://{}/{}", args.bucket, obj.key),
                        size_bytes: obj.size,
                        last_modified: obj.last_modified,
                    };

                    if let Err(e) = output.output(&discovered_file).await {
                        warn!(key = %obj.key, error = %e, "Failed to output file");
                        stats.record_error(format!("Output failed for {}: {}", obj.key, e));
                        continue;
                    }

                    stats.record_output(obj.size);
                    progress.record_output(obj.size);
                    debug!(key = %obj.key, size = obj.size, "Discovered file");
                } else {
                    stats.record_filtered();
                    progress.record_filtered();
                }
            }
            Err(e) => {
                warn!(error = %e, "Error listing S3 objects");
                stats.record_error(format!("S3 listing error: {}", e));
            }
        }
    }

    // Stop progress reporter
    progress.stop().await;

    // Flush any buffered output
    output.flush().await?;

    info!(
        files_output = stats.files_output,
        files_filtered = stats.files_filtered,
        bytes_discovered = stats.bytes_discovered,
        "Discovery complete"
    );

    Ok(stats)
}
