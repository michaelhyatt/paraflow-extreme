//! Main execution logic for pf-worker CLI.

use anyhow::Result;
use aws_config::BehaviorVersion;
use aws_credential_types::provider::ProvideCredentials;
use pf_traits::BatchIndexer;
use pf_worker::{
    FormatDispatchReader, PrefetchConfig, ReaderFactoryConfig, SqsSource, SqsSourceConfig,
    StatsDestination, StdinSource, StdoutDestination, Worker, WorkerConfig,
    destination::OutputFormat,
};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

use crate::args::{Cli, DestinationType, InputType};
use crate::progress::ProgressReporter;

pub use pf_cli_common::init_logging;

/// Resolve AWS credentials using the full AWS SDK credential chain.
/// This supports AWS_PROFILE, SSO, instance roles, and all other AWS credential sources.
async fn resolve_aws_credentials() -> Result<Option<(String, String, Option<String>)>> {
    let config = aws_config::defaults(BehaviorVersion::latest()).load().await;

    let credentials_provider = config.credentials_provider();
    if let Some(provider) = credentials_provider {
        match provider.provide_credentials().await {
            Ok(creds) => {
                info!("AWS credentials resolved successfully");
                Ok(Some((
                    creds.access_key_id().to_string(),
                    creds.secret_access_key().to_string(),
                    creds.session_token().map(|s| s.to_string()),
                )))
            }
            Err(e) => {
                // No credentials available - this is OK for public buckets
                info!("No AWS credentials available: {}", e);
                Ok(None)
            }
        }
    } else {
        Ok(None)
    }
}

/// Execute the worker with the provided arguments.
pub async fn execute(args: Cli) -> Result<pf_worker::stats::StatsSnapshot> {
    // Build prefetch configuration
    let prefetch_config = if args.prefetch_count == 0 {
        PrefetchConfig::disabled()
    } else {
        PrefetchConfig::new()
            .with_max_prefetch_count(args.prefetch_count)
            .with_max_memory_bytes(args.prefetch_memory_mb * 1024 * 1024)
    };

    // Build worker configuration
    let config = WorkerConfig::new()
        .with_thread_count(args.threads)
        .with_batch_size(args.batch_size)
        .with_max_retries(args.max_retries)
        .with_channel_buffer(args.channel_buffer)
        .with_shutdown_timeout(Duration::from_secs(args.shutdown_timeout))
        .with_region(&args.region)
        .with_prefetch(prefetch_config);

    let config = if let Some(ref endpoint) = args.s3_endpoint {
        config.with_s3_endpoint(endpoint)
    } else {
        config
    };

    // Validate configuration
    config.validate().map_err(|e| anyhow::anyhow!("{}", e))?;

    // Create destination
    let destination: Arc<dyn BatchIndexer> = match args.destination {
        DestinationType::Stdout => {
            let format: OutputFormat = args.output_format.into();
            Arc::new(StdoutDestination::new(format))
        }
        DestinationType::Stats => Arc::new(StatsDestination::new()),
    };

    // Resolve AWS credentials using the full AWS SDK credential chain
    // This handles AWS_PROFILE, SSO, instance roles, environment variables, etc.
    let credentials = resolve_aws_credentials().await?;

    // Create reader with format dispatch (supports both Parquet and NDJSON)
    let mut reader_config = ReaderFactoryConfig::new(&args.region);

    if let Some(ref endpoint) = args.s3_endpoint {
        reader_config = reader_config.with_endpoint(endpoint);
    }

    // Pass resolved credentials to the reader
    if let Some((access_key, secret_key, session_token)) = credentials {
        reader_config = reader_config.with_credentials(access_key, secret_key, session_token);
    }

    let reader = FormatDispatchReader::new(reader_config).await?;

    // Execute based on input type
    let stats = match args.input {
        InputType::Stdin => {
            let source = StdinSource::new();
            let worker = Worker::new(config, source, reader, destination);

            // Start progress reporter if enabled
            let mut progress = ProgressReporter::new(args.progress, args.progress_interval);
            progress.start(Arc::clone(worker.stats()));

            let result = worker.run().await;

            // Stop progress reporter
            progress.stop(worker.stats()).await;

            result?
        }
        InputType::Sqs => {
            let queue_url = args
                .sqs_queue_url
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("--sqs-queue-url is required when input=sqs"))?;

            let sqs_config = SqsSourceConfig::new(queue_url)
                .with_visibility_timeout(args.sqs_visibility_timeout)
                .with_wait_time(args.sqs_wait_time)
                .with_drain_mode(args.sqs_drain);

            let source = if let Some(ref endpoint) = args.sqs_endpoint {
                SqsSource::from_config_with_endpoint(sqs_config, endpoint, &args.region).await?
            } else {
                SqsSource::from_config(sqs_config).await?
            };

            let worker = Worker::new(config, source, reader, destination);

            // Start progress reporter if enabled
            let mut progress = ProgressReporter::new(args.progress, args.progress_interval);
            progress.start(Arc::clone(worker.stats()));

            let result = worker.run().await;

            // Stop progress reporter
            progress.stop(worker.stats()).await;

            result?
        }
    };

    Ok(stats)
}
