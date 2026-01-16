//! Main execution logic for pf-worker CLI.

use anyhow::Result;
use pf_worker::{
    FormatDispatchReader, ReaderFactoryConfig, SqsSource, SqsSourceConfig, StatsDestination,
    StdinSource, StdoutDestination, Worker, WorkerConfig,
    destination::OutputFormat,
};
use pf_traits::BatchIndexer;
use std::sync::Arc;
use tracing::Level;
use tracing_subscriber::fmt;

use crate::args::{Cli, DestinationType, InputType, LogLevel};

/// Initialize logging.
pub fn init_logging(level: LogLevel) -> Result<()> {
    let level: Level = level.into();

    let subscriber = fmt::Subscriber::builder()
        .with_max_level(level)
        .with_writer(std::io::stderr); // Log to stderr so stdout is clean for output

    subscriber.init();

    Ok(())
}

/// Execute the worker with the provided arguments.
pub async fn execute(args: Cli) -> Result<pf_worker::stats::StatsSnapshot> {
    // Build worker configuration
    let config = WorkerConfig::new()
        .with_thread_count(args.threads)
        .with_batch_size(args.batch_size)
        .with_max_retries(args.max_retries)
        .with_channel_buffer(args.channel_buffer)
        .with_region(&args.region);

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

    // Create reader with format dispatch (supports both Parquet and NDJSON)
    let mut reader_config = ReaderFactoryConfig::new(&args.region);
    if let Some(ref endpoint) = args.s3_endpoint {
        reader_config = reader_config.with_endpoint(endpoint);
    }
    let reader = FormatDispatchReader::new(reader_config).await?;

    // Execute based on input type
    let stats = match args.input {
        InputType::Stdin => {
            let source = StdinSource::new();
            let worker = Worker::new(config, source, reader, destination);
            worker.run().await?
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
            worker.run().await?
        }
    };

    Ok(stats)
}
