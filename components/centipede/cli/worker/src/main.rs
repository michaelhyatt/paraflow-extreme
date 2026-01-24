//! pf-worker CLI
//!
//! High-throughput data processing worker for paraflow-extreme.

use clap::Parser;

/// Use mimalloc as the global allocator for improved multi-threaded performance.
///
/// mimalloc provides better scaling and reduced contention compared to the
/// system allocator, which is particularly beneficial for data-intensive
/// workloads with many parallel threads.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
use pf_cli_common::{format_bytes, format_number};

mod args;
mod profiling;
mod progress;
mod run;

use args::Cli;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Cli::parse();

    // Disable IMDS if requested (must be set before any AWS SDK calls)
    // SAFETY: This is safe because we're setting the environment variable early in main(),
    // before spawning any threads, and the variable is only used by AWS SDK initialization.
    if args.no_imds {
        unsafe {
            std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
        }
    }

    // Initialize logging (to stderr, so stdout is clean for output)
    run::init_logging(args.log_level)?;

    // Start optional profiling collectors
    if let Some(ref path) = args.metrics_file {
        profiling::start_metrics_collector(path.to_str().unwrap());
    }

    if let Some(ref dir) = args.profile_dir {
        profiling::start_periodic_profiler(dir.to_str().unwrap(), args.profile_interval);
    }

    // Run worker
    let stats = run::execute(args).await?;

    // Report results to stderr
    eprintln!();
    eprintln!("Worker completed:");
    eprintln!(
        "  Files processed:    {}",
        format_number(stats.files_processed as u64)
    );
    eprintln!(
        "  Files failed:       {}",
        format_number(stats.files_failed as u64)
    );
    eprintln!(
        "  Records processed:  {}",
        format_number(stats.records_processed)
    );
    eprintln!(
        "  Records failed:     {}",
        format_number(stats.records_failed)
    );
    eprintln!("  Bytes read:         {}", format_bytes(stats.bytes_read));
    eprintln!(
        "  Bytes written:      {}",
        format_bytes(stats.bytes_written)
    );

    // Report cumulative processing time (actual work done) for accurate throughput
    let cumulative_secs = stats.cumulative_processing_time_secs();
    if cumulative_secs > 0.0 {
        eprintln!("  Active duration:    {:.2}s", cumulative_secs);

        if stats.files_processed > 0 {
            eprintln!(
                "  Throughput:         {:.1} files/sec",
                stats.files_processed as f64 / cumulative_secs
            );
        }

        if stats.records_processed > 0 {
            eprintln!(
                "                      {} records/sec",
                format_number((stats.records_processed as f64 / cumulative_secs) as u64)
            );
        }

        if stats.bytes_read > 0 {
            let throughput_mbps = (stats.bytes_read as f64 / 1_000_000.0) / cumulative_secs;
            eprintln!("                      {:.1} MB/s read", throughput_mbps);
        }
    }

    // Also report total duration for reference
    if let Some(duration) = stats.duration() {
        let total_secs = duration.num_milliseconds() as f64 / 1000.0;
        eprintln!(
            "  Total duration:     {:.2}s (includes startup/drain)",
            total_secs
        );
    }

    if stats.transient_errors > 0 || stats.permanent_errors > 0 {
        eprintln!(
            "  Errors:             {} transient, {} permanent",
            format_number(stats.transient_errors as u64),
            format_number(stats.permanent_errors as u64)
        );
    }

    // Exit with error code if there were failures
    if stats.files_failed > 0 || stats.permanent_errors > 0 {
        std::process::exit(4); // Partial failure
    }

    Ok(())
}
