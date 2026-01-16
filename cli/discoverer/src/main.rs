//! pf-discoverer CLI
//!
//! S3 file discovery for paraflow-extreme.

use clap::Parser;
use pf_cli_common::{format_bytes, format_number};

mod args;
mod progress;
mod run;

use args::Cli;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Cli::parse();
    let dry_run = args.dry_run;

    // Initialize logging (to stderr, so stdout is clean for discovered files)
    run::init_logging(args.log_level)?;

    // Run discoverer
    let stats = run::execute(args).await?;

    // Report results to stderr (skip if dry-run, as it prints its own summary)
    if !dry_run {
        eprintln!();
        eprintln!("Discovery completed:");
        eprintln!("  Files discovered:   {}", format_number(stats.files_discovered as u64));
        eprintln!("  Files filtered:     {}", format_number(stats.files_filtered as u64));
        eprintln!("  Files output:       {}", format_number(stats.files_output as u64));
        eprintln!("  Bytes discovered:   {}", format_bytes(stats.bytes_discovered));
        eprintln!("  Errors:             {}", format_number(stats.errors.len() as u64));

        if let Some(duration) = stats.duration() {
            let secs = duration.num_milliseconds() as f64 / 1000.0;
            eprintln!("  Duration:           {:.2}s", secs);

            if let Some(fps) = stats.files_per_second() {
                eprintln!("  Throughput:         {:.1} files/sec", fps);
            }
        }
    }

    if stats.has_errors() {
        for error in &stats.errors {
            eprintln!("  Error: {}", error);
        }
        std::process::exit(4); // Partial failure
    }

    Ok(())
}
