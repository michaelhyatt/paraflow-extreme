//! pf-discoverer CLI
//!
//! S3 file discovery for paraflow-extreme.

use clap::Parser;
use pf_cli_common::format_bytes;

mod args;
mod progress;
mod run;

use args::Cli;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Cli::parse();

    // Initialize logging (to stderr, so stdout is clean for discovered files)
    run::init_logging(args.log_level)?;

    // Run discoverer
    let stats = run::execute(args).await?;

    // Report results to stderr
    eprintln!();
    eprintln!("Discovery completed:");
    eprintln!("  Files discovered: {}", stats.files_discovered);
    eprintln!("  Files filtered:   {}", stats.files_filtered);
    eprintln!("  Files output:     {}", stats.files_output);
    eprintln!(
        "  Bytes discovered: {}",
        format_bytes(stats.bytes_discovered)
    );
    eprintln!("  Errors:           {}", stats.errors.len());

    if let Some(duration) = stats.duration() {
        eprintln!(
            "  Duration:         {:.2}s",
            duration.num_milliseconds() as f64 / 1000.0
        );

        if let Some(fps) = stats.files_per_second() {
            eprintln!("  Throughput:       {:.1} files/sec", fps);
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
