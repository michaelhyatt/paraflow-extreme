//! pf-discoverer CLI
//!
//! S3 file discovery for paraflow-extreme.

use clap::Parser;

mod args;
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

/// Format bytes as human-readable string.
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}
