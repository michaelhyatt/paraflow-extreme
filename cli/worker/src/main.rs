//! pf-worker CLI
//!
//! High-throughput data processing worker for paraflow-extreme.

use clap::Parser;

mod args;
mod progress;
mod run;

use args::Cli;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Cli::parse();

    // Initialize logging (to stderr, so stdout is clean for output)
    run::init_logging(args.log_level)?;

    // Run worker
    let stats = run::execute(args).await?;

    // Report results to stderr
    eprintln!();
    eprintln!("Worker completed:");
    eprintln!("  Files processed: {}", stats.files_processed);
    eprintln!("  Files failed:    {}", stats.files_failed);
    eprintln!(
        "  Records processed: {}",
        format_number(stats.records_processed)
    );
    eprintln!(
        "  Records failed:    {}",
        format_number(stats.records_failed)
    );
    eprintln!("  Bytes read:      {}", format_bytes(stats.bytes_read));
    eprintln!("  Bytes written:   {}", format_bytes(stats.bytes_written));

    if let Some(duration) = stats.duration() {
        let secs = duration.num_milliseconds() as f64 / 1000.0;
        eprintln!("  Duration:        {:.2}s", secs);

        if secs > 0.0 && stats.files_processed > 0 {
            eprintln!(
                "  Throughput:      {:.1} files/sec",
                stats.files_processed as f64 / secs
            );
        }

        if secs > 0.0 && stats.records_processed > 0 {
            eprintln!(
                "                   {} records/sec",
                format_number((stats.records_processed as f64 / secs) as u64)
            );
        }

        if secs > 0.0 && stats.bytes_read > 0 {
            let throughput_mbps = (stats.bytes_read as f64 / 1_000_000.0) / secs;
            eprintln!("                   {:.1} MB/s read", throughput_mbps);
        }
    }

    if stats.transient_errors > 0 || stats.permanent_errors > 0 {
        eprintln!(
            "  Errors:          {} transient, {} permanent",
            stats.transient_errors, stats.permanent_errors
        );
    }

    // Exit with error code if there were failures
    if stats.files_failed > 0 || stats.permanent_errors > 0 {
        std::process::exit(4); // Partial failure
    }

    Ok(())
}

/// Format a large number with commas.
fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    let mut count = 0;

    for c in s.chars().rev() {
        if count > 0 && count % 3 == 0 {
            result.push(',');
        }
        result.push(c);
        count += 1;
    }

    result.chars().rev().collect()
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
