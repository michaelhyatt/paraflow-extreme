//! Progress reporting for pf-worker.

use pf_worker::stats::WorkerStats;
use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

/// Progress reporter for worker operations.
pub struct ProgressReporter {
    /// Whether progress reporting is enabled
    enabled: bool,
    /// Reporting interval
    interval: Duration,
    /// Shared stop flag
    stop: Arc<AtomicBool>,
    /// Handle to the background reporter task
    handle: Option<JoinHandle<()>>,
    /// Start time
    start_time: Instant,
}

impl ProgressReporter {
    /// Create a new progress reporter.
    pub fn new(enabled: bool, interval_secs: u64) -> Self {
        Self {
            enabled,
            interval: Duration::from_secs(interval_secs),
            stop: Arc::new(AtomicBool::new(false)),
            handle: None,
            start_time: Instant::now(),
        }
    }

    /// Start the background progress reporter.
    pub fn start(&mut self, stats: Arc<WorkerStats>) {
        if !self.enabled {
            return;
        }

        let stop = Arc::clone(&self.stop);
        let interval = self.interval;
        let start_time = self.start_time;

        let handle = tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            interval_timer.tick().await; // Skip first immediate tick

            loop {
                interval_timer.tick().await;

                if stop.load(Ordering::Relaxed) {
                    break;
                }

                let files_processed = stats.files_processed();
                let records_processed = stats.records_processed();
                let bytes_read = stats.bytes_read();
                let elapsed = start_time.elapsed();

                let _ = writeln!(
                    io::stderr(),
                    "[Progress] {} files processed, {} records, {} read ({:.1}s elapsed)",
                    files_processed,
                    format_number(records_processed),
                    format_bytes(bytes_read),
                    elapsed.as_secs_f64()
                );
            }
        });

        self.handle = Some(handle);
    }

    /// Stop the progress reporter.
    pub async fn stop(mut self, stats: &WorkerStats) {
        if !self.enabled {
            return;
        }

        self.stop.store(true, Ordering::Relaxed);

        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }

        // Print final progress
        let files_processed = stats.files_processed();
        let records_processed = stats.records_processed();
        let bytes_read = stats.bytes_read();
        let elapsed = self.start_time.elapsed();

        let _ = writeln!(
            io::stderr(),
            "[Progress] Complete: {} files processed, {} records, {} read ({:.1}s)",
            files_processed,
            format_number(records_processed),
            format_bytes(bytes_read),
            elapsed.as_secs_f64()
        );
    }
}

/// Format bytes as a human-readable string.
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;

    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Format a number with comma separators.
fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    let chars: Vec<char> = s.chars().collect();

    for (i, c) in chars.iter().enumerate() {
        if i > 0 && (chars.len() - i) % 3 == 0 {
            result.push(',');
        }
        result.push(*c);
    }

    result
}
