//! Progress reporting for pf-discoverer.

use std::io::{self, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

/// Progress reporter for discovery operations.
pub struct ProgressReporter {
    /// Whether progress reporting is enabled
    enabled: bool,
    /// Reporting interval
    interval: Duration,
    /// Shared state for progress tracking
    state: Arc<ProgressState>,
    /// Handle to the background reporter task
    handle: Option<JoinHandle<()>>,
}

/// Shared state for progress tracking.
struct ProgressState {
    /// Number of files discovered
    files_discovered: AtomicUsize,
    /// Number of files output (passed filter)
    files_output: AtomicUsize,
    /// Total bytes discovered
    bytes_discovered: AtomicU64,
    /// Whether to stop reporting
    stop: AtomicBool,
    /// Start time
    start_time: Instant,
}

impl ProgressReporter {
    /// Create a new progress reporter.
    pub fn new(enabled: bool, interval_secs: u64) -> Self {
        Self {
            enabled,
            interval: Duration::from_secs(interval_secs),
            state: Arc::new(ProgressState {
                files_discovered: AtomicUsize::new(0),
                files_output: AtomicUsize::new(0),
                bytes_discovered: AtomicU64::new(0),
                stop: AtomicBool::new(false),
                start_time: Instant::now(),
            }),
            handle: None,
        }
    }

    /// Start the background progress reporter.
    pub fn start(&mut self) {
        if !self.enabled {
            return;
        }

        let state = Arc::clone(&self.state);
        let interval = self.interval;

        let handle = tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            interval_timer.tick().await; // Skip first immediate tick

            loop {
                interval_timer.tick().await;

                if state.stop.load(Ordering::Relaxed) {
                    break;
                }

                let files_discovered = state.files_discovered.load(Ordering::Relaxed);
                let files_output = state.files_output.load(Ordering::Relaxed);
                let bytes = state.bytes_discovered.load(Ordering::Relaxed);
                let elapsed = state.start_time.elapsed();

                let _ = writeln!(
                    io::stderr(),
                    "[Progress] {} files discovered, {} files output, {} found ({:.1}s elapsed)",
                    files_discovered,
                    files_output,
                    format_bytes(bytes),
                    elapsed.as_secs_f64()
                );
            }
        });

        self.handle = Some(handle);
    }

    /// Record a file that was output (passed filter).
    pub fn record_output(&self, size_bytes: u64) {
        if self.enabled {
            self.state.files_discovered.fetch_add(1, Ordering::Relaxed);
            self.state.files_output.fetch_add(1, Ordering::Relaxed);
            self.state
                .bytes_discovered
                .fetch_add(size_bytes, Ordering::Relaxed);
        }
    }

    /// Record a file that was filtered out.
    pub fn record_filtered(&self) {
        if self.enabled {
            self.state.files_discovered.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Stop the progress reporter and print final stats.
    pub async fn stop(mut self) {
        if !self.enabled {
            return;
        }

        self.state.stop.store(true, Ordering::Relaxed);

        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }

        // Print final progress
        let files_discovered = self.state.files_discovered.load(Ordering::Relaxed);
        let files_output = self.state.files_output.load(Ordering::Relaxed);
        let bytes = self.state.bytes_discovered.load(Ordering::Relaxed);
        let elapsed = self.state.start_time.elapsed();

        let _ = writeln!(
            io::stderr(),
            "[Progress] Complete: {} files discovered, {} files output, {} ({:.1}s)",
            files_discovered,
            files_output,
            format_bytes(bytes),
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
