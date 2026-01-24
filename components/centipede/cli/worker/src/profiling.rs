//! Optional profiling support for pf-worker
//!
//! Provides tokio runtime metrics collection and CPU flamegraph generation.

use std::fs::OpenOptions;
use std::io::Write;
use std::time::Duration;

/// Start collecting tokio runtime metrics to a JSONL file.
///
/// Metrics are written every second and include:
/// - Worker thread counts and live tasks count
/// - Park counts (thread parking events)
/// - Busy duration statistics (total, min, max)
/// - Global queue depth and elapsed time
///
/// When built with `RUSTFLAGS="--cfg tokio_unstable"`, additional metrics:
/// - Steal counts and operations (work stealing between workers)
/// - Poll counts and mean poll duration
/// - Local queue depths and schedule counts
/// - Remote schedules and overflow counts
/// - Budget forced yield count
/// - I/O driver ready count
pub fn start_metrics_collector(metrics_file: &str) {
    use tokio_metrics::RuntimeMonitor;

    let handle = tokio::runtime::Handle::current();
    let runtime_monitor = RuntimeMonitor::new(&handle);
    let path = metrics_file.to_string();

    tokio::spawn(async move {
        let mut file = match OpenOptions::new().create(true).append(true).open(&path) {
            Ok(f) => f,
            Err(e) => {
                tracing::error!("Failed to open metrics file {}: {}", path, e);
                return;
            }
        };

        for interval in runtime_monitor.intervals() {
            #[allow(unused_mut)]
            let mut metrics = serde_json::json!({
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "workers_count": interval.workers_count,
                "live_tasks_count": interval.live_tasks_count,
                "total_park_count": interval.total_park_count,
                "total_busy_duration_us": interval.total_busy_duration.as_micros(),
                "max_busy_duration_us": interval.max_busy_duration.as_micros(),
                "min_busy_duration_us": interval.min_busy_duration.as_micros(),
                "global_queue_depth": interval.global_queue_depth,
                "elapsed_us": interval.elapsed.as_micros(),
            });

            // Add unstable metrics when built with tokio_unstable
            #[cfg(tokio_unstable)]
            {
                let obj = metrics.as_object_mut().unwrap();
                obj.insert(
                    "total_steal_count".to_string(),
                    interval.total_steal_count.into(),
                );
                obj.insert(
                    "total_steal_operations".to_string(),
                    interval.total_steal_operations.into(),
                );
                obj.insert(
                    "total_polls_count".to_string(),
                    interval.total_polls_count.into(),
                );
                obj.insert(
                    "total_local_queue_depth".to_string(),
                    interval.total_local_queue_depth.into(),
                );
                obj.insert(
                    "total_local_schedule_count".to_string(),
                    interval.total_local_schedule_count.into(),
                );
                obj.insert(
                    "total_overflow_count".to_string(),
                    interval.total_overflow_count.into(),
                );
                obj.insert(
                    "num_remote_schedules".to_string(),
                    interval.num_remote_schedules.into(),
                );
                obj.insert(
                    "budget_forced_yield_count".to_string(),
                    interval.budget_forced_yield_count.into(),
                );
                obj.insert(
                    "mean_poll_duration_ns".to_string(),
                    (interval.mean_poll_duration.as_nanos() as u64).into(),
                );
                obj.insert(
                    "io_driver_ready_count".to_string(),
                    interval.io_driver_ready_count.into(),
                );
            }

            if let Err(e) = writeln!(file, "{}", metrics) {
                tracing::warn!("Failed to write metrics: {}", e);
            }
            let _ = file.flush();

            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });

    tracing::info!("Started tokio metrics collector -> {}", metrics_file);
}

/// Start periodic CPU profiling, saving flamegraphs to directory.
///
/// Each profile captures 10 seconds of CPU activity and saves as SVG.
#[cfg(feature = "profiling")]
pub fn start_periodic_profiler(profile_dir: &str, interval_secs: u64) {
    use pprof::ProfilerGuard;
    use std::fs::File;

    let dir = profile_dir.to_string();
    if let Err(e) = std::fs::create_dir_all(&dir) {
        tracing::error!("Failed to create profile directory {}: {}", dir, e);
        return;
    }

    tokio::spawn(async move {
        let mut snapshot_num = 0;

        loop {
            // Profile for 10 seconds at 100 Hz
            let guard = match ProfilerGuard::new(100) {
                Ok(g) => g,
                Err(e) => {
                    tracing::warn!("Failed to start profiler: {}", e);
                    tokio::time::sleep(Duration::from_secs(interval_secs)).await;
                    continue;
                }
            };

            tokio::time::sleep(Duration::from_secs(10)).await;

            // Generate and save flamegraph
            if let Ok(report) = guard.report().build() {
                let path = format!("{}/profile_{:03}.svg", dir, snapshot_num);
                match File::create(&path) {
                    Ok(file) => {
                        if report.flamegraph(file).is_ok() {
                            tracing::info!("Saved flamegraph to {}", path);
                            snapshot_num += 1;
                        }
                    }
                    Err(e) => tracing::warn!("Failed to create {}: {}", path, e),
                }
            }

            // Wait for remaining interval
            let remaining = interval_secs.saturating_sub(10);
            if remaining > 0 {
                tokio::time::sleep(Duration::from_secs(remaining)).await;
            }
        }
    });

    tracing::info!(
        "Started periodic CPU profiler -> {} (interval: {}s)",
        profile_dir,
        interval_secs
    );
}

#[cfg(not(feature = "profiling"))]
pub fn start_periodic_profiler(_profile_dir: &str, _interval_secs: u64) {
    tracing::warn!(
        "CPU profiling requested but pprof feature not enabled. \
         Rebuild with: cargo build --features profiling"
    );
}
