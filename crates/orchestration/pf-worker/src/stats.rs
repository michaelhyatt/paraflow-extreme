//! Statistics for worker runs.

use chrono::{DateTime, Duration, Utc};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

/// Statistics collected during a worker run.
///
/// Uses atomic counters for thread-safe updates from multiple processing threads.
#[derive(Debug, Default)]
pub struct WorkerStats {
    /// When processing started
    started_at: Option<DateTime<Utc>>,

    /// When processing completed
    completed_at: Option<DateTime<Utc>>,

    /// When the first file started processing (for active duration calculation)
    first_file_at: Mutex<Option<DateTime<Utc>>>,

    /// When the last file finished processing (for active duration calculation)
    last_file_at: Mutex<Option<DateTime<Utc>>>,

    /// Total number of files processed successfully
    files_processed: AtomicU64,

    /// Number of files that failed
    files_failed: AtomicU64,

    /// Total number of records processed
    records_processed: AtomicU64,

    /// Number of records that failed
    records_failed: AtomicU64,

    /// Total bytes read from source files
    bytes_read: AtomicU64,

    /// Total bytes written to destination
    bytes_written: AtomicU64,

    /// Number of batches processed
    batches_processed: AtomicU64,

    /// Number of transient errors encountered
    transient_errors: AtomicU64,

    /// Number of permanent errors encountered
    permanent_errors: AtomicU64,
}

impl WorkerStats {
    /// Create a new stats tracker with the current time as start time.
    pub fn new() -> Self {
        Self {
            started_at: Some(Utc::now()),
            ..Default::default()
        }
    }

    /// Mark processing as complete with the current time.
    pub fn complete(&mut self) {
        self.completed_at = Some(Utc::now());
    }

    /// Record a successfully processed file.
    pub fn record_file_success(&self, records: u64, bytes_read: u64, bytes_written: u64) {
        let now = Utc::now();

        // Track first file timestamp (only set once)
        {
            let mut first = self.first_file_at.lock();
            if first.is_none() {
                *first = Some(now);
            }
        }

        // Always update last file timestamp
        *self.last_file_at.lock() = Some(now);

        self.files_processed.fetch_add(1, Ordering::Relaxed);
        self.records_processed.fetch_add(records, Ordering::Relaxed);
        self.bytes_read.fetch_add(bytes_read, Ordering::Relaxed);
        self.bytes_written
            .fetch_add(bytes_written, Ordering::Relaxed);
    }

    /// Record a failed file.
    pub fn record_file_failure(&self) {
        self.files_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record processed records.
    pub fn record_records(&self, count: u64) {
        self.records_processed.fetch_add(count, Ordering::Relaxed);
    }

    /// Record failed records.
    pub fn record_failed_records(&self, count: u64) {
        self.records_failed.fetch_add(count, Ordering::Relaxed);
    }

    /// Record bytes read.
    pub fn record_bytes_read(&self, bytes: u64) {
        self.bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record bytes written.
    pub fn record_bytes_written(&self, bytes: u64) {
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record a processed batch.
    pub fn record_batch(&self) {
        self.batches_processed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a transient error.
    pub fn record_transient_error(&self) {
        self.transient_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a permanent error.
    pub fn record_permanent_error(&self) {
        self.permanent_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the total duration of the worker run (includes startup and drain time).
    pub fn duration(&self) -> Option<Duration> {
        match (self.started_at, self.completed_at) {
            (Some(start), Some(end)) => Some(end - start),
            (Some(start), None) => Some(Utc::now() - start),
            _ => None,
        }
    }

    /// Get the active processing duration (first file to last file).
    ///
    /// This excludes startup overhead (SQS connection, waiting for first message)
    /// and drain time (waiting for visibility timeout). Use this for accurate
    /// throughput calculations.
    pub fn active_duration(&self) -> Option<Duration> {
        let first = *self.first_file_at.lock();
        let last = *self.last_file_at.lock();
        match (first, last) {
            (Some(f), Some(l)) => Some(l - f),
            _ => None,
        }
    }

    /// Get the first file processing timestamp.
    pub fn first_file_at(&self) -> Option<DateTime<Utc>> {
        *self.first_file_at.lock()
    }

    /// Get the last file processing timestamp.
    pub fn last_file_at(&self) -> Option<DateTime<Utc>> {
        *self.last_file_at.lock()
    }

    /// Get the number of files processed.
    pub fn files_processed(&self) -> u64 {
        self.files_processed.load(Ordering::Relaxed)
    }

    /// Get the number of files failed.
    pub fn files_failed(&self) -> u64 {
        self.files_failed.load(Ordering::Relaxed)
    }

    /// Get the number of records processed.
    pub fn records_processed(&self) -> u64 {
        self.records_processed.load(Ordering::Relaxed)
    }

    /// Get the number of records failed.
    pub fn records_failed(&self) -> u64 {
        self.records_failed.load(Ordering::Relaxed)
    }

    /// Get the number of bytes read.
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read.load(Ordering::Relaxed)
    }

    /// Get the number of bytes written.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    /// Get the number of batches processed.
    pub fn batches_processed(&self) -> u64 {
        self.batches_processed.load(Ordering::Relaxed)
    }

    /// Get the number of transient errors.
    pub fn transient_errors(&self) -> u64 {
        self.transient_errors.load(Ordering::Relaxed)
    }

    /// Get the number of permanent errors.
    pub fn permanent_errors(&self) -> u64 {
        self.permanent_errors.load(Ordering::Relaxed)
    }

    /// Calculate the throughput in files per second using active processing duration.
    pub fn files_per_second(&self) -> Option<f64> {
        self.active_duration().map(|d| {
            let secs = d.num_milliseconds() as f64 / 1000.0;
            if secs > 0.0 {
                self.files_processed() as f64 / secs
            } else {
                0.0
            }
        })
    }

    /// Calculate the throughput in records per second using active processing duration.
    pub fn records_per_second(&self) -> Option<f64> {
        self.active_duration().map(|d| {
            let secs = d.num_milliseconds() as f64 / 1000.0;
            if secs > 0.0 {
                self.records_processed() as f64 / secs
            } else {
                0.0
            }
        })
    }

    /// Calculate the throughput in MB per second (read) using active processing duration.
    pub fn read_throughput_mbps(&self) -> Option<f64> {
        self.active_duration().map(|d| {
            let secs = d.num_milliseconds() as f64 / 1000.0;
            if secs > 0.0 {
                (self.bytes_read() as f64 / 1_000_000.0) / secs
            } else {
                0.0
            }
        })
    }

    /// Calculate the throughput in MB per second (written) using active processing duration.
    pub fn write_throughput_mbps(&self) -> Option<f64> {
        self.active_duration().map(|d| {
            let secs = d.num_milliseconds() as f64 / 1000.0;
            if secs > 0.0 {
                (self.bytes_written() as f64 / 1_000_000.0) / secs
            } else {
                0.0
            }
        })
    }

    /// Create a snapshot of the current statistics.
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            started_at: self.started_at,
            completed_at: self.completed_at,
            first_file_at: self.first_file_at(),
            last_file_at: self.last_file_at(),
            files_processed: self.files_processed(),
            files_failed: self.files_failed(),
            records_processed: self.records_processed(),
            records_failed: self.records_failed(),
            bytes_read: self.bytes_read(),
            bytes_written: self.bytes_written(),
            batches_processed: self.batches_processed(),
            transient_errors: self.transient_errors(),
            permanent_errors: self.permanent_errors(),
        }
    }
}

/// A serializable snapshot of worker statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsSnapshot {
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    /// When the first file started processing
    pub first_file_at: Option<DateTime<Utc>>,
    /// When the last file finished processing
    pub last_file_at: Option<DateTime<Utc>>,
    pub files_processed: u64,
    pub files_failed: u64,
    pub records_processed: u64,
    pub records_failed: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub batches_processed: u64,
    pub transient_errors: u64,
    pub permanent_errors: u64,
}

impl StatsSnapshot {
    /// Get the total duration of the worker run (includes startup and drain time).
    pub fn duration(&self) -> Option<Duration> {
        match (self.started_at, self.completed_at) {
            (Some(start), Some(end)) => Some(end - start),
            _ => None,
        }
    }

    /// Get the active processing duration (first file to last file).
    ///
    /// This excludes startup overhead and drain time. Use this for accurate
    /// throughput calculations.
    pub fn active_duration(&self) -> Option<Duration> {
        match (self.first_file_at, self.last_file_at) {
            (Some(first), Some(last)) => Some(last - first),
            _ => None,
        }
    }

    /// Get the active processing duration in seconds.
    pub fn active_duration_secs(&self) -> Option<f64> {
        self.active_duration()
            .map(|d| d.num_milliseconds() as f64 / 1000.0)
    }

    /// Calculate the throughput in MB per second (read) using active duration.
    pub fn read_throughput_mbps(&self) -> Option<f64> {
        self.active_duration().map(|d| {
            let secs = d.num_milliseconds() as f64 / 1000.0;
            if secs > 0.0 {
                (self.bytes_read as f64 / 1_000_000.0) / secs
            } else {
                0.0
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration as StdDuration;

    #[test]
    fn test_stats_new() {
        let stats = WorkerStats::new();
        assert!(stats.started_at.is_some());
        assert!(stats.completed_at.is_none());
        assert_eq!(stats.files_processed(), 0);
    }

    #[test]
    fn test_stats_record_file_success() {
        let stats = WorkerStats::new();
        stats.record_file_success(1000, 1024, 2048);
        stats.record_file_success(2000, 2048, 4096);

        assert_eq!(stats.files_processed(), 2);
        assert_eq!(stats.records_processed(), 3000);
        assert_eq!(stats.bytes_read(), 3072);
        assert_eq!(stats.bytes_written(), 6144);
    }

    #[test]
    fn test_stats_record_failure() {
        let stats = WorkerStats::new();
        stats.record_file_failure();
        stats.record_file_failure();

        assert_eq!(stats.files_failed(), 2);
    }

    #[test]
    fn test_stats_duration() {
        let mut stats = WorkerStats::new();
        sleep(StdDuration::from_millis(10));
        stats.complete();

        let duration = stats.duration().unwrap();
        assert!(duration.num_milliseconds() >= 10);
    }

    #[test]
    fn test_stats_snapshot() {
        let stats = WorkerStats::new();
        stats.record_file_success(100, 1024, 2048);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.files_processed, 1);
        assert_eq!(snapshot.records_processed, 100);
    }

    #[test]
    fn test_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let stats = Arc::new(WorkerStats::new());
        let mut handles = vec![];

        for _ in 0..10 {
            let stats_clone = Arc::clone(&stats);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    stats_clone.record_file_success(10, 100, 200);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(stats.files_processed(), 1000);
        assert_eq!(stats.records_processed(), 10000);
    }

    #[test]
    fn test_active_duration() {
        let stats = WorkerStats::new();

        // No files processed yet - active_duration should be None
        assert!(stats.active_duration().is_none());
        assert!(stats.first_file_at().is_none());
        assert!(stats.last_file_at().is_none());

        // Process first file
        stats.record_file_success(100, 1024, 2048);
        let first = stats.first_file_at().unwrap();
        let last1 = stats.last_file_at().unwrap();

        // First and last should be the same after one file
        assert_eq!(first, last1);

        // Small delay to ensure timestamps differ
        sleep(StdDuration::from_millis(10));

        // Process second file
        stats.record_file_success(200, 2048, 4096);
        let last2 = stats.last_file_at().unwrap();

        // First should not change, last should be updated
        assert_eq!(stats.first_file_at().unwrap(), first);
        assert!(last2 > first);

        // Active duration should be positive
        let active = stats.active_duration().unwrap();
        assert!(active.num_milliseconds() >= 10);
    }

    #[test]
    fn test_snapshot_includes_active_duration() {
        let stats = WorkerStats::new();
        stats.record_file_success(100, 1024, 2048);
        sleep(StdDuration::from_millis(10));
        stats.record_file_success(200, 2048, 4096);

        let snapshot = stats.snapshot();

        assert!(snapshot.first_file_at.is_some());
        assert!(snapshot.last_file_at.is_some());
        assert!(snapshot.active_duration().is_some());
        assert!(snapshot.active_duration_secs().unwrap() >= 0.01);
    }
}
