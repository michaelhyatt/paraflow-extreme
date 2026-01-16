//! Per-thread statistics tracking.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Per-thread statistics for tracking processing within a single thread.
#[derive(Debug, Default)]
pub struct ThreadStats {
    /// Thread identifier
    thread_id: u32,

    /// Number of files processed
    files_processed: AtomicU64,

    /// Number of files failed
    files_failed: AtomicU64,

    /// Number of records processed
    records_processed: AtomicU64,

    /// Number of records failed
    records_failed: AtomicU64,

    /// Bytes read from source files
    bytes_read: AtomicU64,

    /// Bytes written to destination
    bytes_written: AtomicU64,

    /// Number of batches processed
    batches_processed: AtomicU64,

    /// Total processing time (in microseconds)
    processing_time_us: AtomicU64,
}

impl ThreadStats {
    /// Create new thread statistics for the given thread ID.
    pub fn new(thread_id: u32) -> Self {
        Self {
            thread_id,
            ..Default::default()
        }
    }

    /// Get the thread ID.
    pub fn thread_id(&self) -> u32 {
        self.thread_id
    }

    /// Record a successfully processed file.
    pub fn record_file_success(&self, records: u64, bytes_read: u64, bytes_written: u64, duration: Duration) {
        self.files_processed.fetch_add(1, Ordering::Relaxed);
        self.records_processed.fetch_add(records, Ordering::Relaxed);
        self.bytes_read.fetch_add(bytes_read, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes_written, Ordering::Relaxed);
        self.processing_time_us.fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
    }

    /// Record a failed file.
    pub fn record_file_failure(&self) {
        self.files_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a processed batch.
    pub fn record_batch(&self, records: u64, bytes: u64) {
        self.batches_processed.fetch_add(1, Ordering::Relaxed);
        self.records_processed.fetch_add(records, Ordering::Relaxed);
        self.bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record failed records.
    pub fn record_failed_records(&self, count: u64) {
        self.records_failed.fetch_add(count, Ordering::Relaxed);
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

    /// Get the bytes read.
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read.load(Ordering::Relaxed)
    }

    /// Get the bytes written.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    /// Get the number of batches processed.
    pub fn batches_processed(&self) -> u64 {
        self.batches_processed.load(Ordering::Relaxed)
    }

    /// Get the total processing time.
    pub fn processing_time(&self) -> Duration {
        Duration::from_micros(self.processing_time_us.load(Ordering::Relaxed))
    }

    /// Create a snapshot of the current statistics.
    pub fn snapshot(&self) -> ThreadStatsSnapshot {
        ThreadStatsSnapshot {
            thread_id: self.thread_id,
            files_processed: self.files_processed(),
            files_failed: self.files_failed(),
            records_processed: self.records_processed(),
            records_failed: self.records_failed(),
            bytes_read: self.bytes_read(),
            bytes_written: self.bytes_written(),
            batches_processed: self.batches_processed(),
            processing_time: self.processing_time(),
        }
    }
}

/// A serializable snapshot of thread statistics.
#[derive(Debug, Clone)]
pub struct ThreadStatsSnapshot {
    pub thread_id: u32,
    pub files_processed: u64,
    pub files_failed: u64,
    pub records_processed: u64,
    pub records_failed: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub batches_processed: u64,
    pub processing_time: Duration,
}

impl ThreadStatsSnapshot {
    /// Calculate the throughput in records per second.
    pub fn records_per_second(&self) -> f64 {
        let secs = self.processing_time.as_secs_f64();
        if secs > 0.0 {
            self.records_processed as f64 / secs
        } else {
            0.0
        }
    }

    /// Calculate the throughput in MB per second (read).
    pub fn read_throughput_mbps(&self) -> f64 {
        let secs = self.processing_time.as_secs_f64();
        if secs > 0.0 {
            (self.bytes_read as f64 / 1_000_000.0) / secs
        } else {
            0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_thread_stats_new() {
        let stats = ThreadStats::new(5);
        assert_eq!(stats.thread_id(), 5);
        assert_eq!(stats.files_processed(), 0);
    }

    #[test]
    fn test_thread_stats_record_success() {
        let stats = ThreadStats::new(0);
        stats.record_file_success(1000, 1024, 2048, Duration::from_millis(100));

        assert_eq!(stats.files_processed(), 1);
        assert_eq!(stats.records_processed(), 1000);
        assert_eq!(stats.bytes_read(), 1024);
        assert_eq!(stats.bytes_written(), 2048);
        assert_eq!(stats.processing_time(), Duration::from_millis(100));
    }

    #[test]
    fn test_thread_stats_record_failure() {
        let stats = ThreadStats::new(0);
        stats.record_file_failure();
        stats.record_file_failure();

        assert_eq!(stats.files_failed(), 2);
    }

    #[test]
    fn test_thread_stats_snapshot() {
        let stats = ThreadStats::new(3);
        stats.record_file_success(500, 512, 1024, Duration::from_secs(1));

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.thread_id, 3);
        assert_eq!(snapshot.files_processed, 1);
        assert_eq!(snapshot.records_processed, 500);
        assert_eq!(snapshot.records_per_second(), 500.0);
    }

    #[test]
    fn test_thread_stats_accumulates() {
        let stats = ThreadStats::new(0);
        stats.record_file_success(100, 100, 200, Duration::from_millis(10));
        stats.record_file_success(200, 200, 400, Duration::from_millis(20));

        assert_eq!(stats.files_processed(), 2);
        assert_eq!(stats.records_processed(), 300);
        assert_eq!(stats.bytes_read(), 300);
        assert_eq!(stats.processing_time(), Duration::from_millis(30));
    }
}
