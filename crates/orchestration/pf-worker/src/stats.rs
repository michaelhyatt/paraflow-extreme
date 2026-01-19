//! Statistics for worker runs.

use chrono::{DateTime, Duration, Utc};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

/// Cache line size on most modern CPUs (64 bytes).
const CACHE_LINE_SIZE: usize = 64;

/// A cache-line-padded atomic counter to prevent false sharing.
///
/// When multiple threads update different AtomicU64 values that happen to
/// share a cache line, CPU cache coherency protocols cause performance
/// degradation ("false sharing"). This wrapper ensures each counter occupies
/// its own cache line.
#[repr(C, align(64))]
#[derive(Debug)]
struct PaddedAtomicU64 {
    value: AtomicU64,
    // Padding to fill the rest of the cache line
    _padding: [u8; CACHE_LINE_SIZE - std::mem::size_of::<AtomicU64>()],
}

/// A cache-line-padded atomic usize counter.
#[repr(C, align(64))]
#[derive(Debug)]
struct PaddedAtomicUsize {
    value: std::sync::atomic::AtomicUsize,
    _padding: [u8; CACHE_LINE_SIZE - std::mem::size_of::<std::sync::atomic::AtomicUsize>()],
}

impl Default for PaddedAtomicUsize {
    fn default() -> Self {
        Self::new(0)
    }
}

impl PaddedAtomicUsize {
    fn new(val: usize) -> Self {
        Self {
            value: std::sync::atomic::AtomicUsize::new(val),
            _padding: [0; CACHE_LINE_SIZE - std::mem::size_of::<std::sync::atomic::AtomicUsize>()],
        }
    }

    #[inline]
    fn load(&self, ordering: Ordering) -> usize {
        self.value.load(ordering)
    }

    #[inline]
    fn store(&self, val: usize, ordering: Ordering) {
        self.value.store(val, ordering)
    }
}

impl Default for PaddedAtomicU64 {
    fn default() -> Self {
        Self::new(0)
    }
}

impl PaddedAtomicU64 {
    fn new(val: u64) -> Self {
        Self {
            value: AtomicU64::new(val),
            _padding: [0; CACHE_LINE_SIZE - std::mem::size_of::<AtomicU64>()],
        }
    }

    #[inline]
    fn load(&self, ordering: Ordering) -> u64 {
        self.value.load(ordering)
    }

    #[inline]
    fn fetch_add(&self, val: u64, ordering: Ordering) -> u64 {
        self.value.fetch_add(val, ordering)
    }
}

/// Statistics collected during a worker run.
///
/// Uses cache-line-padded atomic counters for thread-safe updates from multiple
/// processing threads without false sharing. Each counter occupies its own
/// cache line (64 bytes) to prevent CPU cache coherency traffic when multiple
/// threads update different counters concurrently.
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

    // === Padded atomic counters to prevent false sharing ===
    // Each counter is on its own cache line (64 bytes)
    /// Total number of files processed successfully
    files_processed: PaddedAtomicU64,

    /// Number of files that failed
    files_failed: PaddedAtomicU64,

    /// Total number of records processed
    records_processed: PaddedAtomicU64,

    /// Number of records that failed
    records_failed: PaddedAtomicU64,

    /// Total bytes read from source files
    bytes_read: PaddedAtomicU64,

    /// Total bytes written to destination
    bytes_written: PaddedAtomicU64,

    /// Number of batches processed
    batches_processed: PaddedAtomicU64,

    /// Number of transient errors encountered
    transient_errors: PaddedAtomicU64,

    /// Number of permanent errors encountered
    permanent_errors: PaddedAtomicU64,

    /// Cumulative processing time in microseconds (sum of all file processing durations).
    /// This is the actual "work done" time, excluding idle/waiting time between files.
    /// Use this for accurate throughput calculations.
    cumulative_processing_time_us: PaddedAtomicU64,

    /// Number of items currently in prefetch buffers across all worker threads.
    /// This is used during drain mode to ensure all prefetched items are processed
    /// before signaling completion.
    pending_prefetch_items: PaddedAtomicUsize,
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

    /// Record a successfully processed file with its processing duration.
    ///
    /// The duration is the actual time spent processing this file (from start to finish),
    /// which is accumulated into `cumulative_processing_time_us` for accurate throughput
    /// calculations that exclude idle time between files.
    pub fn record_file_success(
        &self,
        records: u64,
        bytes_read: u64,
        bytes_written: u64,
        duration: std::time::Duration,
    ) {
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
        self.cumulative_processing_time_us
            .fetch_add(duration.as_micros() as u64, Ordering::Relaxed);
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

    /// Set the current count of pending prefetch items across all worker threads.
    ///
    /// This is called periodically by worker threads to report how many items
    /// are in their prefetch buffers. The SQS source uses this during drain mode
    /// to ensure all prefetched items are processed before signaling completion.
    pub fn set_pending_prefetch(&self, count: usize) {
        self.pending_prefetch_items.store(count, Ordering::SeqCst);
    }

    /// Get the current count of pending prefetch items.
    ///
    /// During drain mode, this should be checked to ensure workers have
    /// processed all their prefetched items before declaring the queue empty.
    pub fn pending_prefetch_items(&self) -> usize {
        self.pending_prefetch_items.load(Ordering::SeqCst)
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

    /// Get the cumulative processing time (sum of all file processing durations).
    ///
    /// This represents the actual "work done" time and excludes idle time waiting
    /// for messages, SSM signals, or between file processing. Use this for accurate
    /// throughput calculations.
    pub fn cumulative_processing_time(&self) -> std::time::Duration {
        std::time::Duration::from_micros(self.cumulative_processing_time_us.load(Ordering::Relaxed))
    }

    /// Get the cumulative processing time in seconds.
    pub fn cumulative_processing_time_secs(&self) -> f64 {
        self.cumulative_processing_time_us.load(Ordering::Relaxed) as f64 / 1_000_000.0
    }

    /// Calculate the throughput in files per second using cumulative processing time.
    ///
    /// This provides accurate throughput based on actual work done, excluding idle time.
    pub fn files_per_second(&self) -> Option<f64> {
        let secs = self.cumulative_processing_time_secs();
        if secs > 0.0 {
            Some(self.files_processed() as f64 / secs)
        } else {
            None
        }
    }

    /// Calculate the throughput in records per second using cumulative processing time.
    ///
    /// This provides accurate throughput based on actual work done, excluding idle time.
    pub fn records_per_second(&self) -> Option<f64> {
        let secs = self.cumulative_processing_time_secs();
        if secs > 0.0 {
            Some(self.records_processed() as f64 / secs)
        } else {
            None
        }
    }

    /// Calculate the throughput in MB per second (read) using cumulative processing time.
    ///
    /// This provides accurate throughput based on actual work done, excluding idle time.
    pub fn read_throughput_mbps(&self) -> Option<f64> {
        let secs = self.cumulative_processing_time_secs();
        if secs > 0.0 {
            Some((self.bytes_read() as f64 / 1_000_000.0) / secs)
        } else {
            None
        }
    }

    /// Calculate the throughput in MB per second (written) using cumulative processing time.
    ///
    /// This provides accurate throughput based on actual work done, excluding idle time.
    pub fn write_throughput_mbps(&self) -> Option<f64> {
        let secs = self.cumulative_processing_time_secs();
        if secs > 0.0 {
            Some((self.bytes_written() as f64 / 1_000_000.0) / secs)
        } else {
            None
        }
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
            cumulative_processing_time_us: self
                .cumulative_processing_time_us
                .load(Ordering::Relaxed),
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
    /// Cumulative processing time in microseconds (sum of all file processing durations).
    /// This is the actual "work done" time, excluding idle/waiting time.
    #[serde(default)]
    pub cumulative_processing_time_us: u64,
}

impl StatsSnapshot {
    /// Get the total duration of the worker run (includes startup and drain time).
    pub fn duration(&self) -> Option<Duration> {
        match (self.started_at, self.completed_at) {
            (Some(start), Some(end)) => Some(end - start),
            _ => None,
        }
    }

    /// Get the active processing duration (first file to last file) - wall clock time.
    ///
    /// Note: This includes idle time between files. For accurate throughput,
    /// use `cumulative_processing_time_secs()` instead.
    pub fn active_duration(&self) -> Option<Duration> {
        match (self.first_file_at, self.last_file_at) {
            (Some(first), Some(last)) => Some(last - first),
            _ => None,
        }
    }

    /// Get the active processing duration in seconds (wall clock time).
    ///
    /// Note: This includes idle time between files. For accurate throughput,
    /// use `cumulative_processing_time_secs()` instead.
    pub fn active_duration_secs(&self) -> Option<f64> {
        self.active_duration()
            .map(|d| d.num_milliseconds() as f64 / 1000.0)
    }

    /// Get the cumulative processing time (sum of all file processing durations).
    ///
    /// This represents the actual "work done" time and excludes idle time waiting
    /// for messages, SSM signals, or between file processing. Use this for accurate
    /// throughput calculations.
    pub fn cumulative_processing_time(&self) -> std::time::Duration {
        std::time::Duration::from_micros(self.cumulative_processing_time_us)
    }

    /// Get the cumulative processing time in seconds.
    ///
    /// This is the preferred duration metric for throughput calculations as it
    /// only counts actual file processing time.
    pub fn cumulative_processing_time_secs(&self) -> f64 {
        self.cumulative_processing_time_us as f64 / 1_000_000.0
    }

    /// Calculate the throughput in records per second using cumulative processing time.
    pub fn records_per_second(&self) -> Option<f64> {
        let secs = self.cumulative_processing_time_secs();
        if secs > 0.0 {
            Some(self.records_processed as f64 / secs)
        } else {
            None
        }
    }

    /// Calculate the throughput in MB per second (read) using cumulative processing time.
    pub fn read_throughput_mbps(&self) -> Option<f64> {
        let secs = self.cumulative_processing_time_secs();
        if secs > 0.0 {
            Some((self.bytes_read as f64 / 1_000_000.0) / secs)
        } else {
            None
        }
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
        stats.record_file_success(1000, 1024, 2048, StdDuration::from_millis(100));
        stats.record_file_success(2000, 2048, 4096, StdDuration::from_millis(200));

        assert_eq!(stats.files_processed(), 2);
        assert_eq!(stats.records_processed(), 3000);
        assert_eq!(stats.bytes_read(), 3072);
        assert_eq!(stats.bytes_written(), 6144);
        // Cumulative processing time should be 300ms
        assert_eq!(
            stats.cumulative_processing_time(),
            StdDuration::from_millis(300)
        );
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
        stats.record_file_success(100, 1024, 2048, StdDuration::from_millis(50));

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.files_processed, 1);
        assert_eq!(snapshot.records_processed, 100);
        assert_eq!(snapshot.cumulative_processing_time_us, 50_000);
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
                    stats_clone.record_file_success(10, 100, 200, StdDuration::from_micros(100));
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(stats.files_processed(), 1000);
        assert_eq!(stats.records_processed(), 10000);
        // 1000 files × 100μs = 100ms cumulative time
        assert_eq!(
            stats.cumulative_processing_time(),
            StdDuration::from_millis(100)
        );
    }

    #[test]
    fn test_active_duration() {
        let stats = WorkerStats::new();

        // No files processed yet - active_duration should be None
        assert!(stats.active_duration().is_none());
        assert!(stats.first_file_at().is_none());
        assert!(stats.last_file_at().is_none());

        // Process first file
        stats.record_file_success(100, 1024, 2048, StdDuration::from_millis(50));
        let first = stats.first_file_at().unwrap();
        let last1 = stats.last_file_at().unwrap();

        // First and last should be the same after one file
        assert_eq!(first, last1);

        // Small delay to ensure timestamps differ
        sleep(StdDuration::from_millis(10));

        // Process second file
        stats.record_file_success(200, 2048, 4096, StdDuration::from_millis(75));
        let last2 = stats.last_file_at().unwrap();

        // First should not change, last should be updated
        assert_eq!(stats.first_file_at().unwrap(), first);
        assert!(last2 > first);

        // Active duration (wall clock) should be positive and >= 10ms (the sleep)
        let active = stats.active_duration().unwrap();
        assert!(active.num_milliseconds() >= 10);

        // Cumulative processing time should be 125ms (50 + 75)
        assert_eq!(
            stats.cumulative_processing_time(),
            StdDuration::from_millis(125)
        );
    }

    #[test]
    fn test_snapshot_includes_active_duration() {
        let stats = WorkerStats::new();
        stats.record_file_success(100, 1024, 2048, StdDuration::from_millis(50));
        sleep(StdDuration::from_millis(10));
        stats.record_file_success(200, 2048, 4096, StdDuration::from_millis(75));

        let snapshot = stats.snapshot();

        assert!(snapshot.first_file_at.is_some());
        assert!(snapshot.last_file_at.is_some());
        assert!(snapshot.active_duration().is_some());
        assert!(snapshot.active_duration_secs().unwrap() >= 0.01);
        // Cumulative time is 125ms = 0.125s
        assert!((snapshot.cumulative_processing_time_secs() - 0.125).abs() < 0.001);
    }

    #[test]
    fn test_throughput_uses_cumulative_time() {
        let stats = WorkerStats::new();
        // Process 1000 records in 100ms of actual processing time
        stats.record_file_success(1000, 1_000_000, 0, StdDuration::from_millis(100));

        // Throughput should be based on cumulative time (100ms), not wall clock
        let records_per_sec = stats.records_per_second().unwrap();
        // 1000 records / 0.1s = 10000 records/sec
        assert!((records_per_sec - 10000.0).abs() < 1.0);

        let mbps = stats.read_throughput_mbps().unwrap();
        // 1MB / 0.1s = 10 MB/s
        assert!((mbps - 10.0).abs() < 0.1);
    }

    #[test]
    fn test_padded_atomic_alignment() {
        // Verify that PaddedAtomicU64 is properly aligned to cache line boundaries
        assert_eq!(
            std::mem::align_of::<super::PaddedAtomicU64>(),
            64,
            "PaddedAtomicU64 should be 64-byte aligned"
        );
        assert_eq!(
            std::mem::size_of::<super::PaddedAtomicU64>(),
            64,
            "PaddedAtomicU64 should be exactly 64 bytes"
        );
    }

    #[test]
    fn test_padded_atomic_usize_alignment() {
        // Verify that PaddedAtomicUsize is properly aligned to cache line boundaries
        assert_eq!(
            std::mem::align_of::<super::PaddedAtomicUsize>(),
            64,
            "PaddedAtomicUsize should be 64-byte aligned"
        );
        assert_eq!(
            std::mem::size_of::<super::PaddedAtomicUsize>(),
            64,
            "PaddedAtomicUsize should be exactly 64 bytes"
        );
    }

    #[test]
    fn test_pending_prefetch_items_default() {
        let stats = WorkerStats::new();
        // Default value should be 0
        assert_eq!(stats.pending_prefetch_items(), 0);
    }

    #[test]
    fn test_pending_prefetch_items_set_and_get() {
        let stats = WorkerStats::new();

        // Set to a value
        stats.set_pending_prefetch(5);
        assert_eq!(stats.pending_prefetch_items(), 5);

        // Update to a different value
        stats.set_pending_prefetch(10);
        assert_eq!(stats.pending_prefetch_items(), 10);

        // Set back to 0
        stats.set_pending_prefetch(0);
        assert_eq!(stats.pending_prefetch_items(), 0);
    }

    #[test]
    fn test_pending_prefetch_items_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let stats = Arc::new(WorkerStats::new());
        let mut handles = vec![];

        // Spawn multiple threads that concurrently update the counter
        for i in 0..10 {
            let stats_clone = Arc::clone(&stats);
            handles.push(thread::spawn(move || {
                // Each thread sets the value multiple times
                for j in 0..100 {
                    stats_clone.set_pending_prefetch(i * 100 + j);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Final value should be one of the values set by the threads
        // (we can't predict which one due to race conditions, but it should be valid)
        let final_value = stats.pending_prefetch_items();
        assert!(final_value < 1000, "Value should be in expected range");
    }

    #[test]
    fn test_pending_prefetch_items_large_values() {
        let stats = WorkerStats::new();

        // Test with large values
        stats.set_pending_prefetch(usize::MAX);
        assert_eq!(stats.pending_prefetch_items(), usize::MAX);

        // Test with 0 again
        stats.set_pending_prefetch(0);
        assert_eq!(stats.pending_prefetch_items(), 0);
    }
}
