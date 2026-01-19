//! Stats destination implementation.

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use pf_error::Result;
use pf_traits::{BatchIndexer, IndexResult};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Destination that counts records and tracks metrics without outputting data.
///
/// Used for performance testing and throughput measurement.
///
/// In fast mode (default), skips expensive `get_array_memory_size()` calls
/// which can add 5-10% overhead in high-throughput scenarios.
pub struct StatsDestination {
    records: AtomicU64,
    bytes: AtomicU64,
    batches: AtomicU64,
    /// When true, skip expensive byte size calculations in the hot path.
    /// This improves throughput by 5-10% but bytes will always be 0.
    fast_mode: bool,
}

impl StatsDestination {
    /// Create a new stats destination in fast mode (default).
    ///
    /// Fast mode skips byte size calculations for better throughput.
    pub fn new() -> Self {
        Self {
            records: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            batches: AtomicU64::new(0),
            fast_mode: true,
        }
    }

    /// Create a stats destination that tracks byte sizes.
    ///
    /// This is slower than fast mode due to `get_array_memory_size()` calls
    /// on each batch, but provides accurate byte metrics.
    pub fn with_byte_tracking() -> Self {
        Self {
            records: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            batches: AtomicU64::new(0),
            fast_mode: false,
        }
    }

    /// Get the current statistics.
    pub fn get_stats(&self) -> StatsReport {
        StatsReport {
            records: self.records.load(Ordering::Relaxed),
            bytes: self.bytes.load(Ordering::Relaxed),
            batches: self.batches.load(Ordering::Relaxed),
        }
    }

    /// Reset all counters to zero.
    pub fn reset(&self) {
        self.records.store(0, Ordering::Relaxed);
        self.bytes.store(0, Ordering::Relaxed);
        self.batches.store(0, Ordering::Relaxed);
    }
}

impl Default for StatsDestination {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics report from the stats destination.
#[derive(Debug, Clone, Copy)]
pub struct StatsReport {
    /// Total records processed
    pub records: u64,
    /// Total bytes processed (Arrow memory size)
    pub bytes: u64,
    /// Total batches processed
    pub batches: u64,
}

#[async_trait]
impl BatchIndexer for StatsDestination {
    async fn index_batches(&self, batches: &[Arc<RecordBatch>]) -> Result<IndexResult> {
        let start = Instant::now();
        let mut count = 0u64;
        let mut bytes = 0u64;

        if self.fast_mode {
            // Fast path: only count rows, skip expensive byte size calculation
            for batch in batches {
                count += batch.num_rows() as u64;
            }
        } else {
            // Full tracking: count both rows and bytes (slower)
            for batch in batches {
                count += batch.num_rows() as u64;
                bytes += batch.get_array_memory_size() as u64;
            }
            self.bytes.fetch_add(bytes, Ordering::Relaxed);
        }

        self.records.fetch_add(count, Ordering::Relaxed);
        self.batches
            .fetch_add(batches.len() as u64, Ordering::Relaxed);

        Ok(IndexResult::success(count, bytes, start.elapsed()))
    }

    async fn flush(&self) -> Result<()> {
        Ok(())
    }

    async fn health_check(&self) -> Result<bool> {
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn create_test_batch(num_rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let ids: Vec<i64> = (0..num_rows as i64).collect();
        let names: Vec<Option<&str>> = (0..num_rows).map(|_| Some("test")).collect();

        let id_array = Int64Array::from(ids);
        let name_array = StringArray::from(names);

        RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)]).unwrap()
    }

    #[tokio::test]
    async fn test_stats_destination_counts() {
        // Use byte tracking mode to test full functionality
        let dest = StatsDestination::with_byte_tracking();
        let batch = Arc::new(create_test_batch(100));

        let result = dest.index_batches(&[batch]).await.unwrap();

        assert_eq!(result.success_count, 100);
        assert!(result.bytes_sent > 0);

        let stats = dest.get_stats();
        assert_eq!(stats.records, 100);
        assert_eq!(stats.batches, 1);
        assert!(stats.bytes > 0);
    }

    #[tokio::test]
    async fn test_stats_destination_fast_mode() {
        // Fast mode (default) skips byte size calculation
        let dest = StatsDestination::new();
        let batch = Arc::new(create_test_batch(100));

        let result = dest.index_batches(&[batch]).await.unwrap();

        assert_eq!(result.success_count, 100);
        // In fast mode, bytes_sent is 0 (skipped for performance)
        assert_eq!(result.bytes_sent, 0);

        let stats = dest.get_stats();
        assert_eq!(stats.records, 100);
        assert_eq!(stats.batches, 1);
        // bytes stays at 0 in fast mode
        assert_eq!(stats.bytes, 0);
    }

    #[tokio::test]
    async fn test_stats_destination_multiple_batches() {
        let dest = StatsDestination::new();
        let batch1 = Arc::new(create_test_batch(50));
        let batch2 = Arc::new(create_test_batch(75));

        dest.index_batches(&[batch1]).await.unwrap();
        dest.index_batches(&[batch2]).await.unwrap();

        let stats = dest.get_stats();
        assert_eq!(stats.records, 125);
        assert_eq!(stats.batches, 2);
    }

    #[tokio::test]
    async fn test_stats_destination_reset() {
        let dest = StatsDestination::new();
        let batch = Arc::new(create_test_batch(100));

        dest.index_batches(&[batch]).await.unwrap();
        assert_eq!(dest.get_stats().records, 100);

        dest.reset();
        assert_eq!(dest.get_stats().records, 0);
    }

    #[tokio::test]
    async fn test_stats_destination_thread_safety() {
        use std::thread;

        let dest = Arc::new(StatsDestination::new());
        let mut handles = vec![];

        for _ in 0..10 {
            let dest_clone = Arc::clone(&dest);
            handles.push(thread::spawn(move || {
                let batch = Arc::new(create_test_batch(100));
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async {
                    dest_clone.index_batches(&[batch]).await.unwrap();
                });
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let stats = dest.get_stats();
        assert_eq!(stats.records, 1000);
        assert_eq!(stats.batches, 10);
    }
}
