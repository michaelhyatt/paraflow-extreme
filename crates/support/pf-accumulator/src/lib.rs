//! Batch accumulator for optimal indexer batch sizing.
//!
//! The `BatchAccumulator` component aligns file batch sizes with optimal indexer
//! batch sizes. File batches are typically 2-3MB, but Elasticsearch bulk API
//! performs best with 10-15MB batches.
//!
//! # Example
//!
//! ```
//! use pf_accumulator::BatchAccumulator;
//! use arrow::record_batch::RecordBatch;
//! use std::sync::Arc;
//!
//! // Create accumulator with 10MB threshold
//! let mut accumulator = BatchAccumulator::new(10 * 1024 * 1024);
//!
//! // Add batches - returns Some when threshold is exceeded
//! // if let Some(batches_to_flush) = accumulator.add(batch) {
//! //     // Send batches_to_flush to indexer
//! // }
//!
//! // At end of processing, flush remaining batches
//! // let remaining = accumulator.flush();
//! ```

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tracing::trace;

/// Default threshold for Elasticsearch (10MB).
pub const DEFAULT_ES_THRESHOLD_BYTES: usize = 10 * 1024 * 1024;

/// Default threshold for ClickHouse (100MB).
pub const DEFAULT_CLICKHOUSE_THRESHOLD_BYTES: usize = 100 * 1024 * 1024;

/// Accumulates batches until a size threshold is reached.
///
/// This component is used to align file batch sizes with optimal indexer batch
/// sizes. Rather than sending each small batch immediately, batches are accumulated
/// until the threshold is exceeded, then all accumulated batches are flushed.
///
/// # Flush Behavior
///
/// When adding a batch would cause the accumulated size to exceed the threshold:
/// 1. All currently accumulated batches are returned for flushing
/// 2. The new batch is kept in the accumulator
/// 3. The caller should send the returned batches to the indexer
///
/// This ensures batches are always below the threshold and avoids creating
/// oversized batches that might be rejected.
#[derive(Debug)]
pub struct BatchAccumulator {
    /// Accumulated batches waiting to be flushed.
    batches: Vec<Arc<RecordBatch>>,

    /// Current accumulated size in bytes.
    current_size: usize,

    /// Flush threshold in bytes.
    threshold_bytes: usize,

    /// Optional maximum records before flush.
    threshold_records: Option<usize>,

    /// Current record count.
    current_records: usize,
}

/// Result of adding a batch to the accumulator.
#[derive(Debug)]
pub struct AccumulatorResult {
    /// Batches that should be flushed (if threshold was exceeded).
    pub batches_to_flush: Option<Vec<Arc<RecordBatch>>>,

    /// Size of batches being flushed.
    pub flush_size_bytes: usize,

    /// Number of records being flushed.
    pub flush_record_count: usize,
}

impl BatchAccumulator {
    /// Creates a new accumulator with the specified byte threshold.
    ///
    /// # Arguments
    ///
    /// * `threshold_bytes` - Size threshold in bytes that triggers a flush.
    ///   Common values:
    ///   - Elasticsearch: 10-15 MB (`DEFAULT_ES_THRESHOLD_BYTES`)
    ///   - ClickHouse: ~100 MB (`DEFAULT_CLICKHOUSE_THRESHOLD_BYTES`)
    pub fn new(threshold_bytes: usize) -> Self {
        Self {
            batches: Vec::new(),
            current_size: 0,
            threshold_bytes,
            threshold_records: None,
            current_records: 0,
        }
    }

    /// Creates an accumulator with both byte and record thresholds.
    ///
    /// Flush is triggered when either threshold is exceeded.
    pub fn with_record_threshold(mut self, max_records: usize) -> Self {
        self.threshold_records = Some(max_records);
        self
    }

    /// Adds a batch to the accumulator.
    ///
    /// Returns `AccumulatorResult` containing batches to flush if the threshold
    /// was exceeded. The returned batches should be sent to the indexer.
    ///
    /// # Threshold Behavior
    ///
    /// When adding this batch would exceed the threshold:
    /// - Current accumulated batches are returned for flushing
    /// - The new batch is kept in the accumulator
    /// - This ensures each flush is below the threshold
    pub fn add(&mut self, batch: Arc<RecordBatch>) -> AccumulatorResult {
        let batch_size = batch.get_array_memory_size();
        let batch_records = batch.num_rows();

        // Check if adding this batch would exceed threshold
        let would_exceed_bytes = self.current_size + batch_size > self.threshold_bytes;
        let would_exceed_records = self
            .threshold_records
            .is_some_and(|max| self.current_records + batch_records > max);

        if (would_exceed_bytes || would_exceed_records) && !self.batches.is_empty() {
            // Flush current batches, keep new batch
            let flush_size = self.current_size;
            let flush_records = self.current_records;
            let batches_to_flush = std::mem::take(&mut self.batches);

            trace!(
                flush_size_bytes = flush_size,
                flush_records = flush_records,
                flush_batch_count = batches_to_flush.len(),
                "Threshold exceeded, flushing accumulated batches"
            );

            // Add the new batch to start fresh accumulation
            self.batches.push(batch);
            self.current_size = batch_size;
            self.current_records = batch_records;

            AccumulatorResult {
                batches_to_flush: Some(batches_to_flush),
                flush_size_bytes: flush_size,
                flush_record_count: flush_records,
            }
        } else {
            // Accumulate the batch
            self.batches.push(batch);
            self.current_size += batch_size;
            self.current_records += batch_records;

            trace!(
                current_size_bytes = self.current_size,
                current_records = self.current_records,
                batch_count = self.batches.len(),
                "Batch accumulated"
            );

            AccumulatorResult {
                batches_to_flush: None,
                flush_size_bytes: 0,
                flush_record_count: 0,
            }
        }
    }

    /// Flushes all accumulated batches.
    ///
    /// Returns all batches and resets the accumulator to empty state.
    /// Call this at the end of file processing to ensure all data is sent.
    pub fn flush(&mut self) -> Vec<Arc<RecordBatch>> {
        let batches = std::mem::take(&mut self.batches);

        trace!(
            flush_size_bytes = self.current_size,
            flush_records = self.current_records,
            flush_batch_count = batches.len(),
            "Flushing all accumulated batches"
        );

        self.current_size = 0;
        self.current_records = 0;
        batches
    }

    /// Flushes all accumulated batches as a single concatenated batch.
    ///
    /// This is useful for destinations that prefer single large batches
    /// rather than multiple smaller ones. Returns `None` if the accumulator
    /// is empty.
    ///
    /// # Errors
    ///
    /// Returns `None` if concatenation fails (e.g., schema mismatch).
    pub fn flush_concatenated(&mut self) -> Option<Arc<RecordBatch>> {
        if self.batches.is_empty() {
            return None;
        }

        let schema = self.batches[0].schema();
        let refs: Vec<&RecordBatch> = self.batches.iter().map(|b| b.as_ref()).collect();

        match concat_batches(&schema, refs) {
            Ok(concatenated) => {
                trace!(
                    original_batch_count = self.batches.len(),
                    concatenated_rows = concatenated.num_rows(),
                    "Concatenated batches into single batch"
                );

                self.batches.clear();
                self.current_size = 0;
                self.current_records = 0;

                Some(Arc::new(concatenated))
            }
            Err(e) => {
                tracing::warn!(error = %e, "Failed to concatenate batches, returning None");
                None
            }
        }
    }

    /// Returns the current accumulated size in bytes.
    #[inline]
    pub fn current_size(&self) -> usize {
        self.current_size
    }

    /// Returns the current accumulated record count.
    #[inline]
    pub fn current_records(&self) -> usize {
        self.current_records
    }

    /// Returns the number of accumulated batches.
    #[inline]
    pub fn batch_count(&self) -> usize {
        self.batches.len()
    }

    /// Returns true if the accumulator has no batches.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    /// Returns the byte threshold.
    #[inline]
    pub fn threshold_bytes(&self) -> usize {
        self.threshold_bytes
    }

    /// Returns the record threshold if set.
    #[inline]
    pub fn threshold_records(&self) -> Option<usize> {
        self.threshold_records
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn create_test_batch(num_rows: usize) -> Arc<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let ids: Vec<i64> = (0..num_rows as i64).collect();
        let id_array = Int64Array::from(ids);
        Arc::new(RecordBatch::try_new(schema, vec![Arc::new(id_array)]).unwrap())
    }

    fn batch_size(batch: &RecordBatch) -> usize {
        batch.get_array_memory_size()
    }

    #[test]
    fn test_accumulator_no_flush_under_threshold() {
        // Create accumulator with large threshold
        let mut accumulator = BatchAccumulator::new(1024 * 1024); // 1MB

        let batch = create_test_batch(100);
        let result = accumulator.add(batch.clone());

        assert!(result.batches_to_flush.is_none());
        assert_eq!(accumulator.batch_count(), 1);
        assert!(!accumulator.is_empty());
    }

    #[test]
    fn test_accumulator_flush_on_threshold() {
        let batch1 = create_test_batch(1000);
        let batch_size = batch_size(&batch1);

        // Set threshold just above one batch but below two
        let mut accumulator = BatchAccumulator::new(batch_size + batch_size / 2);

        // First batch should accumulate
        let result1 = accumulator.add(batch1);
        assert!(result1.batches_to_flush.is_none());
        assert_eq!(accumulator.batch_count(), 1);

        // Second batch should trigger flush of first batch
        let batch2 = create_test_batch(1000);
        let result2 = accumulator.add(batch2);

        assert!(result2.batches_to_flush.is_some());
        let flushed = result2.batches_to_flush.unwrap();
        assert_eq!(flushed.len(), 1);

        // New batch should now be in accumulator
        assert_eq!(accumulator.batch_count(), 1);
    }

    #[test]
    fn test_accumulator_multiple_batches_before_flush() {
        let batch1 = create_test_batch(100);
        let single_batch_size = batch_size(&batch1);

        // Set threshold to hold ~3 batches
        let mut accumulator = BatchAccumulator::new(single_batch_size * 3 + single_batch_size / 2);

        // Add 3 batches - should all accumulate
        for _ in 0..3 {
            let batch = create_test_batch(100);
            let result = accumulator.add(batch);
            assert!(result.batches_to_flush.is_none());
        }
        assert_eq!(accumulator.batch_count(), 3);

        // 4th batch should trigger flush
        let batch4 = create_test_batch(100);
        let result = accumulator.add(batch4);

        assert!(result.batches_to_flush.is_some());
        let flushed = result.batches_to_flush.unwrap();
        assert_eq!(flushed.len(), 3);
        assert_eq!(accumulator.batch_count(), 1);
    }

    #[test]
    fn test_accumulator_record_threshold() {
        let mut accumulator = BatchAccumulator::new(1024 * 1024 * 1024) // Very large byte threshold
            .with_record_threshold(500);

        // Add batch with 300 records
        let batch1 = create_test_batch(300);
        let result1 = accumulator.add(batch1);
        assert!(result1.batches_to_flush.is_none());
        assert_eq!(accumulator.current_records(), 300);

        // Add batch with 300 more records - should exceed 500 threshold
        let batch2 = create_test_batch(300);
        let result2 = accumulator.add(batch2);

        assert!(result2.batches_to_flush.is_some());
        assert_eq!(result2.flush_record_count, 300);
    }

    #[test]
    fn test_flush_returns_all_batches() {
        let mut accumulator = BatchAccumulator::new(1024 * 1024);

        for _ in 0..5 {
            let batch = create_test_batch(100);
            accumulator.add(batch);
        }

        let flushed = accumulator.flush();
        assert_eq!(flushed.len(), 5);
        assert!(accumulator.is_empty());
        assert_eq!(accumulator.current_size(), 0);
        assert_eq!(accumulator.current_records(), 0);
    }

    #[test]
    fn test_flush_concatenated() {
        let mut accumulator = BatchAccumulator::new(1024 * 1024);

        for i in 0..3 {
            let batch = create_test_batch(100 + i * 50);
            accumulator.add(batch);
        }

        let concatenated = accumulator.flush_concatenated();
        assert!(concatenated.is_some());

        let batch = concatenated.unwrap();
        // 100 + 150 + 200 = 450 rows
        assert_eq!(batch.num_rows(), 450);

        assert!(accumulator.is_empty());
    }

    #[test]
    fn test_flush_concatenated_empty() {
        let mut accumulator = BatchAccumulator::new(1024 * 1024);
        let result = accumulator.flush_concatenated();
        assert!(result.is_none());
    }

    #[test]
    fn test_accumulator_result_metrics() {
        let batch1 = create_test_batch(100);
        let single_batch_size = batch_size(&batch1);

        let mut accumulator = BatchAccumulator::new(single_batch_size + single_batch_size / 2);

        accumulator.add(batch1);

        let batch2 = create_test_batch(100);
        let result = accumulator.add(batch2);

        assert!(result.batches_to_flush.is_some());
        assert_eq!(result.flush_size_bytes, single_batch_size);
        assert_eq!(result.flush_record_count, 100);
    }

    #[test]
    fn test_default_thresholds() {
        assert_eq!(DEFAULT_ES_THRESHOLD_BYTES, 10 * 1024 * 1024);
        assert_eq!(DEFAULT_CLICKHOUSE_THRESHOLD_BYTES, 100 * 1024 * 1024);
    }

    #[test]
    fn test_accessors() {
        let mut accumulator = BatchAccumulator::new(5000).with_record_threshold(1000);

        assert_eq!(accumulator.threshold_bytes(), 5000);
        assert_eq!(accumulator.threshold_records(), Some(1000));

        let batch = create_test_batch(50);
        accumulator.add(batch);

        assert_eq!(accumulator.current_records(), 50);
        assert!(accumulator.current_size() > 0);
        assert_eq!(accumulator.batch_count(), 1);
        assert!(!accumulator.is_empty());
    }
}
