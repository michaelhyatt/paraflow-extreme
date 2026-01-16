//! Batch types for zero-copy Arrow data handling.

use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// A batch of records wrapped with metadata.
///
/// This type provides a zero-copy wrapper around Arrow's [`RecordBatch`],
/// adding source tracking and batch indexing for progress reporting.
///
/// # Zero-Copy Semantics
///
/// The inner `RecordBatch` is stored in an `Arc`, allowing cheap cloning
/// without copying the underlying data. This enables efficient passing
/// through the pipeline stages.
#[derive(Clone)]
pub struct Batch {
    /// The underlying Arrow RecordBatch
    inner: Arc<RecordBatch>,

    /// Metadata about this batch
    metadata: BatchMetadata,
}

/// Metadata associated with a batch.
#[derive(Clone, Debug)]
pub struct BatchMetadata {
    /// Source file URI this batch came from
    pub source_file: String,

    /// Index of this batch within the file (0-indexed)
    pub batch_index: usize,

    /// Number of records in this batch
    pub record_count: usize,

    /// Approximate size in bytes (Arrow array memory size)
    pub size_bytes: usize,
}

impl Batch {
    /// Creates a new batch from a RecordBatch with source tracking.
    ///
    /// # Arguments
    ///
    /// * `batch` - The Arrow RecordBatch
    /// * `source_file` - URI of the source file
    /// * `batch_index` - Index of this batch within the file
    pub fn new(batch: RecordBatch, source_file: impl Into<String>, batch_index: usize) -> Self {
        let record_count = batch.num_rows();
        let size_bytes = batch.get_array_memory_size();

        Self {
            inner: Arc::new(batch),
            metadata: BatchMetadata {
                source_file: source_file.into(),
                batch_index,
                record_count,
                size_bytes,
            },
        }
    }

    /// Creates a batch from an existing Arc<RecordBatch>.
    pub fn from_arc(
        batch: Arc<RecordBatch>,
        source_file: impl Into<String>,
        batch_index: usize,
    ) -> Self {
        let record_count = batch.num_rows();
        let size_bytes = batch.get_array_memory_size();

        Self {
            inner: batch,
            metadata: BatchMetadata {
                source_file: source_file.into(),
                batch_index,
                record_count,
                size_bytes,
            },
        }
    }

    /// Returns a reference to the underlying RecordBatch.
    #[inline]
    pub fn record_batch(&self) -> &RecordBatch {
        &self.inner
    }

    /// Consumes self and returns the Arc<RecordBatch>.
    #[inline]
    pub fn into_arc(self) -> Arc<RecordBatch> {
        self.inner
    }

    /// Returns a clone of the Arc<RecordBatch>.
    #[inline]
    pub fn arc(&self) -> Arc<RecordBatch> {
        Arc::clone(&self.inner)
    }

    /// Returns metadata about this batch.
    #[inline]
    pub fn metadata(&self) -> &BatchMetadata {
        &self.metadata
    }

    /// Returns the number of rows in this batch.
    #[inline]
    pub fn num_rows(&self) -> usize {
        self.inner.num_rows()
    }

    /// Returns the number of columns in this batch.
    #[inline]
    pub fn num_columns(&self) -> usize {
        self.inner.num_columns()
    }

    /// Returns the schema of this batch.
    #[inline]
    pub fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.inner.schema()
    }
}

impl std::fmt::Debug for Batch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Batch")
            .field("metadata", &self.metadata)
            .field("schema", &self.inner.schema())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn create_test_batch(num_rows: usize) -> RecordBatch {
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, false)]);
        let array = Int32Array::from_iter_values(0..num_rows as i32);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap()
    }

    #[test]
    fn test_batch_creation() {
        let rb = create_test_batch(100);
        let batch = Batch::new(rb, "s3://bucket/file.parquet", 0);

        assert_eq!(batch.num_rows(), 100);
        assert_eq!(batch.metadata().batch_index, 0);
        assert_eq!(batch.metadata().source_file, "s3://bucket/file.parquet");
        assert_eq!(batch.metadata().record_count, 100);
    }

    #[test]
    fn test_batch_zero_copy() {
        let rb = create_test_batch(1000);
        let batch1 = Batch::new(rb, "file.parquet", 0);
        let batch2 = batch1.clone();

        // Both batches point to the same underlying data
        assert!(Arc::ptr_eq(&batch1.inner, &batch2.inner));
    }
}
