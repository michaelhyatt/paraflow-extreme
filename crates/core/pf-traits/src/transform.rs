//! Transform trait for record processing.

use arrow::record_batch::RecordBatch;
use pf_error::Result;
use std::sync::Arc;

/// Trait for record transformations.
///
/// Transforms operate on Arrow RecordBatches, enabling:
/// - Filtering (dropping rows)
/// - Field manipulation (adding, modifying, removing columns)
/// - Enrichment (adding data from lookup tables)
///
/// # Zero-Copy Semantics
///
/// Implementations should strive for zero-copy where possible:
/// - Column projection: Share underlying buffers
/// - Row filtering: Use Arrow's filter kernel
/// - Column addition: Only allocate new column
///
/// # Thread Safety
///
/// Transforms must be `Send + Sync` as they may be used across threads.
/// Internal state (like Rhai engines) should use appropriate synchronization.
pub trait Transform: Send + Sync {
    /// Applies the transform to a batch.
    ///
    /// # Arguments
    ///
    /// * `batch` - Input batch to transform
    ///
    /// # Returns
    ///
    /// Transformed batch (may have different row count or columns)
    fn apply(&self, batch: Arc<RecordBatch>) -> Result<Arc<RecordBatch>>;

    /// Returns the name of this transform for logging.
    fn name(&self) -> &str {
        "transform"
    }
}

/// A chain of transforms applied in sequence.
pub struct TransformChain {
    transforms: Vec<Box<dyn Transform>>,
    name: String,
}

impl TransformChain {
    /// Creates a new empty transform chain.
    pub fn new() -> Self {
        Self {
            transforms: Vec::new(),
            name: "chain".to_string(),
        }
    }

    /// Adds a transform to the chain.
    pub fn push(mut self, transform: Box<dyn Transform>) -> Self {
        self.transforms.push(transform);
        self
    }

    /// Sets the name of this chain.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Returns true if the chain has no transforms.
    pub fn is_empty(&self) -> bool {
        self.transforms.is_empty()
    }

    /// Returns the number of transforms in the chain.
    pub fn len(&self) -> usize {
        self.transforms.len()
    }
}

impl Default for TransformChain {
    fn default() -> Self {
        Self::new()
    }
}

impl Transform for TransformChain {
    fn apply(&self, mut batch: Arc<RecordBatch>) -> Result<Arc<RecordBatch>> {
        for transform in &self.transforms {
            batch = transform.apply(batch)?;
        }
        Ok(batch)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// An identity transform that passes batches through unchanged.
///
/// Useful as a default or placeholder.
pub struct IdentityTransform;

impl Transform for IdentityTransform {
    fn apply(&self, batch: Arc<RecordBatch>) -> Result<Arc<RecordBatch>> {
        Ok(batch)
    }

    fn name(&self) -> &str {
        "identity"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn create_test_batch(num_rows: usize) -> Arc<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let array = Int32Array::from_iter_values(0..num_rows as i32);
        Arc::new(RecordBatch::try_new(schema, vec![Arc::new(array)]).unwrap())
    }

    #[test]
    fn test_identity_transform() {
        let batch = create_test_batch(100);
        let transform = IdentityTransform;

        let result = transform.apply(batch.clone()).unwrap();
        assert_eq!(result.num_rows(), 100);
        assert_eq!(transform.name(), "identity");
    }

    #[test]
    fn test_transform_chain() {
        let chain = TransformChain::new()
            .push(Box::new(IdentityTransform))
            .push(Box::new(IdentityTransform))
            .with_name("test-chain");

        assert_eq!(chain.len(), 2);
        assert!(!chain.is_empty());
        assert_eq!(chain.name(), "test-chain");

        let batch = create_test_batch(50);
        let result = chain.apply(batch).unwrap();
        assert_eq!(result.num_rows(), 50);
    }

    #[test]
    fn test_empty_chain() {
        let chain = TransformChain::new();
        assert!(chain.is_empty());

        let batch = create_test_batch(10);
        let result = chain.apply(batch).unwrap();
        assert_eq!(result.num_rows(), 10);
    }
}
