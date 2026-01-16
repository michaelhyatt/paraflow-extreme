//! Stdout destination implementation.

use arrow::array::Array;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use pf_error::{PfError, Result};
use pf_traits::{BatchIndexer, IndexResult};
use std::io::{self, Write};
use std::sync::Arc;
use std::time::Instant;

/// Output format for stdout destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    /// JSON Lines (one JSON object per line)
    Jsonl,
    /// Pretty-printed JSON
    Json,
}

/// Destination that outputs processed batches as JSON/JSONL to stdout.
///
/// Used for debugging and pipeline verification.
pub struct StdoutDestination {
    format: OutputFormat,
}

impl StdoutDestination {
    /// Create a new stdout destination.
    pub fn new(format: OutputFormat) -> Self {
        Self { format }
    }

    /// Create a stdout destination with JSONL format.
    pub fn jsonl() -> Self {
        Self::new(OutputFormat::Jsonl)
    }

    /// Create a stdout destination with pretty JSON format.
    pub fn json() -> Self {
        Self::new(OutputFormat::Json)
    }
}

impl Default for StdoutDestination {
    fn default() -> Self {
        Self::jsonl()
    }
}

/// Convert a RecordBatch to a vector of JSON Value objects.
fn record_batch_to_json(batch: &RecordBatch) -> Vec<serde_json::Value> {
    let schema = batch.schema();
    let num_rows = batch.num_rows();
    let mut rows = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        let mut obj = serde_json::Map::new();

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let column = batch.column(col_idx);
            let value = array_value_to_json(column.as_ref(), row_idx);
            obj.insert(field.name().clone(), value);
        }

        rows.push(serde_json::Value::Object(obj));
    }

    rows
}

/// Extract a single value from an Arrow array and convert to JSON.
fn array_value_to_json(array: &dyn Array, idx: usize) -> serde_json::Value {
    if array.is_null(idx) {
        return serde_json::Value::Null;
    }

    match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<arrow::array::BooleanArray>().unwrap();
            serde_json::Value::Bool(arr.value(idx))
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<arrow::array::Int8Array>().unwrap();
            serde_json::Value::Number(arr.value(idx).into())
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<arrow::array::Int16Array>().unwrap();
            serde_json::Value::Number(arr.value(idx).into())
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
            serde_json::Value::Number(arr.value(idx).into())
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
            serde_json::Value::Number(arr.value(idx).into())
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<arrow::array::UInt8Array>().unwrap();
            serde_json::Value::Number(arr.value(idx).into())
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<arrow::array::UInt16Array>().unwrap();
            serde_json::Value::Number(arr.value(idx).into())
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<arrow::array::UInt32Array>().unwrap();
            serde_json::Value::Number(arr.value(idx).into())
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<arrow::array::UInt64Array>().unwrap();
            serde_json::Value::Number(arr.value(idx).into())
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<arrow::array::Float32Array>().unwrap();
            serde_json::Number::from_f64(arr.value(idx) as f64)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
            serde_json::Number::from_f64(arr.value(idx))
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
            serde_json::Value::String(arr.value(idx).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<arrow::array::LargeStringArray>().unwrap();
            serde_json::Value::String(arr.value(idx).to_string())
        }
        _ => {
            // For unsupported types, return the debug representation
            serde_json::Value::String(format!("{:?}", array.data_type()))
        }
    }
}

#[async_trait]
impl BatchIndexer for StdoutDestination {
    async fn index_batches(&self, batches: &[Arc<RecordBatch>]) -> Result<IndexResult> {
        let start = Instant::now();
        let mut count = 0u64;
        let mut bytes = 0u64;

        let stdout = io::stdout();
        let mut handle = stdout.lock();

        for batch in batches {
            let json_rows = record_batch_to_json(batch);

            for row in json_rows {
                let line = match self.format {
                    OutputFormat::Jsonl => serde_json::to_string(&row).map_err(|e| {
                        PfError::Other(anyhow::anyhow!("Failed to serialize JSON: {}", e))
                    })?,
                    OutputFormat::Json => serde_json::to_string_pretty(&row).map_err(|e| {
                        PfError::Other(anyhow::anyhow!("Failed to serialize JSON: {}", e))
                    })?,
                };

                writeln!(handle, "{}", line).map_err(|e| {
                    PfError::Other(anyhow::anyhow!("Failed to write to stdout: {}", e))
                })?;

                count += 1;
                bytes += line.len() as u64;
            }
        }

        Ok(IndexResult::success(count, bytes, start.elapsed()))
    }

    async fn flush(&self) -> Result<()> {
        io::stdout().flush().map_err(|e| {
            PfError::Other(anyhow::anyhow!("Failed to flush stdout: {}", e))
        })
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

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let id_array = Int64Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec![Some("Alice"), Some("Bob"), None]);

        RecordBatch::try_new(
            schema,
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_stdout_destination_jsonl() {
        let dest = StdoutDestination::jsonl();
        let batch = Arc::new(create_test_batch());

        // This will write to stdout, but we can still verify it doesn't error
        let result = dest.index_batches(&[batch]).await.unwrap();

        assert_eq!(result.success_count, 3);
        assert!(result.bytes_sent > 0);
    }

    #[tokio::test]
    async fn test_stdout_destination_flush() {
        let dest = StdoutDestination::jsonl();
        assert!(dest.flush().await.is_ok());
    }

    #[tokio::test]
    async fn test_stdout_destination_health_check() {
        let dest = StdoutDestination::jsonl();
        assert!(dest.health_check().await.unwrap());
    }

    #[tokio::test]
    async fn test_stdout_destination_empty_batches() {
        let dest = StdoutDestination::jsonl();
        let result = dest.index_batches(&[]).await.unwrap();

        assert_eq!(result.success_count, 0);
        assert_eq!(result.bytes_sent, 0);
    }

    #[test]
    fn test_record_batch_to_json() {
        let batch = create_test_batch();
        let rows = record_batch_to_json(&batch);

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0]["id"], 1);
        assert_eq!(rows[0]["name"], "Alice");
        assert_eq!(rows[1]["id"], 2);
        assert_eq!(rows[1]["name"], "Bob");
        assert_eq!(rows[2]["id"], 3);
        assert!(rows[2]["name"].is_null());
    }
}
