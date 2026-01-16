//! Stdout destination implementation.

use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeListArray,
    LargeStringArray, ListArray, StringArray, StructArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
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
///
/// Supports all common Arrow data types including:
/// - Primitive types (Boolean, Int*, UInt*, Float*)
/// - String types (Utf8, LargeUtf8)
/// - Binary types (Binary, LargeBinary)
/// - Temporal types (Date32, Date64, Timestamp with all time units)
/// - Nested types (List, LargeList, Struct)
fn array_value_to_json(array: &dyn Array, idx: usize) -> serde_json::Value {
    if array.is_null(idx) {
        return serde_json::Value::Null;
    }

    match array.data_type() {
        // Boolean
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            serde_json::Value::Bool(arr.value(idx))
        }

        // Signed integers
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            serde_json::Value::Number(arr.value(idx).into())
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            serde_json::Value::Number(arr.value(idx).into())
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            serde_json::Value::Number(arr.value(idx).into())
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            serde_json::Value::Number(arr.value(idx).into())
        }

        // Unsigned integers
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            serde_json::Value::Number(arr.value(idx).into())
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            serde_json::Value::Number(arr.value(idx).into())
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            serde_json::Value::Number(arr.value(idx).into())
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            serde_json::Value::Number(arr.value(idx).into())
        }

        // Floating point
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            serde_json::Number::from_f64(arr.value(idx) as f64)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            serde_json::Number::from_f64(arr.value(idx))
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }

        // Strings
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            serde_json::Value::String(arr.value(idx).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            serde_json::Value::String(arr.value(idx).to_string())
        }

        // Binary (encode as base64)
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
            use base64::Engine;
            let encoded = base64::engine::general_purpose::STANDARD.encode(arr.value(idx));
            serde_json::Value::String(encoded)
        }
        DataType::LargeBinary => {
            let arr = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            use base64::Engine;
            let encoded = base64::engine::general_purpose::STANDARD.encode(arr.value(idx));
            serde_json::Value::String(encoded)
        }

        // Date types
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = arr.value(idx);
            if let Some(date) = NaiveDate::from_num_days_from_ce_opt(days + 719163) {
                // 719163 = days from year 0 to 1970-01-01
                serde_json::Value::String(date.format("%Y-%m-%d").to_string())
            } else {
                serde_json::Value::Number(days.into())
            }
        }
        DataType::Date64 => {
            let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
            let millis = arr.value(idx);
            if let Some(dt) = DateTime::from_timestamp_millis(millis) {
                serde_json::Value::String(dt.format("%Y-%m-%d").to_string())
            } else {
                serde_json::Value::Number(millis.into())
            }
        }

        // Timestamp types - convert to ISO 8601 string
        DataType::Timestamp(TimeUnit::Second, tz) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            let secs = arr.value(idx);
            timestamp_to_json(secs, 0, tz.as_deref())
        }
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let millis = arr.value(idx);
            let secs = millis / 1000;
            let nanos = ((millis % 1000) * 1_000_000) as u32;
            timestamp_to_json(secs, nanos, tz.as_deref())
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let micros = arr.value(idx);
            let secs = micros / 1_000_000;
            let nanos = ((micros % 1_000_000) * 1000) as u32;
            timestamp_to_json(secs, nanos, tz.as_deref())
        }
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let nanos_total = arr.value(idx);
            let secs = nanos_total / 1_000_000_000;
            let nanos = (nanos_total % 1_000_000_000) as u32;
            timestamp_to_json(secs, nanos, tz.as_deref())
        }

        // List types
        DataType::List(_) => {
            let arr = array.as_any().downcast_ref::<ListArray>().unwrap();
            let list_arr = arr.value(idx);
            let values: Vec<serde_json::Value> = (0..list_arr.len())
                .map(|i| array_value_to_json(list_arr.as_ref(), i))
                .collect();
            serde_json::Value::Array(values)
        }
        DataType::LargeList(_) => {
            let arr = array.as_any().downcast_ref::<LargeListArray>().unwrap();
            let list_arr = arr.value(idx);
            let values: Vec<serde_json::Value> = (0..list_arr.len())
                .map(|i| array_value_to_json(list_arr.as_ref(), i))
                .collect();
            serde_json::Value::Array(values)
        }

        // Struct type
        DataType::Struct(fields) => {
            let arr = array.as_any().downcast_ref::<StructArray>().unwrap();
            let mut obj = serde_json::Map::new();
            for (field_idx, field) in fields.iter().enumerate() {
                let col = arr.column(field_idx);
                let value = array_value_to_json(col.as_ref(), idx);
                obj.insert(field.name().clone(), value);
            }
            serde_json::Value::Object(obj)
        }

        // For unsupported types, return the debug representation
        _ => serde_json::Value::String(format!("<unsupported: {:?}>", array.data_type())),
    }
}

/// Convert a timestamp (seconds + nanoseconds) to a JSON string.
fn timestamp_to_json(secs: i64, nanos: u32, tz: Option<&str>) -> serde_json::Value {
    match DateTime::<Utc>::from_timestamp(secs, nanos) {
        Some(dt) => {
            // Format as ISO 8601
            let formatted = if tz.is_some() {
                dt.to_rfc3339()
            } else {
                dt.format("%Y-%m-%dT%H:%M:%S%.fZ").to_string()
            };
            serde_json::Value::String(formatted)
        }
        None => {
            // If we can't parse the timestamp, return the raw value
            serde_json::Value::Number(secs.into())
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
    use arrow::array::{
        ArrayRef, Int32Array, Int64Array, ListBuilder, StringArray, StringBuilder, StructArray,
        TimestampMillisecondArray,
    };
    use arrow::datatypes::{DataType, Field, Fields, Schema};

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let id_array = Int64Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec![Some("Alice"), Some("Bob"), None]);

        RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)]).unwrap()
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

    #[test]
    fn test_timestamp_to_json() {
        // Create a batch with timestamp column
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]));

        let id_array = Int64Array::from(vec![1, 2]);
        // 1704067200000 = 2024-01-01T00:00:00Z in milliseconds
        let ts_array = TimestampMillisecondArray::from(vec![1704067200000, 1704153600000]);

        let batch = RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(ts_array)])
            .unwrap();

        let rows = record_batch_to_json(&batch);

        assert_eq!(rows.len(), 2);
        // Timestamp should be formatted as ISO 8601 string
        let ts1 = rows[0]["created_at"].as_str().unwrap();
        assert!(ts1.starts_with("2024-01-01T"));

        let ts2 = rows[1]["created_at"].as_str().unwrap();
        assert!(ts2.starts_with("2024-01-02T"));
    }

    #[test]
    fn test_list_to_json() {
        // Create a batch with list column
        let mut list_builder = ListBuilder::new(Int32Array::builder(10));

        // First row: [1, 2, 3]
        list_builder.values().append_value(1);
        list_builder.values().append_value(2);
        list_builder.values().append_value(3);
        list_builder.append(true);

        // Second row: [4, 5]
        list_builder.values().append_value(4);
        list_builder.values().append_value(5);
        list_builder.append(true);

        // Third row: null list
        list_builder.append(false);

        let list_array = list_builder.finish();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "scores",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
        ]));

        let id_array = Int64Array::from(vec![1, 2, 3]);

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(id_array), Arc::new(list_array)],
        )
        .unwrap();

        let rows = record_batch_to_json(&batch);

        assert_eq!(rows.len(), 3);

        // First row should have [1, 2, 3]
        let scores1 = rows[0]["scores"].as_array().unwrap();
        assert_eq!(scores1.len(), 3);
        assert_eq!(scores1[0], 1);
        assert_eq!(scores1[1], 2);
        assert_eq!(scores1[2], 3);

        // Second row should have [4, 5]
        let scores2 = rows[1]["scores"].as_array().unwrap();
        assert_eq!(scores2.len(), 2);
        assert_eq!(scores2[0], 4);
        assert_eq!(scores2[1], 5);

        // Third row should be null
        assert!(rows[2]["scores"].is_null());
    }

    #[test]
    fn test_struct_to_json() {
        // Create a struct with name and age fields
        let name_array: ArrayRef = Arc::new(StringArray::from(vec!["Alice", "Bob"]));
        let age_array: ArrayRef = Arc::new(Int32Array::from(vec![30, 25]));

        let struct_fields = Fields::from(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, false),
        ]);

        let struct_array = StructArray::new(
            struct_fields.clone(),
            vec![name_array, age_array],
            None, // null buffer
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("person", DataType::Struct(struct_fields), false),
        ]));

        let id_array = Int64Array::from(vec![1, 2]);

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(id_array), Arc::new(struct_array)],
        )
        .unwrap();

        let rows = record_batch_to_json(&batch);

        assert_eq!(rows.len(), 2);

        // First row's struct
        let person1 = rows[0]["person"].as_object().unwrap();
        assert_eq!(person1["name"], "Alice");
        assert_eq!(person1["age"], 30);

        // Second row's struct
        let person2 = rows[1]["person"].as_object().unwrap();
        assert_eq!(person2["name"], "Bob");
        assert_eq!(person2["age"], 25);
    }

    #[test]
    fn test_nested_list_of_strings() {
        // Create a batch with list of strings column
        let mut list_builder = ListBuilder::new(StringBuilder::new());

        // First row: ["a", "b"]
        list_builder.values().append_value("a");
        list_builder.values().append_value("b");
        list_builder.append(true);

        // Second row: ["c"]
        list_builder.values().append_value("c");
        list_builder.append(true);

        let list_array = list_builder.finish();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        )]));

        let batch = RecordBatch::try_new(schema, vec![Arc::new(list_array)]).unwrap();

        let rows = record_batch_to_json(&batch);

        assert_eq!(rows.len(), 2);

        let tags1 = rows[0]["tags"].as_array().unwrap();
        assert_eq!(tags1.len(), 2);
        assert_eq!(tags1[0], "a");
        assert_eq!(tags1[1], "b");

        let tags2 = rows[1]["tags"].as_array().unwrap();
        assert_eq!(tags2.len(), 1);
        assert_eq!(tags2[0], "c");
    }
}
