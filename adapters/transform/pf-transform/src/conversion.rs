//! Arrow <-> Rhai type conversion utilities.

use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use pf_error::{Result, TransformError};
use rhai::{Dynamic, Map};
use std::sync::Arc;
use tracing::warn;

/// Converts an Arrow row to a Rhai Dynamic map.
pub fn arrow_row_to_dynamic(batch: &RecordBatch, row_idx: usize) -> Result<Dynamic> {
    let mut map = Map::new();

    for (col_idx, field) in batch.schema().fields().iter().enumerate() {
        let column = batch.column(col_idx);
        let value = arrow_value_to_dynamic(column, row_idx)?;
        map.insert(field.name().clone().into(), value);
    }

    Ok(map.into())
}

/// Converts an Arrow array value at index to Rhai Dynamic.
pub fn arrow_value_to_dynamic(array: &ArrayRef, idx: usize) -> Result<Dynamic> {
    if array.is_null(idx) {
        return Ok(Dynamic::UNIT);
    }

    let value = match array.data_type() {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Dynamic::from(arr.value(idx))
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
            Dynamic::from(arr.value(idx) as i64)
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
            Dynamic::from(arr.value(idx) as i64)
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
            Dynamic::from(arr.value(idx) as i64)
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            Dynamic::from(arr.value(idx))
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            Dynamic::from(arr.value(idx) as i64)
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            Dynamic::from(arr.value(idx) as i64)
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            Dynamic::from(arr.value(idx) as i64)
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            // Note: Large u64 values may lose precision when converted to i64
            Dynamic::from(arr.value(idx) as i64)
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
            Dynamic::from(arr.value(idx) as f64)
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            Dynamic::from(arr.value(idx))
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            Dynamic::from(arr.value(idx).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Dynamic::from(arr.value(idx).to_string())
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            let ts = arr.value(idx);
            let dt = chrono::DateTime::from_timestamp_micros(ts)
                .map(|d| d.to_rfc3339())
                .unwrap_or_default();
            Dynamic::from(dt)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let ts = arr.value(idx);
            let dt = chrono::DateTime::from_timestamp_millis(ts)
                .map(|d| d.to_rfc3339())
                .unwrap_or_default();
            Dynamic::from(dt)
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap();
            let ts = arr.value(idx);
            let dt = chrono::DateTime::from_timestamp(ts, 0)
                .map(|d| d.to_rfc3339())
                .unwrap_or_default();
            Dynamic::from(dt)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap();
            let ts = arr.value(idx);
            // Convert nanos to micros for timestamp creation
            let dt = chrono::DateTime::from_timestamp_nanos(ts).to_rfc3339();
            Dynamic::from(dt)
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = arr.value(idx);
            // Convert days since epoch to date string
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719163) // Add days from year 1 to 1970
                .map(|d| d.to_string())
                .unwrap_or_default();
            Dynamic::from(date)
        }
        DataType::Date64 => {
            let arr = array.as_any().downcast_ref::<Date64Array>().unwrap();
            let ms = arr.value(idx);
            let dt = chrono::DateTime::from_timestamp_millis(ms)
                .map(|d| d.format("%Y-%m-%d").to_string())
                .unwrap_or_default();
            Dynamic::from(dt)
        }
        DataType::List(_) => {
            let arr = array.as_any().downcast_ref::<ListArray>().unwrap();
            let values = arr.value(idx);
            let mut rhai_arr = rhai::Array::new();
            for i in 0..values.len() {
                rhai_arr.push(arrow_value_to_dynamic(&values, i)?);
            }
            Dynamic::from(rhai_arr)
        }
        DataType::Struct(_) => {
            let arr = array.as_any().downcast_ref::<StructArray>().unwrap();
            let mut map = Map::new();
            for (field_idx, field) in arr.fields().iter().enumerate() {
                let column = arr.column(field_idx);
                let value = arrow_value_to_dynamic(column, idx)?;
                map.insert(field.name().clone().into(), value);
            }
            Dynamic::from(map)
        }
        _ => {
            // For unsupported types, convert to string representation
            warn!(data_type = ?array.data_type(), "Unsupported Arrow type, converting to string");
            Dynamic::from(format!("{:?}", array))
        }
    };

    Ok(value)
}

/// Infers Arrow schema from Rhai maps.
///
/// Uses the first non-empty map to determine column types.
/// New columns are inferred based on the Rhai Dynamic type.
pub fn infer_schema_from_maps(results: &[&Map], original_schema: &SchemaRef) -> Result<Schema> {
    if results.is_empty() {
        return Ok(Schema::new(original_schema.fields().to_vec()));
    }

    // Start with original schema columns
    let mut fields: Vec<Field> = Vec::new();
    let mut seen_columns: std::collections::HashSet<String> = std::collections::HashSet::new();

    // Keep original columns that are still present
    for field in original_schema.fields() {
        seen_columns.insert(field.name().clone());
        fields.push(field.as_ref().clone());
    }

    // Find new columns from the first result
    let first_map = results[0];
    for (key, value) in first_map.iter() {
        let col_name = key.to_string();
        if !seen_columns.contains(&col_name) {
            let data_type = infer_arrow_type(value);
            fields.push(Field::new(&col_name, data_type, true));
            seen_columns.insert(col_name);
        }
    }

    Ok(Schema::new(fields))
}

/// Infers Arrow DataType from a Rhai Dynamic value.
fn infer_arrow_type(value: &Dynamic) -> DataType {
    if value.is_unit() {
        DataType::Utf8 // Null values default to string
    } else if value.is_bool() {
        DataType::Boolean
    } else if value.is_int() {
        DataType::Int64
    } else if value.is_float() {
        DataType::Float64
    } else {
        // Strings, arrays, maps, and other types stored as JSON strings
        DataType::Utf8
    }
}

/// Converts Rhai maps back to an Arrow RecordBatch.
pub fn maps_to_record_batch(
    results: &[Option<Map>],
    original_schema: &SchemaRef,
) -> Result<Arc<RecordBatch>> {
    // Filter out None results (dropped records)
    let valid_results: Vec<&Map> = results.iter().filter_map(|r| r.as_ref()).collect();

    if valid_results.is_empty() {
        return Ok(Arc::new(RecordBatch::new_empty(original_schema.clone())));
    }

    // Infer schema from results
    let schema = infer_schema_from_maps(&valid_results, original_schema)?;
    let schema_ref = Arc::new(schema.clone());

    // Build Arrow arrays from maps
    let columns = build_arrow_columns(&valid_results, &schema)?;

    let batch = RecordBatch::try_new(schema_ref, columns)
        .map_err(|e| TransformError::Execution(format!("Failed to create RecordBatch: {e}")))?;

    Ok(Arc::new(batch))
}

/// Builds Arrow columns from Rhai maps.
fn build_arrow_columns(results: &[&Map], schema: &Schema) -> Result<Vec<ArrayRef>> {
    let mut columns = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let column = build_arrow_column(results, field.name(), field.data_type())?;
        columns.push(column);
    }

    Ok(columns)
}

/// Builds a single Arrow column from Rhai maps.
fn build_arrow_column(
    results: &[&Map],
    field_name: &str,
    data_type: &DataType,
) -> Result<ArrayRef> {
    match data_type {
        DataType::Boolean => {
            let values: Vec<Option<bool>> = results
                .iter()
                .map(|map| {
                    map.get(field_name).and_then(|v| {
                        if v.is_unit() {
                            None
                        } else if v.is_bool() {
                            Some(v.as_bool().unwrap())
                        } else {
                            None
                        }
                    })
                })
                .collect();
            Ok(Arc::new(BooleanArray::from(values)))
        }
        DataType::Int64 => {
            let values: Vec<Option<i64>> = results
                .iter()
                .map(|map| {
                    map.get(field_name).and_then(|v| {
                        if v.is_unit() {
                            None
                        } else if v.is_int() {
                            Some(v.as_int().unwrap())
                        } else {
                            None
                        }
                    })
                })
                .collect();
            Ok(Arc::new(Int64Array::from(values)))
        }
        DataType::Float64 => {
            let values: Vec<Option<f64>> = results
                .iter()
                .map(|map| {
                    map.get(field_name).and_then(|v| {
                        if v.is_unit() {
                            None
                        } else if v.is_float() {
                            Some(v.as_float().unwrap())
                        } else if v.is_int() {
                            Some(v.as_int().unwrap() as f64)
                        } else {
                            None
                        }
                    })
                })
                .collect();
            Ok(Arc::new(Float64Array::from(values)))
        }
        _ => {
            // Default: convert to string
            let values: Vec<Option<String>> = results
                .iter()
                .map(|map| {
                    map.get(field_name).and_then(|v| {
                        if v.is_unit() {
                            None
                        } else if v.is_string() {
                            v.clone().into_string().ok()
                        } else if v.is_array() || v.is_map() {
                            // Serialize arrays/maps as JSON
                            serde_json::to_string(&dynamic_to_json(v)).ok()
                        } else {
                            Some(v.to_string())
                        }
                    })
                })
                .collect();
            Ok(Arc::new(StringArray::from(values)))
        }
    }
}

/// Converts a Rhai Dynamic to a serde_json Value for serialization.
fn dynamic_to_json(value: &Dynamic) -> serde_json::Value {
    if value.is_unit() {
        serde_json::Value::Null
    } else if value.is_bool() {
        serde_json::Value::Bool(value.as_bool().unwrap())
    } else if value.is_int() {
        serde_json::Value::Number(serde_json::Number::from(value.as_int().unwrap()))
    } else if value.is_float() {
        serde_json::Number::from_f64(value.as_float().unwrap())
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null)
    } else if value.is_string() {
        serde_json::Value::String(value.clone().into_string().unwrap())
    } else if value.is_array() {
        let arr = value.clone().into_array().unwrap();
        let json_arr: Vec<serde_json::Value> = arr.iter().map(dynamic_to_json).collect();
        serde_json::Value::Array(json_arr)
    } else if value.is_map() {
        let map = value.clone().cast::<Map>();
        let json_obj: serde_json::Map<String, serde_json::Value> = map
            .iter()
            .map(|(k, v)| (k.to_string(), dynamic_to_json(v)))
            .collect();
        serde_json::Value::Object(json_obj)
    } else {
        serde_json::Value::String(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("active", DataType::Boolean, true),
        ]));

        let id_array = Int64Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec!["alice", "bob", "charlie"]);
        let active_array = BooleanArray::from(vec![Some(true), Some(false), None]);

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(active_array),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_arrow_row_to_dynamic() {
        let batch = create_test_batch();

        let row0 = arrow_row_to_dynamic(&batch, 0).unwrap();
        assert!(row0.is_map());

        let map = row0.cast::<Map>();
        assert_eq!(map.get("id").unwrap().as_int().unwrap(), 1);
        assert_eq!(
            map.get("name").unwrap().clone().into_string().unwrap(),
            "alice"
        );
        assert!(map.get("active").unwrap().as_bool().unwrap());
    }

    #[test]
    fn test_arrow_null_value() {
        let batch = create_test_batch();

        let row2 = arrow_row_to_dynamic(&batch, 2).unwrap();
        let map = row2.cast::<Map>();

        // active is null for row 2
        assert!(map.get("active").unwrap().is_unit());
    }

    #[test]
    fn test_maps_to_record_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let mut map1 = Map::new();
        map1.insert("id".into(), Dynamic::from(1_i64));
        map1.insert("name".into(), Dynamic::from("alice".to_string()));

        let mut map2 = Map::new();
        map2.insert("id".into(), Dynamic::from(2_i64));
        map2.insert("name".into(), Dynamic::from("bob".to_string()));

        let results = vec![Some(map1), None, Some(map2)];
        let batch = maps_to_record_batch(&results, &schema).unwrap();

        assert_eq!(batch.num_rows(), 2); // One was filtered
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_infer_schema_new_columns() {
        let original_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let mut map = Map::new();
        map.insert("id".into(), Dynamic::from(1_i64));
        map.insert("new_field".into(), Dynamic::from("value".to_string()));
        map.insert("count".into(), Dynamic::from(42_i64));

        let results = vec![&map];
        let schema = infer_schema_from_maps(&results, &original_schema).unwrap();

        assert_eq!(schema.fields().len(), 3);
        assert!(schema.field_with_name("id").is_ok());
        assert!(schema.field_with_name("new_field").is_ok());
        assert!(schema.field_with_name("count").is_ok());
    }

    #[test]
    fn test_empty_results() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let results: Vec<Option<Map>> = vec![None, None];
        let batch = maps_to_record_batch(&results, &schema).unwrap();

        assert_eq!(batch.num_rows(), 0);
    }
}
