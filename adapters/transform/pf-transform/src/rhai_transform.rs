//! RhaiTransform - the main transform implementation.

use crate::builtin::register_builtin_functions;
use crate::config::{ErrorPolicy, TransformConfig};
use crate::conversion::{arrow_row_to_dynamic, maps_to_record_batch};
use arrow::record_batch::RecordBatch;
use aws_sdk_s3::Client as S3Client;
use pf_enrichment::EnrichmentRegistry;
use pf_error::{ReaderError, Result, TransformError};
use pf_traits::Transform;
use rhai::{AST, Dynamic, Engine, Map, Scope};
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, trace, warn};

/// Rhai-based transform that executes user scripts against each record.
///
/// Implements the [`Transform`] trait for use in the Paraflow pipeline.
pub struct RhaiTransform {
    /// Pre-compiled Rhai AST for efficient repeated execution.
    ast: AST,

    /// Rhai engine with registered functions.
    engine: Engine,

    /// Error handling policy.
    error_policy: ErrorPolicy,

    /// Transform name for logging/metrics.
    name: String,
}

impl std::fmt::Debug for RhaiTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RhaiTransform")
            .field("name", &self.name)
            .field("error_policy", &self.error_policy)
            .finish_non_exhaustive()
    }
}

impl RhaiTransform {
    /// Creates a new RhaiTransform from configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Transform configuration with script and error policy
    /// * `enrichment` - Optional enrichment registry for lookup functions
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Neither script nor script_file is provided
    /// - Script compilation fails
    pub fn new(
        config: &TransformConfig,
        enrichment: Option<Arc<EnrichmentRegistry>>,
    ) -> Result<Self> {
        let mut engine = Self::create_engine();

        // Register built-in functions
        register_builtin_functions(&mut engine);

        // Register enrichment functions if provided
        if let Some(registry) = enrichment {
            registry.register_functions(&mut engine);
        }

        // Get script content
        let script = config
            .script
            .as_ref()
            .ok_or_else(|| TransformError::Compilation("No script provided".to_string()))?;

        // Compile the script
        let ast = engine
            .compile(script)
            .map_err(|e| TransformError::Compilation(format!("Script compilation failed: {e}")))?;

        debug!(error_policy = ?config.error_policy, "Created RhaiTransform");

        Ok(Self {
            ast,
            engine,
            error_policy: config.error_policy,
            name: "rhai_transform".to_string(),
        })
    }

    /// Creates a new RhaiTransform by loading script from a file.
    ///
    /// Supports both local files and S3 URIs.
    pub async fn from_file(
        config: &TransformConfig,
        enrichment: Option<Arc<EnrichmentRegistry>>,
        s3_client: Option<&S3Client>,
    ) -> Result<Self> {
        let script_path = config
            .script_file
            .as_ref()
            .ok_or_else(|| TransformError::Compilation("No script_file provided".to_string()))?;

        let script = load_script(script_path, s3_client).await?;

        let new_config = TransformConfig {
            script: Some(script),
            script_file: None,
            error_policy: config.error_policy,
        };

        Self::new(&new_config, enrichment)
    }

    /// Creates a Rhai engine with safety limits.
    fn create_engine() -> Engine {
        let mut engine = Engine::new();

        // Set engine limits for safety
        engine.set_max_expr_depths(64, 64);
        engine.set_max_operations(100_000);
        engine.set_max_string_size(1_000_000);
        engine.set_max_array_size(10_000);
        engine.set_max_map_size(10_000);

        // Optimize for performance
        engine.set_optimization_level(rhai::OptimizationLevel::Full);

        // Limit recursion depth
        engine.set_max_call_levels(16);

        engine
    }

    /// Sets the transform name.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    /// Executes the Rhai script for a single record.
    ///
    /// # Returns
    ///
    /// - `Ok(Some(map))` - Transformed record
    /// - `Ok(None)` - Record should be dropped (script returned `()`)
    /// - `Err(e)` - Script execution failed
    #[allow(dead_code)]
    fn execute_script(&self, record: Dynamic) -> Result<Option<Map>> {
        let mut scope = Scope::new();
        scope.push("record", record);

        let result: Dynamic = self
            .engine
            .eval_ast_with_scope(&mut scope, &self.ast)
            .map_err(|e| TransformError::Execution(format!("Script execution failed: {e}")))?;

        // Handle script return value
        if result.is_unit() {
            // Script returned () - drop this record
            Ok(None)
        } else if result.is_map() {
            // Script returned a map - use as transformed record
            Ok(Some(result.cast::<Map>()))
        } else {
            Err(TransformError::Execution(
                "Script must return a map or () to drop record".to_string(),
            )
            .into())
        }
    }

    /// Converts a Dynamic value to a Map if possible.
    fn dynamic_to_map(&self, value: Dynamic) -> Option<Map> {
        if value.is_map() {
            Some(value.cast::<Map>())
        } else {
            None
        }
    }
}

impl Transform for RhaiTransform {
    fn apply(&self, batch: Arc<RecordBatch>) -> Result<Arc<RecordBatch>> {
        let schema = batch.schema();
        let num_rows = batch.num_rows();

        trace!(num_rows = num_rows, "Applying RhaiTransform");

        // Process each row through Rhai
        let mut results: Vec<Option<Map>> = Vec::with_capacity(num_rows);
        let mut scope = Scope::new();

        for row_idx in 0..num_rows {
            // Clear scope but keep capacity (optimization)
            scope.clear();

            // Convert Arrow row to Rhai map
            let record = arrow_row_to_dynamic(&batch, row_idx)?;
            scope.push("record", record.clone());

            // Execute script
            match self
                .engine
                .eval_ast_with_scope::<Dynamic>(&mut scope, &self.ast)
            {
                Ok(result) => {
                    if result.is_unit() {
                        results.push(None);
                    } else if result.is_map() {
                        results.push(Some(result.cast::<Map>()));
                    } else {
                        match self.error_policy {
                            ErrorPolicy::Drop => {
                                warn!(row = row_idx, "Script returned non-map, dropping record");
                                results.push(None);
                            }
                            ErrorPolicy::Fail => {
                                return Err(TransformError::Execution(format!(
                                    "Script returned non-map at row {row_idx}"
                                ))
                                .into());
                            }
                            ErrorPolicy::Passthrough => {
                                results.push(self.dynamic_to_map(record));
                            }
                        }
                    }
                }
                Err(e) => match self.error_policy {
                    ErrorPolicy::Drop => {
                        debug!(row = row_idx, error = %e, "Script error, dropping record");
                        results.push(None);
                    }
                    ErrorPolicy::Fail => {
                        return Err(TransformError::Execution(format!(
                            "Transform failed at row {row_idx}: {e}"
                        ))
                        .into());
                    }
                    ErrorPolicy::Passthrough => {
                        debug!(row = row_idx, error = %e, "Script error, passing through original");
                        results.push(self.dynamic_to_map(record));
                    }
                },
            }
        }

        // Convert results back to Arrow RecordBatch
        maps_to_record_batch(&results, &schema)
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Loads a script from file (local or S3).
async fn load_script(path: &str, s3_client: Option<&S3Client>) -> Result<String> {
    if path.starts_with("s3://") {
        load_script_from_s3(path, s3_client).await
    } else {
        load_script_from_file(path).await
    }
}

async fn load_script_from_file(path: &str) -> Result<String> {
    let path = Path::new(path);
    if !path.exists() {
        return Err(ReaderError::NotFound(path.display().to_string()).into());
    }

    tokio::fs::read_to_string(path)
        .await
        .map_err(|e| ReaderError::Io(format!("Failed to read {}: {e}", path.display())).into())
}

async fn load_script_from_s3(uri: &str, s3_client: Option<&S3Client>) -> Result<String> {
    let client = s3_client
        .ok_or_else(|| ReaderError::S3Error("S3 client not provided for S3 URI".to_string()))?;

    let uri = uri
        .strip_prefix("s3://")
        .ok_or_else(|| ReaderError::InvalidUri(format!("Not an S3 URI: {uri}")))?;

    let (bucket, key) = uri
        .split_once('/')
        .ok_or_else(|| ReaderError::InvalidUri(format!("Missing key in S3 URI: {uri}")))?;

    debug!(bucket = %bucket, key = %key, "Loading script from S3");

    let response = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|e| ReaderError::S3Error(format!("Failed to get object: {e}")))?;

    let bytes = response
        .body
        .collect()
        .await
        .map_err(|e| ReaderError::S3Error(format!("Failed to read body: {e}")))?;

    String::from_utf8(bytes.to_vec())
        .map_err(|e| ReaderError::InvalidFormat(format!("Script is not valid UTF-8: {e}")).into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_test_batch(rows: usize) -> Arc<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("level", DataType::Utf8, true),
        ]));

        let ids: Vec<i64> = (0..rows as i64).collect();
        let names: Vec<&str> = (0..rows)
            .map(|i| if i % 2 == 0 { "alice" } else { "bob" })
            .collect();
        let levels: Vec<Option<&str>> = (0..rows)
            .map(|i| match i % 3 {
                0 => Some("DEBUG"),
                1 => Some("INFO"),
                _ => Some("ERROR"),
            })
            .collect();

        Arc::new(
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(StringArray::from(names)),
                    Arc::new(StringArray::from(levels)),
                ],
            )
            .unwrap(),
        )
    }

    #[test]
    fn test_simple_transform() {
        let config = TransformConfig::with_script(
            r#"
            record.processed = true;
            record
            "#,
        );

        let transform = RhaiTransform::new(&config, None).unwrap();
        let batch = create_test_batch(3);
        let result = transform.apply(batch).unwrap();

        assert_eq!(result.num_rows(), 3);
        assert!(result.schema().field_with_name("processed").is_ok());
    }

    #[test]
    fn test_filtering() {
        let config = TransformConfig::with_script(
            r#"
            if record.level == "DEBUG" {
                ()  // Drop DEBUG records
            } else {
                record
            }
            "#,
        );

        let transform = RhaiTransform::new(&config, None).unwrap();
        let batch = create_test_batch(6);
        let result = transform.apply(batch).unwrap();

        // 2 out of 6 should be DEBUG (indices 0, 3)
        assert_eq!(result.num_rows(), 4);
    }

    #[test]
    fn test_field_modification() {
        let config = TransformConfig::with_script(
            r#"
            record.upper_name = to_uppercase(record.name);
            record
            "#,
        );

        let transform = RhaiTransform::new(&config, None).unwrap();
        let batch = create_test_batch(2);
        let result = transform.apply(batch).unwrap();

        assert_eq!(result.num_rows(), 2);
        assert!(result.schema().field_with_name("upper_name").is_ok());
    }

    #[test]
    fn test_builtin_functions() {
        let config = TransformConfig::with_script(
            r#"
            record.uuid = uuid();
            record.ts = timestamp();
            record.unix_ts = unix_timestamp();
            record
            "#,
        );

        let transform = RhaiTransform::new(&config, None).unwrap();
        let batch = create_test_batch(1);
        let result = transform.apply(batch).unwrap();

        assert_eq!(result.num_rows(), 1);
        assert!(result.schema().field_with_name("uuid").is_ok());
        assert!(result.schema().field_with_name("ts").is_ok());
        assert!(result.schema().field_with_name("unix_ts").is_ok());
    }

    #[test]
    fn test_error_policy_drop() {
        let config = TransformConfig::with_script(
            r#"
            if record.id == 1 {
                throw "Intentional error";
            }
            record
            "#,
        )
        .with_error_policy(ErrorPolicy::Drop);

        let transform = RhaiTransform::new(&config, None).unwrap();
        let batch = create_test_batch(3);
        let result = transform.apply(batch).unwrap();

        // Row 1 should be dropped due to error
        assert_eq!(result.num_rows(), 2);
    }

    #[test]
    fn test_error_policy_fail() {
        let config = TransformConfig::with_script(
            r#"
            if record.id == 1 {
                throw "Intentional error";
            }
            record
            "#,
        )
        .with_error_policy(ErrorPolicy::Fail);

        let transform = RhaiTransform::new(&config, None).unwrap();
        let batch = create_test_batch(3);
        let result = transform.apply(batch);

        assert!(result.is_err());
    }

    #[test]
    fn test_error_policy_passthrough() {
        let config = TransformConfig::with_script(
            r#"
            if record.id == 1 {
                throw "Intentional error";
            }
            record.modified = true;
            record
            "#,
        )
        .with_error_policy(ErrorPolicy::Passthrough);

        let transform = RhaiTransform::new(&config, None).unwrap();
        let batch = create_test_batch(3);
        let result = transform.apply(batch).unwrap();

        // All rows should be present
        assert_eq!(result.num_rows(), 3);
    }

    #[test]
    fn test_compilation_error() {
        let config = TransformConfig::with_script("this is not valid rhai {{{");
        let result = RhaiTransform::new(&config, None);

        assert!(result.is_err());
    }

    #[test]
    fn test_empty_batch() {
        let config = TransformConfig::with_script("record");
        let transform = RhaiTransform::new(&config, None).unwrap();
        let batch = create_test_batch(0);
        let result = transform.apply(batch).unwrap();

        assert_eq!(result.num_rows(), 0);
    }

    #[tokio::test]
    async fn test_load_script_from_file() {
        let script = "record.loaded = true; record";
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(script.as_bytes()).unwrap();

        let config = TransformConfig::with_script_file(file.path().to_str().unwrap());
        let transform = RhaiTransform::from_file(&config, None, None).await.unwrap();

        let batch = create_test_batch(1);
        let result = transform.apply(batch).unwrap();

        assert_eq!(result.num_rows(), 1);
        assert!(result.schema().field_with_name("loaded").is_ok());
    }

    #[test]
    fn test_transform_name() {
        let config = TransformConfig::with_script("record");
        let transform = RhaiTransform::new(&config, None)
            .unwrap()
            .with_name("custom_transform");

        assert_eq!(transform.name(), "custom_transform");
    }
}
