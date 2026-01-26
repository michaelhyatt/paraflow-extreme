//! Rhai-based transform for Paraflow Extreme pipelines.
//!
//! This crate provides [`RhaiTransform`], a scriptable data transformation layer
//! that processes Arrow RecordBatches using the Rhai scripting language.
//!
//! # Features
//!
//! - **Per-record transformation**: Each record passes through a Rhai script
//! - **Filtering**: Scripts can drop records by returning `()`
//! - **Enrichment**: Integration with [`pf_enrichment`] for lookup tables
//! - **Built-in functions**: UUID generation, timestamps, IP utilities
//! - **Error policies**: Drop, fail, or passthrough on errors
//!
//! # Example
//!
//! ```rust,ignore
//! use pf_transform::{RhaiTransform, TransformConfig, ErrorPolicy};
//!
//! let config = TransformConfig {
//!     script: Some(r#"
//!         record.processed_at = timestamp();
//!         if record.level == "DEBUG" {
//!             return ();  // Drop debug logs
//!         }
//!         record
//!     "#.to_string()),
//!     script_file: None,
//!     error_policy: ErrorPolicy::Drop,
//! };
//!
//! let transform = RhaiTransform::new(&config, None)?;
//! let result = transform.apply(batch)?;
//! ```

mod builtin;
mod config;
mod conversion;
mod rhai_transform;

pub use config::{ErrorPolicy, TransformConfig};
pub use rhai_transform::RhaiTransform;

// Re-export enrichment types for convenience
pub use pf_enrichment::{
    CidrTable, EnrichmentRegistry, EnrichmentRow, EnrichmentTableConfig, ExactTable, MatchType,
};
