//! Core traits for Paraflow Extreme.
//!
//! This crate defines the main abstractions for the pipeline:
//! - [`WorkQueue`] - Trait for work queue backends (SQS, in-memory)
//! - [`StreamingReader`] - Trait for streaming file readers (Parquet, NDJSON)
//! - [`BatchIndexer`] - Trait for batch indexers (Elasticsearch, stdout)
//! - [`Transform`] - Trait for record transformations (Rhai, enrichment)

pub mod indexer;
pub mod queue;
pub mod reader;
pub mod transform;

pub use indexer::*;
pub use queue::*;
pub use reader::*;
pub use transform::*;
