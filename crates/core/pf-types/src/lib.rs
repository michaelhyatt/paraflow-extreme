//! Core types for Paraflow Extreme.
//!
//! This crate provides the foundational types used throughout the system:
//! - [`WorkItem`] - Queue message payload describing a file to process
//! - [`Batch`] - Zero-copy Arrow RecordBatch wrapper with metadata
//! - [`ProcessingCheckpoint`] - Progress tracking for file processing

pub mod batch;
pub mod checkpoint;
pub mod config;
pub mod work_item;

pub use batch::*;
pub use checkpoint::*;
pub use config::*;
pub use work_item::*;
