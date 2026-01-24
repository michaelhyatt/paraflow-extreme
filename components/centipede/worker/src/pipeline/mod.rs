//! Processing pipeline for per-thread file processing.
//!
//! Each thread has its own pipeline instance that processes files
//! through Reader → Transform → Destination stages.

mod processor;
mod thread_stats;

pub use processor::{Pipeline, ProcessingResult};
pub use thread_stats::ThreadStats;
