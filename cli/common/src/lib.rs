//! Shared utilities for paraflow-extreme CLI binaries.
//!
//! This crate provides common functionality shared between `pf-discoverer`
//! and `pf-worker` CLI applications.

pub mod args;
pub mod format;
pub mod logging;

pub use args::LogLevel;
pub use format::{format_bytes, format_number};
pub use logging::init_logging;
