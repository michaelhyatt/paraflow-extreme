//! Filter system for S3 object discovery.
//!
//! This module provides a trait-based filter infrastructure supporting multiple
//! criteria types with composable AND/OR logic. Filters can be used to select
//! files based on patterns, size, modification date, and other criteria.
//!
//! # Examples
//!
//! ```
//! use pf_discoverer::filter::{Filter, PatternFilter, SizeFilter, CompositeFilter};
//! use pf_discoverer::s3::S3Object;
//!
//! // Create individual filters
//! let pattern = PatternFilter::new("*.parquet").unwrap();
//! let size = SizeFilter::new().with_min_size(1024);
//!
//! // Combine into a composite filter (AND logic)
//! let composite = CompositeFilter::new()
//!     .with_filter(Box::new(pattern))
//!     .with_filter(Box::new(size));
//! ```

mod composite;
mod date;
mod pattern;
mod size;

pub use composite::CompositeFilter;
pub use date::{DateFilter, parse_date};
pub use pattern::{MatchAllFilter, MultiPatternFilter, PatternFilter};
pub use size::SizeFilter;

use crate::s3::S3Object;

/// Trait for filtering S3 objects during discovery.
///
/// Implementations determine which S3 objects should be included in the
/// discovery results based on various criteria like pattern matching,
/// file size, modification date, etc.
///
/// Filters are composable through the [`CompositeFilter`] type which
/// applies AND logic between multiple filters.
pub trait Filter: Send + Sync {
    /// Check if an S3 object should be included.
    ///
    /// Returns `true` if the object passes the filter criteria,
    /// `false` if it should be excluded.
    fn matches(&self, obj: &S3Object) -> bool;

    /// Get a human-readable description of this filter.
    ///
    /// Used for logging and debugging purposes.
    fn description(&self) -> String;
}
