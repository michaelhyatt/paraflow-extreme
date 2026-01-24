//! Partitioning support for S3 discovery.
//!
//! This module provides functionality for parsing partitioning expressions
//! and generating S3 prefixes based on partition filters.
//!
//! # Overview
//!
//! Partitioning expressions describe the hierarchical structure of S3 paths,
//! allowing efficient discovery of partitioned data (like Hive-style partitioning).
//!
//! # Example
//!
//! ```
//! use pf_discoverer::partition::{PartitioningExpression, PartitionFilters};
//!
//! // Parse an expression like "logs/${index}/${year}/${month}/"
//! let expr = PartitioningExpression::parse("logs/${index}/${year}/").unwrap();
//!
//! // Get the variable names
//! assert_eq!(expr.variables(), vec!["index", "year"]);
//!
//! // Create filters for specific partition values
//! let mut filters = PartitionFilters::new();
//! filters.add_filter("index", &["nginx", "apache"]);
//! filters.add_filter("year", &["2024", "2025"]);
//!
//! // Generate prefixes to scan
//! let prefixes = expr.generate_prefixes(&filters);
//! // Results: ["logs/nginx/2024/", "logs/nginx/2025/", "logs/apache/2024/", "logs/apache/2025/"]
//! ```
//!
//! # Time-Based Partitioning
//!
//! The module also supports time-based partitioning with date range filters:
//!
//! ```
//! use pf_discoverer::partition::{PartitioningExpression, PartitionFilters, expand_all_prefixes};
//!
//! // Parse expression with time format specifiers
//! let expr = PartitioningExpression::parse(
//!     "data/YEAR=${_time:%Y}/MONTH=${_time:%m}/${element}/"
//! ).unwrap();
//!
//! // Add time range and variable filters
//! let mut filters = PartitionFilters::new();
//! filters.parse_and_add("_time=2022-01-01..2022-01-05").unwrap();
//! filters.add_filter("element", &["cpu"]);
//!
//! // Generate prefixes (automatically deduplicated)
//! let prefixes = expand_all_prefixes(&expr, &filters).unwrap();
//! // Result: ["data/YEAR=2022/MONTH=01/cpu/"] (deduplicated since all dates in same month)
//! ```

mod expression;
mod filter;
mod time;

pub use expression::{PartitioningExpression, PathSegment, PrefixGenerationResult, TimeFormatSpec};
pub use filter::{PartitionFilter, PartitionFilters, TimeRangeFilter};
pub use time::expand_all_prefixes;
