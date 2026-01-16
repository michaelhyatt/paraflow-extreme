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

mod expression;
mod filter;

pub use expression::{PartitioningExpression, PathSegment, PrefixGenerationResult};
pub use filter::{PartitionFilter, PartitionFilters};
