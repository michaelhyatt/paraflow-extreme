//! Composite filter for combining multiple filters.
//!
//! Provides the ability to combine multiple filters with AND logic.

use super::Filter;
use crate::s3::S3Object;

/// A composite filter that combines multiple filters with AND logic.
///
/// An S3 object passes the filter only if it passes ALL constituent filters.
/// This allows building complex filtering rules from simpler components.
///
/// # Example
///
/// ```
/// use pf_discoverer::filter::{CompositeFilter, PatternFilter, SizeFilter, Filter};
/// use pf_discoverer::s3::S3Object;
///
/// // Create a filter that matches parquet files larger than 1KB
/// let filter = CompositeFilter::new()
///     .with_filter(Box::new(PatternFilter::new("*.parquet").unwrap()))
///     .with_filter(Box::new(SizeFilter::new().with_min_size(1024)));
///
/// let large_parquet = S3Object { key: "data.parquet".to_string(), size: 2048, last_modified: None };
/// let small_parquet = S3Object { key: "data.parquet".to_string(), size: 100, last_modified: None };
/// let large_json = S3Object { key: "data.json".to_string(), size: 2048, last_modified: None };
///
/// // Only large_parquet passes all filters
/// assert!(filter.matches(&large_parquet));
/// assert!(!filter.matches(&small_parquet));  // Too small
/// assert!(!filter.matches(&large_json));     // Wrong extension
/// ```
pub struct CompositeFilter {
    filters: Vec<Box<dyn Filter>>,
}

impl Default for CompositeFilter {
    fn default() -> Self {
        Self::new()
    }
}

impl CompositeFilter {
    /// Create a new empty composite filter.
    ///
    /// An empty filter matches all objects.
    pub fn new() -> Self {
        Self {
            filters: Vec::new(),
        }
    }

    /// Add a filter to the composite (builder pattern).
    pub fn with_filter(mut self, filter: Box<dyn Filter>) -> Self {
        self.filters.push(filter);
        self
    }

    /// Add a filter to the composite.
    pub fn add_filter(&mut self, filter: Box<dyn Filter>) {
        self.filters.push(filter);
    }

    /// Get the number of filters in the composite.
    pub fn len(&self) -> usize {
        self.filters.len()
    }

    /// Check if the composite has no filters.
    pub fn is_empty(&self) -> bool {
        self.filters.is_empty()
    }

    /// Get descriptions of all filters.
    pub fn filter_descriptions(&self) -> Vec<String> {
        self.filters.iter().map(|f| f.description()).collect()
    }
}

impl Filter for CompositeFilter {
    fn matches(&self, obj: &S3Object) -> bool {
        // Empty filter matches everything
        if self.filters.is_empty() {
            return true;
        }
        // AND logic: all filters must match
        // Short-circuit on first failure
        self.filters.iter().all(|f| f.matches(obj))
    }

    fn description(&self) -> String {
        if self.filters.is_empty() {
            "composite(empty)".to_string()
        } else {
            let descriptions: Vec<String> = self.filters.iter().map(|f| f.description()).collect();
            format!("composite({})", descriptions.join(" AND "))
        }
    }
}

// Implement Debug manually since Box<dyn Filter> doesn't implement Debug
impl std::fmt::Debug for CompositeFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompositeFilter")
            .field("filters", &self.filter_descriptions())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::{DateFilter, MatchAllFilter, PatternFilter, SizeFilter};
    use chrono::{Duration, Utc};

    fn make_obj(key: &str, size: u64) -> S3Object {
        S3Object {
            key: key.to_string(),
            size,
            last_modified: None,
        }
    }

    fn make_obj_with_date(key: &str, size: u64, last_modified: chrono::DateTime<Utc>) -> S3Object {
        S3Object {
            key: key.to_string(),
            size,
            last_modified: Some(last_modified),
        }
    }

    #[test]
    fn test_composite_empty() {
        let filter = CompositeFilter::new();

        assert!(filter.is_empty());
        assert_eq!(filter.len(), 0);
        assert!(filter.matches(&make_obj("any.file", 0)));
    }

    #[test]
    fn test_composite_single_filter() {
        let filter =
            CompositeFilter::new().with_filter(Box::new(PatternFilter::new("*.parquet").unwrap()));

        assert_eq!(filter.len(), 1);
        assert!(filter.matches(&make_obj("data.parquet", 0)));
        assert!(!filter.matches(&make_obj("data.json", 0)));
    }

    #[test]
    fn test_composite_multiple_filters() {
        let filter = CompositeFilter::new()
            .with_filter(Box::new(PatternFilter::new("*.parquet").unwrap()))
            .with_filter(Box::new(SizeFilter::new().with_min_size(1024)));

        assert_eq!(filter.len(), 2);

        // Must match both: parquet AND >= 1024 bytes
        assert!(filter.matches(&make_obj("data.parquet", 2048)));
        assert!(!filter.matches(&make_obj("data.parquet", 100))); // Too small
        assert!(!filter.matches(&make_obj("data.json", 2048))); // Wrong extension
        assert!(!filter.matches(&make_obj("data.json", 100))); // Both wrong
    }

    #[test]
    fn test_composite_three_filters() {
        let cutoff = Utc::now() - Duration::hours(24);
        let filter = CompositeFilter::new()
            .with_filter(Box::new(PatternFilter::new("*.parquet").unwrap()))
            .with_filter(Box::new(SizeFilter::new().with_min_size(1024)))
            .with_filter(Box::new(DateFilter::new().with_modified_after(cutoff)));

        // File that matches all three
        let recent_large_parquet =
            make_obj_with_date("data.parquet", 2048, Utc::now() - Duration::hours(1));
        assert!(filter.matches(&recent_large_parquet));

        // File that's too old
        let old_large_parquet =
            make_obj_with_date("data.parquet", 2048, Utc::now() - Duration::days(7));
        assert!(!filter.matches(&old_large_parquet));
    }

    #[test]
    fn test_composite_short_circuit() {
        // First filter fails, shouldn't even check second
        let filter = CompositeFilter::new()
            .with_filter(Box::new(PatternFilter::new("*.parquet").unwrap()))
            .with_filter(Box::new(SizeFilter::new().with_min_size(1024)));

        // JSON file - pattern fails first, size shouldn't be checked
        assert!(!filter.matches(&make_obj("data.json", 2048)));
    }

    #[test]
    fn test_composite_add_filter() {
        let mut filter = CompositeFilter::new();
        assert!(filter.is_empty());

        filter.add_filter(Box::new(PatternFilter::new("*.parquet").unwrap()));
        assert_eq!(filter.len(), 1);

        filter.add_filter(Box::new(SizeFilter::new().with_min_size(1024)));
        assert_eq!(filter.len(), 2);
    }

    #[test]
    fn test_composite_description() {
        let filter = CompositeFilter::new()
            .with_filter(Box::new(PatternFilter::new("*.parquet").unwrap()))
            .with_filter(Box::new(SizeFilter::new().with_min_size(1024)));

        let desc = filter.description();
        assert!(desc.contains("composite"));
        assert!(desc.contains("AND"));
        assert!(desc.contains("pattern"));
        assert!(desc.contains("size"));
    }

    #[test]
    fn test_composite_empty_description() {
        let filter = CompositeFilter::new();
        assert_eq!(filter.description(), "composite(empty)");
    }

    #[test]
    fn test_composite_filter_descriptions() {
        let filter = CompositeFilter::new()
            .with_filter(Box::new(PatternFilter::new("*.parquet").unwrap()))
            .with_filter(Box::new(MatchAllFilter::new()));

        let descriptions = filter.filter_descriptions();
        assert_eq!(descriptions.len(), 2);
        assert!(descriptions[0].contains("pattern"));
        assert_eq!(descriptions[1], "match_all");
    }

    #[test]
    fn test_composite_debug() {
        let filter =
            CompositeFilter::new().with_filter(Box::new(PatternFilter::new("*.parquet").unwrap()));

        let debug_str = format!("{:?}", filter);
        assert!(debug_str.contains("CompositeFilter"));
        assert!(debug_str.contains("filters"));
    }
}
