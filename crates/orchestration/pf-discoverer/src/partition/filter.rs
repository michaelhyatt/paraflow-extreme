//! Partition filters for S3 discovery.
//!
//! Provides filtering of partition values during prefix generation.

use pf_error::{PfError, Result};
use std::collections::{HashMap, HashSet};

/// A filter for a single partition field.
///
/// Represents allowed values for a specific partition variable.
/// Multiple values have OR logic - any value in the set is allowed.
///
/// # Example
///
/// ```
/// use pf_discoverer::partition::PartitionFilter;
///
/// // Parse a filter from CLI format
/// let filter = PartitionFilter::parse("year=2024,2025").unwrap();
/// assert_eq!(filter.field(), "year");
/// assert!(filter.allows("2024"));
/// assert!(filter.allows("2025"));
/// assert!(!filter.allows("2023"));
/// ```
#[derive(Debug, Clone)]
pub struct PartitionFilter {
    /// The field/variable name this filter applies to.
    field: String,
    /// The set of allowed values.
    values: HashSet<String>,
}

impl PartitionFilter {
    /// Create a new partition filter.
    ///
    /// # Arguments
    ///
    /// * `field` - The partition field name
    /// * `values` - Slice of allowed values
    pub fn new(field: impl Into<String>, values: &[&str]) -> Self {
        Self {
            field: field.into(),
            values: values.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// Parse a partition filter from string format.
    ///
    /// Expected format: `field=value1,value2,value3`
    ///
    /// # Arguments
    ///
    /// * `input` - The filter string to parse
    ///
    /// # Returns
    ///
    /// A parsed `PartitionFilter` or an error if the format is invalid.
    pub fn parse(input: &str) -> Result<Self> {
        let input = input.trim();

        let parts: Vec<&str> = input.splitn(2, '=').collect();
        if parts.len() != 2 {
            return Err(PfError::Config(format!(
                "Invalid partition filter format: '{}'. Expected 'field=value1,value2'",
                input
            )));
        }

        let field = parts[0].trim();
        if field.is_empty() {
            return Err(PfError::Config(format!(
                "Empty field name in partition filter: '{}'",
                input
            )));
        }

        let values_str = parts[1].trim();
        if values_str.is_empty() {
            return Err(PfError::Config(format!(
                "No values specified in partition filter: '{}'",
                input
            )));
        }

        let values: HashSet<String> = values_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if values.is_empty() {
            return Err(PfError::Config(format!(
                "No valid values in partition filter: '{}'",
                input
            )));
        }

        Ok(Self {
            field: field.to_string(),
            values,
        })
    }

    /// Get the field name this filter applies to.
    pub fn field(&self) -> &str {
        &self.field
    }

    /// Get the allowed values as a slice.
    pub fn values(&self) -> Vec<&str> {
        self.values.iter().map(|s| s.as_str()).collect()
    }

    /// Check if a value is allowed by this filter.
    pub fn allows(&self, value: &str) -> bool {
        self.values.contains(value)
    }

    /// Get the number of allowed values.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if the filter has no values (invalid state).
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Merge another filter into this one (OR logic).
    ///
    /// Only works if both filters are for the same field.
    pub fn merge(&mut self, other: &PartitionFilter) -> Result<()> {
        if self.field != other.field {
            return Err(PfError::Config(format!(
                "Cannot merge filters for different fields: '{}' and '{}'",
                self.field, other.field
            )));
        }
        self.values.extend(other.values.iter().cloned());
        Ok(())
    }
}

/// Collection of partition filters.
///
/// Holds filters for multiple partition variables with AND logic between fields.
///
/// # Example
///
/// ```
/// use pf_discoverer::partition::PartitionFilters;
///
/// let mut filters = PartitionFilters::new();
/// filters.add_filter("index", &["nginx", "apache"]);
/// filters.add_filter("year", &["2024"]);
///
/// assert!(filters.has_filter("index"));
/// assert!(filters.has_filter("year"));
/// assert!(!filters.has_filter("month"));
/// ```
#[derive(Debug, Clone, Default)]
pub struct PartitionFilters {
    /// Map from field name to filter.
    filters: HashMap<String, PartitionFilter>,
}

impl PartitionFilters {
    /// Create an empty filter collection.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a filter for a field.
    ///
    /// If a filter for the same field already exists, the values are merged.
    pub fn add_filter(&mut self, field: &str, values: &[&str]) {
        let new_filter = PartitionFilter::new(field, values);
        if let Some(existing) = self.filters.get_mut(field) {
            // Merge with existing filter
            let _ = existing.merge(&new_filter);
        } else {
            self.filters.insert(field.to_string(), new_filter);
        }
    }

    /// Add a parsed filter.
    ///
    /// If a filter for the same field already exists, the values are merged.
    pub fn add_parsed_filter(&mut self, filter: PartitionFilter) {
        let field = filter.field.clone();
        if let Some(existing) = self.filters.get_mut(&field) {
            let _ = existing.merge(&filter);
        } else {
            self.filters.insert(field, filter);
        }
    }

    /// Check if a filter exists for a field.
    pub fn has_filter(&self, field: &str) -> bool {
        self.filters.contains_key(field)
    }

    /// Get the filter for a field.
    pub fn get(&self, field: &str) -> Option<&PartitionFilter> {
        self.filters.get(field)
    }

    /// Get the values for a field.
    pub fn get_values(&self, field: &str) -> Option<Vec<&str>> {
        self.filters.get(field).map(|f| f.values())
    }

    /// Get the number of filters.
    pub fn len(&self) -> usize {
        self.filters.len()
    }

    /// Check if there are no filters.
    pub fn is_empty(&self) -> bool {
        self.filters.is_empty()
    }

    /// Get all field names with filters.
    pub fn fields(&self) -> Vec<&str> {
        self.filters.keys().map(|s| s.as_str()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_filter_new() {
        let filter = PartitionFilter::new("year", &["2024", "2025"]);
        assert_eq!(filter.field(), "year");
        assert!(filter.allows("2024"));
        assert!(filter.allows("2025"));
        assert!(!filter.allows("2023"));
    }

    #[test]
    fn test_partition_filter_parse() {
        let filter = PartitionFilter::parse("index=nginx,apache").unwrap();
        assert_eq!(filter.field(), "index");
        assert_eq!(filter.len(), 2);
        assert!(filter.allows("nginx"));
        assert!(filter.allows("apache"));
    }

    #[test]
    fn test_partition_filter_parse_single_value() {
        let filter = PartitionFilter::parse("year=2024").unwrap();
        assert_eq!(filter.field(), "year");
        assert_eq!(filter.len(), 1);
        assert!(filter.allows("2024"));
    }

    #[test]
    fn test_partition_filter_parse_with_spaces() {
        let filter = PartitionFilter::parse("  index = nginx , apache  ").unwrap();
        assert_eq!(filter.field(), "index");
        assert!(filter.allows("nginx"));
        assert!(filter.allows("apache"));
    }

    #[test]
    fn test_partition_filter_parse_invalid_no_equals() {
        assert!(PartitionFilter::parse("index:nginx").is_err());
    }

    #[test]
    fn test_partition_filter_parse_invalid_empty_field() {
        assert!(PartitionFilter::parse("=nginx").is_err());
    }

    #[test]
    fn test_partition_filter_parse_invalid_empty_values() {
        assert!(PartitionFilter::parse("index=").is_err());
    }

    #[test]
    fn test_partition_filter_merge() {
        let mut filter1 = PartitionFilter::new("year", &["2024"]);
        let filter2 = PartitionFilter::new("year", &["2025"]);

        filter1.merge(&filter2).unwrap();

        assert_eq!(filter1.len(), 2);
        assert!(filter1.allows("2024"));
        assert!(filter1.allows("2025"));
    }

    #[test]
    fn test_partition_filter_merge_different_fields() {
        let mut filter1 = PartitionFilter::new("year", &["2024"]);
        let filter2 = PartitionFilter::new("month", &["01"]);

        assert!(filter1.merge(&filter2).is_err());
    }

    #[test]
    fn test_partition_filters_new() {
        let filters = PartitionFilters::new();
        assert!(filters.is_empty());
        assert_eq!(filters.len(), 0);
    }

    #[test]
    fn test_partition_filters_add() {
        let mut filters = PartitionFilters::new();
        filters.add_filter("index", &["nginx"]);
        filters.add_filter("year", &["2024", "2025"]);

        assert!(!filters.is_empty());
        assert_eq!(filters.len(), 2);
        assert!(filters.has_filter("index"));
        assert!(filters.has_filter("year"));
        assert!(!filters.has_filter("month"));
    }

    #[test]
    fn test_partition_filters_add_merge() {
        let mut filters = PartitionFilters::new();
        filters.add_filter("year", &["2024"]);
        filters.add_filter("year", &["2025"]);

        // Should merge into single filter
        assert_eq!(filters.len(), 1);

        let values = filters.get_values("year").unwrap();
        assert!(values.contains(&"2024"));
        assert!(values.contains(&"2025"));
    }

    #[test]
    fn test_partition_filters_get() {
        let mut filters = PartitionFilters::new();
        filters.add_filter("index", &["nginx"]);

        assert!(filters.get("index").is_some());
        assert!(filters.get("other").is_none());
    }

    #[test]
    fn test_partition_filters_get_values() {
        let mut filters = PartitionFilters::new();
        filters.add_filter("index", &["nginx", "apache"]);

        let values = filters.get_values("index").unwrap();
        assert_eq!(values.len(), 2);
        assert!(values.contains(&"nginx"));
        assert!(values.contains(&"apache"));

        assert!(filters.get_values("other").is_none());
    }

    #[test]
    fn test_partition_filters_fields() {
        let mut filters = PartitionFilters::new();
        filters.add_filter("index", &["nginx"]);
        filters.add_filter("year", &["2024"]);

        let fields = filters.fields();
        assert_eq!(fields.len(), 2);
        assert!(fields.contains(&"index"));
        assert!(fields.contains(&"year"));
    }

    #[test]
    fn test_partition_filters_add_parsed() {
        let mut filters = PartitionFilters::new();
        let parsed = PartitionFilter::parse("index=nginx,apache").unwrap();
        filters.add_parsed_filter(parsed);

        assert!(filters.has_filter("index"));
        assert_eq!(filters.get_values("index").unwrap().len(), 2);
    }
}
