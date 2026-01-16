//! Partition filters for S3 discovery.
//!
//! Provides filtering of partition values during prefix generation.

use chrono::NaiveDate;
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

/// Iterator over dates in a range.
struct DateRangeIterator {
    current: NaiveDate,
    end: NaiveDate,
}

impl Iterator for DateRangeIterator {
    type Item = NaiveDate;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current <= self.end {
            let date = self.current;
            self.current = self.current.succ_opt().unwrap_or(self.current);
            Some(date)
        } else {
            None
        }
    }
}

/// A date range filter for time-based partitioning.
///
/// Filters S3 prefixes to only include paths matching dates in the specified range.
/// Used with partitioning expressions containing `${_time:FORMAT}` specifiers.
///
/// # Example
///
/// ```
/// use pf_discoverer::partition::TimeRangeFilter;
///
/// let filter = TimeRangeFilter::parse("2022-01-01..2022-01-05").unwrap();
/// assert_eq!(filter.dates().count(), 5);
/// ```
#[derive(Debug, Clone)]
pub struct TimeRangeFilter {
    /// Start date (inclusive)
    start: NaiveDate,
    /// End date (inclusive)
    end: NaiveDate,
}

impl TimeRangeFilter {
    /// Parse a date range filter from string format.
    ///
    /// Expected format: `YYYY-MM-DD..YYYY-MM-DD`
    ///
    /// # Arguments
    ///
    /// * `input` - The date range string to parse
    ///
    /// # Returns
    ///
    /// A parsed `TimeRangeFilter` or an error if the format is invalid.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The format is not `YYYY-MM-DD..YYYY-MM-DD`
    /// - The dates are invalid
    /// - The end date is before the start date
    pub fn parse(input: &str) -> Result<Self> {
        let input = input.trim();

        let parts: Vec<&str> = input.splitn(2, "..").collect();
        if parts.len() != 2 {
            return Err(PfError::Config(format!(
                "Invalid date range format: '{}'. Expected 'YYYY-MM-DD..YYYY-MM-DD'",
                input
            )));
        }

        let start_str = parts[0].trim();
        let end_str = parts[1].trim();

        let start = NaiveDate::parse_from_str(start_str, "%Y-%m-%d").map_err(|_| {
            PfError::Config(format!(
                "Invalid start date '{}'. Expected format: YYYY-MM-DD",
                start_str
            ))
        })?;

        let end = NaiveDate::parse_from_str(end_str, "%Y-%m-%d").map_err(|_| {
            PfError::Config(format!(
                "Invalid end date '{}'. Expected format: YYYY-MM-DD",
                end_str
            ))
        })?;

        if end < start {
            return Err(PfError::Config(format!(
                "End date {} is before start date {}",
                end, start
            )));
        }

        Ok(Self { start, end })
    }

    /// Get the start date.
    pub fn start(&self) -> NaiveDate {
        self.start
    }

    /// Get the end date.
    pub fn end(&self) -> NaiveDate {
        self.end
    }

    /// Iterate over all dates in the range (inclusive).
    pub fn dates(&self) -> impl Iterator<Item = NaiveDate> {
        DateRangeIterator {
            current: self.start,
            end: self.end,
        }
    }
}

/// Collection of partition filters.
///
/// Holds filters for multiple partition variables with AND logic between fields.
/// Also supports time-based filtering with the special `_time` field.
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
    /// Time-based range filter for the special `_time` field.
    time_filter: Option<TimeRangeFilter>,
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

    /// Parse and add a filter, detecting time range vs value list.
    ///
    /// This method handles both standard partition filters (`field=value1,value2`)
    /// and time range filters (`_time=YYYY-MM-DD..YYYY-MM-DD`).
    ///
    /// # Arguments
    ///
    /// * `input` - The filter string to parse
    ///
    /// # Returns
    ///
    /// `Ok(())` if the filter was added successfully, or an error if parsing failed.
    pub fn parse_and_add(&mut self, input: &str) -> Result<()> {
        let input = input.trim();

        // Check for time range filter
        if input.starts_with("_time=") && input.contains("..") {
            let range_str = input
                .strip_prefix("_time=")
                .expect("checked prefix above");
            self.time_filter = Some(TimeRangeFilter::parse(range_str)?);
        } else {
            let filter = PartitionFilter::parse(input)?;
            self.add_parsed_filter(filter);
        }
        Ok(())
    }

    /// Get the time range filter if set.
    pub fn time_filter(&self) -> Option<&TimeRangeFilter> {
        self.time_filter.as_ref()
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

    // TimeRangeFilter tests

    #[test]
    fn test_time_range_filter_parse() {
        let filter = TimeRangeFilter::parse("2022-01-01..2022-01-05").unwrap();
        assert_eq!(filter.start(), NaiveDate::from_ymd_opt(2022, 1, 1).unwrap());
        assert_eq!(filter.end(), NaiveDate::from_ymd_opt(2022, 1, 5).unwrap());
    }

    #[test]
    fn test_time_range_filter_parse_with_spaces() {
        let filter = TimeRangeFilter::parse("  2022-01-01  ..  2022-01-05  ").unwrap();
        assert_eq!(filter.start(), NaiveDate::from_ymd_opt(2022, 1, 1).unwrap());
        assert_eq!(filter.end(), NaiveDate::from_ymd_opt(2022, 1, 5).unwrap());
    }

    #[test]
    fn test_time_range_filter_parse_same_date() {
        let filter = TimeRangeFilter::parse("2022-01-01..2022-01-01").unwrap();
        assert_eq!(filter.start(), filter.end());
    }

    #[test]
    fn test_time_range_filter_parse_invalid_format() {
        assert!(TimeRangeFilter::parse("2022-01-01").is_err());
        assert!(TimeRangeFilter::parse("foo..bar").is_err());
        assert!(TimeRangeFilter::parse("2022/01/01..2022/01/05").is_err());
    }

    #[test]
    fn test_time_range_filter_parse_end_before_start() {
        assert!(TimeRangeFilter::parse("2022-01-05..2022-01-01").is_err());
    }

    #[test]
    fn test_time_range_filter_dates_iterator() {
        let filter = TimeRangeFilter::parse("2022-01-01..2022-01-05").unwrap();
        let dates: Vec<NaiveDate> = filter.dates().collect();

        assert_eq!(dates.len(), 5);
        assert_eq!(dates[0], NaiveDate::from_ymd_opt(2022, 1, 1).unwrap());
        assert_eq!(dates[1], NaiveDate::from_ymd_opt(2022, 1, 2).unwrap());
        assert_eq!(dates[2], NaiveDate::from_ymd_opt(2022, 1, 3).unwrap());
        assert_eq!(dates[3], NaiveDate::from_ymd_opt(2022, 1, 4).unwrap());
        assert_eq!(dates[4], NaiveDate::from_ymd_opt(2022, 1, 5).unwrap());
    }

    #[test]
    fn test_time_range_filter_dates_single_day() {
        let filter = TimeRangeFilter::parse("2022-01-01..2022-01-01").unwrap();
        let dates: Vec<NaiveDate> = filter.dates().collect();

        assert_eq!(dates.len(), 1);
        assert_eq!(dates[0], NaiveDate::from_ymd_opt(2022, 1, 1).unwrap());
    }

    // PartitionFilters.parse_and_add tests

    #[test]
    fn test_partition_filters_parse_and_add_value_filter() {
        let mut filters = PartitionFilters::new();
        filters.parse_and_add("index=nginx,apache").unwrap();

        assert!(filters.has_filter("index"));
        assert!(filters.time_filter().is_none());
    }

    #[test]
    fn test_partition_filters_parse_and_add_time_filter() {
        let mut filters = PartitionFilters::new();
        filters.parse_and_add("_time=2022-01-01..2022-01-05").unwrap();

        assert!(!filters.has_filter("_time")); // _time is not a regular filter
        assert!(filters.time_filter().is_some());
        assert_eq!(filters.time_filter().unwrap().dates().count(), 5);
    }

    #[test]
    fn test_partition_filters_parse_and_add_both() {
        let mut filters = PartitionFilters::new();
        filters.parse_and_add("_time=2022-01-01..2022-01-05").unwrap();
        filters.parse_and_add("element=cpu,memory").unwrap();

        assert!(filters.has_filter("element"));
        assert!(filters.time_filter().is_some());
    }
}
