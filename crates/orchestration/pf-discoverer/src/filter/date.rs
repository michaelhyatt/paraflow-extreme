//! Date-based filtering for file discovery.
//!
//! Provides filtering of S3 objects based on modification date ranges.

use chrono::{DateTime, Utc};

use super::Filter;
use crate::s3::S3Object;

/// A filter for matching files based on modification date constraints.
///
/// Can filter by "modified after" date, "modified before" date, or both.
/// A file passes the filter if its modification date falls within the specified range.
///
/// # Example
///
/// ```
/// use chrono::{Utc, Duration};
/// use pf_discoverer::filter::DateFilter;
///
/// // Only files modified in the last 24 hours
/// let recent_filter = DateFilter::new()
///     .with_modified_after(Utc::now() - Duration::hours(24));
///
/// // Only files modified before a specific date
/// let before_filter = DateFilter::new()
///     .with_modified_before(Utc::now());
/// ```
#[derive(Debug, Clone, Default)]
pub struct DateFilter {
    /// Include files modified after this time (exclusive)
    modified_after: Option<DateTime<Utc>>,
    /// Include files modified before this time (exclusive)
    modified_before: Option<DateTime<Utc>>,
}

impl DateFilter {
    /// Create a new date filter with no constraints.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the "modified after" constraint.
    ///
    /// Files modified at or before this time will be filtered out.
    pub fn with_modified_after(mut self, time: DateTime<Utc>) -> Self {
        self.modified_after = Some(time);
        self
    }

    /// Set the "modified before" constraint.
    ///
    /// Files modified at or after this time will be filtered out.
    pub fn with_modified_before(mut self, time: DateTime<Utc>) -> Self {
        self.modified_before = Some(time);
        self
    }

    /// Check if a modification time matches the filter constraints.
    ///
    /// Returns `true` if no last_modified is available (permissive behavior).
    pub fn matches_time(&self, last_modified: Option<DateTime<Utc>>) -> bool {
        let Some(modified) = last_modified else {
            // No timestamp available - pass through (permissive)
            return true;
        };

        if let Some(after) = self.modified_after {
            if modified <= after {
                return false;
            }
        }
        if let Some(before) = self.modified_before {
            if modified >= before {
                return false;
            }
        }
        true
    }

    /// Get the "modified after" constraint, if set.
    pub fn modified_after(&self) -> Option<DateTime<Utc>> {
        self.modified_after
    }

    /// Get the "modified before" constraint, if set.
    pub fn modified_before(&self) -> Option<DateTime<Utc>> {
        self.modified_before
    }

    /// Check if this filter has any constraints.
    pub fn has_constraints(&self) -> bool {
        self.modified_after.is_some() || self.modified_before.is_some()
    }
}

impl Filter for DateFilter {
    fn matches(&self, obj: &S3Object) -> bool {
        self.matches_time(obj.last_modified)
    }

    fn description(&self) -> String {
        match (&self.modified_after, &self.modified_before) {
            (Some(after), Some(before)) => {
                format!(
                    "date(after={}, before={})",
                    after.format("%Y-%m-%d %H:%M:%S"),
                    before.format("%Y-%m-%d %H:%M:%S")
                )
            }
            (Some(after), None) => {
                format!("date(after={})", after.format("%Y-%m-%d %H:%M:%S"))
            }
            (None, Some(before)) => {
                format!("date(before={})", before.format("%Y-%m-%d %H:%M:%S"))
            }
            (None, None) => "date(any)".to_string(),
        }
    }
}

/// Parse a date string in various formats.
///
/// Supported formats:
/// - ISO 8601: `2024-01-15T10:30:00Z`
/// - Date only: `2024-01-15` (assumes 00:00:00 UTC)
/// - Relative: `-24h`, `-7d` (hours/days ago from now)
pub fn parse_date(input: &str) -> Result<DateTime<Utc>, String> {
    let input = input.trim();

    // Try relative format first
    if input.starts_with('-') {
        return parse_relative_date(input);
    }

    // Try ISO 8601 with time
    if let Ok(dt) = DateTime::parse_from_rfc3339(input) {
        return Ok(dt.with_timezone(&Utc));
    }

    // Try date only format (YYYY-MM-DD)
    if let Ok(date) = chrono::NaiveDate::parse_from_str(input, "%Y-%m-%d") {
        let datetime = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| format!("Invalid date: {input}"))?;
        return Ok(DateTime::from_naive_utc_and_offset(datetime, Utc));
    }

    Err(format!(
        "Invalid date format: {input}. Expected ISO 8601 (2024-01-15T10:30:00Z), \
         date only (2024-01-15), or relative (-24h, -7d)"
    ))
}

/// Parse a relative date string like "-24h" or "-7d".
fn parse_relative_date(input: &str) -> Result<DateTime<Utc>, String> {
    let input = input.trim_start_matches('-');

    if input.is_empty() {
        return Err("Empty relative date".to_string());
    }

    let (num_str, unit) = if input.ends_with('h') || input.ends_with('H') {
        (&input[..input.len() - 1], 'h')
    } else if input.ends_with('d') || input.ends_with('D') {
        (&input[..input.len() - 1], 'd')
    } else if input.ends_with('w') || input.ends_with('W') {
        (&input[..input.len() - 1], 'w')
    } else {
        return Err(format!(
            "Invalid relative date unit: {input}. Use 'h' (hours), 'd' (days), or 'w' (weeks)"
        ));
    };

    let num: i64 = num_str
        .parse()
        .map_err(|_| format!("Invalid number in relative date: {num_str}"))?;

    let duration = match unit {
        'h' => chrono::Duration::hours(num),
        'd' => chrono::Duration::days(num),
        'w' => chrono::Duration::weeks(num),
        _ => unreachable!(),
    };

    Ok(Utc::now() - duration)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn make_obj(last_modified: Option<DateTime<Utc>>) -> S3Object {
        S3Object {
            key: "test.parquet".to_string(),
            size: 1024,
            last_modified,
        }
    }

    #[test]
    fn test_date_filter_no_constraints() {
        let filter = DateFilter::new();

        assert!(filter.matches(&make_obj(None)));
        assert!(filter.matches(&make_obj(Some(Utc::now()))));
        assert!(!filter.has_constraints());
    }

    #[test]
    fn test_date_filter_no_timestamp() {
        // Files without timestamps should pass (permissive)
        let filter = DateFilter::new().with_modified_after(Utc::now() - Duration::days(1));

        assert!(filter.matches(&make_obj(None)));
    }

    #[test]
    fn test_date_filter_modified_after() {
        let cutoff = Utc::now() - Duration::hours(12);
        let filter = DateFilter::new().with_modified_after(cutoff);

        let old_file = make_obj(Some(Utc::now() - Duration::days(1)));
        let recent_file = make_obj(Some(Utc::now() - Duration::hours(1)));

        assert!(!filter.matches(&old_file));
        assert!(filter.matches(&recent_file));
    }

    #[test]
    fn test_date_filter_modified_before() {
        let cutoff = Utc::now() - Duration::hours(12);
        let filter = DateFilter::new().with_modified_before(cutoff);

        let old_file = make_obj(Some(Utc::now() - Duration::days(1)));
        let recent_file = make_obj(Some(Utc::now() - Duration::hours(1)));

        assert!(filter.matches(&old_file));
        assert!(!filter.matches(&recent_file));
    }

    #[test]
    fn test_date_filter_range() {
        let after = Utc::now() - Duration::days(7);
        let before = Utc::now() - Duration::days(1);
        let filter = DateFilter::new()
            .with_modified_after(after)
            .with_modified_before(before);

        let very_old = make_obj(Some(Utc::now() - Duration::days(30)));
        let in_range = make_obj(Some(Utc::now() - Duration::days(3)));
        let recent = make_obj(Some(Utc::now() - Duration::hours(1)));

        assert!(!filter.matches(&very_old));
        assert!(filter.matches(&in_range));
        assert!(!filter.matches(&recent));
    }

    #[test]
    fn test_date_filter_getters() {
        let after = Utc::now() - Duration::days(1);
        let before = Utc::now();
        let filter = DateFilter::new()
            .with_modified_after(after)
            .with_modified_before(before);

        assert_eq!(filter.modified_after(), Some(after));
        assert_eq!(filter.modified_before(), Some(before));
        assert!(filter.has_constraints());
    }

    #[test]
    fn test_date_filter_description() {
        let filter_none = DateFilter::new();
        assert_eq!(filter_none.description(), "date(any)");

        let filter_after = DateFilter::new().with_modified_after(Utc::now());
        assert!(filter_after.description().contains("after="));

        let filter_before = DateFilter::new().with_modified_before(Utc::now());
        assert!(filter_before.description().contains("before="));

        let filter_both = DateFilter::new()
            .with_modified_after(Utc::now())
            .with_modified_before(Utc::now());
        assert!(filter_both.description().contains("after="));
        assert!(filter_both.description().contains("before="));
    }

    #[test]
    fn test_parse_date_iso8601() {
        let result = parse_date("2024-01-15T10:30:00Z");
        assert!(result.is_ok());
        let dt = result.unwrap();
        assert_eq!(dt.year(), 2024);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 15);
    }

    #[test]
    fn test_parse_date_date_only() {
        let result = parse_date("2024-01-15");
        assert!(result.is_ok());
        let dt = result.unwrap();
        assert_eq!(dt.year(), 2024);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 15);
        assert_eq!(dt.hour(), 0);
    }

    #[test]
    fn test_parse_date_relative_hours() {
        let result = parse_date("-24h");
        assert!(result.is_ok());
        let dt = result.unwrap();
        let expected = Utc::now() - Duration::hours(24);
        // Allow some tolerance for test execution time
        assert!((dt - expected).num_seconds().abs() < 2);
    }

    #[test]
    fn test_parse_date_relative_days() {
        let result = parse_date("-7d");
        assert!(result.is_ok());
        let dt = result.unwrap();
        let expected = Utc::now() - Duration::days(7);
        assert!((dt - expected).num_seconds().abs() < 2);
    }

    #[test]
    fn test_parse_date_relative_weeks() {
        let result = parse_date("-2w");
        assert!(result.is_ok());
        let dt = result.unwrap();
        let expected = Utc::now() - Duration::weeks(2);
        assert!((dt - expected).num_seconds().abs() < 2);
    }

    #[test]
    fn test_parse_date_invalid() {
        assert!(parse_date("not a date").is_err());
        assert!(parse_date("").is_err());
        assert!(parse_date("-").is_err());
        assert!(parse_date("-24x").is_err()); // invalid unit
    }

    use chrono::Datelike;
    use chrono::Timelike;
}
