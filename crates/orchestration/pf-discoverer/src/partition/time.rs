//! Time-based prefix generation for partitioned data.
//!
//! Provides functionality to expand partitioning expressions with time format
//! specifiers into concrete S3 prefixes based on date ranges.

use chrono::NaiveDate;
use pf_error::{PfError, Result};
use std::collections::{HashMap, HashSet};

use super::expression::{PartitioningExpression, PathSegment};
use super::filter::PartitionFilters;

/// Generate all S3 prefixes by expanding date range and variable combinations.
///
/// This function handles the expansion of partitioning expressions that contain
/// both regular variables and time format specifiers. It generates the cartesian
/// product of all date values and variable values, then deduplicates the resulting
/// prefixes.
///
/// # Arguments
///
/// * `expr` - The partitioning expression to expand
/// * `filters` - The partition filters including time range and variable values
///
/// # Returns
///
/// A vector of unique S3 prefixes to scan.
///
/// # Errors
///
/// Returns an error if:
/// - The expression uses time formats but no time filter is provided
/// - A required variable has no filter values
///
/// # Example
///
/// ```
/// use pf_discoverer::partition::{PartitioningExpression, PartitionFilters, expand_all_prefixes};
///
/// let expr = PartitioningExpression::parse(
///     "data/YEAR=${_time:%Y}/MONTH=${_time:%m}/${element}/"
/// ).unwrap();
///
/// let mut filters = PartitionFilters::new();
/// filters.parse_and_add("_time=2022-01-01..2022-01-02").unwrap();
/// filters.add_filter("element", &["cpu"]);
///
/// let prefixes = expand_all_prefixes(&expr, &filters).unwrap();
/// // Result: ["data/YEAR=2022/MONTH=01/cpu/"]
/// // (deduplicated since both dates are in the same month)
/// ```
pub fn expand_all_prefixes(
    expr: &PartitioningExpression,
    filters: &PartitionFilters,
) -> Result<Vec<String>> {
    // Check if expression uses time formats
    if expr.has_time_formats() && filters.time_filter().is_none() {
        return Err(PfError::Config(
            "Partitioning expression uses ${_time:...} but no time filter provided. \
             Use --filter \"_time=YYYY-MM-DD..YYYY-MM-DD\" to specify date range"
                .to_string(),
        ));
    }

    // Get dates from time filter (or empty vec if no time formats)
    let dates: Vec<Option<NaiveDate>> = if expr.has_time_formats() {
        filters
            .time_filter()
            .map(|tf| tf.dates().map(Some).collect())
            .unwrap_or_else(|| vec![None])
    } else {
        vec![None]
    };

    // Get variable combinations
    let var_combos = generate_variable_combinations(expr, filters)?;

    // Generate all prefix combinations
    let mut all_prefixes = HashSet::new();

    for date in &dates {
        for combo in &var_combos {
            let prefix = build_prefix(expr, combo, *date)?;
            all_prefixes.insert(prefix);
        }
    }

    // Sort for deterministic output
    let mut result: Vec<String> = all_prefixes.into_iter().collect();
    result.sort();

    Ok(result)
}

/// Generate all combinations of variable values.
///
/// Returns a vector of hashmaps, where each hashmap represents one combination
/// of variable values. If no variables exist, returns a vec with one empty map.
fn generate_variable_combinations(
    expr: &PartitioningExpression,
    filters: &PartitionFilters,
) -> Result<Vec<HashMap<String, String>>> {
    let variables = expr.variables();

    if variables.is_empty() {
        // No variables, return single empty combination
        return Ok(vec![HashMap::new()]);
    }

    // Check that all variables have filters
    for var in &variables {
        if !filters.has_filter(var) {
            return Err(PfError::Config(format!(
                "Variable '{}' in partitioning expression has no filter. \
                 Use --partition-filter \"{}=value1,value2\" to specify values",
                var, var
            )));
        }
    }

    // Build cartesian product of all variable values
    let mut combinations = vec![HashMap::new()];

    for var in variables {
        let values = filters
            .get_values(var)
            .expect("checked above that filter exists");

        let mut new_combinations = Vec::new();
        for combo in combinations {
            for value in &values {
                let mut new_combo = combo.clone();
                new_combo.insert(var.to_string(), value.to_string());
                new_combinations.push(new_combo);
            }
        }
        combinations = new_combinations;
    }

    Ok(combinations)
}

/// Build a prefix string from an expression and variable/date values.
fn build_prefix(
    expr: &PartitioningExpression,
    vars: &HashMap<String, String>,
    date: Option<NaiveDate>,
) -> Result<String> {
    let mut prefix = String::new();
    let mut need_separator = false;

    for segment in expr.segments() {
        match segment {
            PathSegment::Static(text) => {
                // Add separator if needed before this static segment
                // (unless it starts with '=' which means it's a continuation)
                if need_separator && !text.starts_with('=') {
                    prefix.push('/');
                }
                prefix.push_str(text);
                // After a static segment, we need a separator unless it ends with '='
                need_separator = !text.ends_with('=');
            }
            PathSegment::Variable(name) => {
                // Variables are always preceded by a separator (they're full path components)
                if need_separator {
                    prefix.push('/');
                }
                if let Some(value) = vars.get(name) {
                    prefix.push_str(value);
                } else {
                    return Err(PfError::Config(format!(
                        "Missing value for variable '{}' during prefix generation",
                        name
                    )));
                }
                need_separator = true;
            }
            PathSegment::TimeFormat(spec) => {
                // Time formats are continuations (no separator before them)
                // They follow static segments like "YEAR="
                if let Some(d) = date {
                    prefix.push_str(&spec.format_date(&d));
                } else {
                    return Err(PfError::Config(
                        "Time format segment found but no date provided".to_string(),
                    ));
                }
                // After a time format, we need a separator for the next segment
                need_separator = true;
            }
        }
    }

    // Add trailing slash if the expression had one
    if expr.has_trailing_slash() && !prefix.ends_with('/') {
        prefix.push('/');
    }

    Ok(prefix)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_prefixes_simple_variables() {
        let expr = PartitioningExpression::parse("data/${region}/${year}/").unwrap();
        let mut filters = PartitionFilters::new();
        filters.add_filter("region", &["us-east"]);
        filters.add_filter("year", &["2024"]);

        let prefixes = expand_all_prefixes(&expr, &filters).unwrap();

        assert_eq!(prefixes, vec!["data/us-east/2024/"]);
    }

    #[test]
    fn test_expand_prefixes_multiple_values() {
        let expr = PartitioningExpression::parse("data/${region}/${year}/").unwrap();
        let mut filters = PartitionFilters::new();
        filters.add_filter("region", &["us-east", "us-west"]);
        filters.add_filter("year", &["2024", "2025"]);

        let prefixes = expand_all_prefixes(&expr, &filters).unwrap();

        assert_eq!(prefixes.len(), 4);
        assert!(prefixes.contains(&"data/us-east/2024/".to_string()));
        assert!(prefixes.contains(&"data/us-east/2025/".to_string()));
        assert!(prefixes.contains(&"data/us-west/2024/".to_string()));
        assert!(prefixes.contains(&"data/us-west/2025/".to_string()));
    }

    #[test]
    fn test_expand_prefixes_time_format_daily() {
        let expr = PartitioningExpression::parse(
            "data/YEAR=${_time:%Y}/MONTH=${_time:%m}/DAY=${_time:%d}/"
        ).unwrap();

        let mut filters = PartitionFilters::new();
        filters.parse_and_add("_time=2022-01-01..2022-01-03").unwrap();

        let prefixes = expand_all_prefixes(&expr, &filters).unwrap();

        assert_eq!(prefixes.len(), 3);
        assert!(prefixes.contains(&"data/YEAR=2022/MONTH=01/DAY=01/".to_string()));
        assert!(prefixes.contains(&"data/YEAR=2022/MONTH=01/DAY=02/".to_string()));
        assert!(prefixes.contains(&"data/YEAR=2022/MONTH=01/DAY=03/".to_string()));
    }

    #[test]
    fn test_expand_prefixes_time_format_monthly_deduplication() {
        // 5-day range with monthly partitioning should produce 1 prefix
        let expr = PartitioningExpression::parse(
            "data/YEAR=${_time:%Y}/MONTH=${_time:%m}/"
        ).unwrap();

        let mut filters = PartitionFilters::new();
        filters.parse_and_add("_time=2022-01-01..2022-01-05").unwrap();

        let prefixes = expand_all_prefixes(&expr, &filters).unwrap();

        // All dates are in January 2022, so only 1 unique prefix
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes[0], "data/YEAR=2022/MONTH=01/");
    }

    #[test]
    fn test_expand_prefixes_time_format_yearly_deduplication() {
        // Date range across months but yearly partitioning should dedupe
        let expr = PartitioningExpression::parse("data/YEAR=${_time:%Y}/").unwrap();

        let mut filters = PartitionFilters::new();
        filters.parse_and_add("_time=2022-01-01..2022-12-31").unwrap();

        let prefixes = expand_all_prefixes(&expr, &filters).unwrap();

        // All dates are in 2022, so only 1 unique prefix
        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes[0], "data/YEAR=2022/");
    }

    #[test]
    fn test_expand_prefixes_time_format_cross_year() {
        let expr = PartitioningExpression::parse("data/YEAR=${_time:%Y}/").unwrap();

        let mut filters = PartitionFilters::new();
        filters.parse_and_add("_time=2022-12-30..2023-01-02").unwrap();

        let prefixes = expand_all_prefixes(&expr, &filters).unwrap();

        // Dates span two years
        assert_eq!(prefixes.len(), 2);
        assert!(prefixes.contains(&"data/YEAR=2022/".to_string()));
        assert!(prefixes.contains(&"data/YEAR=2023/".to_string()));
    }

    #[test]
    fn test_expand_prefixes_mixed_time_and_variables() {
        let expr = PartitioningExpression::parse(
            "data/YEAR=${_time:%Y}/MONTH=${_time:%m}/${element}/"
        ).unwrap();

        let mut filters = PartitionFilters::new();
        filters.parse_and_add("_time=2022-01-01..2022-01-02").unwrap();
        filters.add_filter("element", &["cpu", "memory"]);

        let prefixes = expand_all_prefixes(&expr, &filters).unwrap();

        // 2 dates (deduplicated to 1 month) * 2 elements = 2 prefixes
        assert_eq!(prefixes.len(), 2);
        assert!(prefixes.contains(&"data/YEAR=2022/MONTH=01/cpu/".to_string()));
        assert!(prefixes.contains(&"data/YEAR=2022/MONTH=01/memory/".to_string()));
    }

    #[test]
    fn test_expand_prefixes_missing_time_filter() {
        let expr = PartitioningExpression::parse("data/YEAR=${_time:%Y}/").unwrap();
        let filters = PartitionFilters::new();

        let result = expand_all_prefixes(&expr, &filters);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("time filter"));
    }

    #[test]
    fn test_expand_prefixes_missing_variable_filter() {
        let expr = PartitioningExpression::parse("data/${region}/${year}/").unwrap();
        let mut filters = PartitionFilters::new();
        filters.add_filter("region", &["us-east"]);
        // Missing filter for "year"

        let result = expand_all_prefixes(&expr, &filters);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("year"));
    }

    #[test]
    fn test_expand_prefixes_static_only() {
        let expr = PartitioningExpression::parse("data/processed/logs/").unwrap();
        let filters = PartitionFilters::new();

        let prefixes = expand_all_prefixes(&expr, &filters).unwrap();

        assert_eq!(prefixes.len(), 1);
        assert_eq!(prefixes[0], "data/processed/logs/");
    }

    #[test]
    fn test_expand_prefixes_no_trailing_slash() {
        let expr = PartitioningExpression::parse("data/${region}").unwrap();
        let mut filters = PartitionFilters::new();
        filters.add_filter("region", &["us-east"]);

        let prefixes = expand_all_prefixes(&expr, &filters).unwrap();

        assert_eq!(prefixes[0], "data/us-east");
        assert!(!prefixes[0].ends_with('/'));
    }

    #[test]
    fn test_generate_variable_combinations_empty() {
        let expr = PartitioningExpression::parse("data/logs/").unwrap();
        let filters = PartitionFilters::new();

        let combos = generate_variable_combinations(&expr, &filters).unwrap();

        assert_eq!(combos.len(), 1);
        assert!(combos[0].is_empty());
    }

    #[test]
    fn test_generate_variable_combinations_single() {
        let expr = PartitioningExpression::parse("data/${region}/").unwrap();
        let mut filters = PartitionFilters::new();
        filters.add_filter("region", &["us-east", "us-west"]);

        let combos = generate_variable_combinations(&expr, &filters).unwrap();

        assert_eq!(combos.len(), 2);
    }

    #[test]
    fn test_generate_variable_combinations_multiple() {
        let expr = PartitioningExpression::parse("data/${region}/${year}/").unwrap();
        let mut filters = PartitionFilters::new();
        filters.add_filter("region", &["a", "b"]);
        filters.add_filter("year", &["1", "2", "3"]);

        let combos = generate_variable_combinations(&expr, &filters).unwrap();

        assert_eq!(combos.len(), 6); // 2 * 3 = 6
    }
}
