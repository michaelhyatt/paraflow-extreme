//! Partitioning expression parsing and prefix generation.
//!
//! Parses expressions like `logs/${index}/${year}/` and generates S3 prefixes
//! based on filter constraints. Also supports time-based partitioning with
//! `${_time:%Y}`, `${_time:%m}`, `${_time:%d}` format specifiers.

use chrono::NaiveDate;
use pf_error::{PfError, Result};
use std::collections::HashMap;

use super::filter::PartitionFilters;

/// A strftime format specifier for time-based path segments.
///
/// Used to format dates in partitioning expressions.
///
/// # Example
///
/// ```
/// use pf_discoverer::partition::TimeFormatSpec;
/// use chrono::NaiveDate;
///
/// let spec = TimeFormatSpec::new("%Y");
/// let date = NaiveDate::from_ymd_opt(2022, 1, 15).unwrap();
/// assert_eq!(spec.format_date(&date), "2022");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeFormatSpec {
    /// The strftime format string (e.g., "%Y", "%m", "%d")
    format: String,
}

impl TimeFormatSpec {
    /// Create a new time format specifier.
    ///
    /// # Arguments
    ///
    /// * `format` - The strftime format string
    pub fn new(format: impl Into<String>) -> Self {
        Self {
            format: format.into(),
        }
    }

    /// Format a date using this specifier.
    ///
    /// # Arguments
    ///
    /// * `date` - The date to format
    ///
    /// # Returns
    ///
    /// The formatted date string.
    pub fn format_date(&self, date: &NaiveDate) -> String {
        date.format(&self.format).to_string()
    }

    /// Get the format string.
    pub fn format(&self) -> &str {
        &self.format
    }
}

/// A segment of a partitioning path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathSegment {
    /// A static path component (e.g., "logs")
    Static(String),
    /// A variable path component (e.g., "index" from "${index}")
    Variable(String),
    /// A time format specifier (e.g., "%Y" from "${_time:%Y}")
    TimeFormat(TimeFormatSpec),
}

/// Result of prefix generation from a partitioning expression.
#[derive(Debug, Clone)]
pub struct PrefixGenerationResult {
    /// Static prefixes that can be listed directly.
    pub static_prefixes: Vec<String>,
    /// Variables that need dynamic discovery (no filter provided).
    pub variables_to_discover: Vec<String>,
    /// The depth at which dynamic discovery should start.
    pub discovery_depth: usize,
}

/// A parsed partitioning expression.
///
/// Represents a hierarchical path structure with static and variable components.
/// Used to generate S3 prefixes for efficient partitioned data discovery.
///
/// # Expression Format
///
/// - Static segments: literal path components like `logs` or `data`
/// - Variable segments: `${variable_name}` patterns
/// - Path separators: `/` characters separate segments
/// - Trailing slash: indicates a directory prefix
///
/// # Examples
///
/// ```
/// use pf_discoverer::partition::PartitioningExpression;
///
/// // Simple expression with two variables
/// let expr = PartitioningExpression::parse("logs/${index}/${year}/").unwrap();
/// assert_eq!(expr.variables(), vec!["index", "year"]);
///
/// // Static-only expression
/// let static_expr = PartitioningExpression::parse("data/processed/").unwrap();
/// assert!(static_expr.variables().is_empty());
/// ```
#[derive(Debug, Clone)]
pub struct PartitioningExpression {
    /// The parsed path segments.
    segments: Vec<PathSegment>,
    /// Whether the expression ends with a trailing slash.
    trailing_slash: bool,
    /// Original expression string for debugging.
    original: String,
}

impl PartitioningExpression {
    /// Parse a partitioning expression string.
    ///
    /// # Arguments
    ///
    /// * `expr` - The expression string to parse
    ///
    /// # Returns
    ///
    /// A parsed `PartitioningExpression` or an error if the expression is invalid.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A variable name is empty (`${}`)
    /// - A variable name contains invalid characters
    /// - There's an unclosed variable (`${name`)
    pub fn parse(expr: &str) -> Result<Self> {
        let expr = expr.trim();
        if expr.is_empty() {
            return Err(PfError::Config("Empty partitioning expression".to_string()));
        }

        let trailing_slash = expr.ends_with('/');
        let expr_without_trailing = expr.trim_end_matches('/');

        let mut segments = Vec::new();
        let mut current_pos = 0;
        let chars: Vec<char> = expr_without_trailing.chars().collect();

        while current_pos < chars.len() {
            // Check for variable start
            if current_pos + 1 < chars.len()
                && chars[current_pos] == '$'
                && chars[current_pos + 1] == '{'
            {
                // Find the closing brace
                let start = current_pos + 2;
                let mut end = start;
                while end < chars.len() && chars[end] != '}' {
                    end += 1;
                }

                if end >= chars.len() {
                    return Err(PfError::Config(format!(
                        "Unclosed variable in expression: {}",
                        expr
                    )));
                }

                let var_content: String = chars[start..end].iter().collect();
                let var_content = var_content.trim();

                if var_content.is_empty() {
                    return Err(PfError::Config(format!(
                        "Empty variable name in expression: {}",
                        expr
                    )));
                }

                // Check for time format specifier: _time:%Y, _time:%m, etc.
                if var_content.starts_with("_time:") {
                    let format_spec = var_content
                        .strip_prefix("_time:")
                        .expect("checked prefix above");
                    if format_spec.is_empty() {
                        return Err(PfError::Config(format!(
                            "Empty format specifier for _time in expression: {}",
                            expr
                        )));
                    }
                    segments.push(PathSegment::TimeFormat(TimeFormatSpec::new(format_spec)));
                } else {
                    // Regular variable
                    if !is_valid_variable_name(var_content) {
                        return Err(PfError::Config(format!(
                            "Invalid variable name '{}' in expression: {}",
                            var_content, expr
                        )));
                    }
                    segments.push(PathSegment::Variable(var_content.to_string()));
                }
                current_pos = end + 1;
            } else if chars[current_pos] == '/' {
                // Skip path separator
                current_pos += 1;
            } else {
                // Static segment
                let start = current_pos;
                while current_pos < chars.len()
                    && chars[current_pos] != '/'
                    && !(current_pos + 1 < chars.len()
                        && chars[current_pos] == '$'
                        && chars[current_pos + 1] == '{')
                {
                    current_pos += 1;
                }

                let static_text: String = chars[start..current_pos].iter().collect();
                if !static_text.is_empty() {
                    segments.push(PathSegment::Static(static_text));
                }
            }
        }

        Ok(Self {
            segments,
            trailing_slash,
            original: expr.to_string(),
        })
    }

    /// Get all variable names in order of appearance.
    pub fn variables(&self) -> Vec<&str> {
        self.segments
            .iter()
            .filter_map(|s| match s {
                PathSegment::Variable(name) => Some(name.as_str()),
                PathSegment::Static(_) | PathSegment::TimeFormat(_) => None,
            })
            .collect()
    }

    /// Get all segments.
    pub fn segments(&self) -> &[PathSegment] {
        &self.segments
    }

    /// Check if the expression has a trailing slash.
    pub fn has_trailing_slash(&self) -> bool {
        self.trailing_slash
    }

    /// Get the original expression string.
    pub fn original(&self) -> &str {
        &self.original
    }

    /// Check if the expression contains any time format specifiers.
    pub fn has_time_formats(&self) -> bool {
        self.segments
            .iter()
            .any(|s| matches!(s, PathSegment::TimeFormat(_)))
    }

    /// Generate S3 prefixes based on the provided filters.
    ///
    /// For variables with filters, generates all combinations.
    /// For variables without filters, returns information for dynamic discovery.
    ///
    /// # Arguments
    ///
    /// * `filters` - The partition filters to apply
    ///
    /// # Returns
    ///
    /// A `PrefixGenerationResult` containing:
    /// - Static prefixes that can be listed directly
    /// - Variables that need dynamic discovery
    pub fn generate_prefixes(&self, filters: &PartitionFilters) -> PrefixGenerationResult {
        let variables = self.variables();

        // Check which variables have filters
        let mut first_unfiltered_var: Option<usize> = None;
        for (i, var) in variables.iter().enumerate() {
            if !filters.has_filter(var) {
                first_unfiltered_var = Some(i);
                break;
            }
        }

        // If all variables have filters, generate all combinations
        if first_unfiltered_var.is_none() {
            let prefixes = self.enumerate_prefixes(filters, 0, String::new());
            return PrefixGenerationResult {
                static_prefixes: prefixes,
                variables_to_discover: vec![],
                discovery_depth: 0,
            };
        }

        // Generate prefixes up to the first unfiltered variable
        let unfiltered_idx = first_unfiltered_var.unwrap();

        // Generate partial prefixes up to the unfiltered variable
        let partial_prefixes = self.generate_partial_prefixes(filters, unfiltered_idx);

        // Collect variables that need discovery
        let vars_to_discover: Vec<String> = variables[unfiltered_idx..]
            .iter()
            .filter(|v| !filters.has_filter(v))
            .map(|s| s.to_string())
            .collect();

        PrefixGenerationResult {
            static_prefixes: partial_prefixes,
            variables_to_discover: vars_to_discover,
            discovery_depth: unfiltered_idx,
        }
    }

    /// Generate partial prefixes up to a specific variable index.
    fn generate_partial_prefixes(
        &self,
        filters: &PartitionFilters,
        up_to_var_idx: usize,
    ) -> Vec<String> {
        let mut current_prefixes = vec![String::new()];
        let mut var_count = 0;

        for segment in &self.segments {
            match segment {
                PathSegment::Static(text) => {
                    // Append static text to all current prefixes
                    for prefix in &mut current_prefixes {
                        if !prefix.is_empty() {
                            prefix.push('/');
                        }
                        prefix.push_str(text);
                    }
                }
                PathSegment::Variable(name) => {
                    if var_count >= up_to_var_idx {
                        // Stop here - we need dynamic discovery for this variable
                        break;
                    }

                    // Get filter values for this variable
                    if let Some(values) = filters.get_values(name) {
                        let mut new_prefixes = Vec::new();
                        for prefix in &current_prefixes {
                            for value in &values {
                                let mut new_prefix = prefix.clone();
                                if !new_prefix.is_empty() {
                                    new_prefix.push('/');
                                }
                                new_prefix.push_str(value);
                                new_prefixes.push(new_prefix);
                            }
                        }
                        current_prefixes = new_prefixes;
                    }
                    var_count += 1;
                }
                PathSegment::TimeFormat(_) => {
                    // TimeFormat segments require expand_all_prefixes() from time.rs
                    // which handles date iteration. Stop here for partial prefix generation.
                    break;
                }
            }
        }

        // Add trailing slash if present
        if self.trailing_slash || up_to_var_idx < self.variables().len() {
            for prefix in &mut current_prefixes {
                prefix.push('/');
            }
        }

        current_prefixes
    }

    /// Recursively enumerate all prefix combinations.
    fn enumerate_prefixes(
        &self,
        filters: &PartitionFilters,
        segment_idx: usize,
        current_prefix: String,
    ) -> Vec<String> {
        if segment_idx >= self.segments.len() {
            // End of segments - add trailing slash if needed
            let mut final_prefix = current_prefix;
            if self.trailing_slash {
                final_prefix.push('/');
            }
            return vec![final_prefix];
        }

        match &self.segments[segment_idx] {
            PathSegment::Static(text) => {
                let mut new_prefix = current_prefix;
                if !new_prefix.is_empty() {
                    new_prefix.push('/');
                }
                new_prefix.push_str(text);
                self.enumerate_prefixes(filters, segment_idx + 1, new_prefix)
            }
            PathSegment::Variable(name) => {
                if let Some(values) = filters.get_values(name) {
                    let mut all_prefixes = Vec::new();
                    for value in values {
                        let mut new_prefix = current_prefix.clone();
                        if !new_prefix.is_empty() {
                            new_prefix.push('/');
                        }
                        new_prefix.push_str(value);
                        all_prefixes.extend(self.enumerate_prefixes(
                            filters,
                            segment_idx + 1,
                            new_prefix,
                        ));
                    }
                    all_prefixes
                } else {
                    // No filter for this variable - shouldn't happen if called correctly
                    vec![]
                }
            }
            PathSegment::TimeFormat(_) => {
                // TimeFormat segments are handled by expand_all_prefixes() in time.rs
                // This method doesn't support time-based prefix generation
                vec![]
            }
        }
    }

    /// Check if an S3 key matches this expression pattern.
    pub fn matches(&self, key: &str) -> bool {
        let parts: Vec<&str> = key.trim_end_matches('/').split('/').collect();
        let mut segment_idx = 0;
        let mut part_idx = 0;
        let mut part_offset = 0; // Offset within current part for partial matching

        while segment_idx < self.segments.len() && part_idx < parts.len() {
            let part = parts[part_idx];
            let remaining = &part[part_offset..];

            match &self.segments[segment_idx] {
                PathSegment::Static(text) => {
                    if text.ends_with('=') {
                        // This is a prefix like "YEAR=" - check if part starts with it
                        if !remaining.starts_with(text.as_str()) {
                            return false;
                        }
                        part_offset += text.len();
                    } else {
                        // Full path component - must match exactly
                        if remaining != text {
                            return false;
                        }
                        part_idx += 1;
                        part_offset = 0;
                    }
                }
                PathSegment::Variable(_) | PathSegment::TimeFormat(_) => {
                    // Variable or time format consumes the rest of the current part
                    part_idx += 1;
                    part_offset = 0;
                }
            }
            segment_idx += 1;
        }

        // All segments must be matched
        segment_idx == self.segments.len()
    }

    /// Extract variable values from a matching key.
    ///
    /// Returns `None` if the key doesn't match the expression.
    /// Note: TimeFormat segments are skipped (they don't produce named values).
    pub fn extract_values(&self, key: &str) -> Option<HashMap<String, String>> {
        let parts: Vec<&str> = key.trim_end_matches('/').split('/').collect();
        let mut segment_idx = 0;
        let mut part_idx = 0;
        let mut part_offset = 0; // Offset within current part for partial matching
        let mut values = HashMap::new();

        while segment_idx < self.segments.len() && part_idx < parts.len() {
            let part = parts[part_idx];
            let remaining = &part[part_offset..];

            match &self.segments[segment_idx] {
                PathSegment::Static(text) => {
                    if text.ends_with('=') {
                        // This is a prefix like "YEAR=" - check if part starts with it
                        if !remaining.starts_with(text.as_str()) {
                            return None;
                        }
                        part_offset += text.len();
                    } else {
                        // Full path component - must match exactly
                        if remaining != text {
                            return None;
                        }
                        part_idx += 1;
                        part_offset = 0;
                    }
                }
                PathSegment::Variable(name) => {
                    // Extract the remaining part of the current path component as the value
                    values.insert(name.clone(), remaining.to_string());
                    part_idx += 1;
                    part_offset = 0;
                }
                PathSegment::TimeFormat(_) => {
                    // Time format segments match but don't extract named values
                    part_idx += 1;
                    part_offset = 0;
                }
            }
            segment_idx += 1;
        }

        if segment_idx == self.segments.len() {
            Some(values)
        } else {
            None
        }
    }
}

/// Check if a variable name is valid.
///
/// Valid names contain only alphanumeric characters and underscores,
/// and must start with a letter or underscore.
fn is_valid_variable_name(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }

    let chars: Vec<char> = name.chars().collect();

    // First character must be a letter or underscore
    if !chars[0].is_alphabetic() && chars[0] != '_' {
        return false;
    }

    // Remaining characters must be alphanumeric or underscore
    chars.iter().all(|c| c.is_alphanumeric() || *c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_expression() {
        let expr = PartitioningExpression::parse("logs/${index}/${year}/").unwrap();
        assert_eq!(expr.variables(), vec!["index", "year"]);
        assert!(expr.has_trailing_slash());
    }

    #[test]
    fn test_parse_static_only() {
        let expr = PartitioningExpression::parse("data/processed/").unwrap();
        assert!(expr.variables().is_empty());
        assert!(expr.has_trailing_slash());
        assert_eq!(expr.segments().len(), 2);
    }

    #[test]
    fn test_parse_no_trailing_slash() {
        let expr = PartitioningExpression::parse("logs/${index}").unwrap();
        assert!(!expr.has_trailing_slash());
    }

    #[test]
    fn test_parse_single_variable() {
        let expr = PartitioningExpression::parse("${partition}/").unwrap();
        assert_eq!(expr.variables(), vec!["partition"]);
    }

    #[test]
    fn test_parse_complex_expression() {
        let expr =
            PartitioningExpression::parse("data/${region}/${year}/${month}/${day}/").unwrap();
        assert_eq!(expr.variables(), vec!["region", "year", "month", "day"]);
    }

    #[test]
    fn test_parse_empty_expression() {
        assert!(PartitioningExpression::parse("").is_err());
        assert!(PartitioningExpression::parse("   ").is_err());
    }

    #[test]
    fn test_parse_empty_variable_name() {
        assert!(PartitioningExpression::parse("logs/${}/data").is_err());
    }

    #[test]
    fn test_parse_unclosed_variable() {
        assert!(PartitioningExpression::parse("logs/${index/data").is_err());
    }

    #[test]
    fn test_parse_invalid_variable_name() {
        assert!(PartitioningExpression::parse("logs/${123}/data").is_err()); // starts with number
        assert!(PartitioningExpression::parse("logs/${foo-bar}/data").is_err()); // contains hyphen
    }

    #[test]
    fn test_parse_valid_variable_names() {
        assert!(PartitioningExpression::parse("${_private}").is_ok());
        assert!(PartitioningExpression::parse("${var_name}").is_ok());
        assert!(PartitioningExpression::parse("${var123}").is_ok());
    }

    #[test]
    fn test_generate_prefixes_all_filtered() {
        let expr = PartitioningExpression::parse("logs/${index}/${year}/").unwrap();
        let mut filters = PartitionFilters::new();
        filters.add_filter("index", &["nginx"]);
        filters.add_filter("year", &["2024", "2025"]);

        let result = expr.generate_prefixes(&filters);

        assert!(result.variables_to_discover.is_empty());
        assert_eq!(result.static_prefixes.len(), 2);
        assert!(
            result
                .static_prefixes
                .contains(&"logs/nginx/2024/".to_string())
        );
        assert!(
            result
                .static_prefixes
                .contains(&"logs/nginx/2025/".to_string())
        );
    }

    #[test]
    fn test_generate_prefixes_multiple_values() {
        let expr = PartitioningExpression::parse("logs/${index}/${year}/").unwrap();
        let mut filters = PartitionFilters::new();
        filters.add_filter("index", &["nginx", "apache"]);
        filters.add_filter("year", &["2024", "2025"]);

        let result = expr.generate_prefixes(&filters);

        assert_eq!(result.static_prefixes.len(), 4);
        assert!(
            result
                .static_prefixes
                .contains(&"logs/nginx/2024/".to_string())
        );
        assert!(
            result
                .static_prefixes
                .contains(&"logs/nginx/2025/".to_string())
        );
        assert!(
            result
                .static_prefixes
                .contains(&"logs/apache/2024/".to_string())
        );
        assert!(
            result
                .static_prefixes
                .contains(&"logs/apache/2025/".to_string())
        );
    }

    #[test]
    fn test_generate_prefixes_partial_filter() {
        let expr = PartitioningExpression::parse("logs/${index}/${year}/").unwrap();
        let mut filters = PartitionFilters::new();
        filters.add_filter("year", &["2024"]); // No filter for index

        let result = expr.generate_prefixes(&filters);

        // Should stop at first unfiltered variable (index)
        assert_eq!(result.static_prefixes, vec!["logs/"]);
        assert_eq!(result.variables_to_discover, vec!["index"]);
    }

    #[test]
    fn test_generate_prefixes_no_filters() {
        let expr = PartitioningExpression::parse("logs/${index}/${year}/").unwrap();
        let filters = PartitionFilters::new();

        let result = expr.generate_prefixes(&filters);

        // Should return base prefix only
        assert_eq!(result.static_prefixes, vec!["logs/"]);
        assert_eq!(result.variables_to_discover, vec!["index", "year"]);
    }

    #[test]
    fn test_generate_prefixes_first_filtered_second_not() {
        let expr = PartitioningExpression::parse("logs/${index}/${year}/").unwrap();
        let mut filters = PartitionFilters::new();
        filters.add_filter("index", &["nginx", "apache"]);
        // No filter for year

        let result = expr.generate_prefixes(&filters);

        assert_eq!(result.static_prefixes.len(), 2);
        assert!(result.static_prefixes.contains(&"logs/nginx/".to_string()));
        assert!(result.static_prefixes.contains(&"logs/apache/".to_string()));
        assert_eq!(result.variables_to_discover, vec!["year"]);
    }

    #[test]
    fn test_matches_key() {
        let expr = PartitioningExpression::parse("logs/${index}/${year}/").unwrap();

        assert!(expr.matches("logs/nginx/2024/"));
        assert!(expr.matches("logs/apache/2025"));
        assert!(expr.matches("logs/any-value/any-year/file.parquet"));
        assert!(!expr.matches("other/nginx/2024/"));
        assert!(!expr.matches("logs/nginx/"));
    }

    #[test]
    fn test_extract_values() {
        let expr = PartitioningExpression::parse("logs/${index}/${year}/").unwrap();

        let values = expr.extract_values("logs/nginx/2024/file.parquet").unwrap();
        assert_eq!(values.get("index"), Some(&"nginx".to_string()));
        assert_eq!(values.get("year"), Some(&"2024".to_string()));

        assert!(expr.extract_values("other/nginx/2024/").is_none());
    }

    #[test]
    fn test_is_valid_variable_name() {
        assert!(is_valid_variable_name("foo"));
        assert!(is_valid_variable_name("_foo"));
        assert!(is_valid_variable_name("foo123"));
        assert!(is_valid_variable_name("foo_bar"));
        assert!(!is_valid_variable_name("123foo"));
        assert!(!is_valid_variable_name("foo-bar"));
        assert!(!is_valid_variable_name(""));
    }

    // TimeFormatSpec tests

    #[test]
    fn test_time_format_spec_new() {
        let spec = TimeFormatSpec::new("%Y");
        assert_eq!(spec.format(), "%Y");
    }

    #[test]
    fn test_time_format_spec_format_date() {
        let date = NaiveDate::from_ymd_opt(2022, 1, 15).unwrap();

        assert_eq!(TimeFormatSpec::new("%Y").format_date(&date), "2022");
        assert_eq!(TimeFormatSpec::new("%m").format_date(&date), "01");
        assert_eq!(TimeFormatSpec::new("%d").format_date(&date), "15");
        assert_eq!(
            TimeFormatSpec::new("%Y-%m-%d").format_date(&date),
            "2022-01-15"
        );
    }

    // Time format parsing tests

    #[test]
    fn test_parse_time_format_expression() {
        let expr =
            PartitioningExpression::parse("data/YEAR=${_time:%Y}/MONTH=${_time:%m}/").unwrap();

        assert!(expr.has_time_formats());
        assert!(expr.variables().is_empty()); // _time formats are not regular variables

        let segments = expr.segments();
        assert_eq!(segments.len(), 5);
        assert!(matches!(segments[0], PathSegment::Static(ref s) if s == "data"));
        assert!(matches!(segments[1], PathSegment::Static(ref s) if s == "YEAR="));
        assert!(matches!(segments[2], PathSegment::TimeFormat(ref spec) if spec.format() == "%Y"));
        assert!(matches!(segments[3], PathSegment::Static(ref s) if s == "MONTH="));
        assert!(matches!(segments[4], PathSegment::TimeFormat(ref spec) if spec.format() == "%m"));
    }

    #[test]
    fn test_parse_mixed_expression() {
        let expr = PartitioningExpression::parse(
            "data/${region}/YEAR=${_time:%Y}/MONTH=${_time:%m}/${element}/",
        )
        .unwrap();

        assert!(expr.has_time_formats());
        assert_eq!(expr.variables(), vec!["region", "element"]);
    }

    #[test]
    fn test_parse_time_format_empty_specifier() {
        assert!(PartitioningExpression::parse("data/${_time:}/").is_err());
    }

    #[test]
    fn test_parse_expression_no_time_formats() {
        let expr = PartitioningExpression::parse("data/${region}/${year}/").unwrap();
        assert!(!expr.has_time_formats());
    }

    #[test]
    fn test_matches_with_time_format() {
        let expr =
            PartitioningExpression::parse("data/YEAR=${_time:%Y}/MONTH=${_time:%m}/").unwrap();

        assert!(expr.matches("data/YEAR=2022/MONTH=01/"));
        assert!(expr.matches("data/YEAR=2022/MONTH=01/file.parquet"));
        assert!(!expr.matches("data/YEAR=2022/"));
        assert!(!expr.matches("other/YEAR=2022/MONTH=01/"));
    }

    #[test]
    fn test_extract_values_with_time_format() {
        let expr =
            PartitioningExpression::parse("data/${region}/YEAR=${_time:%Y}/${element}/").unwrap();

        let values = expr
            .extract_values("data/us-east/YEAR=2022/cpu/file.parquet")
            .unwrap();

        // Should extract region and element, but not time format
        assert_eq!(values.get("region"), Some(&"us-east".to_string()));
        assert_eq!(values.get("element"), Some(&"cpu".to_string()));
        assert_eq!(values.len(), 2);
    }
}
