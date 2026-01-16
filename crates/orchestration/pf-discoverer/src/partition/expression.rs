//! Partitioning expression parsing and prefix generation.
//!
//! Parses expressions like `logs/${index}/${year}/` and generates S3 prefixes
//! based on filter constraints.

use pf_error::{PfError, Result};
use std::collections::HashMap;

use super::filter::PartitionFilters;

/// A segment of a partitioning path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathSegment {
    /// A static path component (e.g., "logs")
    Static(String),
    /// A variable path component (e.g., "index" from "${index}")
    Variable(String),
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

                let var_name: String = chars[start..end].iter().collect();
                let var_name = var_name.trim();

                if var_name.is_empty() {
                    return Err(PfError::Config(format!(
                        "Empty variable name in expression: {}",
                        expr
                    )));
                }

                if !is_valid_variable_name(var_name) {
                    return Err(PfError::Config(format!(
                        "Invalid variable name '{}' in expression: {}",
                        var_name, expr
                    )));
                }

                segments.push(PathSegment::Variable(var_name.to_string()));
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
                PathSegment::Static(_) => None,
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
                        all_prefixes
                            .extend(self.enumerate_prefixes(filters, segment_idx + 1, new_prefix));
                    }
                    all_prefixes
                } else {
                    // No filter for this variable - shouldn't happen if called correctly
                    vec![]
                }
            }
        }
    }

    /// Check if an S3 key matches this expression pattern.
    pub fn matches(&self, key: &str) -> bool {
        let parts: Vec<&str> = key.trim_end_matches('/').split('/').collect();
        let mut segment_idx = 0;
        let mut part_idx = 0;

        while segment_idx < self.segments.len() && part_idx < parts.len() {
            match &self.segments[segment_idx] {
                PathSegment::Static(text) => {
                    if parts[part_idx] != text {
                        return false;
                    }
                    part_idx += 1;
                }
                PathSegment::Variable(_) => {
                    // Any value matches a variable
                    part_idx += 1;
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
    pub fn extract_values(&self, key: &str) -> Option<HashMap<String, String>> {
        let parts: Vec<&str> = key.trim_end_matches('/').split('/').collect();
        let mut segment_idx = 0;
        let mut part_idx = 0;
        let mut values = HashMap::new();

        while segment_idx < self.segments.len() && part_idx < parts.len() {
            match &self.segments[segment_idx] {
                PathSegment::Static(text) => {
                    if parts[part_idx] != text {
                        return None;
                    }
                    part_idx += 1;
                }
                PathSegment::Variable(name) => {
                    values.insert(name.clone(), parts[part_idx].to_string());
                    part_idx += 1;
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
        assert!(result.static_prefixes.contains(&"logs/nginx/2024/".to_string()));
        assert!(result.static_prefixes.contains(&"logs/nginx/2025/".to_string()));
    }

    #[test]
    fn test_generate_prefixes_multiple_values() {
        let expr = PartitioningExpression::parse("logs/${index}/${year}/").unwrap();
        let mut filters = PartitionFilters::new();
        filters.add_filter("index", &["nginx", "apache"]);
        filters.add_filter("year", &["2024", "2025"]);

        let result = expr.generate_prefixes(&filters);

        assert_eq!(result.static_prefixes.len(), 4);
        assert!(result.static_prefixes.contains(&"logs/nginx/2024/".to_string()));
        assert!(result.static_prefixes.contains(&"logs/nginx/2025/".to_string()));
        assert!(result.static_prefixes.contains(&"logs/apache/2024/".to_string()));
        assert!(result.static_prefixes.contains(&"logs/apache/2025/".to_string()));
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
}
