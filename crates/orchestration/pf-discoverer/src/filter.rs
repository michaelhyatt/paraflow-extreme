//! Pattern filtering for file discovery.
//!
//! Provides glob-style pattern matching for filtering discovered files.

use glob::Pattern;
use pf_error::{PfError, Result};

/// A filter for matching file keys against glob patterns.
///
/// Matches against the filename portion of the key (after the last `/`),
/// not the full key path. This allows patterns like `*.parquet` to match
/// files in any directory.
///
/// # Pattern Syntax
///
/// - `*` matches any sequence of characters (except `/`)
/// - `?` matches any single character
/// - `[abc]` matches any character in the brackets
/// - `[!abc]` matches any character not in the brackets
///
/// # Example
///
/// ```
/// use pf_discoverer::PatternFilter;
///
/// let filter = PatternFilter::new("*.parquet").unwrap();
///
/// assert!(filter.matches("data/file.parquet"));
/// assert!(filter.matches("file.parquet"));
/// assert!(!filter.matches("file.json"));
/// ```
#[derive(Debug, Clone)]
pub struct PatternFilter {
    pattern: String,
    compiled: Pattern,
}

impl PatternFilter {
    /// Create a new pattern filter.
    ///
    /// # Arguments
    ///
    /// * `pattern` - A glob pattern to match against filenames
    ///
    /// # Returns
    ///
    /// Returns an error if the pattern is invalid.
    pub fn new(pattern: &str) -> Result<Self> {
        let compiled = Pattern::new(pattern)
            .map_err(|e| PfError::Config(format!("Invalid glob pattern '{pattern}': {e}")))?;

        Ok(Self {
            pattern: pattern.to_string(),
            compiled,
        })
    }

    /// Check if a key matches the pattern.
    ///
    /// Matches against the filename (basename) of the key.
    pub fn matches(&self, key: &str) -> bool {
        // Extract filename from key (after last /)
        let filename = key.rsplit('/').next().unwrap_or(key);
        self.compiled.matches(filename)
    }

    /// Check if a key matches the pattern (case-insensitive).
    pub fn matches_case_insensitive(&self, key: &str) -> bool {
        let filename = key.rsplit('/').next().unwrap_or(key);
        self.compiled.matches(&filename.to_lowercase())
    }

    /// Get the original pattern string.
    pub fn pattern(&self) -> &str {
        &self.pattern
    }
}

/// A filter that matches all files (no filtering).
#[derive(Debug, Clone, Default)]
pub struct MatchAllFilter;

impl MatchAllFilter {
    /// Create a new match-all filter.
    pub fn new() -> Self {
        Self
    }

    /// Always returns true.
    pub fn matches(&self, _key: &str) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_filter_parquet() {
        let filter = PatternFilter::new("*.parquet").unwrap();

        assert!(filter.matches("file.parquet"));
        assert!(filter.matches("data/file.parquet"));
        assert!(filter.matches("a/b/c/file.parquet"));
        assert!(!filter.matches("file.json"));
        assert!(!filter.matches("file.parquet.gz"));
    }

    #[test]
    fn test_pattern_filter_wildcard() {
        let filter = PatternFilter::new("*.pq*").unwrap();

        assert!(filter.matches("file.pq"));
        assert!(filter.matches("file.pqt"));
        // Note: *.pq* does NOT match file.parquet because "pq" is not in "parquet"
        assert!(!filter.matches("file.parquet"));

        // Use *.p* to match both
        let filter2 = PatternFilter::new("*.p*").unwrap();
        assert!(filter2.matches("file.parquet"));
        assert!(filter2.matches("file.pq"));
    }

    #[test]
    fn test_pattern_filter_question_mark() {
        let filter = PatternFilter::new("file?.parquet").unwrap();

        assert!(filter.matches("file1.parquet"));
        assert!(filter.matches("fileA.parquet"));
        assert!(!filter.matches("file.parquet"));
        assert!(!filter.matches("file12.parquet"));
    }

    #[test]
    fn test_pattern_filter_brackets() {
        let filter = PatternFilter::new("file[0-9].parquet").unwrap();

        assert!(filter.matches("file0.parquet"));
        assert!(filter.matches("file9.parquet"));
        assert!(!filter.matches("fileA.parquet"));
    }

    #[test]
    fn test_pattern_filter_exact_match() {
        let filter = PatternFilter::new("specific-file.parquet").unwrap();

        assert!(filter.matches("specific-file.parquet"));
        assert!(filter.matches("dir/specific-file.parquet"));
        assert!(!filter.matches("other-file.parquet"));
    }

    #[test]
    fn test_pattern_filter_invalid() {
        let result = PatternFilter::new("[invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_pattern_filter_get_pattern() {
        let filter = PatternFilter::new("*.parquet").unwrap();
        assert_eq!(filter.pattern(), "*.parquet");
    }

    #[test]
    fn test_match_all_filter() {
        let filter = MatchAllFilter::new();

        assert!(filter.matches("anything.txt"));
        assert!(filter.matches(""));
        assert!(filter.matches("path/to/file.parquet"));
    }

    #[test]
    fn test_pattern_filter_ndjson() {
        let filter = PatternFilter::new("*.ndjson").unwrap();

        assert!(filter.matches("logs.ndjson"));
        assert!(filter.matches("data/events.ndjson"));
        assert!(!filter.matches("events.jsonl"));
    }

    #[test]
    fn test_pattern_filter_multiple_extensions() {
        // Test matching multiple possible extensions
        let parquet_filter = PatternFilter::new("*.parquet").unwrap();
        let json_filter = PatternFilter::new("*.json*").unwrap();

        // Parquet only
        assert!(parquet_filter.matches("file.parquet"));
        assert!(!parquet_filter.matches("file.ndjson"));

        // JSON variations
        assert!(json_filter.matches("file.json"));
        assert!(json_filter.matches("file.jsonl"));
    }
}
