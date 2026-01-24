//! Pattern-based filtering for file discovery.
//!
//! Provides glob-style pattern matching for filtering discovered files.
//! Supports both single patterns and multiple patterns with OR logic.

use glob::Pattern;
use pf_error::{PfError, Result};

use super::Filter;
use crate::s3::S3Object;

/// A filter for matching file keys against a single glob pattern.
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
/// use pf_discoverer::filter::PatternFilter;
/// use pf_discoverer::s3::S3Object;
///
/// let filter = PatternFilter::new("*.parquet").unwrap();
///
/// let parquet = S3Object { key: "data/file.parquet".to_string(), size: 100, last_modified: None };
/// let json = S3Object { key: "data/file.json".to_string(), size: 100, last_modified: None };
///
/// assert!(filter.matches_key("data/file.parquet"));
/// assert!(!filter.matches_key("file.json"));
/// ```
#[derive(Debug, Clone)]
pub struct PatternFilter {
    pattern: String,
    compiled: Pattern,
    match_full_path: bool,
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
            match_full_path: false,
        })
    }

    /// Set whether to match against the full path or just the filename.
    ///
    /// By default, matches against the filename only (basename).
    pub fn with_match_full_path(mut self, match_full_path: bool) -> Self {
        self.match_full_path = match_full_path;
        self
    }

    /// Check if a key matches the pattern.
    ///
    /// Matches against the filename (basename) of the key by default,
    /// or the full path if `match_full_path` is enabled.
    pub fn matches_key(&self, key: &str) -> bool {
        let target = if self.match_full_path {
            key
        } else {
            // Extract filename from key (after last /)
            key.rsplit('/').next().unwrap_or(key)
        };
        self.compiled.matches(target)
    }

    /// Check if a key matches the pattern (case-insensitive).
    pub fn matches_case_insensitive(&self, key: &str) -> bool {
        let target = if self.match_full_path {
            key.to_lowercase()
        } else {
            let filename = key.rsplit('/').next().unwrap_or(key);
            filename.to_lowercase()
        };
        self.compiled.matches(&target)
    }

    /// Get the original pattern string.
    pub fn pattern(&self) -> &str {
        &self.pattern
    }
}

impl Filter for PatternFilter {
    fn matches(&self, obj: &S3Object) -> bool {
        self.matches_key(&obj.key)
    }

    fn description(&self) -> String {
        if self.match_full_path {
            format!("pattern(full_path='{}')", self.pattern)
        } else {
            format!("pattern('{}')", self.pattern)
        }
    }
}

/// A filter that matches files against multiple patterns with OR logic.
///
/// A file passes the filter if it matches ANY of the configured patterns.
/// This is useful when you want to discover files matching multiple extensions
/// or naming conventions.
///
/// # Example
///
/// ```
/// use pf_discoverer::filter::MultiPatternFilter;
///
/// // Match parquet OR ndjson files
/// let filter = MultiPatternFilter::new(&["*.parquet", "*.ndjson"]).unwrap();
///
/// assert!(filter.matches_key("data.parquet"));
/// assert!(filter.matches_key("logs.ndjson"));
/// assert!(!filter.matches_key("config.json"));
/// ```
#[derive(Debug, Clone)]
pub struct MultiPatternFilter {
    patterns: Vec<PatternFilter>,
    match_full_path: bool,
}

impl MultiPatternFilter {
    /// Create a new multi-pattern filter.
    ///
    /// # Arguments
    ///
    /// * `patterns` - A slice of glob patterns. A file matches if it matches ANY pattern.
    ///
    /// # Returns
    ///
    /// Returns an error if any pattern is invalid.
    pub fn new(patterns: &[&str]) -> Result<Self> {
        let mut compiled = Vec::with_capacity(patterns.len());
        for pattern in patterns {
            compiled.push(PatternFilter::new(pattern)?);
        }

        Ok(Self {
            patterns: compiled,
            match_full_path: false,
        })
    }

    /// Set whether to match against the full path or just the filename.
    pub fn with_match_full_path(mut self, match_full_path: bool) -> Self {
        self.match_full_path = match_full_path;
        for pattern in &mut self.patterns {
            pattern.match_full_path = match_full_path;
        }
        self
    }

    /// Check if a key matches any of the patterns.
    pub fn matches_key(&self, key: &str) -> bool {
        // Empty patterns matches everything
        if self.patterns.is_empty() {
            return true;
        }
        self.patterns.iter().any(|p| p.matches_key(key))
    }

    /// Get the pattern strings.
    pub fn patterns(&self) -> Vec<&str> {
        self.patterns.iter().map(|p| p.pattern()).collect()
    }
}

impl Filter for MultiPatternFilter {
    fn matches(&self, obj: &S3Object) -> bool {
        self.matches_key(&obj.key)
    }

    fn description(&self) -> String {
        let patterns: Vec<&str> = self.patterns.iter().map(|p| p.pattern()).collect();
        format!("patterns({:?})", patterns)
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
}

impl Filter for MatchAllFilter {
    fn matches(&self, _obj: &S3Object) -> bool {
        true
    }

    fn description(&self) -> String {
        "match_all".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_obj(key: &str) -> S3Object {
        S3Object {
            key: key.to_string(),
            size: 1024,
            last_modified: None,
        }
    }

    #[test]
    fn test_pattern_filter_parquet() {
        let filter = PatternFilter::new("*.parquet").unwrap();

        assert!(filter.matches(&make_obj("file.parquet")));
        assert!(filter.matches(&make_obj("data/file.parquet")));
        assert!(filter.matches(&make_obj("a/b/c/file.parquet")));
        assert!(!filter.matches(&make_obj("file.json")));
        assert!(!filter.matches(&make_obj("file.parquet.gz")));
    }

    #[test]
    fn test_pattern_filter_full_path() {
        let filter = PatternFilter::new("data/*.parquet")
            .unwrap()
            .with_match_full_path(true);

        assert!(filter.matches(&make_obj("data/file.parquet")));
        assert!(!filter.matches(&make_obj("other/file.parquet")));
    }

    #[test]
    fn test_pattern_filter_wildcard() {
        let filter = PatternFilter::new("*.pq*").unwrap();

        assert!(filter.matches(&make_obj("file.pq")));
        assert!(filter.matches(&make_obj("file.pqt")));
        assert!(!filter.matches(&make_obj("file.parquet")));
    }

    #[test]
    fn test_pattern_filter_question_mark() {
        let filter = PatternFilter::new("file?.parquet").unwrap();

        assert!(filter.matches(&make_obj("file1.parquet")));
        assert!(filter.matches(&make_obj("fileA.parquet")));
        assert!(!filter.matches(&make_obj("file.parquet")));
        assert!(!filter.matches(&make_obj("file12.parquet")));
    }

    #[test]
    fn test_pattern_filter_brackets() {
        let filter = PatternFilter::new("file[0-9].parquet").unwrap();

        assert!(filter.matches(&make_obj("file0.parquet")));
        assert!(filter.matches(&make_obj("file9.parquet")));
        assert!(!filter.matches(&make_obj("fileA.parquet")));
    }

    #[test]
    fn test_pattern_filter_exact_match() {
        let filter = PatternFilter::new("specific-file.parquet").unwrap();

        assert!(filter.matches(&make_obj("specific-file.parquet")));
        assert!(filter.matches(&make_obj("dir/specific-file.parquet")));
        assert!(!filter.matches(&make_obj("other-file.parquet")));
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
    fn test_pattern_filter_description() {
        let filter = PatternFilter::new("*.parquet").unwrap();
        assert_eq!(filter.description(), "pattern('*.parquet')");

        let filter_full = PatternFilter::new("data/*.parquet")
            .unwrap()
            .with_match_full_path(true);
        assert_eq!(
            filter_full.description(),
            "pattern(full_path='data/*.parquet')"
        );
    }

    #[test]
    fn test_multi_pattern_filter() {
        let filter = MultiPatternFilter::new(&["*.parquet", "*.ndjson"]).unwrap();

        assert!(filter.matches(&make_obj("file.parquet")));
        assert!(filter.matches(&make_obj("file.ndjson")));
        assert!(!filter.matches(&make_obj("file.json")));
    }

    #[test]
    fn test_multi_pattern_filter_empty() {
        let filter = MultiPatternFilter::new(&[]).unwrap();

        // Empty filter matches everything
        assert!(filter.matches(&make_obj("anything.txt")));
    }

    #[test]
    fn test_multi_pattern_filter_single() {
        let filter = MultiPatternFilter::new(&["*.parquet"]).unwrap();

        assert!(filter.matches(&make_obj("file.parquet")));
        assert!(!filter.matches(&make_obj("file.json")));
    }

    #[test]
    fn test_multi_pattern_filter_description() {
        let filter = MultiPatternFilter::new(&["*.parquet", "*.ndjson"]).unwrap();
        assert!(filter.description().contains("*.parquet"));
        assert!(filter.description().contains("*.ndjson"));
    }

    #[test]
    fn test_match_all_filter() {
        let filter = MatchAllFilter::new();

        assert!(filter.matches(&make_obj("anything.txt")));
        assert!(filter.matches(&make_obj("")));
        assert!(filter.matches(&make_obj("path/to/file.parquet")));
    }

    #[test]
    fn test_match_all_filter_description() {
        let filter = MatchAllFilter::new();
        assert_eq!(filter.description(), "match_all");
    }

    #[test]
    fn test_pattern_filter_ndjson() {
        let filter = PatternFilter::new("*.ndjson").unwrap();

        assert!(filter.matches(&make_obj("logs.ndjson")));
        assert!(filter.matches(&make_obj("data/events.ndjson")));
        assert!(!filter.matches(&make_obj("events.jsonl")));
    }

    #[test]
    fn test_pattern_filter_multiple_extensions() {
        let parquet_filter = PatternFilter::new("*.parquet").unwrap();
        let json_filter = PatternFilter::new("*.json*").unwrap();

        assert!(parquet_filter.matches(&make_obj("file.parquet")));
        assert!(!parquet_filter.matches(&make_obj("file.ndjson")));

        assert!(json_filter.matches(&make_obj("file.json")));
        assert!(json_filter.matches(&make_obj("file.jsonl")));
    }
}
