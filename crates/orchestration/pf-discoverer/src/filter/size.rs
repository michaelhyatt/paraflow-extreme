//! Size-based filtering for file discovery.
//!
//! Provides filtering of S3 objects based on file size ranges.

use super::Filter;
use crate::s3::S3Object;

/// A filter for matching files based on size constraints.
///
/// Can filter by minimum size, maximum size, or both. A file passes the filter
/// if its size falls within the specified range (inclusive).
///
/// # Example
///
/// ```
/// use pf_discoverer::filter::SizeFilter;
/// use pf_discoverer::s3::S3Object;
///
/// // Only files larger than 1KB
/// let min_filter = SizeFilter::new().with_min_size(1024);
///
/// // Only files smaller than 1GB
/// let max_filter = SizeFilter::new().with_max_size(1024 * 1024 * 1024);
///
/// // Files between 1KB and 1GB
/// let range_filter = SizeFilter::new()
///     .with_min_size(1024)
///     .with_max_size(1024 * 1024 * 1024);
/// ```
#[derive(Debug, Clone, Default)]
pub struct SizeFilter {
    /// Minimum file size in bytes (inclusive)
    min_size: Option<u64>,
    /// Maximum file size in bytes (inclusive)
    max_size: Option<u64>,
}

impl SizeFilter {
    /// Create a new size filter with no constraints.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the minimum file size in bytes.
    ///
    /// Files smaller than this size will be filtered out.
    pub fn with_min_size(mut self, min_size: u64) -> Self {
        self.min_size = Some(min_size);
        self
    }

    /// Set the maximum file size in bytes.
    ///
    /// Files larger than this size will be filtered out.
    pub fn with_max_size(mut self, max_size: u64) -> Self {
        self.max_size = Some(max_size);
        self
    }

    /// Check if a file size matches the filter constraints.
    pub fn matches_size(&self, size: u64) -> bool {
        if let Some(min) = self.min_size {
            if size < min {
                return false;
            }
        }
        if let Some(max) = self.max_size {
            if size > max {
                return false;
            }
        }
        true
    }

    /// Get the minimum size constraint, if set.
    pub fn min_size(&self) -> Option<u64> {
        self.min_size
    }

    /// Get the maximum size constraint, if set.
    pub fn max_size(&self) -> Option<u64> {
        self.max_size
    }

    /// Check if this filter has any constraints.
    pub fn has_constraints(&self) -> bool {
        self.min_size.is_some() || self.max_size.is_some()
    }
}

impl Filter for SizeFilter {
    fn matches(&self, obj: &S3Object) -> bool {
        self.matches_size(obj.size)
    }

    fn description(&self) -> String {
        match (self.min_size, self.max_size) {
            (Some(min), Some(max)) => {
                format!("size(min={}, max={})", format_size(min), format_size(max))
            }
            (Some(min), None) => format!("size(min={})", format_size(min)),
            (None, Some(max)) => format!("size(max={})", format_size(max)),
            (None, None) => "size(any)".to_string(),
        }
    }
}

/// Format a size in bytes as a human-readable string.
fn format_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;

    if bytes >= GB {
        format!("{:.1}GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1}MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1}KB", bytes as f64 / KB as f64)
    } else {
        format!("{}B", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_obj(size: u64) -> S3Object {
        S3Object {
            key: "test.parquet".to_string(),
            size,
            last_modified: None,
        }
    }

    #[test]
    fn test_size_filter_no_constraints() {
        let filter = SizeFilter::new();

        assert!(filter.matches(&make_obj(0)));
        assert!(filter.matches(&make_obj(1024)));
        assert!(filter.matches(&make_obj(u64::MAX)));
        assert!(!filter.has_constraints());
    }

    #[test]
    fn test_size_filter_min_only() {
        let filter = SizeFilter::new().with_min_size(1024);

        assert!(!filter.matches(&make_obj(0)));
        assert!(!filter.matches(&make_obj(1023)));
        assert!(filter.matches(&make_obj(1024)));
        assert!(filter.matches(&make_obj(1025)));
        assert!(filter.matches(&make_obj(u64::MAX)));
    }

    #[test]
    fn test_size_filter_max_only() {
        let filter = SizeFilter::new().with_max_size(1024);

        assert!(filter.matches(&make_obj(0)));
        assert!(filter.matches(&make_obj(1023)));
        assert!(filter.matches(&make_obj(1024)));
        assert!(!filter.matches(&make_obj(1025)));
        assert!(!filter.matches(&make_obj(u64::MAX)));
    }

    #[test]
    fn test_size_filter_range() {
        let filter = SizeFilter::new().with_min_size(100).with_max_size(1000);

        assert!(!filter.matches(&make_obj(99)));
        assert!(filter.matches(&make_obj(100)));
        assert!(filter.matches(&make_obj(500)));
        assert!(filter.matches(&make_obj(1000)));
        assert!(!filter.matches(&make_obj(1001)));
    }

    #[test]
    fn test_size_filter_exact_size() {
        // Same min and max
        let filter = SizeFilter::new().with_min_size(1024).with_max_size(1024);

        assert!(!filter.matches(&make_obj(1023)));
        assert!(filter.matches(&make_obj(1024)));
        assert!(!filter.matches(&make_obj(1025)));
    }

    #[test]
    fn test_size_filter_getters() {
        let filter = SizeFilter::new().with_min_size(100).with_max_size(1000);

        assert_eq!(filter.min_size(), Some(100));
        assert_eq!(filter.max_size(), Some(1000));
        assert!(filter.has_constraints());
    }

    #[test]
    fn test_size_filter_description() {
        let filter_none = SizeFilter::new();
        assert_eq!(filter_none.description(), "size(any)");

        let filter_min = SizeFilter::new().with_min_size(1024);
        assert!(filter_min.description().contains("min="));

        let filter_max = SizeFilter::new().with_max_size(1024 * 1024);
        assert!(filter_max.description().contains("max="));

        let filter_both = SizeFilter::new().with_min_size(1024).with_max_size(1024 * 1024);
        assert!(filter_both.description().contains("min="));
        assert!(filter_both.description().contains("max="));
    }

    #[test]
    fn test_format_size() {
        assert_eq!(format_size(500), "500B");
        assert_eq!(format_size(1024), "1.0KB");
        assert_eq!(format_size(1536), "1.5KB");
        assert_eq!(format_size(1024 * 1024), "1.0MB");
        assert_eq!(format_size(1024 * 1024 * 1024), "1.0GB");
    }
}
