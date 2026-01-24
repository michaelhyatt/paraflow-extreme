//! Statistics for discovery runs.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

/// Statistics collected during a discovery run.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiscoveryStats {
    /// When discovery started
    pub started_at: Option<DateTime<Utc>>,

    /// When discovery completed
    pub completed_at: Option<DateTime<Utc>>,

    /// Total number of files discovered (before filtering)
    pub files_discovered: usize,

    /// Number of files filtered out by pattern matching
    pub files_filtered: usize,

    /// Number of files output (passed filter)
    pub files_output: usize,

    /// Total bytes of discovered files (that passed filter)
    pub bytes_discovered: u64,

    /// Errors encountered during discovery
    pub errors: Vec<String>,
}

impl DiscoveryStats {
    /// Create a new stats tracker with the current time as start time.
    pub fn new() -> Self {
        Self {
            started_at: Some(Utc::now()),
            ..Default::default()
        }
    }

    /// Mark discovery as complete with the current time.
    pub fn complete(&mut self) {
        self.completed_at = Some(Utc::now());
    }

    /// Record a discovered file that passed the filter.
    pub fn record_output(&mut self, size_bytes: u64) {
        self.files_discovered += 1;
        self.files_output += 1;
        self.bytes_discovered += size_bytes;
    }

    /// Record a discovered file that was filtered out.
    pub fn record_filtered(&mut self) {
        self.files_discovered += 1;
        self.files_filtered += 1;
    }

    /// Record an error.
    pub fn record_error(&mut self, error: impl ToString) {
        self.errors.push(error.to_string());
    }

    /// Get the duration of the discovery run.
    pub fn duration(&self) -> Option<Duration> {
        match (self.started_at, self.completed_at) {
            (Some(start), Some(end)) => Some(end - start),
            _ => None,
        }
    }

    /// Check if any errors occurred.
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// Get the number of errors.
    pub fn error_count(&self) -> usize {
        self.errors.len()
    }

    /// Calculate the throughput in files per second.
    pub fn files_per_second(&self) -> Option<f64> {
        self.duration().map(|d| {
            let secs = d.num_milliseconds() as f64 / 1000.0;
            if secs > 0.0 {
                self.files_output as f64 / secs
            } else {
                0.0
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration as StdDuration;

    #[test]
    fn test_stats_new() {
        let stats = DiscoveryStats::new();
        assert!(stats.started_at.is_some());
        assert!(stats.completed_at.is_none());
        assert_eq!(stats.files_discovered, 0);
    }

    #[test]
    fn test_stats_record_output() {
        let mut stats = DiscoveryStats::new();
        stats.record_output(1024);
        stats.record_output(2048);

        assert_eq!(stats.files_discovered, 2);
        assert_eq!(stats.files_output, 2);
        assert_eq!(stats.files_filtered, 0);
        assert_eq!(stats.bytes_discovered, 3072);
    }

    #[test]
    fn test_stats_record_filtered() {
        let mut stats = DiscoveryStats::new();
        stats.record_output(1024);
        stats.record_filtered();
        stats.record_filtered();

        assert_eq!(stats.files_discovered, 3);
        assert_eq!(stats.files_output, 1);
        assert_eq!(stats.files_filtered, 2);
    }

    #[test]
    fn test_stats_errors() {
        let mut stats = DiscoveryStats::new();
        assert!(!stats.has_errors());

        stats.record_error("something went wrong");
        assert!(stats.has_errors());
        assert_eq!(stats.error_count(), 1);
    }

    #[test]
    fn test_stats_duration() {
        let mut stats = DiscoveryStats::new();
        sleep(StdDuration::from_millis(10));
        stats.complete();

        let duration = stats.duration().unwrap();
        assert!(duration.num_milliseconds() >= 10);
    }

    #[test]
    fn test_stats_default() {
        let stats = DiscoveryStats::default();
        assert!(stats.started_at.is_none());
        assert!(stats.completed_at.is_none());
        assert_eq!(stats.files_discovered, 0);
    }
}
