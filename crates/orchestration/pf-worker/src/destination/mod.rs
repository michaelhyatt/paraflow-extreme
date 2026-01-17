//! Destination implementations.
//!
//! This module provides implementations of the [`BatchIndexer`](pf_traits::BatchIndexer)
//! trait for different output destinations:
//!
//! - [`StdoutDestination`]: Outputs records as JSON/JSONL to stdout
//! - [`StatsDestination`]: Counts records without outputting (for performance testing)

mod stats;
mod stdout;

pub use stats::StatsDestination;
pub use stdout::{OutputFormat, StdoutDestination};

use pf_traits::BatchIndexer;
use std::sync::Arc;

/// Destination type enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestinationType {
    /// Output to stdout as JSON/JSONL
    Stdout,
    /// Count records without output (for performance testing)
    Stats,
}

/// Create a destination based on the type.
pub fn create_destination(dest_type: DestinationType) -> Arc<dyn BatchIndexer> {
    match dest_type {
        DestinationType::Stdout => Arc::new(StdoutDestination::new(OutputFormat::Jsonl)),
        DestinationType::Stats => Arc::new(StatsDestination::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_destination_stdout() {
        let _dest = create_destination(DestinationType::Stdout);
        // Just verify it creates without panic
    }

    #[test]
    fn test_create_destination_stats() {
        let _dest = create_destination(DestinationType::Stats);
        // Just verify it creates without panic
    }
}
