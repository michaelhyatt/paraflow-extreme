//! Logging initialization utilities.

use anyhow::Result;
use tracing::Level;
use tracing_subscriber::fmt;

use crate::LogLevel;

/// Initialize logging with the specified level.
///
/// Logs are written to stderr so stdout remains clean for program output.
pub fn init_logging(level: LogLevel) -> Result<()> {
    let level: Level = level.into();

    let subscriber = fmt::Subscriber::builder()
        .with_max_level(level)
        .with_writer(std::io::stderr); // Log to stderr so stdout is clean for output

    subscriber.init();

    Ok(())
}
