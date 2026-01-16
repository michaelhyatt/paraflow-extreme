//! Retry logic for S3 operations.
//!
//! Provides exponential backoff with jitter for transient S3 errors.

use rand::Rng;
use std::time::Duration;
use tokio::time::sleep;
use tracing::warn;

/// Configuration for retry behavior.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retries before giving up.
    pub max_retries: u32,
    /// Initial backoff duration in milliseconds.
    pub initial_backoff_ms: u64,
    /// Maximum backoff duration in milliseconds.
    pub max_backoff_ms: u64,
    /// Whether to add jitter to backoff times.
    pub jitter: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff_ms: 100,
            max_backoff_ms: 10000,
            jitter: true,
        }
    }
}

impl RetryConfig {
    /// Create a new retry configuration with defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the maximum number of retries.
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Set the initial backoff in milliseconds.
    pub fn with_initial_backoff_ms(mut self, initial_backoff_ms: u64) -> Self {
        self.initial_backoff_ms = initial_backoff_ms;
        self
    }

    /// Set the maximum backoff in milliseconds.
    pub fn with_max_backoff_ms(mut self, max_backoff_ms: u64) -> Self {
        self.max_backoff_ms = max_backoff_ms;
        self
    }

    /// Enable or disable jitter.
    pub fn with_jitter(mut self, jitter: bool) -> Self {
        self.jitter = jitter;
        self
    }

    /// Calculate the backoff duration for a given attempt.
    pub fn backoff_duration(&self, attempt: u32) -> Duration {
        let base_ms = self.initial_backoff_ms * 2u64.pow(attempt);
        let capped_ms = base_ms.min(self.max_backoff_ms);

        let final_ms = if self.jitter {
            let jitter_range = capped_ms / 4; // 25% jitter
            let jitter = rand::rng().random_range(0..=jitter_range);
            capped_ms.saturating_add(jitter)
        } else {
            capped_ms
        };

        Duration::from_millis(final_ms)
    }
}

/// Error classification for retry decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClassification {
    /// The error is transient and can be retried.
    Retryable,
    /// The error is permanent and should not be retried.
    NonRetryable,
}

/// Classify an S3 error for retry purposes.
///
/// Retryable errors:
/// - HTTP 5xx (server errors)
/// - SlowDown (throttling)
/// - TooManyRequests
/// - Network timeouts
/// - Service unavailable
///
/// Non-retryable errors:
/// - HTTP 4xx (client errors) except throttling
/// - NoSuchKey
/// - AccessDenied
/// - InvalidRequest
pub fn classify_error(error: &str) -> ErrorClassification {
    let error_lower = error.to_lowercase();

    // Check for retryable conditions
    if error_lower.contains("slowdown")
        || error_lower.contains("toomanyrequests")
        || error_lower.contains("throttl")
        || error_lower.contains("service unavailable")
        || error_lower.contains("500")
        || error_lower.contains("502")
        || error_lower.contains("503")
        || error_lower.contains("504")
        || error_lower.contains("timeout")
        || error_lower.contains("connection reset")
        || error_lower.contains("connection refused")
    {
        return ErrorClassification::Retryable;
    }

    // Check for non-retryable conditions
    if error_lower.contains("nosuchkey")
        || error_lower.contains("accessdenied")
        || error_lower.contains("invalidrequest")
        || error_lower.contains("nosuchbucket")
        || error_lower.contains("403")
        || error_lower.contains("404")
        || error_lower.contains("400")
    {
        return ErrorClassification::NonRetryable;
    }

    // Default to retryable for unknown errors (be optimistic)
    ErrorClassification::Retryable
}

/// Execute an async operation with retry logic.
///
/// # Arguments
///
/// * `config` - Retry configuration
/// * `operation_name` - Name of the operation for logging
/// * `operation` - The async operation to execute
///
/// # Returns
///
/// The result of the operation, or the last error if all retries failed.
pub async fn with_retry<F, Fut, T, E>(
    config: &RetryConfig,
    operation_name: &str,
    mut operation: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut last_error: Option<E> = None;

    for attempt in 0..=config.max_retries {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) => {
                let classification = classify_error(&e.to_string());

                if classification == ErrorClassification::NonRetryable {
                    warn!(
                        operation = operation_name,
                        attempt = attempt,
                        error = %e,
                        "Non-retryable error"
                    );
                    return Err(e);
                }

                if attempt < config.max_retries {
                    let backoff = config.backoff_duration(attempt);
                    warn!(
                        operation = operation_name,
                        attempt = attempt,
                        error = %e,
                        backoff_ms = backoff.as_millis(),
                        "Retryable error, backing off"
                    );
                    sleep(backoff).await;
                }

                last_error = Some(e);
            }
        }
    }

    Err(last_error.expect("should have last error after all retries"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_config_defaults() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.initial_backoff_ms, 100);
        assert_eq!(config.max_backoff_ms, 10000);
        assert!(config.jitter);
    }

    #[test]
    fn test_retry_config_builder() {
        let config = RetryConfig::new()
            .with_max_retries(5)
            .with_initial_backoff_ms(200)
            .with_max_backoff_ms(5000)
            .with_jitter(false);

        assert_eq!(config.max_retries, 5);
        assert_eq!(config.initial_backoff_ms, 200);
        assert_eq!(config.max_backoff_ms, 5000);
        assert!(!config.jitter);
    }

    #[test]
    fn test_backoff_duration_no_jitter() {
        let config = RetryConfig::new()
            .with_initial_backoff_ms(100)
            .with_max_backoff_ms(10000)
            .with_jitter(false);

        assert_eq!(config.backoff_duration(0), Duration::from_millis(100));
        assert_eq!(config.backoff_duration(1), Duration::from_millis(200));
        assert_eq!(config.backoff_duration(2), Duration::from_millis(400));
        assert_eq!(config.backoff_duration(3), Duration::from_millis(800));
    }

    #[test]
    fn test_backoff_duration_capped() {
        let config = RetryConfig::new()
            .with_initial_backoff_ms(1000)
            .with_max_backoff_ms(2000)
            .with_jitter(false);

        assert_eq!(config.backoff_duration(0), Duration::from_millis(1000));
        assert_eq!(config.backoff_duration(1), Duration::from_millis(2000));
        assert_eq!(config.backoff_duration(2), Duration::from_millis(2000)); // Capped
        assert_eq!(config.backoff_duration(10), Duration::from_millis(2000)); // Capped
    }

    #[test]
    fn test_classify_error_retryable() {
        assert_eq!(
            classify_error("SlowDown: reduce request rate"),
            ErrorClassification::Retryable
        );
        assert_eq!(
            classify_error("Service Unavailable"),
            ErrorClassification::Retryable
        );
        assert_eq!(
            classify_error("503 Service Temporarily Unavailable"),
            ErrorClassification::Retryable
        );
        assert_eq!(
            classify_error("Connection timeout"),
            ErrorClassification::Retryable
        );
        assert_eq!(
            classify_error("TooManyRequests"),
            ErrorClassification::Retryable
        );
    }

    #[test]
    fn test_classify_error_non_retryable() {
        assert_eq!(
            classify_error("NoSuchKey: key not found"),
            ErrorClassification::NonRetryable
        );
        assert_eq!(
            classify_error("AccessDenied: permission denied"),
            ErrorClassification::NonRetryable
        );
        assert_eq!(
            classify_error("404 Not Found"),
            ErrorClassification::NonRetryable
        );
        assert_eq!(
            classify_error("403 Forbidden"),
            ErrorClassification::NonRetryable
        );
    }

    #[tokio::test]
    async fn test_with_retry_success_first_try() {
        let config = RetryConfig::new();
        let mut call_count = 0;

        let result: Result<i32, &str> = with_retry(&config, "test_op", || {
            call_count += 1;
            async { Ok(42) }
        })
        .await;

        assert_eq!(result, Ok(42));
        assert_eq!(call_count, 1);
    }

    #[tokio::test]
    async fn test_with_retry_success_after_retry() {
        let config = RetryConfig::new()
            .with_initial_backoff_ms(1)
            .with_jitter(false);
        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let call_count_clone = call_count.clone();

        let result: Result<i32, String> = with_retry(&config, "test_op", || {
            let count = call_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async move {
                if count < 2 {
                    Err("timeout error".to_string())
                } else {
                    Ok(42)
                }
            }
        })
        .await;

        assert_eq!(result, Ok(42));
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_with_retry_non_retryable_error() {
        let config = RetryConfig::new();
        let call_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let call_count_clone = call_count.clone();

        let result: Result<i32, String> = with_retry(&config, "test_op", || {
            call_count_clone.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            async { Err("NoSuchKey: key not found".to_string()) }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(call_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}
