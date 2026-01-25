//! HTTP callback integration for discoverer.
//!
//! Provides HTTP callback support for reporting discovery completion
//! to external API endpoints. This enables integration with orchestration
//! systems that expect HTTP callbacks.

use pf_error::{PfError, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{debug, info, warn};

/// Default timeout for HTTP callback requests in seconds.
pub const DEFAULT_CALLBACK_TIMEOUT_SECS: u64 = 30;

/// Configuration for HTTP callback integration.
#[derive(Debug, Clone)]
pub struct CallbackConfig {
    /// The base URL for the callback API.
    /// If None, HTTP callbacks are disabled.
    pub callback_url: Option<String>,

    /// Bearer token for authorization.
    pub callback_token: Option<String>,

    /// Job ID to include in the callback payload.
    pub job_id: Option<String>,

    /// Timeout for HTTP requests in seconds.
    pub timeout_secs: u64,
}

impl CallbackConfig {
    /// Create a new configuration with optional callback URL.
    pub fn new(callback_url: Option<String>) -> Self {
        Self {
            callback_url,
            callback_token: None,
            job_id: None,
            timeout_secs: DEFAULT_CALLBACK_TIMEOUT_SECS,
        }
    }

    /// Set the bearer token for authorization.
    pub fn with_token(mut self, token: Option<String>) -> Self {
        self.callback_token = token;
        self
    }

    /// Set the job ID.
    pub fn with_job_id(mut self, job_id: Option<String>) -> Self {
        self.job_id = job_id;
        self
    }

    /// Set the timeout in seconds.
    pub fn with_timeout(mut self, timeout_secs: u64) -> Self {
        self.timeout_secs = timeout_secs;
        self
    }

    /// Check if HTTP callback is enabled.
    pub fn is_enabled(&self) -> bool {
        self.callback_url.is_some()
    }
}

/// Payload for discovery completion callback.
#[derive(Debug, Serialize, Deserialize)]
pub struct DiscoveryCompletePayload {
    /// Job ID
    pub job_id: String,

    /// Discovery status
    pub status: DiscoveryStatus,

    /// Number of files discovered
    pub files_discovered: usize,

    /// Number of files output to destination
    pub files_output: usize,

    /// Total bytes discovered
    pub bytes_discovered: u64,

    /// Error message if status is Failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Status of discovery completion.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DiscoveryStatus {
    /// Discovery completed successfully
    Completed,
    /// Discovery failed with an error
    Failed,
}

/// HTTP callback handler.
///
/// Handles sending completion callbacks to external APIs
/// when the discoverer completes.
pub struct HttpCallback {
    client: Client,
    callback_url: String,
    callback_token: Option<String>,
    job_id: String,
}

impl HttpCallback {
    /// Create a new HTTP callback handler.
    pub fn new(config: &CallbackConfig) -> Result<Option<Self>> {
        let callback_url = match &config.callback_url {
            Some(url) => url.clone(),
            None => return Ok(None),
        };

        let job_id = config
            .job_id
            .clone()
            .unwrap_or_else(|| "unknown".to_string());

        let client = Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .build()
            .map_err(|e| PfError::Config(format!("Failed to create HTTP client: {e}")))?;

        info!(
            callback_url = %callback_url,
            job_id = %job_id,
            "HTTP callback enabled"
        );

        Ok(Some(Self {
            client,
            callback_url,
            callback_token: config.callback_token.clone(),
            job_id,
        }))
    }

    /// Send a discovery completion callback.
    ///
    /// # Arguments
    ///
    /// * `files_discovered` - Number of files discovered
    /// * `files_output` - Number of files output to destination
    /// * `bytes_discovered` - Total bytes discovered
    pub async fn send_completion(
        &self,
        files_discovered: usize,
        files_output: usize,
        bytes_discovered: u64,
    ) -> Result<()> {
        let payload = DiscoveryCompletePayload {
            job_id: self.job_id.clone(),
            status: DiscoveryStatus::Completed,
            files_discovered,
            files_output,
            bytes_discovered,
            error: None,
        };

        self.send_callback(&payload).await
    }

    /// Send a discovery failure callback.
    ///
    /// # Arguments
    ///
    /// * `error` - Error message describing the failure
    pub async fn send_failure(&self, error: &str) -> Result<()> {
        let payload = DiscoveryCompletePayload {
            job_id: self.job_id.clone(),
            status: DiscoveryStatus::Failed,
            files_discovered: 0,
            files_output: 0,
            bytes_discovered: 0,
            error: Some(error.to_string()),
        };

        self.send_callback(&payload).await
    }

    /// Send the callback payload.
    async fn send_callback(&self, payload: &DiscoveryCompletePayload) -> Result<()> {
        let url = format!(
            "{}/internal/jobs/{}/discovery/complete",
            self.callback_url.trim_end_matches('/'),
            self.job_id
        );

        debug!(
            url = %url,
            job_id = %self.job_id,
            status = ?payload.status,
            files_output = payload.files_output,
            "Sending discovery completion callback"
        );

        let mut request = self.client.post(&url).json(payload);

        // Add authorization header if token is provided
        if let Some(token) = &self.callback_token {
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        let response = request.send().await.map_err(|e| {
            warn!(error = %e, url = %url, "Failed to send callback");
            PfError::Config(format!("Failed to send callback: {e}"))
        })?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            warn!(
                status = %status,
                body = %body,
                "Callback returned error status"
            );
            return Err(PfError::Config(format!(
                "Callback failed with status {}: {}",
                status, body
            )));
        }

        info!(
            url = %url,
            job_id = %self.job_id,
            status = ?payload.status,
            "Discovery callback sent successfully"
        );

        Ok(())
    }

    /// Get a reference to the job ID.
    pub fn job_id(&self) -> &str {
        &self.job_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_is_enabled() {
        let config = CallbackConfig::new(None);
        assert!(!config.is_enabled());

        let config = CallbackConfig::new(Some("http://api.example.com".to_string()));
        assert!(config.is_enabled());
    }

    #[test]
    fn test_config_builder() {
        let config = CallbackConfig::new(Some("http://api.example.com".to_string()))
            .with_token(Some("secret-token".to_string()))
            .with_job_id(Some("job-123".to_string()))
            .with_timeout(60);

        assert_eq!(
            config.callback_url,
            Some("http://api.example.com".to_string())
        );
        assert_eq!(config.callback_token, Some("secret-token".to_string()));
        assert_eq!(config.job_id, Some("job-123".to_string()));
        assert_eq!(config.timeout_secs, 60);
    }

    #[test]
    fn test_payload_serialization() {
        let payload = DiscoveryCompletePayload {
            job_id: "test-job".to_string(),
            status: DiscoveryStatus::Completed,
            files_discovered: 100,
            files_output: 95,
            bytes_discovered: 1024 * 1024,
            error: None,
        };

        let json = serde_json::to_string(&payload).unwrap();
        assert!(json.contains("\"status\":\"completed\""));
        assert!(json.contains("\"files_output\":95"));
        assert!(!json.contains("\"error\""));
    }

    #[test]
    fn test_failure_payload_serialization() {
        let payload = DiscoveryCompletePayload {
            job_id: "test-job".to_string(),
            status: DiscoveryStatus::Failed,
            files_discovered: 0,
            files_output: 0,
            bytes_discovered: 0,
            error: Some("Connection refused".to_string()),
        };

        let json = serde_json::to_string(&payload).unwrap();
        assert!(json.contains("\"status\":\"failed\""));
        assert!(json.contains("\"error\":\"Connection refused\""));
    }
}
