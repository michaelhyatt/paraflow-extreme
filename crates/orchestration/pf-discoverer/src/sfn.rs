//! Step Functions integration for discoverer.
//!
//! Provides task token callback support for AWS Step Functions integration.
//! When the discoverer is invoked as part of a Step Functions workflow,
//! it can report success/failure back to the workflow using task tokens.

use aws_config::BehaviorVersion;
use aws_sdk_sfn::Client as SfnClient;
use pf_error::{PfError, Result};
use serde::Serialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

/// Default heartbeat interval in seconds.
/// Step Functions tasks typically have a 5-10 minute timeout,
/// so sending heartbeats every 30 seconds provides good coverage.
pub const DEFAULT_HEARTBEAT_INTERVAL_SECS: u64 = 30;

/// Configuration for Step Functions integration.
#[derive(Debug, Clone)]
pub struct StepFunctionsConfig {
    /// The task token received from Step Functions.
    /// If None, Step Functions callbacks are disabled.
    pub task_token: Option<String>,

    /// AWS region for Step Functions API calls.
    pub region: Option<String>,

    /// Custom endpoint URL (for LocalStack/testing).
    pub endpoint: Option<String>,

    /// Heartbeat interval in seconds.
    pub heartbeat_interval_secs: u64,
}

impl StepFunctionsConfig {
    /// Create a new configuration with optional task token.
    pub fn new(task_token: Option<String>) -> Self {
        Self {
            task_token,
            region: None,
            endpoint: None,
            heartbeat_interval_secs: DEFAULT_HEARTBEAT_INTERVAL_SECS,
        }
    }

    /// Set the AWS region.
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Set a custom endpoint (for LocalStack/testing).
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Set the heartbeat interval in seconds.
    pub fn with_heartbeat_interval(mut self, interval_secs: u64) -> Self {
        self.heartbeat_interval_secs = interval_secs;
        self
    }

    /// Check if Step Functions integration is enabled.
    pub fn is_enabled(&self) -> bool {
        self.task_token.is_some()
    }
}

/// Step Functions callback handler.
///
/// Handles sending task success/failure callbacks to Step Functions
/// when the discoverer completes.
pub struct StepFunctionsCallback {
    client: SfnClient,
    task_token: String,
}

impl StepFunctionsCallback {
    /// Create a new Step Functions callback handler.
    pub async fn new(config: &StepFunctionsConfig) -> Result<Option<Self>> {
        let task_token = match &config.task_token {
            Some(token) => token.clone(),
            None => return Ok(None),
        };

        let client = build_sfn_client(config).await?;

        info!("Step Functions callback enabled");

        Ok(Some(Self { client, task_token }))
    }

    /// Send a task success callback to Step Functions.
    ///
    /// # Arguments
    ///
    /// * `output` - The output to send (will be JSON serialized)
    pub async fn send_task_success<T: Serialize>(&self, output: &T) -> Result<()> {
        let output_json = serde_json::to_string(output)
            .map_err(|e| PfError::Config(format!("Failed to serialize output: {e}")))?;

        debug!(output_len = output_json.len(), "Sending task success");

        self.client
            .send_task_success()
            .task_token(&self.task_token)
            .output(output_json)
            .send()
            .await
            .map_err(|e| PfError::Config(format!("Failed to send task success: {e}")))?;

        info!("Step Functions task success sent");
        Ok(())
    }

    /// Send a task failure callback to Step Functions.
    ///
    /// # Arguments
    ///
    /// * `error` - Error code/type
    /// * `cause` - Error message/description
    pub async fn send_task_failure(&self, error: &str, cause: &str) -> Result<()> {
        warn!(error = %error, cause = %cause, "Sending task failure");

        self.client
            .send_task_failure()
            .task_token(&self.task_token)
            .error(error)
            .cause(cause)
            .send()
            .await
            .map_err(|e| PfError::Config(format!("Failed to send task failure: {e}")))?;

        info!("Step Functions task failure sent");
        Ok(())
    }

    /// Get a reference to the task token.
    pub fn task_token(&self) -> &str {
        &self.task_token
    }

    /// Get a clone of the SFN client for use in heartbeat loop.
    pub fn client(&self) -> SfnClient {
        self.client.clone()
    }
}

/// Heartbeat loop for long-running Step Functions tasks.
///
/// Sends periodic heartbeats to Step Functions to prevent task timeout.
/// The loop runs in the background and should be stopped when the task completes.
pub struct HeartbeatLoop {
    stop_flag: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl HeartbeatLoop {
    /// Start a new heartbeat loop.
    ///
    /// # Arguments
    ///
    /// * `client` - The Step Functions client
    /// * `task_token` - The task token to send heartbeats for
    /// * `interval` - The interval between heartbeats
    pub fn start(client: SfnClient, task_token: String, interval: Duration) -> Self {
        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_clone = Arc::clone(&stop_flag);

        info!(
            interval_secs = interval.as_secs(),
            "Starting Step Functions heartbeat loop"
        );

        let handle = tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            interval_timer.tick().await; // Skip first immediate tick

            loop {
                interval_timer.tick().await;

                if stop_clone.load(Ordering::Relaxed) {
                    debug!("Heartbeat loop stopping");
                    break;
                }

                match client
                    .send_task_heartbeat()
                    .task_token(&task_token)
                    .send()
                    .await
                {
                    Ok(_) => {
                        debug!("Step Functions heartbeat sent");
                    }
                    Err(e) => {
                        // Log but don't fail - the task may have already completed
                        // or the workflow may have been cancelled
                        warn!(error = %e, "Failed to send Step Functions heartbeat");
                    }
                }
            }
        });

        Self {
            stop_flag,
            handle: Some(handle),
        }
    }

    /// Stop the heartbeat loop.
    pub async fn stop(mut self) {
        self.stop_flag.store(true, Ordering::Relaxed);

        if let Some(handle) = self.handle.take() {
            // Wait for the loop to finish (with a timeout)
            let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
        }

        info!("Step Functions heartbeat loop stopped");
    }
}

/// Build a Step Functions client from configuration.
async fn build_sfn_client(config: &StepFunctionsConfig) -> Result<SfnClient> {
    use aws_config::Region;

    let mut aws_config_loader = aws_config::defaults(BehaviorVersion::latest());

    // Set region if provided
    if let Some(region) = &config.region {
        aws_config_loader = aws_config_loader.region(Region::new(region.clone()));
    }

    // Set custom endpoint if provided (for LocalStack)
    if let Some(endpoint) = &config.endpoint {
        aws_config_loader = aws_config_loader.endpoint_url(endpoint);
    }

    let aws_config = aws_config_loader.load().await;
    Ok(SfnClient::new(&aws_config))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_is_enabled() {
        let config = StepFunctionsConfig::new(None);
        assert!(!config.is_enabled());

        let config = StepFunctionsConfig::new(Some("token123".to_string()));
        assert!(config.is_enabled());
    }

    #[test]
    fn test_config_builder() {
        let config = StepFunctionsConfig::new(Some("token123".to_string()))
            .with_region("us-west-2")
            .with_endpoint("http://localhost:4566");

        assert_eq!(config.task_token, Some("token123".to_string()));
        assert_eq!(config.region, Some("us-west-2".to_string()));
        assert_eq!(config.endpoint, Some("http://localhost:4566".to_string()));
    }
}
