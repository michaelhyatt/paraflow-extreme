//! SQS output implementation for discovered files.

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_sqs::Client;
use pf_error::{PfError, Result};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::debug;

use super::Output;
use crate::DiscoveredFile;

/// Configuration for SQS output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqsConfig {
    /// SQS queue URL
    pub queue_url: String,

    /// Custom endpoint URL (for LocalStack)
    pub endpoint: Option<String>,

    /// AWS region
    pub region: Option<String>,

    /// Explicit AWS access key (optional, uses default credentials if not set)
    pub access_key: Option<String>,

    /// Explicit AWS secret key (optional, uses default credentials if not set)
    pub secret_key: Option<String>,

    /// AWS profile name (optional)
    pub profile: Option<String>,

    /// Batch size for SQS messages (max 10 per SQS API limit)
    pub batch_size: usize,
}

impl SqsConfig {
    /// Create a new SqsConfig with the required queue URL.
    pub fn new(queue_url: impl Into<String>) -> Self {
        Self {
            queue_url: queue_url.into(),
            endpoint: None,
            region: None,
            access_key: None,
            secret_key: None,
            profile: None,
            batch_size: 10, // SQS max
        }
    }

    /// Set a custom endpoint (for LocalStack).
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Set the AWS region.
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Set the batch size (max 10).
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size.min(10); // SQS limit
        self
    }
}

/// SQS output implementation with batching support.
///
/// Sends discovered files as JSON messages to an SQS queue.
/// Supports batching (up to 10 messages per request).
pub struct SqsOutput {
    client: Client,
    queue_url: String,
    batch_size: usize,
    buffer: Arc<Mutex<Vec<DiscoveredFile>>>,
}

impl SqsOutput {
    /// Create a new SqsOutput from configuration.
    pub async fn new(config: SqsConfig) -> Result<Self> {
        let client = build_sqs_client(&config).await?;
        Ok(Self {
            client,
            queue_url: config.queue_url,
            batch_size: config.batch_size.min(10), // SQS max is 10
            buffer: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// Create a new SqsOutput with an existing client (useful for testing).
    pub fn with_client(client: Client, queue_url: impl Into<String>) -> Self {
        Self {
            client,
            queue_url: queue_url.into(),
            batch_size: 10,
            buffer: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Send a batch of messages to SQS.
    async fn send_batch(&self, files: Vec<DiscoveredFile>) -> Result<()> {
        if files.is_empty() {
            return Ok(());
        }

        debug!(count = files.len(), "Sending SQS batch");

        let mut entries = Vec::with_capacity(files.len());
        for (i, file) in files.iter().enumerate() {
            let body = serde_json::to_string(file)
                .map_err(|e| PfError::Config(format!("JSON serialization failed: {e}")))?;

            let entry = aws_sdk_sqs::types::SendMessageBatchRequestEntry::builder()
                .id(format!("msg-{}", i))
                .message_body(body)
                .build()
                .map_err(|e| PfError::Config(format!("Failed to build SQS entry: {e}")))?;

            entries.push(entry);
        }

        let result = self
            .client
            .send_message_batch()
            .queue_url(&self.queue_url)
            .set_entries(Some(entries))
            .send()
            .await
            .map_err(|e| PfError::Config(format!("Failed to send SQS batch: {e}")))?;

        // Check for failures
        let failed = result.failed();
        if !failed.is_empty() {
            let codes: Vec<_> = failed.iter().map(|f| f.code()).collect();
            return Err(PfError::Config(format!(
                "Some SQS messages failed: {:?}",
                codes
            )));
        }

        Ok(())
    }
}

#[async_trait]
impl Output for SqsOutput {
    async fn output(&self, file: &DiscoveredFile) -> Result<()> {
        let mut buffer = self.buffer.lock().await;

        buffer.push(file.clone());

        // Send batch if buffer is full
        if buffer.len() >= self.batch_size {
            let batch: Vec<DiscoveredFile> = buffer.drain(..).collect();
            drop(buffer); // Release lock before sending
            self.send_batch(batch).await?;
        }

        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let mut buffer = self.buffer.lock().await;

        if buffer.is_empty() {
            return Ok(());
        }

        let batch: Vec<DiscoveredFile> = buffer.drain(..).collect();
        drop(buffer); // Release lock before sending
        self.send_batch(batch).await
    }
}

/// Build an SQS client from configuration.
async fn build_sqs_client(config: &SqsConfig) -> Result<Client> {
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

    // Set explicit credentials if provided
    if let (Some(access_key), Some(secret_key)) = (&config.access_key, &config.secret_key) {
        let credentials = aws_sdk_sqs::config::Credentials::new(
            access_key,
            secret_key,
            None,
            None,
            "pf-discoverer",
        );
        aws_config_loader = aws_config_loader.credentials_provider(credentials);
    }

    // Set profile if provided
    if let Some(profile) = &config.profile {
        aws_config_loader = aws_config_loader.profile_name(profile);
    }

    let aws_config = aws_config_loader.load().await;
    Ok(Client::new(&aws_config))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqs_config_builder() {
        let config = SqsConfig::new("http://localhost:4566/000000000000/test-queue")
            .with_endpoint("http://localhost:4566")
            .with_region("us-east-1");

        assert_eq!(
            config.queue_url,
            "http://localhost:4566/000000000000/test-queue"
        );
        assert_eq!(config.endpoint, Some("http://localhost:4566".to_string()));
        assert_eq!(config.region, Some("us-east-1".to_string()));
    }

    #[test]
    fn test_sqs_config_batch_size() {
        let config = SqsConfig::new("test-queue").with_batch_size(5);
        assert_eq!(config.batch_size, 5);

        // Should cap at 10
        let config = SqsConfig::new("test-queue").with_batch_size(20);
        assert_eq!(config.batch_size, 10);
    }
}
