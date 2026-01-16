//! S3 client configuration and creation.

use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use pf_error::Result;
use serde::{Deserialize, Serialize};

/// Configuration for S3 access.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Config {
    /// S3 bucket name
    pub bucket: String,

    /// Optional prefix to filter objects
    pub prefix: Option<String>,

    /// AWS region
    pub region: Option<String>,

    /// Custom endpoint URL (for LocalStack)
    pub endpoint: Option<String>,

    /// Explicit AWS access key (optional)
    pub access_key: Option<String>,

    /// Explicit AWS secret key (optional)
    pub secret_key: Option<String>,

    /// AWS profile name (optional)
    pub profile: Option<String>,

    /// Request timeout in seconds
    pub timeout_secs: u64,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            prefix: None,
            region: None,
            endpoint: None,
            access_key: None,
            secret_key: None,
            profile: None,
            timeout_secs: 30,
        }
    }
}

impl S3Config {
    /// Create a new S3Config with the required bucket name.
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            ..Default::default()
        }
    }

    /// Set the prefix for filtering objects.
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
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

    /// Set explicit credentials.
    pub fn with_credentials(
        mut self,
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Self {
        self.access_key = Some(access_key.into());
        self.secret_key = Some(secret_key.into());
        self
    }

    /// Set the AWS profile.
    pub fn with_profile(mut self, profile: impl Into<String>) -> Self {
        self.profile = Some(profile.into());
        self
    }

    /// Set the request timeout in seconds.
    pub fn with_timeout(mut self, timeout_secs: u64) -> Self {
        self.timeout_secs = timeout_secs;
        self
    }
}

/// Create an S3 client from configuration.
pub async fn create_s3_client(config: &S3Config) -> Result<Client> {
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
        let credentials = aws_sdk_s3::config::Credentials::new(
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

    // Build S3 client with path-style access for LocalStack compatibility
    let s3_config_builder = aws_sdk_s3::config::Builder::from(&aws_config);

    // Enable path-style access if using a custom endpoint (LocalStack)
    let s3_config = if config.endpoint.is_some() {
        s3_config_builder.force_path_style(true).build()
    } else {
        s3_config_builder.build()
    };

    Ok(Client::from_conf(s3_config))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_config_builder() {
        let config = S3Config::new("test-bucket")
            .with_prefix("data/")
            .with_endpoint("http://localhost:4566")
            .with_region("us-east-1")
            .with_timeout(60);

        assert_eq!(config.bucket, "test-bucket");
        assert_eq!(config.prefix, Some("data/".to_string()));
        assert_eq!(config.endpoint, Some("http://localhost:4566".to_string()));
        assert_eq!(config.region, Some("us-east-1".to_string()));
        assert_eq!(config.timeout_secs, 60);
    }

    #[test]
    fn test_s3_config_with_credentials() {
        let config = S3Config::new("test-bucket").with_credentials("access", "secret");

        assert_eq!(config.access_key, Some("access".to_string()));
        assert_eq!(config.secret_key, Some("secret".to_string()));
    }

    #[test]
    fn test_s3_config_default() {
        let config = S3Config::default();

        assert!(config.bucket.is_empty());
        assert!(config.prefix.is_none());
        assert!(config.endpoint.is_none());
        assert_eq!(config.timeout_secs, 30);
    }
}
