//! S3 discoverer integration tests using LocalStack.
//!
//! These tests verify that the Discoverer can correctly list and filter
//! files from S3 buckets using LocalStack.

use crate::common::{generate_test_ndjson, generate_test_parquet, LocalStackTestContext};
use pf_discoverer::{
    DiscoveredFile, Discoverer, DiscoveryConfig, MatchAllFilter, Output, PatternFilter,
    S3Config, SizeFilter, create_s3_client,
};
use pf_error::Result;
use std::sync::{Arc, Mutex};

/// Collecting output that stores discovered files for verification.
#[derive(Default, Clone)]
struct CollectingOutput {
    files: Arc<Mutex<Vec<DiscoveredFile>>>,
}

impl CollectingOutput {
    fn new() -> Self {
        Self::default()
    }

    fn files(&self) -> Vec<DiscoveredFile> {
        self.files.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl Output for CollectingOutput {
    async fn output(&self, file: &DiscoveredFile) -> Result<()> {
        self.files.lock().unwrap().push(file.clone());
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_discover_files_from_s3() {
    let ctx = LocalStackTestContext::new().await;

    if !ctx.is_available().await {
        eprintln!("LocalStack not available, skipping test");
        return;
    }

    let bucket = "test-discover-bucket";
    ctx.create_bucket(bucket).await.unwrap();

    // Upload test files
    let ndjson1 = generate_test_ndjson(10);
    let ndjson2 = generate_test_ndjson(20);
    ctx.upload_ndjson(bucket, "data/file1.ndjson", &ndjson1)
        .await
        .unwrap();
    ctx.upload_ndjson(bucket, "data/file2.ndjson", &ndjson2)
        .await
        .unwrap();
    ctx.upload_ndjson(bucket, "other/file3.ndjson", &ndjson1)
        .await
        .unwrap();

    // Create S3 client with LocalStack endpoint
    let s3_config = S3Config::new(bucket)
        .with_prefix("data/")
        .with_endpoint(&ctx.endpoint);

    let client = create_s3_client(&s3_config).await.unwrap();

    // Run discovery
    let config = DiscoveryConfig::new();
    let filter = MatchAllFilter;
    let output = CollectingOutput::new();

    let discoverer = Discoverer::new(
        client,
        bucket,
        Some("data/".to_string()),
        output.clone(),
        filter,
        config,
    );

    let stats = discoverer.discover().await.unwrap();

    // Verify results
    assert_eq!(stats.files_output, 2);
    let files = output.files();
    assert_eq!(files.len(), 2);

    // Should only find files in data/ prefix
    assert!(files.iter().any(|f| f.uri.contains("file1.ndjson")));
    assert!(files.iter().any(|f| f.uri.contains("file2.ndjson")));
    assert!(!files.iter().any(|f| f.uri.contains("file3.ndjson")));

    // Cleanup
    ctx.delete_object(bucket, "data/file1.ndjson").await.ok();
    ctx.delete_object(bucket, "data/file2.ndjson").await.ok();
    ctx.delete_object(bucket, "other/file3.ndjson").await.ok();
}

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_discover_with_pattern_filter() {
    let ctx = LocalStackTestContext::new().await;

    if !ctx.is_available().await {
        eprintln!("LocalStack not available, skipping test");
        return;
    }

    let bucket = "test-pattern-bucket";
    ctx.create_bucket(bucket).await.unwrap();

    // Upload mixed file types
    let ndjson = generate_test_ndjson(10);
    let parquet = generate_test_parquet(10);
    ctx.upload_ndjson(bucket, "data/events.ndjson", &ndjson)
        .await
        .unwrap();
    ctx.upload_parquet(bucket, "data/events.parquet", parquet.clone())
        .await
        .unwrap();
    ctx.upload_ndjson(bucket, "data/logs.ndjson", &ndjson)
        .await
        .unwrap();
    ctx.upload_parquet(bucket, "data/logs.parquet", parquet)
        .await
        .unwrap();

    // Create S3 client
    let s3_config = S3Config::new(bucket).with_endpoint(&ctx.endpoint);
    let client = create_s3_client(&s3_config).await.unwrap();

    // Filter for only parquet files
    let config = DiscoveryConfig::new();
    let filter = PatternFilter::new("*.parquet").unwrap();
    let output = CollectingOutput::new();

    let discoverer = Discoverer::new(
        client,
        bucket,
        Some("data/".to_string()),
        output.clone(),
        filter,
        config,
    );

    let stats = discoverer.discover().await.unwrap();

    // Should only find parquet files
    assert_eq!(stats.files_output, 2);
    let files = output.files();
    assert!(files.iter().all(|f| f.uri.ends_with(".parquet")));

    // Cleanup
    ctx.delete_object(bucket, "data/events.ndjson").await.ok();
    ctx.delete_object(bucket, "data/events.parquet").await.ok();
    ctx.delete_object(bucket, "data/logs.ndjson").await.ok();
    ctx.delete_object(bucket, "data/logs.parquet").await.ok();
}

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_discover_with_size_filter() {
    let ctx = LocalStackTestContext::new().await;

    if !ctx.is_available().await {
        eprintln!("LocalStack not available, skipping test");
        return;
    }

    let bucket = "test-size-bucket";
    ctx.create_bucket(bucket).await.unwrap();

    // Upload files of different sizes
    let small = generate_test_ndjson(5);
    let medium = generate_test_ndjson(50);
    let large = generate_test_ndjson(500);

    ctx.upload_ndjson(bucket, "data/small.ndjson", &small)
        .await
        .unwrap();
    ctx.upload_ndjson(bucket, "data/medium.ndjson", &medium)
        .await
        .unwrap();
    ctx.upload_ndjson(bucket, "data/large.ndjson", &large)
        .await
        .unwrap();

    // Create S3 client
    let s3_config = S3Config::new(bucket).with_endpoint(&ctx.endpoint);
    let client = create_s3_client(&s3_config).await.unwrap();

    // Filter for files larger than medium size
    let config = DiscoveryConfig::new();
    let filter = SizeFilter::new().with_min_size(medium.len() as u64 + 1);
    let output = CollectingOutput::new();

    let discoverer = Discoverer::new(
        client,
        bucket,
        Some("data/".to_string()),
        output.clone(),
        filter,
        config,
    );

    let stats = discoverer.discover().await.unwrap();

    // Should only find the large file
    assert_eq!(stats.files_output, 1);
    let files = output.files();
    assert!(files[0].uri.contains("large.ndjson"));

    // Cleanup
    ctx.delete_object(bucket, "data/small.ndjson").await.ok();
    ctx.delete_object(bucket, "data/medium.ndjson").await.ok();
    ctx.delete_object(bucket, "data/large.ndjson").await.ok();
}

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_discover_pagination() {
    let ctx = LocalStackTestContext::new().await;

    if !ctx.is_available().await {
        eprintln!("LocalStack not available, skipping test");
        return;
    }

    let bucket = "test-pagination-bucket";
    ctx.create_bucket(bucket).await.unwrap();

    // Upload many files to test pagination
    let ndjson = generate_test_ndjson(5);
    for i in 0..25 {
        ctx.upload_ndjson(bucket, &format!("data/file_{:03}.ndjson", i), &ndjson)
            .await
            .unwrap();
    }

    // Create S3 client
    let s3_config = S3Config::new(bucket).with_endpoint(&ctx.endpoint);
    let client = create_s3_client(&s3_config).await.unwrap();

    // Test pagination (the discoverer handles pagination internally)
    let config = DiscoveryConfig::new();
    let filter = MatchAllFilter;
    let output = CollectingOutput::new();

    let discoverer = Discoverer::new(
        client,
        bucket,
        Some("data/".to_string()),
        output.clone(),
        filter,
        config,
    );

    let stats = discoverer.discover().await.unwrap();

    // Should find all files despite pagination
    assert_eq!(stats.files_output, 25);
    let files = output.files();
    assert_eq!(files.len(), 25);

    // Cleanup
    for i in 0..25 {
        ctx.delete_object(bucket, &format!("data/file_{:03}.ndjson", i))
            .await
            .ok();
    }
}

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_discover_empty_bucket() {
    let ctx = LocalStackTestContext::new().await;

    if !ctx.is_available().await {
        eprintln!("LocalStack not available, skipping test");
        return;
    }

    let bucket = "test-empty-bucket";
    ctx.create_bucket(bucket).await.unwrap();

    // Create S3 client
    let s3_config = S3Config::new(bucket).with_endpoint(&ctx.endpoint);
    let client = create_s3_client(&s3_config).await.unwrap();

    let config = DiscoveryConfig::new();
    let filter = MatchAllFilter;
    let output = CollectingOutput::new();

    let discoverer = Discoverer::new(
        client,
        bucket,
        Some("data/".to_string()),
        output.clone(),
        filter,
        config,
    );

    let stats = discoverer.discover().await.unwrap();

    // Should handle empty bucket gracefully
    assert_eq!(stats.files_output, 0);
    assert!(output.files().is_empty());
}

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_discover_max_files_limit() {
    let ctx = LocalStackTestContext::new().await;

    if !ctx.is_available().await {
        eprintln!("LocalStack not available, skipping test");
        return;
    }

    let bucket = "test-maxfiles-bucket";
    ctx.create_bucket(bucket).await.unwrap();

    // Upload 10 files
    let ndjson = generate_test_ndjson(5);
    for i in 0..10 {
        ctx.upload_ndjson(bucket, &format!("data/file_{}.ndjson", i), &ndjson)
            .await
            .unwrap();
    }

    // Create S3 client
    let s3_config = S3Config::new(bucket).with_endpoint(&ctx.endpoint);
    let client = create_s3_client(&s3_config).await.unwrap();

    // Limit to 5 files
    let config = DiscoveryConfig::new().with_max_files(5);
    let filter = MatchAllFilter;
    let output = CollectingOutput::new();

    let discoverer = Discoverer::new(
        client,
        bucket,
        Some("data/".to_string()),
        output.clone(),
        filter,
        config,
    );

    let stats = discoverer.discover().await.unwrap();

    // Should stop at max_files
    assert_eq!(stats.files_output, 5);
    assert_eq!(output.files().len(), 5);

    // Cleanup
    for i in 0..10 {
        ctx.delete_object(bucket, &format!("data/file_{}.ndjson", i))
            .await
            .ok();
    }
}

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_discover_nested_prefixes() {
    let ctx = LocalStackTestContext::new().await;

    if !ctx.is_available().await {
        eprintln!("LocalStack not available, skipping test");
        return;
    }

    let bucket = "test-nested-bucket";
    ctx.create_bucket(bucket).await.unwrap();

    // Upload files in nested directories
    let ndjson = generate_test_ndjson(5);
    ctx.upload_ndjson(bucket, "data/2024/01/file1.ndjson", &ndjson)
        .await
        .unwrap();
    ctx.upload_ndjson(bucket, "data/2024/01/file2.ndjson", &ndjson)
        .await
        .unwrap();
    ctx.upload_ndjson(bucket, "data/2024/02/file3.ndjson", &ndjson)
        .await
        .unwrap();
    ctx.upload_ndjson(bucket, "data/2023/12/file4.ndjson", &ndjson)
        .await
        .unwrap();

    // Create S3 client
    let s3_config = S3Config::new(bucket).with_endpoint(&ctx.endpoint);
    let client = create_s3_client(&s3_config).await.unwrap();

    // Discover only 2024/01 prefix
    let config = DiscoveryConfig::new();
    let filter = MatchAllFilter;
    let output = CollectingOutput::new();

    let discoverer = Discoverer::new(
        client,
        bucket,
        Some("data/2024/01/".to_string()),
        output.clone(),
        filter,
        config,
    );

    let stats = discoverer.discover().await.unwrap();

    // Should only find files in 2024/01
    assert_eq!(stats.files_output, 2);
    let files = output.files();
    assert!(files.iter().all(|f| f.uri.contains("2024/01")));

    // Cleanup
    ctx.delete_object(bucket, "data/2024/01/file1.ndjson")
        .await
        .ok();
    ctx.delete_object(bucket, "data/2024/01/file2.ndjson")
        .await
        .ok();
    ctx.delete_object(bucket, "data/2024/02/file3.ndjson")
        .await
        .ok();
    ctx.delete_object(bucket, "data/2023/12/file4.ndjson")
        .await
        .ok();
}
