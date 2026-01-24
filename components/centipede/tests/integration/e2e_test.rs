//! End-to-end integration tests using LocalStack.
//!
//! These tests verify the complete pipeline: discover → queue → process.
//! They exercise the full flow from file discovery through to worker processing.

use crate::common::{LocalStackTestContext, generate_test_ndjson};
use chrono::Utc;
use pf_discoverer::{
    Discoverer, DiscoveryConfig, MatchAllFilter, PatternFilter, S3Config, SqsConfig, SqsOutput,
    create_s3_client,
};
use pf_reader_ndjson::{NdjsonReader, NdjsonReaderConfig};
use pf_types::{DestinationConfig, FileFormat, WorkItem};
use pf_worker::{SqsSource, SqsSourceConfig, StatsDestination, Worker, WorkerConfig};
use std::sync::Arc;

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_e2e_discover_to_worker() {
    let ctx = LocalStackTestContext::new().await;

    if !ctx.is_available().await {
        eprintln!("LocalStack not available, skipping test");
        return;
    }

    let bucket = "e2e-test-bucket";
    let queue_url = ctx.create_queue("e2e-work-queue").await.unwrap();

    // Setup: create bucket and upload test files
    ctx.create_bucket(bucket).await.unwrap();
    ctx.purge_queue(&queue_url).await.ok();

    // Upload test files
    for i in 0..3 {
        let ndjson_data = generate_test_ndjson(100);
        ctx.upload_ndjson(bucket, &format!("data/file_{}.ndjson", i), &ndjson_data)
            .await
            .unwrap();
    }

    // --- Phase 1: Discovery ---

    // Create S3 client for discovery
    let s3_config = S3Config::new(bucket)
        .with_prefix("data/")
        .with_endpoint(&ctx.endpoint);
    let s3_client = create_s3_client(&s3_config).await.unwrap();

    // Create SQS output
    let sqs_config = SqsConfig::new(&queue_url)
        .with_endpoint(&ctx.endpoint)
        .with_batch_size(10);
    let sqs_output = SqsOutput::new(sqs_config).await.unwrap();

    // Run discovery
    let discovery_config = DiscoveryConfig::new();
    let filter = PatternFilter::new("*.ndjson").unwrap();

    let discoverer = Discoverer::new(
        s3_client,
        bucket,
        Some("data/".to_string()),
        sqs_output,
        filter,
        discovery_config,
    );

    let discovery_stats = discoverer.discover().await.unwrap();

    // Verify discovery
    assert_eq!(discovery_stats.files_output, 3);

    // Wait for messages to be available
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // --- Phase 2: Worker Processing ---

    // Create worker
    let sqs_source_config = SqsSourceConfig::new(&queue_url)
        .with_wait_time(1)
        .with_drain_mode(true);

    let source =
        SqsSource::from_config_with_endpoint(sqs_source_config, &ctx.endpoint, &ctx.region)
            .await
            .unwrap();

    let reader_config = NdjsonReaderConfig::new(&ctx.region)
        .with_endpoint(&ctx.endpoint)
        .with_credentials("test", "test", None);
    let reader = NdjsonReader::new(reader_config).await.unwrap();

    let destination = Arc::new(StatsDestination::new());

    let worker_config = WorkerConfig::new()
        .with_thread_count(2)
        .with_batch_size(1000)
        .with_prefetch_disabled();

    // Run worker
    let worker = Worker::new(worker_config, source, reader, destination);
    let worker_stats = worker.run().await.unwrap();

    // Verify worker processed all discovered files
    assert_eq!(worker_stats.files_processed, 3);
    assert_eq!(worker_stats.records_processed, 300); // 3 files * 100 records
    assert_eq!(worker_stats.files_failed, 0);

    // Queue should be empty
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let remaining = ctx.get_queue_message_count(&queue_url).await.unwrap();
    assert_eq!(remaining, 0);

    // Cleanup
    for i in 0..3 {
        ctx.delete_object(bucket, &format!("data/file_{}.ndjson", i))
            .await
            .ok();
    }
    ctx.delete_queue(&queue_url).await.ok();
}

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_e2e_with_filtering() {
    let ctx = LocalStackTestContext::new().await;

    if !ctx.is_available().await {
        eprintln!("LocalStack not available, skipping test");
        return;
    }

    let bucket = "e2e-filter-bucket";
    let queue_url = ctx.create_queue("e2e-filter-queue").await.unwrap();

    // Setup
    ctx.create_bucket(bucket).await.unwrap();
    ctx.purge_queue(&queue_url).await.ok();

    // Upload mixed files - only some should be processed
    let ndjson = generate_test_ndjson(50);
    ctx.upload_ndjson(bucket, "logs/app.ndjson", &ndjson)
        .await
        .unwrap();
    ctx.upload_ndjson(bucket, "logs/error.ndjson", &ndjson)
        .await
        .unwrap();
    ctx.upload_ndjson(bucket, "logs/debug.txt", &ndjson) // Won't match .ndjson filter
        .await
        .unwrap();
    ctx.upload_ndjson(bucket, "other/data.ndjson", &ndjson) // Wrong prefix
        .await
        .unwrap();

    // Discovery with filter
    let s3_config = S3Config::new(bucket)
        .with_prefix("logs/")
        .with_endpoint(&ctx.endpoint);
    let s3_client = create_s3_client(&s3_config).await.unwrap();

    let sqs_config = SqsConfig::new(&queue_url)
        .with_endpoint(&ctx.endpoint)
        .with_batch_size(10);
    let sqs_output = SqsOutput::new(sqs_config).await.unwrap();

    // Only discover .ndjson files
    let filter = PatternFilter::new("*.ndjson").unwrap();

    let discoverer = Discoverer::new(
        s3_client,
        bucket,
        Some("logs/".to_string()),
        sqs_output,
        filter,
        DiscoveryConfig::new(),
    );

    let discovery_stats = discoverer.discover().await.unwrap();

    // Should only find 2 files (app.ndjson, error.ndjson)
    assert_eq!(discovery_stats.files_output, 2);

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Run worker
    let sqs_source_config = SqsSourceConfig::new(&queue_url)
        .with_wait_time(1)
        .with_drain_mode(true);

    let source =
        SqsSource::from_config_with_endpoint(sqs_source_config, &ctx.endpoint, &ctx.region)
            .await
            .unwrap();

    let reader_config = NdjsonReaderConfig::new(&ctx.region)
        .with_endpoint(&ctx.endpoint)
        .with_credentials("test", "test", None);
    let reader = NdjsonReader::new(reader_config).await.unwrap();

    let destination = Arc::new(StatsDestination::new());

    let worker = Worker::new(
        WorkerConfig::new()
            .with_thread_count(1)
            .with_prefetch_disabled(),
        source,
        reader,
        destination,
    );

    let worker_stats = worker.run().await.unwrap();

    // Verify only filtered files processed
    assert_eq!(worker_stats.files_processed, 2);
    assert_eq!(worker_stats.records_processed, 100); // 2 * 50

    // Cleanup
    ctx.delete_object(bucket, "logs/app.ndjson").await.ok();
    ctx.delete_object(bucket, "logs/error.ndjson").await.ok();
    ctx.delete_object(bucket, "logs/debug.txt").await.ok();
    ctx.delete_object(bucket, "other/data.ndjson").await.ok();
    ctx.delete_queue(&queue_url).await.ok();
}

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_e2e_with_dlq() {
    let ctx = LocalStackTestContext::new().await;

    if !ctx.is_available().await {
        eprintln!("LocalStack not available, skipping test");
        return;
    }

    let bucket = "e2e-dlq-bucket";
    let queue_url = ctx.create_queue("e2e-dlq-main").await.unwrap();
    let dlq_url = ctx.create_queue("e2e-dlq-dead").await.unwrap();

    // Setup
    ctx.create_bucket(bucket).await.unwrap();
    ctx.purge_queue(&queue_url).await.ok();
    ctx.purge_queue(&dlq_url).await.ok();

    // Upload one good file and send one bad work item
    let ndjson = generate_test_ndjson(50);
    ctx.upload_ndjson(bucket, "data/good.ndjson", &ndjson)
        .await
        .unwrap();

    // Manually send work items - one good, one for missing file
    let good_item = WorkItem {
        job_id: "e2e-dlq-test".to_string(),
        file_uri: format!("s3://{}/data/good.ndjson", bucket),
        file_size_bytes: ndjson.len() as u64,
        format: FileFormat::NdJson,
        destination: DestinationConfig {
            endpoint: "http://localhost:9200".to_string(),
            index: "test".to_string(),
            credentials: None,
        },
        transform: None,
        attempt: 0,
        enqueued_at: Utc::now(),
    };

    let bad_item = WorkItem {
        job_id: "e2e-dlq-test".to_string(),
        file_uri: format!("s3://{}/data/missing.ndjson", bucket),
        file_size_bytes: 1024,
        format: FileFormat::NdJson,
        destination: DestinationConfig {
            endpoint: "http://localhost:9200".to_string(),
            index: "test".to_string(),
            credentials: None,
        },
        transform: None,
        attempt: 0,
        enqueued_at: Utc::now(),
    };

    ctx.send_message(&queue_url, &serde_json::to_string(&good_item).unwrap())
        .await
        .unwrap();
    ctx.send_message(&queue_url, &serde_json::to_string(&bad_item).unwrap())
        .await
        .unwrap();

    // Run worker with DLQ
    let sqs_source_config = SqsSourceConfig::new(&queue_url)
        .with_dlq_url(&dlq_url)
        .with_wait_time(1)
        .with_drain_mode(true);

    let source =
        SqsSource::from_config_with_endpoint(sqs_source_config, &ctx.endpoint, &ctx.region)
            .await
            .unwrap();

    let reader_config = NdjsonReaderConfig::new(&ctx.region)
        .with_endpoint(&ctx.endpoint)
        .with_credentials("test", "test", None);
    let reader = NdjsonReader::new(reader_config).await.unwrap();

    let destination = Arc::new(StatsDestination::new());

    let worker_config = WorkerConfig::new()
        .with_thread_count(1)
        .with_max_retries(0) // Immediate DLQ
        .with_prefetch_disabled();

    let worker = Worker::new(worker_config, source, reader, destination);
    let worker_stats = worker.run().await.unwrap();

    // One success, one failure
    assert_eq!(worker_stats.files_processed, 1);
    assert_eq!(worker_stats.files_failed, 1);
    assert_eq!(worker_stats.records_processed, 50);

    // Wait for DLQ
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Failed message should be in DLQ
    let dlq_count = ctx.get_queue_message_count(&dlq_url).await.unwrap();
    assert_eq!(dlq_count, 1);

    // Main queue should be empty
    let main_count = ctx.get_queue_message_count(&queue_url).await.unwrap();
    assert_eq!(main_count, 0);

    // Cleanup
    ctx.delete_object(bucket, "data/good.ndjson").await.ok();
    ctx.delete_queue(&queue_url).await.ok();
    ctx.delete_queue(&dlq_url).await.ok();
}

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_e2e_large_batch() {
    let ctx = LocalStackTestContext::new().await;

    if !ctx.is_available().await {
        eprintln!("LocalStack not available, skipping test");
        return;
    }

    let bucket = "e2e-large-bucket";
    let queue_url = ctx.create_queue("e2e-large-queue").await.unwrap();

    // Setup
    ctx.create_bucket(bucket).await.unwrap();
    ctx.purge_queue(&queue_url).await.ok();

    // Upload 20 files
    let num_files = 20;
    let records_per_file = 200;

    for i in 0..num_files {
        let ndjson = generate_test_ndjson(records_per_file);
        ctx.upload_ndjson(bucket, &format!("batch/file_{:03}.ndjson", i), &ndjson)
            .await
            .unwrap();
    }

    // Discover all files
    let s3_config = S3Config::new(bucket)
        .with_prefix("batch/")
        .with_endpoint(&ctx.endpoint);
    let s3_client = create_s3_client(&s3_config).await.unwrap();

    let sqs_config = SqsConfig::new(&queue_url)
        .with_endpoint(&ctx.endpoint)
        .with_batch_size(10);
    let sqs_output = SqsOutput::new(sqs_config).await.unwrap();

    let discoverer = Discoverer::new(
        s3_client,
        bucket,
        Some("batch/".to_string()),
        sqs_output,
        MatchAllFilter,
        DiscoveryConfig::new(),
    );

    let discovery_stats = discoverer.discover().await.unwrap();
    assert_eq!(discovery_stats.files_output, num_files);

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Process with multiple threads and prefetch
    let sqs_source_config = SqsSourceConfig::new(&queue_url)
        .with_wait_time(1)
        .with_drain_mode(true);

    let source =
        SqsSource::from_config_with_endpoint(sqs_source_config, &ctx.endpoint, &ctx.region)
            .await
            .unwrap();

    let reader_config = NdjsonReaderConfig::new(&ctx.region)
        .with_endpoint(&ctx.endpoint)
        .with_credentials("test", "test", None);
    let reader = NdjsonReader::new(reader_config).await.unwrap();

    let destination = Arc::new(StatsDestination::new());

    // Use multiple threads and enable prefetch
    let worker_config = WorkerConfig::new()
        .with_thread_count(4)
        .with_batch_size(500);

    let worker = Worker::new(worker_config, source, reader, destination);
    let worker_stats = worker.run().await.unwrap();

    // Verify all processed
    assert_eq!(worker_stats.files_processed, num_files as u64);
    assert_eq!(
        worker_stats.records_processed,
        (num_files * records_per_file) as u64
    );
    assert_eq!(worker_stats.files_failed, 0);

    // Cleanup
    for i in 0..num_files {
        ctx.delete_object(bucket, &format!("batch/file_{:03}.ndjson", i))
            .await
            .ok();
    }
    ctx.delete_queue(&queue_url).await.ok();
}

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_e2e_nested_directory_structure() {
    let ctx = LocalStackTestContext::new().await;

    if !ctx.is_available().await {
        eprintln!("LocalStack not available, skipping test");
        return;
    }

    let bucket = "e2e-nested-bucket";
    let queue_url = ctx.create_queue("e2e-nested-queue").await.unwrap();

    // Setup
    ctx.create_bucket(bucket).await.unwrap();
    ctx.purge_queue(&queue_url).await.ok();

    // Create nested directory structure (simulating date partitioning)
    let ndjson = generate_test_ndjson(25);
    ctx.upload_ndjson(bucket, "logs/2024/01/01/events.ndjson", &ndjson)
        .await
        .unwrap();
    ctx.upload_ndjson(bucket, "logs/2024/01/02/events.ndjson", &ndjson)
        .await
        .unwrap();
    ctx.upload_ndjson(bucket, "logs/2024/01/03/events.ndjson", &ndjson)
        .await
        .unwrap();
    ctx.upload_ndjson(bucket, "logs/2024/02/01/events.ndjson", &ndjson)
        .await
        .unwrap();

    // Discover only January 2024 files
    let s3_config = S3Config::new(bucket)
        .with_prefix("logs/2024/01/")
        .with_endpoint(&ctx.endpoint);
    let s3_client = create_s3_client(&s3_config).await.unwrap();

    let sqs_config = SqsConfig::new(&queue_url)
        .with_endpoint(&ctx.endpoint)
        .with_batch_size(10);
    let sqs_output = SqsOutput::new(sqs_config).await.unwrap();

    let discoverer = Discoverer::new(
        s3_client,
        bucket,
        Some("logs/2024/01/".to_string()),
        sqs_output,
        MatchAllFilter,
        DiscoveryConfig::new(),
    );

    let discovery_stats = discoverer.discover().await.unwrap();
    assert_eq!(discovery_stats.files_output, 3); // Only January files

    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Process
    let sqs_source_config = SqsSourceConfig::new(&queue_url)
        .with_wait_time(1)
        .with_drain_mode(true);

    let source =
        SqsSource::from_config_with_endpoint(sqs_source_config, &ctx.endpoint, &ctx.region)
            .await
            .unwrap();

    let reader_config = NdjsonReaderConfig::new(&ctx.region)
        .with_endpoint(&ctx.endpoint)
        .with_credentials("test", "test", None);
    let reader = NdjsonReader::new(reader_config).await.unwrap();

    let destination = Arc::new(StatsDestination::new());

    let worker = Worker::new(
        WorkerConfig::new()
            .with_thread_count(1)
            .with_prefetch_disabled(),
        source,
        reader,
        destination,
    );

    let worker_stats = worker.run().await.unwrap();

    // Only January files processed
    assert_eq!(worker_stats.files_processed, 3);
    assert_eq!(worker_stats.records_processed, 75); // 3 * 25

    // Cleanup
    ctx.delete_object(bucket, "logs/2024/01/01/events.ndjson")
        .await
        .ok();
    ctx.delete_object(bucket, "logs/2024/01/02/events.ndjson")
        .await
        .ok();
    ctx.delete_object(bucket, "logs/2024/01/03/events.ndjson")
        .await
        .ok();
    ctx.delete_object(bucket, "logs/2024/02/01/events.ndjson")
        .await
        .ok();
    ctx.delete_queue(&queue_url).await.ok();
}
