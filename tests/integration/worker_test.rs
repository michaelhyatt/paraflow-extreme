//! Worker pipeline integration tests using LocalStack.
//!
//! These tests verify that the Worker can correctly process files from SQS
//! and read data from S3 using LocalStack.

use crate::common::{LocalStackTestContext, generate_test_ndjson, generate_test_parquet};
use chrono::Utc;
use pf_reader_ndjson::{NdjsonReader, NdjsonReaderConfig};
use pf_types::{DestinationConfig, FileFormat, WorkItem};
use pf_worker::{SqsSource, SqsSourceConfig, StatsDestination, Worker, WorkerConfig};
use std::sync::Arc;

/// Create a test WorkItem for the given file URI.
fn create_test_work_item(file_uri: &str, format: FileFormat, size: u64) -> WorkItem {
    WorkItem {
        job_id: "worker-test".to_string(),
        file_uri: file_uri.to_string(),
        file_size_bytes: size,
        format,
        destination: DestinationConfig {
            endpoint: "http://localhost:9200".to_string(),
            index: "test".to_string(),
            credentials: None,
        },
        transform: None,
        attempt: 0,
        enqueued_at: Utc::now(),
    }
}

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_worker_processes_ndjson_from_sqs() {
    let ctx = LocalStackTestContext::new().await;

    if !ctx.is_available().await {
        eprintln!("LocalStack not available, skipping test");
        return;
    }

    let bucket = "worker-test-bucket";
    let queue_url = ctx.create_queue("worker-test-queue").await.unwrap();

    // Setup: create bucket and upload test data
    ctx.create_bucket(bucket).await.unwrap();
    let ndjson_data = generate_test_ndjson(100);
    ctx.upload_ndjson(bucket, "data/test.ndjson", &ndjson_data)
        .await
        .unwrap();
    ctx.purge_queue(&queue_url).await.ok();

    // Send work item to queue
    let work_item = create_test_work_item(
        &format!("s3://{}/data/test.ndjson", bucket),
        FileFormat::NdJson,
        ndjson_data.len() as u64,
    );
    let json = serde_json::to_string(&work_item).unwrap();
    ctx.send_message(&queue_url, &json).await.unwrap();

    // Create worker components
    let sqs_config = SqsSourceConfig::new(&queue_url)
        .with_wait_time(1)
        .with_drain_mode(true);

    let source = SqsSource::from_config_with_endpoint(sqs_config, &ctx.endpoint, &ctx.region)
        .await
        .unwrap();

    let reader_config = NdjsonReaderConfig::new(&ctx.region)
        .with_endpoint(&ctx.endpoint)
        .with_credentials("test", "test", None);
    let reader = NdjsonReader::new(reader_config).await.unwrap();

    let destination = Arc::new(StatsDestination::new());

    let worker_config = WorkerConfig::new()
        .with_thread_count(1)
        .with_batch_size(1000)
        .with_prefetch_disabled();

    // Run worker
    let worker = Worker::new(worker_config, source, reader, destination);
    let stats = worker.run().await.unwrap();

    // Verify results
    assert_eq!(stats.files_processed, 1);
    assert_eq!(stats.records_processed, 100);
    assert_eq!(stats.files_failed, 0);

    // Cleanup
    ctx.delete_object(bucket, "data/test.ndjson").await.ok();
    ctx.delete_queue(&queue_url).await.ok();
}

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_worker_processes_multiple_files() {
    let ctx = LocalStackTestContext::new().await;

    if !ctx.is_available().await {
        eprintln!("LocalStack not available, skipping test");
        return;
    }

    let bucket = "worker-multi-bucket";
    let queue_url = ctx.create_queue("worker-multi-queue").await.unwrap();

    // Setup
    ctx.create_bucket(bucket).await.unwrap();
    ctx.purge_queue(&queue_url).await.ok();

    // Upload multiple files
    for i in 0..5 {
        let ndjson_data = generate_test_ndjson(50 + i * 10);
        ctx.upload_ndjson(bucket, &format!("data/file_{}.ndjson", i), &ndjson_data)
            .await
            .unwrap();

        let work_item = create_test_work_item(
            &format!("s3://{}/data/file_{}.ndjson", bucket, i),
            FileFormat::NdJson,
            ndjson_data.len() as u64,
        );
        let json = serde_json::to_string(&work_item).unwrap();
        ctx.send_message(&queue_url, &json).await.unwrap();
    }

    // Create worker
    let sqs_config = SqsSourceConfig::new(&queue_url)
        .with_wait_time(1)
        .with_drain_mode(true);

    let source = SqsSource::from_config_with_endpoint(sqs_config, &ctx.endpoint, &ctx.region)
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
    let stats = worker.run().await.unwrap();

    // Verify all files processed
    assert_eq!(stats.files_processed, 5);
    // Total records: 50 + 60 + 70 + 80 + 90 = 350
    assert_eq!(stats.records_processed, 350);
    assert_eq!(stats.files_failed, 0);

    // Cleanup
    for i in 0..5 {
        ctx.delete_object(bucket, &format!("data/file_{}.ndjson", i))
            .await
            .ok();
    }
    ctx.delete_queue(&queue_url).await.ok();
}

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_worker_handles_missing_file() {
    let ctx = LocalStackTestContext::new().await;

    if !ctx.is_available().await {
        eprintln!("LocalStack not available, skipping test");
        return;
    }

    let bucket = "worker-missing-bucket";
    let queue_url = ctx.create_queue("worker-missing-queue").await.unwrap();
    let dlq_url = ctx.create_queue("worker-missing-dlq").await.unwrap();

    // Setup
    ctx.create_bucket(bucket).await.unwrap();
    ctx.purge_queue(&queue_url).await.ok();
    ctx.purge_queue(&dlq_url).await.ok();

    // Send work item for non-existent file
    let work_item = create_test_work_item(
        &format!("s3://{}/data/missing.ndjson", bucket),
        FileFormat::NdJson,
        1024,
    );
    let json = serde_json::to_string(&work_item).unwrap();
    ctx.send_message(&queue_url, &json).await.unwrap();

    // Create worker with DLQ
    let sqs_config = SqsSourceConfig::new(&queue_url)
        .with_dlq_url(&dlq_url)
        .with_wait_time(1)
        .with_drain_mode(true);

    let source = SqsSource::from_config_with_endpoint(sqs_config, &ctx.endpoint, &ctx.region)
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

    // Run worker
    let worker = Worker::new(worker_config, source, reader, destination);
    let stats = worker.run().await.unwrap();

    // File should have failed
    assert_eq!(stats.files_processed, 0);
    assert_eq!(stats.files_failed, 1);

    // Should be in DLQ
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let dlq_count = ctx.get_queue_message_count(&dlq_url).await.unwrap();
    assert_eq!(dlq_count, 1);

    // Cleanup
    ctx.delete_queue(&queue_url).await.ok();
    ctx.delete_queue(&dlq_url).await.ok();
}

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_worker_with_prefetch_enabled() {
    let ctx = LocalStackTestContext::new().await;

    if !ctx.is_available().await {
        eprintln!("LocalStack not available, skipping test");
        return;
    }

    let bucket = "worker-prefetch-bucket";
    let queue_url = ctx.create_queue("worker-prefetch-queue").await.unwrap();

    // Setup
    ctx.create_bucket(bucket).await.unwrap();
    ctx.purge_queue(&queue_url).await.ok();

    // Upload files
    for i in 0..3 {
        let ndjson_data = generate_test_ndjson(100);
        ctx.upload_ndjson(bucket, &format!("data/file_{}.ndjson", i), &ndjson_data)
            .await
            .unwrap();

        let work_item = create_test_work_item(
            &format!("s3://{}/data/file_{}.ndjson", bucket, i),
            FileFormat::NdJson,
            ndjson_data.len() as u64,
        );
        let json = serde_json::to_string(&work_item).unwrap();
        ctx.send_message(&queue_url, &json).await.unwrap();
    }

    // Create worker with prefetch enabled
    let sqs_config = SqsSourceConfig::new(&queue_url)
        .with_wait_time(1)
        .with_drain_mode(true);

    let source = SqsSource::from_config_with_endpoint(sqs_config, &ctx.endpoint, &ctx.region)
        .await
        .unwrap();

    let reader_config = NdjsonReaderConfig::new(&ctx.region)
        .with_endpoint(&ctx.endpoint)
        .with_credentials("test", "test", None);
    let reader = NdjsonReader::new(reader_config).await.unwrap();

    let destination = Arc::new(StatsDestination::new());

    // Enable prefetch (default)
    let worker_config = WorkerConfig::new()
        .with_thread_count(1)
        .with_batch_size(1000);

    // Run worker
    let worker = Worker::new(worker_config, source, reader, destination);
    let stats = worker.run().await.unwrap();

    // Verify
    assert_eq!(stats.files_processed, 3);
    assert_eq!(stats.records_processed, 300);
    assert_eq!(stats.files_failed, 0);

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
async fn test_worker_parquet_format() {
    let ctx = LocalStackTestContext::new().await;

    if !ctx.is_available().await {
        eprintln!("LocalStack not available, skipping test");
        return;
    }

    let bucket = "worker-parquet-bucket";
    let queue_url = ctx.create_queue("worker-parquet-queue").await.unwrap();

    // Setup
    ctx.create_bucket(bucket).await.unwrap();
    ctx.purge_queue(&queue_url).await.ok();

    // Upload parquet file
    let parquet_data = generate_test_parquet(100);
    ctx.upload_parquet(bucket, "data/test.parquet", parquet_data.clone())
        .await
        .unwrap();

    // Send work item - note: using NDJSON reader won't work for parquet
    // This test verifies format detection/error handling
    let work_item = create_test_work_item(
        &format!("s3://{}/data/test.parquet", bucket),
        FileFormat::Parquet,
        parquet_data.len() as u64,
    );
    let json = serde_json::to_string(&work_item).unwrap();
    ctx.send_message(&queue_url, &json).await.unwrap();

    // Create worker with NDJSON reader (should fail for parquet)
    let sqs_config = SqsSourceConfig::new(&queue_url)
        .with_wait_time(1)
        .with_drain_mode(true);

    let source = SqsSource::from_config_with_endpoint(sqs_config, &ctx.endpoint, &ctx.region)
        .await
        .unwrap();

    let reader_config = NdjsonReaderConfig::new(&ctx.region)
        .with_endpoint(&ctx.endpoint)
        .with_credentials("test", "test", None);
    let reader = NdjsonReader::new(reader_config).await.unwrap();

    let destination = Arc::new(StatsDestination::new());

    let worker_config = WorkerConfig::new()
        .with_thread_count(1)
        .with_max_retries(0)
        .with_prefetch_disabled();

    // Run worker
    let worker = Worker::new(worker_config, source, reader, destination);
    let stats = worker.run().await.unwrap();

    // Should fail (wrong format for reader)
    assert_eq!(stats.files_failed, 1);

    // Cleanup
    ctx.delete_object(bucket, "data/test.parquet").await.ok();
    ctx.delete_queue(&queue_url).await.ok();
}

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_worker_empty_queue() {
    let ctx = LocalStackTestContext::new().await;

    if !ctx.is_available().await {
        eprintln!("LocalStack not available, skipping test");
        return;
    }

    let queue_url = ctx.create_queue("worker-empty-queue").await.unwrap();
    ctx.purge_queue(&queue_url).await.ok();

    // Create worker with empty queue
    let sqs_config = SqsSourceConfig::new(&queue_url)
        .with_wait_time(1)
        .with_drain_mode(true);

    let source = SqsSource::from_config_with_endpoint(sqs_config, &ctx.endpoint, &ctx.region)
        .await
        .unwrap();

    let reader_config = NdjsonReaderConfig::new(&ctx.region)
        .with_endpoint(&ctx.endpoint)
        .with_credentials("test", "test", None);
    let reader = NdjsonReader::new(reader_config).await.unwrap();

    let destination = Arc::new(StatsDestination::new());

    let worker_config = WorkerConfig::new()
        .with_thread_count(1)
        .with_prefetch_disabled();

    // Run worker
    let worker = Worker::new(worker_config, source, reader, destination);
    let stats = worker.run().await.unwrap();

    // Should handle empty queue gracefully
    assert_eq!(stats.files_processed, 0);
    assert_eq!(stats.files_failed, 0);
    assert_eq!(stats.records_processed, 0);

    // Cleanup
    ctx.delete_queue(&queue_url).await.ok();
}
