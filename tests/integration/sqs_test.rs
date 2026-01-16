//! SQS integration tests using LocalStack.
//!
//! These tests verify that the SqsSource implementation works correctly
//! with a real SQS queue (via LocalStack).

use crate::common::{generate_test_ndjson, LocalStackTestContext};
use chrono::Utc;
use pf_traits::{FailureContext, WorkQueue};
use pf_types::{DestinationConfig, FileFormat, WorkItem};
use pf_worker::{SqsSource, SqsSourceConfig};

/// Create a test WorkItem for the given file URI.
fn create_test_work_item(file_uri: &str) -> WorkItem {
    WorkItem {
        job_id: "test-job".to_string(),
        file_uri: file_uri.to_string(),
        file_size_bytes: 1024,
        format: FileFormat::Parquet,
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
async fn test_sqs_source_receive_messages() {
    let ctx = LocalStackTestContext::new().await;

    if !ctx.is_available().await {
        eprintln!("LocalStack not available, skipping test");
        return;
    }

    // Create a test queue
    let queue_url = ctx.create_queue("test-receive-queue").await.unwrap();

    // Clean the queue
    ctx.purge_queue(&queue_url).await.ok();

    // Send some test messages
    let work_item = create_test_work_item("s3://test-bucket/file1.parquet");
    let json = serde_json::to_string(&work_item).unwrap();
    ctx.send_message(&queue_url, &json).await.unwrap();

    // Create SqsSource
    let config = SqsSourceConfig::new(&queue_url)
        .with_wait_time(1)
        .with_max_batch_size(10);

    let source = SqsSource::from_config_with_endpoint(config, &ctx.endpoint, &ctx.region)
        .await
        .unwrap();

    // Receive messages
    let messages = source.receive_batch(10).await.unwrap().unwrap();

    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].work_item.file_uri, "s3://test-bucket/file1.parquet");
    assert_eq!(messages[0].work_item.job_id, "test-job");

    // Ack the message
    source.ack(&messages[0].receipt_handle).await.unwrap();

    // Cleanup
    ctx.delete_queue(&queue_url).await.ok();
}

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_sqs_source_discovered_file_format() {
    let ctx = LocalStackTestContext::new().await;

    if !ctx.is_available().await {
        eprintln!("LocalStack not available, skipping test");
        return;
    }

    let queue_url = ctx.create_queue("test-discovered-queue").await.unwrap();
    ctx.purge_queue(&queue_url).await.ok();

    // Send a DiscoveredFile format message (from pf-discoverer)
    let discovered = r#"{"uri":"s3://bucket/data.ndjson","size_bytes":2048,"last_modified":"2024-01-01T00:00:00Z"}"#;
    ctx.send_message(&queue_url, discovered).await.unwrap();

    let config = SqsSourceConfig::new(&queue_url)
        .with_wait_time(1);

    let source = SqsSource::from_config_with_endpoint(config, &ctx.endpoint, &ctx.region)
        .await
        .unwrap();

    let messages = source.receive_batch(10).await.unwrap().unwrap();

    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].work_item.file_uri, "s3://bucket/data.ndjson");
    assert_eq!(messages[0].work_item.format, FileFormat::NdJson);
    assert_eq!(messages[0].work_item.job_id, "discovered");

    source.ack(&messages[0].receipt_handle).await.unwrap();
    ctx.delete_queue(&queue_url).await.ok();
}

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_sqs_source_nack_returns_to_queue() {
    let ctx = LocalStackTestContext::new().await;

    if !ctx.is_available().await {
        eprintln!("LocalStack not available, skipping test");
        return;
    }

    let queue_url = ctx.create_queue("test-nack-queue").await.unwrap();
    ctx.purge_queue(&queue_url).await.ok();

    // Send a message
    let work_item = create_test_work_item("s3://test-bucket/file.parquet");
    let json = serde_json::to_string(&work_item).unwrap();
    ctx.send_message(&queue_url, &json).await.unwrap();

    let config = SqsSourceConfig::new(&queue_url)
        .with_wait_time(1)
        .with_visibility_timeout(2); // Short visibility timeout for testing

    let source = SqsSource::from_config_with_endpoint(config, &ctx.endpoint, &ctx.region)
        .await
        .unwrap();

    // Receive the message
    let messages = source.receive_batch(10).await.unwrap().unwrap();
    assert_eq!(messages.len(), 1);
    let receipt = messages[0].receipt_handle.clone();

    // Nack the message (return to queue)
    source.nack(&receipt).await.unwrap();

    // Wait a moment for visibility to reset
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Message should be receivable again
    let messages2 = source.receive_batch(10).await.unwrap().unwrap();
    assert_eq!(messages2.len(), 1);
    assert_eq!(messages2[0].work_item.file_uri, "s3://test-bucket/file.parquet");

    // Clean up
    source.ack(&messages2[0].receipt_handle).await.unwrap();
    ctx.delete_queue(&queue_url).await.ok();
}

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_sqs_source_drain_mode() {
    let ctx = LocalStackTestContext::new().await;

    if !ctx.is_available().await {
        eprintln!("LocalStack not available, skipping test");
        return;
    }

    let queue_url = ctx.create_queue("test-drain-queue").await.unwrap();
    ctx.purge_queue(&queue_url).await.ok();

    // Send two messages
    for i in 0..2 {
        let work_item = create_test_work_item(&format!("s3://test-bucket/file{}.parquet", i));
        let json = serde_json::to_string(&work_item).unwrap();
        ctx.send_message(&queue_url, &json).await.unwrap();
    }

    let config = SqsSourceConfig::new(&queue_url)
        .with_wait_time(1)
        .with_drain_mode(true);

    let source = SqsSource::from_config_with_endpoint(config, &ctx.endpoint, &ctx.region)
        .await
        .unwrap();

    // Receive and ack all messages
    let mut total_received = 0;
    while source.has_more() {
        if let Some(messages) = source.receive_batch(10).await.unwrap() {
            for m in &messages {
                source.ack(&m.receipt_handle).await.unwrap();
            }
            total_received += messages.len();
            if messages.is_empty() {
                break; // Empty receive in drain mode
            }
        } else {
            break; // Source exhausted
        }
    }

    assert_eq!(total_received, 2);
    assert!(!source.has_more());

    ctx.delete_queue(&queue_url).await.ok();
}

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_sqs_source_dlq_handling() {
    let ctx = LocalStackTestContext::new().await;

    if !ctx.is_available().await {
        eprintln!("LocalStack not available, skipping test");
        return;
    }

    let queue_url = ctx.create_queue("test-dlq-main").await.unwrap();
    let dlq_url = ctx.create_queue("test-dlq-dead").await.unwrap();
    ctx.purge_queue(&queue_url).await.ok();
    ctx.purge_queue(&dlq_url).await.ok();

    // Send a message
    let work_item = create_test_work_item("s3://test-bucket/bad-file.parquet");
    let json = serde_json::to_string(&work_item).unwrap();
    ctx.send_message(&queue_url, &json).await.unwrap();

    let config = SqsSourceConfig::new(&queue_url)
        .with_dlq_url(&dlq_url)
        .with_wait_time(1);

    let source = SqsSource::from_config_with_endpoint(config, &ctx.endpoint, &ctx.region)
        .await
        .unwrap();

    // Receive the message
    let messages = source.receive_batch(10).await.unwrap().unwrap();
    assert_eq!(messages.len(), 1);

    // Move to DLQ
    let failure = FailureContext::new(
        messages[0].work_item.clone(),
        "Permanent",
        "File not found",
        "S3Download",
    );
    source
        .move_to_dlq(&messages[0].receipt_handle, &failure)
        .await
        .unwrap();

    // Check DLQ has the message
    let dlq_messages = ctx.receive_messages(&dlq_url, 10).await.unwrap();
    assert_eq!(dlq_messages.len(), 1);
    assert!(dlq_messages[0].contains("File not found"));

    // Original queue should be empty
    let main_count = ctx.get_queue_message_count(&queue_url).await.unwrap();
    assert_eq!(main_count, 0);

    ctx.delete_queue(&queue_url).await.ok();
    ctx.delete_queue(&dlq_url).await.ok();
}

#[tokio::test]
#[ignore = "requires LocalStack"]
async fn test_sqs_s3_integration_with_ndjson() {
    let ctx = LocalStackTestContext::new().await;

    if !ctx.is_available().await {
        eprintln!("LocalStack not available, skipping test");
        return;
    }

    let bucket = "integration-test-bucket";
    let queue_url = ctx.create_queue("test-integration-queue").await.unwrap();

    // Setup: create bucket and upload test data
    ctx.create_bucket(bucket).await.unwrap();
    let ndjson_data = generate_test_ndjson(10);
    ctx.upload_ndjson(bucket, "data/test.ndjson", &ndjson_data)
        .await
        .unwrap();
    ctx.purge_queue(&queue_url).await.ok();

    // Send a work item for the uploaded file
    let work_item = WorkItem {
        job_id: "integration-test".to_string(),
        file_uri: format!("s3://{}/data/test.ndjson", bucket),
        file_size_bytes: ndjson_data.len() as u64,
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

    let json = serde_json::to_string(&work_item).unwrap();
    ctx.send_message(&queue_url, &json).await.unwrap();

    // Verify SQS source can receive the work item
    let config = SqsSourceConfig::new(&queue_url).with_wait_time(1);
    let source = SqsSource::from_config_with_endpoint(config, &ctx.endpoint, &ctx.region)
        .await
        .unwrap();

    let messages = source.receive_batch(10).await.unwrap().unwrap();
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].work_item.job_id, "integration-test");
    assert_eq!(messages[0].work_item.format, FileFormat::NdJson);

    // Verify S3 has the file
    let files = ctx.list_objects(bucket, Some("data/")).await.unwrap();
    assert!(files.contains(&"data/test.ndjson".to_string()));

    // Cleanup
    source.ack(&messages[0].receipt_handle).await.unwrap();
    ctx.delete_object(bucket, "data/test.ndjson").await.ok();
    ctx.delete_queue(&queue_url).await.ok();
}
