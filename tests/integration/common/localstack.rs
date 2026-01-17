//! LocalStack test context and utilities.

use aws_sdk_s3::Client as S3Client;
use aws_sdk_sqs::Client as SqsClient;
use std::time::Duration;

/// LocalStack test context providing S3 and SQS clients.
pub struct LocalStackTestContext {
    pub s3: S3Client,
    pub sqs: SqsClient,
    pub endpoint: String,
    pub region: String,
}

impl LocalStackTestContext {
    /// Create a new LocalStack test context.
    ///
    /// Uses the `LOCALSTACK_ENDPOINT` environment variable if set,
    /// otherwise defaults to `http://localhost:4566`.
    pub async fn new() -> Self {
        let endpoint = std::env::var("LOCALSTACK_ENDPOINT")
            .unwrap_or_else(|_| "http://localhost:4566".to_string());
        let region = "us-east-1".to_string();

        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new(region.clone()))
            .endpoint_url(&endpoint)
            .load()
            .await;

        Self {
            s3: S3Client::new(&config),
            sqs: SqsClient::new(&config),
            endpoint,
            region,
        }
    }

    /// Check if LocalStack is available and healthy.
    pub async fn is_available(&self) -> bool {
        // Try to list S3 buckets - this will fail quickly if LocalStack isn't running
        self.s3.list_buckets().send().await.is_ok()
    }

    /// Create an S3 bucket for testing.
    pub async fn create_bucket(&self, name: &str) -> Result<(), aws_sdk_s3::Error> {
        // First check if bucket exists
        let buckets = self.s3.list_buckets().send().await?;
        let exists = buckets
            .buckets()
            .iter()
            .any(|b| b.name().unwrap_or_default() == name);

        if !exists {
            self.s3.create_bucket().bucket(name).send().await?;
        }
        Ok(())
    }

    /// Create an SQS queue for testing.
    ///
    /// Returns the queue URL.
    pub async fn create_queue(&self, name: &str) -> Result<String, aws_sdk_sqs::Error> {
        // Check if queue already exists
        let queues = self.sqs.list_queues().send().await?;
        for url in queues.queue_urls() {
            if url.ends_with(&format!("/{}", name)) {
                return Ok(url.to_string());
            }
        }

        let result = self.sqs.create_queue().queue_name(name).send().await?;
        Ok(result.queue_url.unwrap_or_default())
    }

    /// Delete an SQS queue.
    pub async fn delete_queue(&self, queue_url: &str) -> Result<(), aws_sdk_sqs::Error> {
        self.sqs.delete_queue().queue_url(queue_url).send().await?;
        Ok(())
    }

    /// Purge all messages from an SQS queue.
    pub async fn purge_queue(&self, queue_url: &str) -> Result<(), aws_sdk_sqs::Error> {
        self.sqs.purge_queue().queue_url(queue_url).send().await?;
        // Wait a moment for purge to take effect
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok(())
    }

    /// Upload test NDJSON data to S3.
    pub async fn upload_ndjson(
        &self,
        bucket: &str,
        key: &str,
        data: &str,
    ) -> Result<(), aws_sdk_s3::Error> {
        self.s3
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(data.as_bytes().to_vec().into())
            .content_type("application/x-ndjson")
            .send()
            .await?;
        Ok(())
    }

    /// Upload test Parquet data to S3.
    pub async fn upload_parquet(
        &self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
    ) -> Result<(), aws_sdk_s3::Error> {
        self.s3
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(data.into())
            .content_type("application/octet-stream")
            .send()
            .await?;
        Ok(())
    }

    /// Delete an S3 object.
    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), aws_sdk_s3::Error> {
        self.s3
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await?;
        Ok(())
    }

    /// List objects in an S3 bucket with optional prefix.
    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<String>, aws_sdk_s3::Error> {
        let mut request = self.s3.list_objects_v2().bucket(bucket);
        if let Some(p) = prefix {
            request = request.prefix(p);
        }

        let result = request.send().await?;
        Ok(result
            .contents()
            .iter()
            .filter_map(|o| o.key().map(String::from))
            .collect())
    }

    /// Get the number of messages in an SQS queue.
    pub async fn get_queue_message_count(
        &self,
        queue_url: &str,
    ) -> Result<i32, aws_sdk_sqs::Error> {
        let attrs = self
            .sqs
            .get_queue_attributes()
            .queue_url(queue_url)
            .attribute_names(aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages)
            .send()
            .await?;

        let count = attrs
            .attributes()
            .and_then(|a| {
                a.get(&aws_sdk_sqs::types::QueueAttributeName::ApproximateNumberOfMessages)
            })
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or(0);

        Ok(count)
    }

    /// Send a message to an SQS queue.
    pub async fn send_message(
        &self,
        queue_url: &str,
        body: &str,
    ) -> Result<(), aws_sdk_sqs::Error> {
        self.sqs
            .send_message()
            .queue_url(queue_url)
            .message_body(body)
            .send()
            .await?;
        Ok(())
    }

    /// Receive messages from an SQS queue.
    pub async fn receive_messages(
        &self,
        queue_url: &str,
        max: i32,
    ) -> Result<Vec<String>, aws_sdk_sqs::Error> {
        let result = self
            .sqs
            .receive_message()
            .queue_url(queue_url)
            .max_number_of_messages(max)
            .wait_time_seconds(1)
            .send()
            .await?;

        Ok(result
            .messages()
            .iter()
            .filter_map(|m| m.body().map(String::from))
            .collect())
    }
}

/// Generate test NDJSON data with the specified number of records.
pub fn generate_test_ndjson(num_records: usize) -> String {
    (0..num_records)
        .map(|i| {
            format!(
                r#"{{"id":{},"name":"user_{}","score":{}}}"#,
                i,
                i,
                (i * 7) % 100
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Generate test Parquet data with the specified number of records.
pub fn generate_test_parquet(num_records: usize) -> Vec<u8> {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::io::Cursor;
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("score", DataType::Int64, false),
    ]));

    let ids: Vec<i64> = (0..num_records as i64).collect();
    let names: Vec<String> = (0..num_records).map(|i| format!("user_{}", i)).collect();
    let scores: Vec<i64> = (0..num_records).map(|i| (i as i64 * 7) % 100).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(scores)),
        ],
    )
    .expect("Failed to create record batch");

    let mut cursor = Cursor::new(Vec::new());
    {
        let mut writer = ArrowWriter::try_new(&mut cursor, schema, None)
            .expect("Failed to create parquet writer");
        writer.write(&batch).expect("Failed to write batch");
        writer.close().expect("Failed to close writer");
    }

    cursor.into_inner()
}
