//! Integration tests for paraflow-extreme.
//!
//! These tests require LocalStack to be running. They are marked as `#[ignore]`
//! by default to avoid running them in CI without proper setup.
//!
//! ## Running Integration Tests
//!
//! 1. Start LocalStack:
//!    ```bash
//!    docker compose up -d localstack
//!    ```
//!
//! 2. Run the integration tests:
//!    ```bash
//!    LOCALSTACK_ENDPOINT=http://localhost:4566 cargo test -p integration-tests -- --ignored
//!    ```
//!
//! ## Test Categories
//!
//! - `sqs_test` - SQS queue operations (receive, ack, nack, DLQ)
//! - `discoverer_test` - S3 file discovery with filtering
//! - `worker_test` - Worker processing pipeline
//! - `e2e_test` - Full end-to-end pipeline tests

mod common;
mod discoverer_test;
mod e2e_test;
mod sqs_test;
mod worker_test;
