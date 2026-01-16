//! Integration tests for paraflow-extreme.
//!
//! These tests require LocalStack to be running. They are marked as `#[ignore]`
//! by default to avoid running them in CI without proper setup.
//!
//! ## Running Integration Tests
//!
//! 1. Start LocalStack:
//!    ```bash
//!    cd testing/localstack && docker-compose up -d
//!    ```
//!
//! 2. Run the integration tests:
//!    ```bash
//!    LOCALSTACK_ENDPOINT=http://localhost:4566 cargo test -p integration-tests -- --ignored
//!    ```

mod common;
mod sqs_test;
