//! Common utilities for integration tests.
//!
//! This module provides shared test infrastructure for LocalStack-based
//! integration testing, including client setup and test data generation.

pub mod localstack;

pub use localstack::{generate_test_ndjson, generate_test_parquet, LocalStackTestContext};
