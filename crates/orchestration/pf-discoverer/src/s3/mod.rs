//! S3 client and listing functionality.
//!
//! This module provides S3 operations for discovering files:
//! - Client configuration with LocalStack support
//! - Paginated object listing with streaming

mod client;
mod list;

pub use client::{S3Config, create_s3_client};
pub use list::{S3Object, list_objects};
