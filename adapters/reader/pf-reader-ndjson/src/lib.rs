//! NDJSON (Newline-Delimited JSON) file reader with S3 streaming support.
//!
//! This crate provides a streaming NDJSON reader that implements the `StreamingReader`
//! trait. It supports reading from both local files and S3 URIs.
//!
//! # Example
//!
//! ```ignore
//! use pf_reader_ndjson::NdjsonReader;
//! use pf_traits::StreamingReader;
//!
//! let reader = NdjsonReader::new("us-east-1", None).await?;
//! let mut stream = reader.read_stream("s3://bucket/file.ndjson").await?;
//!
//! while let Some(batch) = stream.next().await {
//!     let batch = batch?;
//!     println!("Got {} records", batch.num_rows());
//! }
//! ```

mod reader;
mod s3;

pub use reader::{NdjsonReader, NdjsonReaderConfig};
