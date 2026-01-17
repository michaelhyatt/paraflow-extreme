//! pf-worker - High-throughput data processing worker for paraflow-extreme.
//!
//! This crate provides functionality for processing files from work queues
//! and writing to pluggable destination backends. It supports:
//!
//! - Dual input mode: SQS for production, stdin for local testing
//! - Pluggable destinations: stdout, stats (more coming)
//! - Multi-threaded processing with per-thread pipelines
//! - Zero-copy Arrow RecordBatch data path
//! - Streaming memory: O(batch_size) per thread regardless of file size
//!
//! # Example
//!
//! ```ignore
//! use pf_worker::{Worker, WorkerConfig, StdinSource, StatsDestination};
//!
//! // Configure the worker
//! let config = WorkerConfig::new()
//!     .with_thread_count(4)
//!     .with_batch_size(10_000);
//!
//! // Create source and destination
//! let source = StdinSource::new();
//! let destination = StatsDestination::new();
//!
//! // Run the worker
//! let worker = Worker::new(config, source, destination);
//! let stats = worker.run().await?;
//!
//! eprintln!("Processed {} files, {} records", stats.files_processed, stats.records_processed);
//! ```

pub mod config;
pub mod destination;
pub mod pipeline;
pub mod reader;
pub mod router;
pub mod source;
pub mod stats;
pub mod worker;

pub use config::WorkerConfig;
pub use destination::{DestinationType, StatsDestination, StdoutDestination, create_destination};
pub use pipeline::{Pipeline, ThreadStats};
pub use reader::{FormatDispatchReader, ReaderFactory, ReaderFactoryConfig, create_reader};
pub use router::WorkRouter;
pub use source::{QueueMessage, SqsSource, SqsSourceConfig, StdinSource, WorkQueue};
pub use stats::WorkerStats;
pub use worker::Worker;
