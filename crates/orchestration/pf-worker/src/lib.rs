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
//! - Async I/O prefetching for improved throughput
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
//!
//! # Async I/O Prefetching
//!
//! The worker supports async I/O prefetching to overlap S3 downloads with
//! processing. This can significantly improve throughput when workloads are
//! I/O-latency-bound.
//!
//! ```ignore
//! use pf_worker::{WorkerConfig, prefetch::PrefetchConfig};
//!
//! // Configure prefetching
//! let config = WorkerConfig::new()
//!     .with_prefetch(
//!         PrefetchConfig::new()
//!             .with_max_prefetch_count(2)
//!             .with_max_memory_bytes(30 * 1024 * 1024)
//!     );
//!
//! // Or disable prefetching
//! let config = WorkerConfig::new().with_prefetch_disabled();
//! ```

pub mod config;
pub mod destination;
pub mod metadata_cache;
pub mod pipeline;
pub mod prefetch;
pub mod reader;
pub mod router;
pub mod source;
pub mod stats;
pub mod worker;

pub use config::WorkerConfig;
pub use destination::{DestinationType, StatsDestination, StdoutDestination, create_destination};
pub use metadata_cache::MetadataCache;
pub use pipeline::{Pipeline, ThreadStats};
pub use prefetch::{PrefetchConfig, PrefetchBuffer, Prefetcher};
pub use reader::{FormatDispatchReader, ReaderFactory, ReaderFactoryConfig, create_reader};
pub use router::WorkRouter;
pub use source::{QueueMessage, SqsSource, SqsSourceConfig, StdinSource, WorkQueue};
pub use stats::WorkerStats;
pub use worker::Worker;
