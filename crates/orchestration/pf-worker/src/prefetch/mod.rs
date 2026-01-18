//! Async I/O prefetching for improved worker throughput.
//!
//! This module provides infrastructure for prefetching files from S3
//! while the worker is processing other files. This overlaps I/O latency
//! with processing, improving throughput when workloads are I/O-bound.
//!
//! # Architecture
//!
//! The prefetch system consists of three main components:
//!
//! - [`PrefetchConfig`]: Configuration for prefetch behavior
//! - [`PrefetchBuffer`]: Bounded buffer holding prefetched streams
//! - [`Prefetcher`]: Spawns async prefetch tasks
//!
//! # Usage
//!
//! ```ignore
//! use pf_worker::prefetch::{Prefetcher, PrefetchConfig};
//!
//! // Create prefetcher with configuration
//! let config = PrefetchConfig::new()
//!     .with_max_prefetch_count(2)
//!     .with_max_memory_bytes(30 * 1024 * 1024);
//! let prefetcher = Prefetcher::new(thread_id, config, reader);
//!
//! // Check if we can prefetch more
//! if prefetcher.can_prefetch() {
//!     prefetcher.spawn_prefetch(message);
//! }
//!
//! // Get prefetched items
//! let buffer = prefetcher.buffer();
//! if let Some(item) = buffer.pop_ready().await {
//!     // Process with pre-opened stream
//!     pipeline.process_with_stream(&item.message, item.stream).await;
//! }
//! ```
//!
//! # Memory Management
//!
//! The prefetch buffer tracks memory usage and limits prefetch operations
//! based on configured thresholds. This prevents unbounded memory growth
//! while still providing the benefits of I/O overlap.
//!
//! Default configuration:
//! - Max 2 files prefetched per thread
//! - Max 30MB memory budget per thread
//!
//! # Expected Improvements
//!
//! Based on ARM64 benchmark analysis:
//! - Throughput: 1.5-2x improvement (831 MB/s → 1.2-1.5 GB/s)
//! - CPU utilization: 2-3x increase (10-23% → 30-50%)
//! - Processing time: 30-45% reduction

mod buffer;
mod config;
mod prefetcher;

pub use buffer::{PrefetchBuffer, PrefetchError, PrefetchedItem};
pub use config::{DEFAULT_MAX_MEMORY_BYTES, DEFAULT_MAX_PREFETCH_COUNT, PrefetchConfig};
pub use prefetcher::{Prefetcher, PrefetcherStats};
