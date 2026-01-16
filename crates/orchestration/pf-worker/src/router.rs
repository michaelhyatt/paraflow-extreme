//! Work router for distributing work items to thread pool.

use crate::source::WorkMessage;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::mpsc;
use tracing::{debug, trace};

/// Work router that distributes work items to a pool of workers.
///
/// Uses bounded channels to provide backpressure when workers are busy.
pub struct WorkRouter {
    /// Senders for each worker thread
    senders: Vec<mpsc::Sender<WorkMessage>>,

    /// Round-robin counter for distribution
    next_worker: AtomicUsize,

    /// Whether the router is shutdown
    shutdown: AtomicBool,
}

impl WorkRouter {
    /// Create a new work router with the specified number of workers and buffer size.
    pub fn new(num_workers: usize, buffer_size: usize) -> (Self, Vec<mpsc::Receiver<WorkMessage>>) {
        let mut senders = Vec::with_capacity(num_workers);
        let mut receivers = Vec::with_capacity(num_workers);

        for _ in 0..num_workers {
            let (tx, rx) = mpsc::channel(buffer_size);
            senders.push(tx);
            receivers.push(rx);
        }

        let router = Self {
            senders,
            next_worker: AtomicUsize::new(0),
            shutdown: AtomicBool::new(false),
        };

        (router, receivers)
    }

    /// Route a work message to a worker using round-robin distribution.
    ///
    /// Returns `Ok(())` if the message was sent successfully.
    /// Returns `Err(message)` if the router is shutdown or all channels are closed.
    pub async fn route(&self, message: WorkMessage) -> Result<(), WorkMessage> {
        if self.shutdown.load(Ordering::Relaxed) {
            return Err(message);
        }

        // Round-robin selection
        let worker_idx = self.next_worker.fetch_add(1, Ordering::Relaxed) % self.senders.len();
        let sender = &self.senders[worker_idx];

        trace!(
            worker = worker_idx,
            message_id = %message.id,
            "Routing message to worker"
        );

        sender.send(message).await.map_err(|e| e.0)
    }

    /// Route a batch of work messages to workers.
    ///
    /// Returns the messages that could not be routed.
    pub async fn route_batch(&self, messages: Vec<WorkMessage>) -> Vec<WorkMessage> {
        let mut failed = Vec::new();

        for message in messages {
            if let Err(msg) = self.route(message).await {
                failed.push(msg);
            }
        }

        if !failed.is_empty() {
            debug!("{} messages could not be routed", failed.len());
        }

        failed
    }

    /// Signal shutdown to all workers.
    ///
    /// After calling this, `route` will return errors for new messages.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        debug!("Work router shutdown signaled");
    }

    /// Check if the router is shutdown.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }

    /// Get the number of workers.
    pub fn num_workers(&self) -> usize {
        self.senders.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use pf_types::{DestinationConfig, FileFormat, WorkItem};

    fn create_test_message(id: &str) -> WorkMessage {
        WorkMessage {
            id: id.to_string(),
            work_item: WorkItem {
                job_id: "test-job".to_string(),
                file_uri: format!("s3://bucket/{}.parquet", id),
                file_size_bytes: 1024,
                format: FileFormat::Parquet,
                destination: DestinationConfig {
                    endpoint: "http://localhost".to_string(),
                    index: "test".to_string(),
                    credentials: None,
                },
                transform: None,
                attempt: 0,
                enqueued_at: Utc::now(),
            },
            receive_count: 1,
        }
    }

    #[tokio::test]
    async fn test_router_creation() {
        let (router, receivers) = WorkRouter::new(4, 10);

        assert_eq!(router.num_workers(), 4);
        assert_eq!(receivers.len(), 4);
        assert!(!router.is_shutdown());
    }

    #[tokio::test]
    async fn test_router_round_robin() {
        let (router, mut receivers) = WorkRouter::new(3, 10);

        // Route 6 messages
        for i in 0..6 {
            let msg = create_test_message(&format!("msg-{}", i));
            router.route(msg).await.unwrap();
        }

        // Each worker should have 2 messages
        for rx in &mut receivers {
            let mut count = 0;
            while rx.try_recv().is_ok() {
                count += 1;
            }
            assert_eq!(count, 2);
        }
    }

    #[tokio::test]
    async fn test_router_shutdown() {
        let (router, _receivers) = WorkRouter::new(2, 10);

        // Route should work before shutdown
        let msg = create_test_message("before");
        assert!(router.route(msg).await.is_ok());

        // Signal shutdown
        router.shutdown();
        assert!(router.is_shutdown());

        // Route should fail after shutdown
        let msg = create_test_message("after");
        assert!(router.route(msg).await.is_err());
    }

    #[tokio::test]
    async fn test_router_batch() {
        let (router, mut receivers) = WorkRouter::new(2, 10);

        let messages: Vec<_> = (0..4).map(|i| create_test_message(&format!("batch-{}", i))).collect();

        let failed = router.route_batch(messages).await;
        assert!(failed.is_empty());

        // Verify messages were distributed
        let count1 = std::iter::from_fn(|| receivers[0].try_recv().ok()).count();
        let count2 = std::iter::from_fn(|| receivers[1].try_recv().ok()).count();

        assert_eq!(count1 + count2, 4);
        assert_eq!(count1, 2);
        assert_eq!(count2, 2);
    }

    #[tokio::test]
    async fn test_router_backpressure() {
        // Create router with small buffer
        let (router, mut receivers) = WorkRouter::new(1, 2);

        // Fill the buffer
        for i in 0..2 {
            let msg = create_test_message(&format!("fill-{}", i));
            router.route(msg).await.unwrap();
        }

        // Next message should wait or fail depending on implementation
        // With tokio::sync::mpsc, it will wait, so let's drain first
        let received: Vec<_> = std::iter::from_fn(|| receivers[0].try_recv().ok()).collect();
        assert_eq!(received.len(), 2);

        // Now we can send more
        let msg = create_test_message("after-drain");
        router.route(msg).await.unwrap();
    }
}
