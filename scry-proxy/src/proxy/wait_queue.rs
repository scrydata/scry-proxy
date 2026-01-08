// scry-proxy/src/proxy/wait_queue.rs

//! Bounded wait queue for clients waiting for pool connections
//!
//! When the connection pool is exhausted, clients can wait in this queue
//! until a connection becomes available. The queue has a maximum depth
//! to prevent unbounded queueing.
//!
//! This implementation uses a FIFO queue with individual notification channels
//! for each waiter to ensure strict ordering.

use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::oneshot;

/// A bounded wait queue for clients waiting for pool connections
pub struct WaitQueue {
    /// Maximum queue depth
    max_depth: usize,
    /// Current queue depth (atomic for fast reads)
    depth: AtomicUsize,
    /// Queue of waiting notification channels (FIFO)
    waiters: Mutex<VecDeque<oneshot::Sender<()>>>,
}

impl WaitQueue {
    /// Create a new wait queue with the specified maximum depth
    pub fn new(max_depth: usize) -> Arc<Self> {
        Arc::new(Self {
            max_depth,
            depth: AtomicUsize::new(0),
            waiters: Mutex::new(VecDeque::new()),
        })
    }

    /// Try to enqueue a waiter
    ///
    /// Returns a Waiter if queue has space, or an error if full.
    pub async fn enqueue(self: &Arc<Self>) -> Result<Waiter, QueueFullError> {
        // Create a channel for this waiter
        let (tx, rx) = oneshot::channel();

        {
            let mut waiters = self.waiters.lock();

            // Check if queue is full
            if waiters.len() >= self.max_depth {
                return Err(QueueFullError);
            }

            // Add to queue and increment depth
            waiters.push_back(tx);
            self.depth.fetch_add(1, Ordering::SeqCst);
        }

        Ok(Waiter { queue: Arc::clone(self), receiver: Some(rx) })
    }

    /// Notify one waiter that a connection is available
    ///
    /// Notifies the first waiter in the queue (FIFO order).
    pub fn notify_one(&self) {
        let sender = {
            let mut waiters = self.waiters.lock();
            waiters.pop_front()
        };

        if let Some(tx) = sender {
            // Send notification (ignore error if receiver was dropped)
            let _ = tx.send(());
        }
    }

    /// Get current queue depth
    pub fn depth(&self) -> usize {
        self.depth.load(Ordering::SeqCst)
    }

    /// Get maximum queue depth
    pub fn max_depth(&self) -> usize {
        self.max_depth
    }
}

/// A waiter in the queue
pub struct Waiter {
    queue: Arc<WaitQueue>,
    receiver: Option<oneshot::Receiver<()>>,
}

impl Waiter {
    /// Wait until notified
    ///
    /// Returns when a connection becomes available (notified by `notify_one`).
    pub async fn wait(&mut self) {
        if let Some(rx) = self.receiver.take() {
            // Wait for notification (ignore error if sender was dropped)
            let _ = rx.await;
        }
    }
}

impl Drop for Waiter {
    fn drop(&mut self) {
        // Decrement queue depth when waiter is dropped
        self.queue.depth.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Error returned when queue is full
#[derive(Debug, Clone)]
pub struct QueueFullError;

impl std::fmt::Display for QueueFullError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "connection pool queue is full")
    }
}

impl std::error::Error for QueueFullError {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_queue_accepts_under_limit() {
        let queue = WaitQueue::new(10);

        let result = queue.enqueue().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_queue_rejects_over_limit() {
        let queue = WaitQueue::new(1);

        // Fill the queue
        let waiter1 = queue.enqueue().await.unwrap();

        // Second should be rejected
        let result = queue.enqueue().await;
        assert!(result.is_err());

        drop(waiter1);
    }

    #[tokio::test]
    async fn test_waiter_notified() {
        let queue = WaitQueue::new(10);

        let mut waiter = queue.enqueue().await.unwrap();

        // Notify in another task
        let queue_clone = Arc::clone(&queue);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            queue_clone.notify_one();
        });

        // Should complete without timeout
        let result = tokio::time::timeout(Duration::from_millis(100), waiter.wait()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_queue_depth_metric() {
        let queue = WaitQueue::new(10);

        assert_eq!(queue.depth(), 0);

        let _waiter1 = queue.enqueue().await.unwrap();
        assert_eq!(queue.depth(), 1);

        let _waiter2 = queue.enqueue().await.unwrap();
        assert_eq!(queue.depth(), 2);
    }

    #[tokio::test]
    async fn test_fifo_ordering() {
        let queue = WaitQueue::new(10);

        let mut waiter1 = queue.enqueue().await.unwrap();
        let mut waiter2 = queue.enqueue().await.unwrap();

        // Notify first waiter
        queue.notify_one();

        // waiter1 should be notified
        let result1 = tokio::time::timeout(Duration::from_millis(10), waiter1.wait()).await;
        assert!(result1.is_ok());

        // waiter2 should not be notified yet
        let result2 = tokio::time::timeout(Duration::from_millis(10), waiter2.wait()).await;
        assert!(result2.is_err()); // timeout
    }
}
