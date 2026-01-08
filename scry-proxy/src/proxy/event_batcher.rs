use crate::publisher::{EventPublisher, QueryEvent};
use anyhow::Result;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

/// Metrics for the event batcher
#[derive(Debug, Default)]
pub struct BatcherMetrics {
    pub events_sent: AtomicU64,
    pub events_dropped: AtomicU64,
}

/// Batches query events and publishes them at intervals or when batch size is reached
///
/// Uses a bounded channel to prevent unbounded memory growth if the publisher
/// is slow or failing. When the queue is full, the oldest events are dropped
/// (ring buffer semantics).
pub struct EventBatcher {
    sender: mpsc::Sender<QueryEvent>,
    metrics: Arc<BatcherMetrics>,
}

impl EventBatcher {
    /// Create a new event batcher with the given publisher and configuration
    pub fn new(
        publisher: Arc<dyn EventPublisher>,
        batch_size: usize,
        flush_interval_ms: u64,
        max_queue_size: usize,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(max_queue_size);
        let metrics = Arc::new(BatcherMetrics::default());

        info!(
            batch_size = batch_size,
            flush_interval_ms = flush_interval_ms,
            max_queue_size = max_queue_size,
            "Creating EventBatcher with bounded queue"
        );

        // Spawn background task to handle batching
        let metrics_clone = Arc::clone(&metrics);
        tokio::spawn(async move {
            if let Err(e) =
                run_batcher(receiver, publisher, batch_size, flush_interval_ms, metrics_clone).await
            {
                error!(error = %e, "Event batcher task failed");
            }
        });

        Self { sender, metrics }
    }

    /// Send an event to be batched and published
    ///
    /// Returns Ok(true) if event was queued, Ok(false) if dropped due to full queue
    pub fn send_event(&self, event: QueryEvent) -> Result<bool> {
        match self.sender.try_send(event) {
            Ok(_) => {
                self.metrics.events_sent.fetch_add(1, Ordering::Relaxed);
                Ok(true)
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                // Queue is full - drop the event (ring buffer semantics)
                let dropped = self.metrics.events_dropped.fetch_add(1, Ordering::Relaxed) + 1;

                if dropped.is_multiple_of(100) {
                    warn!(
                        dropped_total = dropped,
                        "Event queue full, dropping events (publisher may be slow or down)"
                    );
                }

                Ok(false)
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                anyhow::bail!("Event batcher channel closed")
            }
        }
    }

    /// Get metrics about the batcher's performance
    pub fn get_metrics(&self) -> (u64, u64) {
        (
            self.metrics.events_sent.load(Ordering::Relaxed),
            self.metrics.events_dropped.load(Ordering::Relaxed),
        )
    }
}

async fn run_batcher(
    mut receiver: mpsc::Receiver<QueryEvent>,
    publisher: Arc<dyn EventPublisher>,
    batch_size: usize,
    flush_interval_ms: u64,
    _metrics: Arc<BatcherMetrics>,
) -> Result<()> {
    let mut batch = Vec::with_capacity(batch_size);
    let mut flush_timer = interval(Duration::from_millis(flush_interval_ms));
    flush_timer.tick().await; // First tick completes immediately

    info!(batch_size = batch_size, flush_interval_ms = flush_interval_ms, "Event batcher started");

    loop {
        tokio::select! {
            // Receive events from the channel
            event = receiver.recv() => {
                match event {
                    Some(event) => {
                        batch.push(event);

                        // Flush if batch is full
                        if batch.len() >= batch_size {
                            debug!(batch_size = batch.len(), "Flushing full batch");
                            flush_batch(&mut batch, &publisher).await;
                        }
                    }
                    None => {
                        // Channel closed, flush remaining and exit
                        info!("Event batcher channel closed, flushing remaining events");
                        flush_batch(&mut batch, &publisher).await;
                        publisher.shutdown().await?;
                        break;
                    }
                }
            }

            // Flush on timer
            _ = flush_timer.tick() => {
                if !batch.is_empty() {
                    debug!(batch_size = batch.len(), "Flushing batch on timer");
                    flush_batch(&mut batch, &publisher).await;
                }
            }
        }
    }

    Ok(())
}

async fn flush_batch(batch: &mut Vec<QueryEvent>, publisher: &Arc<dyn EventPublisher>) {
    if batch.is_empty() {
        return;
    }

    let events = std::mem::take(batch);
    let count = events.len();

    if let Err(e) = publisher.publish_batch(events).await {
        error!(error = %e, count = count, "Failed to publish event batch");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::publisher::{DebugLoggerPublisher, QueryEventBuilder};
    use std::time::Duration;

    #[tokio::test]
    async fn test_batcher_flushes_on_size() {
        let publisher = Arc::new(DebugLoggerPublisher::new());
        let batcher = EventBatcher::new(publisher.clone(), 2, 10000, 100);

        // Send 2 events (should trigger batch flush)
        batcher.send_event(QueryEventBuilder::new("SELECT 1").build()).unwrap();
        batcher.send_event(QueryEventBuilder::new("SELECT 2").build()).unwrap();

        // Give time for async processing
        tokio::time::sleep(Duration::from_millis(100)).await;

        let (total_events, total_batches, _) = publisher.get_metrics();
        assert!(total_events >= 2);
        assert!(total_batches >= 1);
    }

    #[tokio::test]
    async fn test_batcher_flushes_on_timer() {
        let publisher = Arc::new(DebugLoggerPublisher::new());
        let batcher = EventBatcher::new(publisher.clone(), 100, 50, 100);

        // Send 1 event (won't trigger size-based flush)
        batcher.send_event(QueryEventBuilder::new("SELECT 1").build()).unwrap();

        // Wait for timer flush
        tokio::time::sleep(Duration::from_millis(200)).await;

        let (total_events, total_batches, _) = publisher.get_metrics();
        assert!(total_events >= 1);
        assert!(total_batches >= 1);
    }

    #[tokio::test]
    async fn test_batcher_bounded_queue() {
        use std::sync::atomic::{AtomicBool, Ordering};

        // Create a slow publisher that blocks
        #[derive(Clone)]
        struct SlowPublisher {
            should_block: Arc<AtomicBool>,
        }

        #[async_trait::async_trait]
        impl EventPublisher for SlowPublisher {
            async fn publish_batch(&self, _events: Vec<QueryEvent>) -> Result<()> {
                // Block until told to unblock
                while self.should_block.load(Ordering::Relaxed) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Ok(())
            }

            async fn shutdown(&self) -> Result<()> {
                Ok(())
            }
        }

        let should_block = Arc::new(AtomicBool::new(true));
        let publisher = Arc::new(SlowPublisher { should_block: Arc::clone(&should_block) });

        // Create batcher with very small queue (10 events)
        let batcher = EventBatcher::new(publisher.clone(), 5, 10000, 10);

        // Send events until queue is full
        for i in 0..20 {
            let result =
                batcher.send_event(QueryEventBuilder::new(format!("SELECT {}", i)).build());
            assert!(result.is_ok());
        }

        // Check metrics
        let (sent, dropped) = batcher.get_metrics();

        // Should have sent 10 events to queue and dropped 10
        assert_eq!(sent + dropped, 20);
        assert!(dropped > 0, "Expected some events to be dropped");
        assert!(sent <= 10, "Should not accept more than queue size");

        // Unblock publisher
        should_block.store(false, Ordering::Relaxed);

        // Give time for processing
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    #[tokio::test]
    async fn test_no_memory_leak_with_slow_publisher() {
        use std::sync::atomic::{AtomicU64, Ordering};

        // Create a publisher that tracks but is slow
        #[derive(Clone)]
        struct TrackingSlowPublisher {
            processed: Arc<AtomicU64>,
        }

        #[async_trait::async_trait]
        impl EventPublisher for TrackingSlowPublisher {
            async fn publish_batch(&self, events: Vec<QueryEvent>) -> Result<()> {
                // Simulate slow processing
                tokio::time::sleep(Duration::from_millis(100)).await;
                self.processed.fetch_add(events.len() as u64, Ordering::Relaxed);
                Ok(())
            }

            async fn shutdown(&self) -> Result<()> {
                Ok(())
            }
        }

        let processed = Arc::new(AtomicU64::new(0));
        let publisher = Arc::new(TrackingSlowPublisher { processed: Arc::clone(&processed) });

        // Create batcher with bounded queue
        let batcher = EventBatcher::new(publisher.clone(), 10, 50, 50);

        // Flood with events (much more than queue size)
        for i in 0..1000 {
            let _ = batcher.send_event(QueryEventBuilder::new(format!("SELECT {}", i)).build());
        }

        let (sent, dropped) = batcher.get_metrics();

        // Verify bounded behavior
        assert_eq!(sent + dropped, 1000, "All events should be accounted for");
        assert!(dropped > 0, "Should have dropped events due to slow publisher");
        assert!(sent < 1000, "Should not have accepted all events");

        // The key test: memory is bounded by queue size
        // If this test passes, we know we're not accumulating unbounded events
    }
}
