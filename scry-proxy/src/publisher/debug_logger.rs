use super::{EventPublisher, QueryEvent};
use anyhow::Result;
use async_trait::async_trait;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, info};

/// Debug logger implementation of EventPublisher
///
/// Logs query events at DEBUG level with JSON serialization for inspection.
/// Tracks metrics about events published, batch sizes, and throughput.
///
/// This is a stub implementation for development - in production, this will
/// be replaced with an HTTP/gRPC publisher that sends events to a central service.
pub struct DebugLoggerPublisher {
    metrics: Arc<PublisherMetrics>,
}

struct PublisherMetrics {
    total_events: AtomicU64,
    total_batches: AtomicU64,
    total_bytes: AtomicU64,
}

impl DebugLoggerPublisher {
    pub fn new() -> Self {
        info!("Initializing DebugLoggerPublisher (stub implementation)");
        Self {
            metrics: Arc::new(PublisherMetrics {
                total_events: AtomicU64::new(0),
                total_batches: AtomicU64::new(0),
                total_bytes: AtomicU64::new(0),
            }),
        }
    }

    pub fn get_metrics(&self) -> (u64, u64, u64) {
        (
            self.metrics.total_events.load(Ordering::Relaxed),
            self.metrics.total_batches.load(Ordering::Relaxed),
            self.metrics.total_bytes.load(Ordering::Relaxed),
        )
    }
}

impl Default for DebugLoggerPublisher {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl EventPublisher for DebugLoggerPublisher {
    async fn publish_batch(&self, events: Vec<QueryEvent>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let batch_size = events.len();

        // Serialize batch to JSON for logging
        let json = serde_json::to_string(&events)?;
        let byte_size = json.len() as u64;

        // Update metrics
        self.metrics.total_events.fetch_add(batch_size as u64, Ordering::Relaxed);
        self.metrics.total_batches.fetch_add(1, Ordering::Relaxed);
        self.metrics.total_bytes.fetch_add(byte_size, Ordering::Relaxed);

        // Log the batch. The full serialized batch can contain raw query text
        // when anonymization is disabled, so only dump it when the operator has
        // opted into unsafe debug logging (P1 §4.4); otherwise log just sizes.
        if crate::observability::unsafe_debug_logging() {
            debug!(
                batch_size = batch_size,
                byte_size = byte_size,
                events = %json,
                "Published query event batch"
            );
        } else {
            debug!(batch_size = batch_size, byte_size = byte_size, "Published query event batch");
        }

        // Log summary info
        let (total_events, total_batches, total_bytes) = self.get_metrics();
        info!(
            batch_size = batch_size,
            total_events = total_events,
            total_batches = total_batches,
            total_bytes = total_bytes,
            avg_batch_size = total_events as f64 / total_batches as f64,
            "Query events published"
        );

        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        let (total_events, total_batches, total_bytes) = self.get_metrics();
        info!(
            total_events = total_events,
            total_batches = total_batches,
            total_bytes = total_bytes,
            "DebugLoggerPublisher shutting down"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::publisher::QueryEventBuilder;
    use std::time::Duration;

    #[tokio::test]
    async fn test_debug_logger_publishes_batch() {
        let publisher = DebugLoggerPublisher::new();

        let events = vec![
            QueryEventBuilder::new("SELECT * FROM users")
                .duration(Duration::from_millis(5))
                .rows(10)
                .build(),
            QueryEventBuilder::new("INSERT INTO logs VALUES ($1, $2)")
                .duration(Duration::from_millis(2))
                .rows(1)
                .build(),
        ];

        let result = publisher.publish_batch(events).await;
        assert!(result.is_ok());

        let (total_events, total_batches, _) = publisher.get_metrics();
        assert_eq!(total_events, 2);
        assert_eq!(total_batches, 1);
    }

    #[tokio::test]
    async fn test_empty_batch() {
        let publisher = DebugLoggerPublisher::new();
        let result = publisher.publish_batch(vec![]).await;
        assert!(result.is_ok());

        let (total_events, total_batches, _) = publisher.get_metrics();
        assert_eq!(total_events, 0);
        assert_eq!(total_batches, 0);
    }
}
