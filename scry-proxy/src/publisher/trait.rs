use super::QueryEvent;
use anyhow::Result;
use async_trait::async_trait;

/// Trait for publishing query events to downstream consumers
///
/// This abstraction allows for easy swapping of implementations:
/// - Debug logger (current stub)
/// - HTTP/gRPC publisher (future)
/// - Kafka/streaming platform (future)
#[async_trait]
pub trait EventPublisher: Send + Sync {
    /// Publish a batch of query events
    ///
    /// Implementations should handle batching efficiently and return quickly.
    /// Events are published best-effort - failures should be logged but not
    /// block the proxy.
    async fn publish_batch(&self, events: Vec<QueryEvent>) -> Result<()>;

    /// Shutdown the publisher gracefully, flushing any pending events
    async fn shutdown(&self) -> Result<()>;
}
