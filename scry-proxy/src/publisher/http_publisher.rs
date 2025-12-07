use super::{EventPublisher, QueryEvent};
use scry_protocol::FlatBuffersSerializer;
use anyhow::{Context, Result};
use async_trait::async_trait;
use reqwest::Client;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// HTTP-based event publisher using FlatBuffers serialization
///
/// Sends batches of QueryEvents to a central analytics service via HTTP POST.
/// Uses FlatBuffers for efficient, zero-copy serialization.
///
/// Features:
/// - Non-blocking, best-effort delivery
/// - Configurable retries with exponential backoff
/// - Timeout protection
/// - Optional gzip compression
/// - API key authentication support
pub struct HttpPublisher {
    client: Client,
    endpoint: String,
    max_retries: u32,
    api_key: Option<String>,
    proxy_id: String,
    metrics: Arc<PublisherMetrics>,
}

pub struct PublisherMetrics {
    pub total_events: AtomicU64,
    pub total_batches: AtomicU64,
    pub total_bytes: AtomicU64,
    pub batch_seq: AtomicU64,
    pub successful_publishes: AtomicU64,
    pub failed_publishes: AtomicU64,
}

impl HttpPublisher {
    /// Create a new HTTP publisher
    pub fn new(
        endpoint: String,
        timeout_ms: u64,
        max_retries: u32,
        api_key: Option<String>,
        compression: bool,
    ) -> Result<Self> {
        // Generate a unique proxy ID for this instance
        let proxy_id = format!("scry-{}", uuid::Uuid::new_v4());

        // Build HTTP client with timeout
        let mut client_builder = Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .tcp_nodelay(true); // Disable Nagle's algorithm for lower latency

        if compression {
            client_builder = client_builder.gzip(true);
        }

        let client = client_builder
            .build()
            .context("Failed to build HTTP client")?;

        info!(
            endpoint = %endpoint,
            timeout_ms = timeout_ms,
            max_retries = max_retries,
            compression = compression,
            proxy_id = %proxy_id,
            "Initializing HttpPublisher"
        );

        Ok(Self {
            client,
            endpoint,
            max_retries,
            api_key,
            proxy_id,
            metrics: Arc::new(PublisherMetrics {
                total_events: AtomicU64::new(0),
                total_batches: AtomicU64::new(0),
                total_bytes: AtomicU64::new(0),
                batch_seq: AtomicU64::new(0),
                successful_publishes: AtomicU64::new(0),
                failed_publishes: AtomicU64::new(0),
            }),
        })
    }

    /// Publish with retry logic
    async fn publish_with_retries(&self, payload: Vec<u8>) -> Result<()> {
        let mut attempts = 0;
        let mut backoff_ms = 50; // Start with 50ms backoff

        loop {
            match self.send_request(&payload).await {
                Ok(_) => {
                    if attempts > 0 {
                        debug!(attempts = attempts + 1, "Publish succeeded after retries");
                    }
                    self.metrics.successful_publishes.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
                Err(e) => {
                    attempts += 1;

                    if attempts > self.max_retries {
                        error!(
                            error = %e,
                            attempts = attempts,
                            "Publish failed after all retries"
                        );
                        self.metrics.failed_publishes.fetch_add(1, Ordering::Relaxed);
                        return Err(e);
                    }

                    warn!(
                        error = %e,
                        attempt = attempts,
                        max_retries = self.max_retries,
                        backoff_ms = backoff_ms,
                        "Publish failed, retrying"
                    );

                    // Exponential backoff
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(5000); // Cap at 5 seconds
                }
            }
        }
    }

    /// Send HTTP request with the payload
    async fn send_request(&self, payload: &[u8]) -> Result<()> {
        let mut request = self
            .client
            .post(&self.endpoint)
            .header("Content-Type", "application/x-flexbuffer")
            .body(payload.to_vec());

        // Add API key if configured
        if let Some(api_key) = &self.api_key {
            request = request.header("X-API-Key", api_key);
        }

        let response = request
            .send()
            .await
            .context("Failed to send HTTP request")?;

        let status = response.status();

        if !status.is_success() {
            let error_body = response
                .text()
                .await
                .unwrap_or_else(|_| "Unable to read response body".to_string());

            anyhow::bail!(
                "HTTP request failed with status {}: {}",
                status,
                error_body
            );
        }

        debug!(status = status.as_u16(), "HTTP request successful");
        Ok(())
    }

    pub fn get_metrics(&self) -> PublisherMetrics {
        PublisherMetrics {
            total_events: AtomicU64::new(self.metrics.total_events.load(Ordering::Relaxed)),
            total_batches: AtomicU64::new(self.metrics.total_batches.load(Ordering::Relaxed)),
            total_bytes: AtomicU64::new(self.metrics.total_bytes.load(Ordering::Relaxed)),
            batch_seq: AtomicU64::new(self.metrics.batch_seq.load(Ordering::Relaxed)),
            successful_publishes: AtomicU64::new(
                self.metrics.successful_publishes.load(Ordering::Relaxed),
            ),
            failed_publishes: AtomicU64::new(
                self.metrics.failed_publishes.load(Ordering::Relaxed),
            ),
        }
    }
}

#[async_trait]
impl EventPublisher for HttpPublisher {
    async fn publish_batch(&self, events: Vec<QueryEvent>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let batch_size = events.len();
        let batch_seq = self.metrics.batch_seq.fetch_add(1, Ordering::Relaxed);

        // Serialize to FlatBuffers
        let payload = FlatBuffersSerializer::serialize_batch(&events, &self.proxy_id, batch_seq);
        let byte_size = payload.len() as u64;

        debug!(
            batch_size = batch_size,
            batch_seq = batch_seq,
            byte_size = byte_size,
            "Serialized batch to FlatBuffers"
        );

        // Update metrics before attempting publish
        self.metrics.total_events.fetch_add(batch_size as u64, Ordering::Relaxed);
        self.metrics.total_batches.fetch_add(1, Ordering::Relaxed);
        self.metrics.total_bytes.fetch_add(byte_size, Ordering::Relaxed);

        // Publish with retries (best-effort)
        // We don't propagate errors to avoid blocking the proxy
        if let Err(e) = self.publish_with_retries(payload).await {
            warn!(
                error = %e,
                batch_size = batch_size,
                batch_seq = batch_seq,
                "Failed to publish batch (continuing)"
            );
        }

        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        let metrics = self.get_metrics();
        info!(
            total_events = metrics.total_events.load(Ordering::Relaxed),
            total_batches = metrics.total_batches.load(Ordering::Relaxed),
            total_bytes = metrics.total_bytes.load(Ordering::Relaxed),
            successful = metrics.successful_publishes.load(Ordering::Relaxed),
            failed = metrics.failed_publishes.load(Ordering::Relaxed),
            "HttpPublisher shutting down"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::publisher::QueryEventBuilder;
    use std::time::Duration as StdDuration;

    #[tokio::test]
    async fn test_http_publisher_creation() {
        let publisher = HttpPublisher::new(
            "http://localhost:8080/events".to_string(),
            1000,
            2,
            None,
            false,
        );

        assert!(publisher.is_ok());
    }

    #[tokio::test]
    async fn test_serialization() {
        let events = vec![
            QueryEventBuilder::new("SELECT 1")
                .connection_id("conn-1")
                .database("db1")
                .duration(StdDuration::from_millis(5))
                .build(),
        ];

        let payload = FlatBuffersSerializer::serialize_batch(&events, "test-proxy", 0);
        assert!(!payload.is_empty());
    }

    // Note: Full end-to-end HTTP tests would require a mock server
    // For now, we test serialization and publisher creation
}
