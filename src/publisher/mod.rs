mod debug_logger;
mod event;
mod flatbuffers_serializer;
mod http_publisher;
mod r#trait;

pub use debug_logger::DebugLoggerPublisher;
pub use event::{QueryEvent, QueryEventBuilder};
pub use http_publisher::HttpPublisher;
pub use r#trait::EventPublisher;

use crate::config::PublisherConfig;
use anyhow::{Context, Result};
use std::sync::Arc;
use tracing::info;

/// Create an EventPublisher based on configuration
pub fn create_publisher(config: &PublisherConfig) -> Result<Arc<dyn EventPublisher>> {
    match config.publisher_type.as_str() {
        "debug" => {
            info!("Creating DebugLoggerPublisher");
            Ok(Arc::new(DebugLoggerPublisher::new()))
        }
        "http" => {
            let endpoint = config
                .http_endpoint
                .as_ref()
                .context("http_endpoint is required when publisher_type = 'http'")?;

            info!(endpoint = %endpoint, "Creating HttpPublisher");

            let publisher = HttpPublisher::new(
                endpoint.clone(),
                config.http_timeout_ms,
                config.http_max_retries,
                config.http_api_key.clone(),
                config.http_compression,
            )?;

            Ok(Arc::new(publisher))
        }
        other => {
            anyhow::bail!("Unknown publisher type: {}", other)
        }
    }
}
