mod debug_logger;
mod http_publisher;
mod r#trait;

pub use debug_logger::DebugLoggerPublisher;
pub use scry_protocol::{QueryEvent, QueryEventBuilder};
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

            // Get shadow_id from config or from SHADOW_ID_FILE environment variable
            let shadow_id = config.shadow_id.clone().or_else(|| {
                std::env::var("SHADOW_ID_FILE").ok().and_then(|path| {
                    std::fs::read_to_string(&path).ok().map(|s| s.trim().to_string())
                })
            });

            info!(
                endpoint = %endpoint,
                shadow_id = ?shadow_id,
                "Creating HttpPublisher"
            );

            let publisher = HttpPublisher::new(
                endpoint.clone(),
                config.http_timeout_ms,
                config.http_max_retries,
                config.http_api_key.clone(),
                shadow_id,
                config.http_compression,
            )?;

            Ok(Arc::new(publisher))
        }
        other => {
            anyhow::bail!("Unknown publisher type: {}", other)
        }
    }
}
