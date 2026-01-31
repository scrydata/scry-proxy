use scry::{config, observability, proxy, publisher};

use anyhow::Result;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<()> {
    // Load configuration first
    let config = config::Config::load()?;

    // Initialize tracing/observability with config
    observability::init(&config.observability)?;

    // Validate configuration and log warnings
    match config.validate() {
        Ok(warnings) => {
            for warning in warnings {
                tracing::warn!("{}", warning);
            }
        }
        Err(e) => {
            tracing::error!("Configuration validation failed: {}", e);
            return Err(e);
        }
    }

    tracing::info!("Starting Scry transparent SQL proxy");
    tracing::info!(
        listen_address = %config.proxy.listen_address,
        backend_host = %config.backend.host,
        backend_port = config.backend.port,
        "Configuration loaded"
    );

    // Initialize metrics
    let hot_data_top_k = 100; // Track top 100 hot data fingerprints
    let health_config = observability::HealthConfig::default();
    let metrics = Arc::new(observability::ProxyMetrics::new(hot_data_top_k, health_config));
    tracing::info!("Metrics system initialized");

    // Start metrics server if enabled
    if config.observability.enable_metrics_server {
        let metrics_server_config = observability::metrics_server::MetricsServerConfig {
            listen_address: config.observability.metrics_server_address.clone(),
        };
        let metrics_server =
            observability::MetricsServer::new(Arc::clone(&metrics), metrics_server_config);

        tokio::spawn(async move {
            if let Err(e) = metrics_server.run().await {
                tracing::error!(error = %e, "Metrics server failed");
            }
        });

        tracing::info!(
            address = %config.observability.metrics_server_address,
            "Metrics server started"
        );
    }

    // Start background observability tasks
    {
        let metrics_clone = Arc::clone(&metrics);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
            loop {
                interval.tick().await;

                // Run health check (creates snapshot and checks baselines)
                metrics_clone.run_health_check();

                // Apply temporal decay to hot data tracker
                metrics_clone.hot_data().apply_decay();

                tracing::debug!("Health check and decay applied");
            }
        });

        tracing::info!("Background observability tasks started");
    }

    // Initialize event publisher based on config
    let publisher: Arc<dyn publisher::EventPublisher> =
        publisher::create_publisher(&config.publisher)?;

    // Start proxy server
    tracing::info!("Starting proxy server");
    proxy::start_proxy(config, publisher, metrics).await?;

    Ok(())
}
