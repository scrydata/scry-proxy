// Module declarations
pub mod health;
pub mod hot_data;
pub mod metrics;
pub mod metrics_server;
pub mod prometheus;
pub mod timeline;

// Re-exports for convenience
pub use health::{HealthConfig, HealthMonitor, HealthSnapshot, HealthStatus, HealthWarning};
pub use hot_data::{HotDataEntry, HotDataTracker};
pub use metrics::{
    LatencyPercentiles, LatencyPercentilesMs, PinReasonCounts, PoolStatus, ProxyMetrics,
};
pub use metrics_server::MetricsServer;
pub use timeline::{QueryTimeline, TimelinePhases};

use anyhow::Result;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::config::ObservabilityConfig;

/// Initialize observability (tracing, metrics, OpenTelemetry)
pub fn init(config: &ObservabilityConfig) -> Result<()> {
    // Set up tracing subscriber with env filter
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,scry=debug"));

    let registry = tracing_subscriber::registry().with(env_filter);

    // Initialize OpenTelemetry if enabled
    if config.enable_tracing {
        if let Some(otlp_endpoint) = &config.otlp_endpoint {
            tracing::info!(
                service_name = %config.service_name,
                otlp_endpoint = %otlp_endpoint,
                "Initializing OpenTelemetry with OTLP exporter"
            );

            use opentelemetry::KeyValue;
            use opentelemetry_otlp::WithExportConfig;
            use opentelemetry_sdk::{runtime, trace as sdktrace, Resource};
            use tracing_opentelemetry::OpenTelemetryLayer;

            // Create OTLP trace exporter
            let tracer = opentelemetry_otlp::new_pipeline()
                .tracing()
                .with_exporter(
                    opentelemetry_otlp::new_exporter().tonic().with_endpoint(otlp_endpoint),
                )
                .with_trace_config(sdktrace::config().with_resource(Resource::new(vec![
                    KeyValue::new("service.name", config.service_name.clone()),
                ])))
                .install_batch(runtime::TokioCurrentThread)?;

            // Add OpenTelemetry layer to tracing
            let telemetry_layer = OpenTelemetryLayer::new(tracer);

            registry
                .with(tracing_subscriber::fmt::layer().with_target(true))
                .with(telemetry_layer)
                .init();
        } else {
            // No OTLP endpoint configured, use default tracing only
            registry.with(tracing_subscriber::fmt::layer().with_target(true)).init();
        }
    } else {
        // Tracing disabled, use basic logging only
        registry.with(tracing_subscriber::fmt::layer().with_target(true)).init();
    }

    tracing::info!("Observability initialized");

    Ok(())
}
