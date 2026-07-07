// Module declarations
pub mod health;
pub mod hot_data;
pub mod metrics;
pub mod metrics_server;
pub mod prometheus;
#[cfg(test)]
pub(crate) mod test_support;
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
use std::sync::atomic::{AtomicBool, Ordering};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::config::ObservabilityConfig;

/// Process-global mirror of `observability.unsafe_debug_logging`, set once at
/// startup by [`init`]. When `false` (the default), sensitive log sites emit a
/// fixed redaction placeholder instead of raw query text, parameters, or full
/// event payloads (P1 §4.4). Kept as a global (rather than threaded through
/// every call site) because the sensitive sites span low-level protocol code
/// with no config access.
static UNSAFE_DEBUG_LOGGING: AtomicBool = AtomicBool::new(false);

/// Substituted for raw query/event text at a log site when unsafe debug
/// logging is disabled.
pub const REDACTED_LOG_PLACEHOLDER: &str = "<redacted>";

/// Enable/disable unsafe debug logging (raw query/param/event text in logs).
pub fn set_unsafe_debug_logging(enabled: bool) {
    UNSAFE_DEBUG_LOGGING.store(enabled, Ordering::Relaxed);
}

/// Whether raw query/param/event text may be written to logs.
pub fn unsafe_debug_logging() -> bool {
    UNSAFE_DEBUG_LOGGING.load(Ordering::Relaxed)
}

/// Return `text` verbatim when unsafe debug logging is enabled, otherwise a
/// fixed redaction placeholder. Wrap every log field that could carry raw SQL,
/// parameter values, or a Postgres error message with this.
pub fn loggable(text: &str) -> &str {
    if unsafe_debug_logging() {
        text
    } else {
        REDACTED_LOG_PLACEHOLDER
    }
}

/// Initialize observability (tracing, metrics, OpenTelemetry)
pub fn init(config: &ObservabilityConfig) -> Result<()> {
    // Latch the log-hygiene flag before any sensitive log site can run.
    set_unsafe_debug_logging(config.unsafe_debug_logging);

    // Set up tracing subscriber with env filter
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));

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

#[cfg(test)]
mod log_hygiene_tests {
    use super::*;

    #[test]
    fn loggable_redacts_by_default_and_reveals_when_enabled() {
        // Default (and after disabling): raw text is redacted.
        set_unsafe_debug_logging(false);
        assert_eq!(loggable("SELECT * FROM t WHERE ssn = '123'"), REDACTED_LOG_PLACEHOLDER);
        assert!(!unsafe_debug_logging());

        // Opt-in: raw text passes through verbatim.
        set_unsafe_debug_logging(true);
        assert_eq!(loggable("SELECT 1"), "SELECT 1");
        assert!(unsafe_debug_logging());

        // Restore the safe default so we don't leak state to other tests.
        set_unsafe_debug_logging(false);
    }
}
