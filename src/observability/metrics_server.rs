/// HTTP metrics server using Axum
///
/// Provides 5 endpoints for observability:
/// 1. GET /metrics - Prometheus text format
/// 2. GET /health - JSON health status with warnings
/// 3. GET /debug/pool - Pool internals and utilization
/// 4. GET /debug/timeline - Query timeline phase breakdown
/// 5. GET /debug/hotdata - Hot data fingerprints (top-K)

use super::metrics::ProxyMetrics;
use super::prometheus;
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use tracing::{error, info};

/// Configuration for metrics server
#[derive(Debug, Clone)]
pub struct MetricsServerConfig {
    pub listen_address: String,
}

impl Default for MetricsServerConfig {
    fn default() -> Self {
        Self {
            listen_address: "127.0.0.1:9090".to_string(),
        }
    }
}

/// HTTP metrics server
pub struct MetricsServer {
    metrics: Arc<ProxyMetrics>,
    config: MetricsServerConfig,
}

impl MetricsServer {
    /// Create a new metrics server
    pub fn new(metrics: Arc<ProxyMetrics>, config: MetricsServerConfig) -> Self {
        Self { metrics, config }
    }

    /// Run the metrics server (blocking)
    pub async fn run(self) -> anyhow::Result<()> {
        let addr: SocketAddr = self.config.listen_address.parse()?;

        info!(
            listen_address = %addr,
            "Starting metrics server"
        );

        // Build router with all endpoints
        let app = Router::new()
            .route("/metrics", get(metrics_handler))
            .route("/health", get(health_handler))
            .route("/debug/pool", get(pool_handler))
            .route("/debug/timeline", get(timeline_handler))
            .route("/debug/hotdata", get(hotdata_handler))
            .layer(TraceLayer::new_for_http())
            .with_state(self.metrics);

        // Start server
        let listener = TcpListener::bind(&addr).await?;
        info!("Metrics server listening on {}", addr);

        axum::serve(listener, app).await?;

        Ok(())
    }
}

/// GET /metrics - Prometheus text format
async fn metrics_handler(State(metrics): State<Arc<ProxyMetrics>>) -> Response {
    let output = prometheus::export_metrics(&metrics);
    (StatusCode::OK, output).into_response()
}

/// GET /health - JSON health status
async fn health_handler(
    State(metrics): State<Arc<ProxyMetrics>>,
) -> Json<HealthResponse> {
    let health_monitor = metrics.health_monitor();
    let status = health_monitor.get_status();
    let warnings = health_monitor.get_warnings();
    let baseline = health_monitor.get_baseline();

    Json(HealthResponse {
        status,
        warnings,
        baseline,
        uptime_seconds: metrics.uptime().as_secs(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// GET /debug/pool - Pool internals
async fn pool_handler(
    State(metrics): State<Arc<ProxyMetrics>>,
) -> Json<PoolResponse> {
    let pool_status = metrics.pool_metrics().get_status();

    Json(PoolResponse {
        current_size: pool_status.size,
        available: pool_status.available,
        max_size: pool_status.max_size,
        utilization: pool_status.utilization(),
        in_use: pool_status.size - pool_status.available,
    })
}

/// GET /debug/timeline - Query timeline breakdown
async fn timeline_handler(
    State(metrics): State<Arc<ProxyMetrics>>,
) -> Json<TimelineResponse> {
    let query_metrics = metrics.query_metrics();

    // Get percentiles for each phase
    let latency = query_metrics.get_latency_percentiles().to_millis();
    let queue = query_metrics.get_queue_percentiles().to_millis();
    let pool_acquire = query_metrics.get_pool_acquire_percentiles().to_millis();
    let backend = query_metrics.get_backend_percentiles().to_millis();

    Json(TimelineResponse {
        average_breakdown_ms: PhaseBreakdown {
            queue_time: queue.mean,
            pool_acquire: pool_acquire.mean,
            backend_execution: backend.mean,
            total: latency.mean,
        },
        p50_breakdown_ms: PhaseBreakdown {
            queue_time: queue.p50,
            pool_acquire: pool_acquire.p50,
            backend_execution: backend.p50,
            total: latency.p50,
        },
        p95_breakdown_ms: PhaseBreakdown {
            queue_time: queue.p95,
            pool_acquire: pool_acquire.p95,
            backend_execution: backend.p95,
            total: latency.p95,
        },
        p99_breakdown_ms: PhaseBreakdown {
            queue_time: queue.p99,
            pool_acquire: pool_acquire.p99,
            backend_execution: backend.p99,
            total: latency.p99,
        },
    })
}

/// GET /debug/hotdata - Hot data fingerprints
async fn hotdata_handler(
    State(metrics): State<Arc<ProxyMetrics>>,
) -> Json<HotDataResponse> {
    let hot_data = metrics.hot_data();
    let top_k = hot_data.get_top_k();
    let unique = hot_data.unique_fingerprints();

    Json(HotDataResponse {
        top_fingerprints: top_k,
        total_unique_fingerprints: unique,
        decay_factor: 0.99,
    })
}

// Response types

#[derive(Debug, Serialize, Deserialize)]
struct HealthResponse {
    status: super::health::HealthStatus,
    warnings: Vec<super::health::HealthWarning>,
    baseline: super::health::BaselineSnapshot,
    uptime_seconds: u64,
    version: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct PoolResponse {
    current_size: usize,
    available: usize,
    max_size: usize,
    utilization: f64,
    in_use: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct TimelineResponse {
    average_breakdown_ms: PhaseBreakdown,
    p50_breakdown_ms: PhaseBreakdown,
    p95_breakdown_ms: PhaseBreakdown,
    p99_breakdown_ms: PhaseBreakdown,
}

#[derive(Debug, Serialize, Deserialize)]
struct PhaseBreakdown {
    queue_time: f64,
    pool_acquire: f64,
    backend_execution: f64,
    total: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct HotDataResponse {
    top_fingerprints: Vec<super::hot_data::HotDataEntry>,
    total_unique_fingerprints: usize,
    decay_factor: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observability::{HealthConfig, QueryTimeline};

    #[tokio::test]
    async fn test_health_endpoint_structure() {
        let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));

        // Call handler
        let response = health_handler(State(metrics)).await;

        // Verify structure
        assert_eq!(response.0.status, super::super::health::HealthStatus::Healthy);
        assert!(response.0.warnings.is_empty());
        assert!(response.0.uptime_seconds >= 0);
        assert!(!response.0.version.is_empty());
    }

    #[tokio::test]
    async fn test_pool_endpoint_structure() {
        let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));
        metrics.update_pool_metrics(10, 5, 10);

        // Call handler
        let response = pool_handler(State(metrics)).await;

        // Verify values
        assert_eq!(response.0.current_size, 10);
        assert_eq!(response.0.available, 5);
        assert_eq!(response.0.max_size, 10);
        assert_eq!(response.0.utilization, 0.5);
        assert_eq!(response.0.in_use, 5);
    }

    #[tokio::test]
    async fn test_timeline_endpoint_structure() {
        let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));

        // Record a query
        let timeline = QueryTimeline::new();
        metrics.record_query(&timeline, true);

        // Call handler
        let response = timeline_handler(State(metrics)).await;

        // Verify structure (all fields should be present)
        assert!(response.0.average_breakdown_ms.total >= 0.0);
        assert!(response.0.p99_breakdown_ms.total >= 0.0);
    }

    #[tokio::test]
    async fn test_hotdata_endpoint_structure() {
        let metrics = Arc::new(ProxyMetrics::new(10, HealthConfig::default()));

        // Record some hot data
        let fingerprints = vec!["blake3:test1".to_string(), "blake3:test2".to_string()];
        metrics.record_hot_data(&fingerprints);

        // Call handler
        let response = hotdata_handler(State(metrics)).await;

        // Verify structure
        assert!(response.0.total_unique_fingerprints >= 2);
        assert_eq!(response.0.decay_factor, 0.99);
    }

    #[tokio::test]
    async fn test_metrics_endpoint() {
        let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));

        // Record a query
        let timeline = QueryTimeline::new();
        metrics.record_query(&timeline, true);

        // Call handler
        let response = metrics_handler(State(metrics)).await;

        // Extract body
        let body = match response.into_body() {
            axum::body::Body::from(bytes) => String::from_utf8_lossy(&bytes.to_vec()).to_string(),
            _ => panic!("Unexpected body type"),
        };

        // Verify Prometheus format
        assert!(body.contains("scry_queries_total"));
        assert!(body.contains("# HELP"));
        assert!(body.contains("# TYPE"));
    }
}
