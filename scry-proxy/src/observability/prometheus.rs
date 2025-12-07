/// Prometheus text format exporter
///
/// Exports ProxyMetrics in Prometheus text exposition format for scraping.
/// Reference: https://prometheus.io/docs/instrumenting/exposition_formats/
///
/// Example output:
/// ```text
/// # HELP scry_query_latency_seconds Query latency in seconds
/// # TYPE scry_query_latency_seconds summary
/// scry_query_latency_seconds{quantile="0.5"} 0.002
/// scry_query_latency_seconds{quantile="0.99"} 0.008
/// scry_query_latency_seconds_sum 15.234
/// scry_query_latency_seconds_count 1000
/// ```

use super::metrics::ProxyMetrics;
use std::fmt::Write;
use std::sync::atomic::Ordering;

/// Export all metrics in Prometheus text format
pub fn export_metrics(metrics: &ProxyMetrics) -> String {
    let mut output = String::with_capacity(4096);

    // Export query counters
    export_query_counters(&mut output, metrics);

    // Export latency histograms
    export_latency_metrics(&mut output, metrics);

    // Export timeline phase histograms
    export_timeline_metrics(&mut output, metrics);

    // Export pool metrics
    export_pool_metrics(&mut output, metrics);

    // Export active connections
    export_connection_metrics(&mut output, metrics);

    // Export uptime
    export_uptime(&mut output, metrics);

    // Export circuit breaker metrics
    export_circuit_breaker_metrics(&mut output, metrics);

    output
}

fn export_query_counters(output: &mut String, metrics: &ProxyMetrics) {
    let query_metrics = metrics.query_metrics();
    let total = query_metrics.total_queries.load(Ordering::Relaxed);
    let errors = query_metrics.total_errors.load(Ordering::Relaxed);

    // Total queries
    writeln!(
        output,
        "# HELP scry_queries_total Total number of queries processed"
    )
    .unwrap();
    writeln!(output, "# TYPE scry_queries_total counter").unwrap();
    writeln!(output, "scry_queries_total {}", total).unwrap();

    // Total errors
    writeln!(
        output,
        "# HELP scry_query_errors_total Total number of query errors"
    )
    .unwrap();
    writeln!(output, "# TYPE scry_query_errors_total counter").unwrap();
    writeln!(output, "scry_query_errors_total {}", errors).unwrap();

    // Error rate
    let error_rate = if total > 0 {
        errors as f64 / total as f64
    } else {
        0.0
    };
    writeln!(output, "# HELP scry_query_error_rate Query error rate").unwrap();
    writeln!(output, "# TYPE scry_query_error_rate gauge").unwrap();
    writeln!(output, "scry_query_error_rate {:.6}", error_rate).unwrap();
}

fn export_latency_metrics(output: &mut String, metrics: &ProxyMetrics) {
    let query_metrics = metrics.query_metrics();
    let percentiles = query_metrics.get_latency_percentiles();

    writeln!(
        output,
        "# HELP scry_query_latency_seconds Query latency in seconds"
    )
    .unwrap();
    writeln!(output, "# TYPE scry_query_latency_seconds summary").unwrap();

    // Export quantiles
    writeln!(
        output,
        "scry_query_latency_seconds{{quantile=\"0.5\"}} {:.9}",
        percentiles.p50_micros as f64 / 1_000_000.0
    )
    .unwrap();
    writeln!(
        output,
        "scry_query_latency_seconds{{quantile=\"0.9\"}} {:.9}",
        percentiles.p90_micros as f64 / 1_000_000.0
    )
    .unwrap();
    writeln!(
        output,
        "scry_query_latency_seconds{{quantile=\"0.95\"}} {:.9}",
        percentiles.p95_micros as f64 / 1_000_000.0
    )
    .unwrap();
    writeln!(
        output,
        "scry_query_latency_seconds{{quantile=\"0.99\"}} {:.9}",
        percentiles.p99_micros as f64 / 1_000_000.0
    )
    .unwrap();
    writeln!(
        output,
        "scry_query_latency_seconds{{quantile=\"0.999\"}} {:.9}",
        percentiles.p999_micros as f64 / 1_000_000.0
    )
    .unwrap();

    // Export sum and count (calculated from histogram)
    let total = metrics.query_metrics().total_queries.load(Ordering::Relaxed);
    let sum_seconds = percentiles.mean_micros / 1_000_000.0 * total as f64;
    writeln!(output, "scry_query_latency_seconds_sum {:.9}", sum_seconds).unwrap();
    writeln!(output, "scry_query_latency_seconds_count {}", total).unwrap();
}

fn export_timeline_metrics(output: &mut String, metrics: &ProxyMetrics) {
    let query_metrics = metrics.query_metrics();

    // Queue time
    let queue_percentiles = query_metrics.get_queue_percentiles();
    writeln!(
        output,
        "# HELP scry_query_queue_time_seconds Time spent waiting before pool acquisition"
    )
    .unwrap();
    writeln!(output, "# TYPE scry_query_queue_time_seconds summary").unwrap();
    export_quantiles(output, "scry_query_queue_time_seconds", &queue_percentiles);

    // Pool acquire time
    let pool_percentiles = query_metrics.get_pool_acquire_percentiles();
    writeln!(
        output,
        "# HELP scry_query_pool_acquire_seconds Time spent acquiring connection from pool"
    )
    .unwrap();
    writeln!(
        output,
        "# TYPE scry_query_pool_acquire_seconds summary"
    )
    .unwrap();
    export_quantiles(output, "scry_query_pool_acquire_seconds", &pool_percentiles);

    // Backend execution time
    let backend_percentiles = query_metrics.get_backend_percentiles();
    writeln!(
        output,
        "# HELP scry_query_backend_seconds Time spent executing on backend database"
    )
    .unwrap();
    writeln!(output, "# TYPE scry_query_backend_seconds summary").unwrap();
    export_quantiles(output, "scry_query_backend_seconds", &backend_percentiles);
}

fn export_pool_metrics(output: &mut String, metrics: &ProxyMetrics) {
    let pool_status = metrics.pool_metrics().get_status();

    // Pool size
    writeln!(
        output,
        "# HELP scry_pool_connections_total Current pool size (total connections)"
    )
    .unwrap();
    writeln!(output, "# TYPE scry_pool_connections_total gauge").unwrap();
    writeln!(output, "scry_pool_connections_total {}", pool_status.size).unwrap();

    // Available connections
    writeln!(
        output,
        "# HELP scry_pool_connections_available Available connections in pool"
    )
    .unwrap();
    writeln!(output, "# TYPE scry_pool_connections_available gauge").unwrap();
    writeln!(
        output,
        "scry_pool_connections_available {}",
        pool_status.available
    )
    .unwrap();

    // Max pool size
    writeln!(
        output,
        "# HELP scry_pool_connections_max Maximum pool size"
    )
    .unwrap();
    writeln!(output, "# TYPE scry_pool_connections_max gauge").unwrap();
    writeln!(output, "scry_pool_connections_max {}", pool_status.max_size).unwrap();

    // Pool utilization
    writeln!(
        output,
        "# HELP scry_pool_utilization Pool utilization (0.0 to 1.0)"
    )
    .unwrap();
    writeln!(output, "# TYPE scry_pool_utilization gauge").unwrap();
    writeln!(output, "scry_pool_utilization {:.4}", pool_status.utilization()).unwrap();
}

fn export_connection_metrics(output: &mut String, metrics: &ProxyMetrics) {
    let active = metrics.get_active_connections();

    writeln!(
        output,
        "# HELP scry_active_connections Current number of active client connections"
    )
    .unwrap();
    writeln!(output, "# TYPE scry_active_connections gauge").unwrap();
    writeln!(output, "scry_active_connections {}", active).unwrap();
}

fn export_uptime(output: &mut String, metrics: &ProxyMetrics) {
    let uptime_secs = metrics.uptime().as_secs();

    writeln!(output, "# HELP scry_uptime_seconds Proxy uptime in seconds").unwrap();
    writeln!(output, "# TYPE scry_uptime_seconds counter").unwrap();
    writeln!(output, "scry_uptime_seconds {}", uptime_secs).unwrap();
}

fn export_circuit_breaker_metrics(output: &mut String, metrics: &ProxyMetrics) {
    if let Some(cb_metrics) = metrics.circuit_breaker_metrics() {
        // Circuit breaker state (0=Closed, 1=Open, 2=HalfOpen)
        let state_value = match cb_metrics.state.as_str() {
            "closed" => 0,
            "open" => 1,
            "half_open" => 2,
            _ => 0,
        };

        writeln!(
            output,
            "# HELP scry_circuit_breaker_state Circuit breaker state (0=Closed, 1=Open, 2=HalfOpen)"
        )
        .unwrap();
        writeln!(output, "# TYPE scry_circuit_breaker_state gauge").unwrap();
        writeln!(output, "scry_circuit_breaker_state {}", state_value).unwrap();

        // Consecutive failures
        writeln!(
            output,
            "# HELP scry_circuit_breaker_consecutive_failures Consecutive failures in current window"
        )
        .unwrap();
        writeln!(output, "# TYPE scry_circuit_breaker_consecutive_failures gauge").unwrap();
        writeln!(
            output,
            "scry_circuit_breaker_consecutive_failures {}",
            cb_metrics.consecutive_failures
        )
        .unwrap();

        // Consecutive successes
        writeln!(
            output,
            "# HELP scry_circuit_breaker_consecutive_successes Consecutive successes in half-open state"
        )
        .unwrap();
        writeln!(output, "# TYPE scry_circuit_breaker_consecutive_successes gauge").unwrap();
        writeln!(
            output,
            "scry_circuit_breaker_consecutive_successes {}",
            cb_metrics.consecutive_successes
        )
        .unwrap();

        // Requests allowed
        writeln!(
            output,
            "# HELP scry_circuit_breaker_requests_allowed_total Total requests allowed through"
        )
        .unwrap();
        writeln!(output, "# TYPE scry_circuit_breaker_requests_allowed_total counter").unwrap();
        writeln!(
            output,
            "scry_circuit_breaker_requests_allowed_total {}",
            cb_metrics.requests_allowed
        )
        .unwrap();

        // Requests rejected
        writeln!(
            output,
            "# HELP scry_circuit_breaker_requests_rejected_total Total requests rejected (circuit open)"
        )
        .unwrap();
        writeln!(output, "# TYPE scry_circuit_breaker_requests_rejected_total counter").unwrap();
        writeln!(
            output,
            "scry_circuit_breaker_requests_rejected_total {}",
            cb_metrics.requests_rejected
        )
        .unwrap();
    }
}

/// Helper to export quantiles for a histogram
fn export_quantiles(
    output: &mut String,
    metric_name: &str,
    percentiles: &super::metrics::LatencyPercentiles,
) {
    writeln!(
        output,
        "{}{{quantile=\"0.5\"}} {:.9}",
        metric_name,
        percentiles.p50_micros as f64 / 1_000_000.0
    )
    .unwrap();
    writeln!(
        output,
        "{}{{quantile=\"0.9\"}} {:.9}",
        metric_name,
        percentiles.p90_micros as f64 / 1_000_000.0
    )
    .unwrap();
    writeln!(
        output,
        "{}{{quantile=\"0.95\"}} {:.9}",
        metric_name,
        percentiles.p95_micros as f64 / 1_000_000.0
    )
    .unwrap();
    writeln!(
        output,
        "{}{{quantile=\"0.99\"}} {:.9}",
        metric_name,
        percentiles.p99_micros as f64 / 1_000_000.0
    )
    .unwrap();
    writeln!(
        output,
        "{}{{quantile=\"0.999\"}} {:.9}",
        metric_name,
        percentiles.p999_micros as f64 / 1_000_000.0
    )
    .unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observability::{HealthConfig, ProxyMetrics, QueryTimeline};

    #[test]
    fn test_export_empty_metrics() {
        let metrics = ProxyMetrics::new(100, HealthConfig::default());
        let output = export_metrics(&metrics);

        // Should contain metric declarations
        assert!(output.contains("scry_queries_total"));
        assert!(output.contains("scry_query_latency_seconds"));
        assert!(output.contains("scry_pool_connections_total"));
        assert!(output.contains("scry_uptime_seconds"));
    }

    #[test]
    fn test_export_with_queries() {
        let metrics = ProxyMetrics::new(100, HealthConfig::default());

        // Record some queries
        let timeline = QueryTimeline::new();
        metrics.record_query(&timeline, true);
        metrics.record_query(&timeline, false); // Error

        let output = export_metrics(&metrics);

        // Should show 2 total queries
        assert!(output.contains("scry_queries_total 2"));

        // Should show 1 error
        assert!(output.contains("scry_query_errors_total 1"));

        // Should have error rate of 0.5
        assert!(output.contains("scry_query_error_rate 0.5"));
    }

    #[test]
    fn test_prometheus_format() {
        let metrics = ProxyMetrics::new(100, HealthConfig::default());
        let output = export_metrics(&metrics);

        // Check for Prometheus format conventions
        assert!(output.contains("# HELP"));
        assert!(output.contains("# TYPE"));
        assert!(output.contains("counter") || output.contains("gauge") || output.contains("summary"));

        // Check for valid quantile labels
        assert!(output.contains("quantile=\"0.5\""));
        assert!(output.contains("quantile=\"0.99\""));
        assert!(output.contains("quantile=\"0.999\""));
    }

    #[test]
    fn test_pool_metrics_export() {
        let metrics = ProxyMetrics::new(100, HealthConfig::default());
        metrics.update_pool_metrics(10, 5, 10);

        let output = export_metrics(&metrics);

        assert!(output.contains("scry_pool_connections_total 10"));
        assert!(output.contains("scry_pool_connections_available 5"));
        assert!(output.contains("scry_pool_connections_max 10"));
        assert!(output.contains("scry_pool_utilization 0.5"));
    }
}
