//! Metrics/health/debug HTTP surface access control (WP-10, P4 §4.5/§5.5).
//!
//! Before this task, `MetricsServer::run` always mounted `/metrics`,
//! `/health`, AND `/debug/pool`, `/debug/timeline`, `/debug/hotdata` (the last
//! of which returns blake3 value fingerprints — the most sensitive endpoint
//! in the proxy) with no access control and no gate on binding to a public
//! address. This suite is the guardrail:
//!
//! - `/debug/*` is mounted only when `enable_debug_endpoints = true` AND the
//!   server is bound to a loopback address. It is NEVER reachable on a
//!   non-loopback bind, regardless of the flag (chosen design: loopback-only,
//!   see `docs/deployment.md` "Metrics and Debug Endpoint Access Control").
//! - `/metrics` and `/health` are always mounted (no secrets, safe to scrape)
//!   regardless of bind address or the debug flag.
//! - `Config::validate()` refuses a non-loopback `metrics_server_address`
//!   unless `metrics_allow_non_loopback = true` is set (mirrors
//!   `auth.allow_trust`).
//! - `/metrics` output never contains a value that would only appear if a
//!   debug/secret code path had been (incorrectly) exercised.
//!
//! Runs the real Axum router via `MetricsServer::serve` on an ephemeral port
//! (`:0`) — ephemeral so this suite can run concurrently / in CI without a
//! fixed-port collision, and so the "non-loopback bind" case (`0.0.0.0:0`)
//! doesn't require any real external network interface.

use scry::config::Config;
use scry::observability::metrics_server::{MetricsServer, MetricsServerConfig};
use scry::observability::{HealthConfig, ProxyMetrics};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;

/// Bind an ephemeral listener on `host`, start `MetricsServer::serve` on it in
/// the background, and return the real bound address to hit with an HTTP
/// client. `host` is `"127.0.0.1"` for a loopback bind or `"0.0.0.0"` for a
/// non-loopback (wildcard) bind; `0.0.0.0` still accepts connections made to
/// `127.0.0.1`, so the test client can always dial back through loopback.
async fn spawn_metrics_server(host: &str, enable_debug_endpoints: bool) -> SocketAddr {
    let listener = TcpListener::bind((host, 0)).await.expect("bind ephemeral metrics listener");
    let addr = listener.local_addr().expect("local_addr");

    let metrics = Arc::new(ProxyMetrics::new(10, HealthConfig::default()));
    metrics.record_hot_data(&["blake3:distinctive-fingerprint-abc123".to_string()]);

    let config = MetricsServerConfig { listen_address: addr.to_string(), enable_debug_endpoints };
    let server = MetricsServer::new(metrics, config);

    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });

    // Give the spawned server a moment to start accepting; the listener is
    // already bound (accepting at the OS level) before `serve` is called, so
    // this is a small safety margin rather than a hard requirement.
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Reconnect via loopback regardless of bind host, matching how a
    // scraper/operator on the same host would reach a wildcard bind.
    format!("127.0.0.1:{}", addr.port()).parse().unwrap()
}

/// Like `spawn_metrics_server`, but also returns the live `Arc<ProxyMetrics>`
/// handle so a test can drive real metric-recording calls (the exact
/// production methods `proxy/connection.rs` calls at its P3 enforcement
/// sites) and then scrape `/metrics` over real HTTP to see them reflected
/// (WP-10 Task 8, P4 §4.5).
async fn spawn_metrics_server_with_handle(
    host: &str,
    enable_debug_endpoints: bool,
) -> (SocketAddr, Arc<ProxyMetrics>) {
    let listener = TcpListener::bind((host, 0)).await.expect("bind ephemeral metrics listener");
    let addr = listener.local_addr().expect("local_addr");

    let metrics = Arc::new(ProxyMetrics::new(10, HealthConfig::default()));

    let config = MetricsServerConfig { listen_address: addr.to_string(), enable_debug_endpoints };
    let server = MetricsServer::new(Arc::clone(&metrics), config);

    tokio::spawn(async move {
        let _ = server.serve(listener).await;
    });

    tokio::time::sleep(Duration::from_millis(20)).await;

    let real_addr: SocketAddr = format!("127.0.0.1:{}", addr.port()).parse().unwrap();
    (real_addr, metrics)
}

async fn get(addr: SocketAddr, path: &str) -> reqwest::StatusCode {
    let client = reqwest::Client::new();
    client
        .get(format!("http://{addr}{path}"))
        .send()
        .await
        .unwrap_or_else(|e| panic!("request to {addr}{path} failed: {e}"))
        .status()
}

async fn get_body(addr: SocketAddr, path: &str) -> (reqwest::StatusCode, String) {
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{addr}{path}"))
        .send()
        .await
        .unwrap_or_else(|e| panic!("request to {addr}{path} failed: {e}"));
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    (status, body)
}

// --- /debug/* gating ---

#[tokio::test]
async fn debug_endpoints_off_by_default_even_on_loopback() {
    let addr = spawn_metrics_server("127.0.0.1", false).await;

    assert_eq!(get(addr, "/debug/pool").await, reqwest::StatusCode::NOT_FOUND);
    assert_eq!(get(addr, "/debug/timeline").await, reqwest::StatusCode::NOT_FOUND);
    assert_eq!(get(addr, "/debug/hotdata").await, reqwest::StatusCode::NOT_FOUND);

    // /metrics and /health must still be up.
    assert_eq!(get(addr, "/metrics").await, reqwest::StatusCode::OK);
    assert_eq!(get(addr, "/health").await, reqwest::StatusCode::OK);
}

#[tokio::test]
async fn debug_endpoints_off_on_non_loopback_bind_even_if_enabled() {
    // The public-exposure guarantee: enabling the flag on a non-loopback bind
    // must NOT expose /debug/*.
    let addr = spawn_metrics_server("0.0.0.0", true).await;

    assert_eq!(
        get(addr, "/debug/hotdata").await,
        reqwest::StatusCode::NOT_FOUND,
        "/debug/hotdata (blake3 fingerprints) must not be reachable on a non-loopback bind"
    );
    assert_eq!(get(addr, "/debug/pool").await, reqwest::StatusCode::NOT_FOUND);
    assert_eq!(get(addr, "/debug/timeline").await, reqwest::StatusCode::NOT_FOUND);

    // /metrics and /health are unaffected.
    assert_eq!(get(addr, "/metrics").await, reqwest::StatusCode::OK);
    assert_eq!(get(addr, "/health").await, reqwest::StatusCode::OK);
}

#[tokio::test]
async fn debug_endpoints_reachable_when_enabled_and_loopback() {
    let addr = spawn_metrics_server("127.0.0.1", true).await;

    assert_eq!(get(addr, "/debug/pool").await, reqwest::StatusCode::OK);
    assert_eq!(get(addr, "/debug/timeline").await, reqwest::StatusCode::OK);
    let (status, body) = get_body(addr, "/debug/hotdata").await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert!(
        body.contains("distinctive-fingerprint-abc123"),
        "expected the recorded hot-data fingerprint in /debug/hotdata body: {body}"
    );
}

#[tokio::test]
async fn metrics_output_never_carries_a_debug_only_value() {
    // /metrics must never leak the hot-data fingerprint (that's /debug/hotdata's
    // job, and only when explicitly enabled + loopback).
    let addr = spawn_metrics_server("127.0.0.1", false).await;
    let (status, body) = get_body(addr, "/metrics").await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert!(
        !body.contains("distinctive-fingerprint-abc123"),
        "/metrics leaked a hot-data fingerprint that should be debug-endpoint-only: {body}"
    );
}

// --- Config::validate() non-loopback-bind acknowledgement ---

fn base_config() -> Config {
    let mut config = Config::default();
    // validate() has other fail-closed checks (backend password, auth trust,
    // ...) unrelated to this suite; satisfy them so only the metrics-bind
    // check under test can fail.
    config.backend.password = "irrelevant-for-this-suite".to_string();
    config.auth.allow_trust = true;
    // Default publisher.anonymize = true requires a salt; irrelevant to this
    // suite's metrics-bind checks, so just satisfy it.
    config.publisher.anonymize = false;
    config
}

#[test]
fn validate_rejects_non_loopback_metrics_bind_without_ack() {
    let mut config = base_config();
    config.observability.metrics_server_address = "0.0.0.0:9090".to_string();
    config.observability.metrics_allow_non_loopback = false;

    let err = config
        .validate()
        .expect_err("a non-loopback metrics_server_address without the ack flag must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("metrics_allow_non_loopback"),
        "error should name the ack flag operators need to set: {msg}"
    );
}

#[test]
fn validate_accepts_non_loopback_metrics_bind_with_ack() {
    let mut config = base_config();
    config.observability.metrics_server_address = "0.0.0.0:9090".to_string();
    config.observability.metrics_allow_non_loopback = true;

    config
        .validate()
        .expect("a non-loopback metrics_server_address WITH the ack flag must be accepted");
}

#[test]
fn validate_accepts_default_loopback_metrics_bind_with_no_ack_needed() {
    let config = base_config();
    // Default metrics_server_address (127.0.0.1:9090) must Just Work with no
    // ack required (safe-by-default constraint).
    assert!(!config.observability.metrics_allow_non_loopback);
    config.validate().expect("the default loopback metrics bind must validate with no ack");
}

#[test]
fn validate_warns_when_debug_endpoints_enabled_but_bind_is_non_loopback() {
    let mut config = base_config();
    config.observability.metrics_server_address = "0.0.0.0:9090".to_string();
    config.observability.metrics_allow_non_loopback = true;
    config.observability.enable_debug_endpoints = true;

    let warnings = config
        .validate()
        .expect("non-loopback bind with ack + debug endpoints enabled is not fatal");
    assert!(
        warnings.iter().any(|w| w.contains("enable_debug_endpoints")),
        "expected a warning that /debug/* won't actually be mounted: {warnings:?}"
    );
}

// --- Metric completeness: P3 timeout/shed counters, P5 overhead, breaker
// placeholder (WP-10 Task 8, P4 §4.5) ---
//
// These close the coverage gaps found by the observability scout: no
// timeout-count metric existed anywhere, the already-computed proxy-overhead
// percentiles were never exported, and the circuit-breaker family vanished
// entirely from the scrape when no backend was registered.

#[tokio::test]
async fn metrics_scrape_includes_query_timeouts_counter_present_and_incrementing() {
    let (addr, metrics) = spawn_metrics_server_with_handle("127.0.0.1", false).await;

    let (status, body) = get_body(addr, "/metrics").await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert!(
        body.contains("# TYPE scry_query_timeouts_total counter"),
        "scry_query_timeouts_total must be registered even before any timeout fires: {body}"
    );
    assert!(
        body.contains("scry_query_timeouts_total 0"),
        "expected a zero sample before any timeout: {body}"
    );

    // Drive the exact production call made at the query_timeout enforcement
    // site in proxy/connection.rs when a query exceeds its deadline.
    metrics.query_metrics().record_query_timeout();
    metrics.query_metrics().record_query_timeout();

    let (_, body) = get_body(addr, "/metrics").await;
    assert!(
        body.contains("scry_query_timeouts_total 2"),
        "expected the counter to reflect the two driven timeouts: {body}"
    );
}

#[tokio::test]
async fn metrics_scrape_includes_connection_timeouts_counter_present_and_incrementing() {
    let (addr, metrics) = spawn_metrics_server_with_handle("127.0.0.1", false).await;

    let (status, body) = get_body(addr, "/metrics").await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert!(
        body.contains("# TYPE scry_connection_timeouts_total counter"),
        "scry_connection_timeouts_total must be registered: {body}"
    );
    assert!(
        body.contains("scry_connection_timeouts_total{kind=\"pool_wait\"} 0"),
        "expected a zero pool_wait sample before any timeout: {body}"
    );
    assert!(
        body.contains("scry_connection_timeouts_total{kind=\"backend_connect\"} 0"),
        "expected a zero backend_connect sample before any timeout: {body}"
    );

    // Drive the exact production calls made at the two P3 §4.5 enforcement
    // sites in proxy/connection.rs's handle_acquire_error: the typed
    // AcquireError::WaitTimeout (pool_timeout_secs/wait_timeout_ms) and the
    // message-classified backend TCP connect timeout (connection_timeout_ms).
    metrics.pool_metrics().record_pool_wait_timeout();
    metrics.pool_metrics().record_backend_connect_timeout();
    metrics.pool_metrics().record_backend_connect_timeout();

    let (_, body) = get_body(addr, "/metrics").await;
    assert!(
        body.contains("scry_connection_timeouts_total{kind=\"pool_wait\"} 1"),
        "expected the pool_wait counter to reflect the driven timeout: {body}"
    );
    assert!(
        body.contains("scry_connection_timeouts_total{kind=\"backend_connect\"} 2"),
        "expected the backend_connect counter to reflect the two driven timeouts: {body}"
    );
}

#[tokio::test]
async fn metrics_scrape_includes_requests_shed_counter_present_and_incrementing() {
    let (addr, metrics) = spawn_metrics_server_with_handle("127.0.0.1", false).await;

    let (status, body) = get_body(addr, "/metrics").await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert!(
        body.contains("# TYPE scry_requests_shed_total counter"),
        "scry_requests_shed_total must be registered: {body}"
    );
    assert!(
        body.contains("scry_requests_shed_total 0"),
        "expected a zero sample before any shed: {body}"
    );

    // Drive the exact production call made when a per-backend circuit
    // breaker rejects a request (proxy/connection.rs handle_acquire_error,
    // classifying the "Circuit breaker" pool error distinctly from a
    // generic pool error or a queue-full rejection).
    metrics.pool_metrics().record_request_shed();

    let (_, body) = get_body(addr, "/metrics").await;
    assert!(
        body.contains("scry_requests_shed_total 1"),
        "expected the shed counter to reflect the driven event: {body}"
    );
}

#[tokio::test]
async fn metrics_scrape_includes_proxy_overhead_quantiles() {
    let (addr, _metrics) = spawn_metrics_server_with_handle("127.0.0.1", false).await;

    let (status, body) = get_body(addr, "/metrics").await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert!(
        body.contains("# TYPE scry_proxy_overhead_seconds summary"),
        "the already-computed proxy_overhead_percentiles (P5 self-overhead) must be exported: {body}"
    );
    assert!(body.contains("scry_proxy_overhead_seconds{quantile=\"0.5\"}"));
    assert!(body.contains("scry_proxy_overhead_seconds{quantile=\"0.99\"}"));
    assert!(body.contains("scry_proxy_overhead_seconds{quantile=\"0.999\"}"));
}

#[tokio::test]
async fn metrics_scrape_circuit_breaker_family_present_with_no_backend_registered() {
    // No backend/database pool was ever created for this ProxyMetrics
    // instance, so `circuit_breaker_metrics_all()` is empty — exactly the
    // "healthy, no breakers" vs. "feature not wired" ambiguity from the
    // gap report. The family must still show up in the scrape.
    let (addr, _metrics) = spawn_metrics_server_with_handle("127.0.0.1", false).await;

    let (status, body) = get_body(addr, "/metrics").await;
    assert_eq!(status, reqwest::StatusCode::OK);

    assert!(
        body.contains("# TYPE scry_circuit_breaker_state gauge"),
        "circuit breaker family must be present even with zero backends registered: {body}"
    );
    assert!(
        body.contains("scry_circuit_breaker_state{backend=\"none\"} 0"),
        "expected the documented backend=\"none\" sentinel series: {body}"
    );
    assert!(body.contains("scry_circuit_breaker_consecutive_failures{backend=\"none\"} 0"));
    assert!(body.contains("scry_circuit_breaker_consecutive_successes{backend=\"none\"} 0"));
    assert!(body.contains("scry_circuit_breaker_requests_allowed_total{backend=\"none\"} 0"));
    assert!(body.contains("scry_circuit_breaker_requests_rejected_total{backend=\"none\"} 0"));
}

#[tokio::test]
async fn metrics_scrape_circuit_breaker_family_present_with_real_backend_registered() {
    // With a real breaker registered, the sentinel must NOT appear and the
    // real backend's series must be present instead (regression guard so the
    // always-present fix doesn't mask real per-backend data).
    let (addr, metrics) = spawn_metrics_server_with_handle("127.0.0.1", false).await;

    let breaker_config = scry::config::CircuitBreakerConfig {
        enabled: true,
        failure_threshold: 5,
        success_threshold: 2,
        window_secs: 30,
        open_timeout_secs: 60,
        use_health_monitor: false,
    };
    let cb = std::sync::Arc::new(scry::resilience::CircuitBreaker::new(breaker_config, None));
    metrics.register_circuit_breaker("primary".to_string(), cb);

    let (status, body) = get_body(addr, "/metrics").await;
    assert_eq!(status, reqwest::StatusCode::OK);

    assert!(
        body.contains("scry_circuit_breaker_state{backend=\"primary\"}"),
        "expected the real backend's series: {body}"
    );
    // Note: the HELP text itself documents the `backend="none"` sentinel
    // convention (without curly braces), so it always mentions the string
    // `backend="none"`; check for an actual *sample line* using the sentinel
    // (curly-brace label form) instead, which must disappear once a real
    // breaker is registered.
    assert!(
        !body.contains("{backend=\"none\"}"),
        "the none sentinel sample must not appear once a real breaker is registered: {body}"
    );
}
