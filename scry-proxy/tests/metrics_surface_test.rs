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
