//! Circuit-breaker scope test (WP-8 Task 8.1, P3 §4.1/§5.4).
//!
//! Breakers are per-backend: a failing backend must open only its own breaker,
//! leaving healthy backends' breakers closed and serving traffic. This is the
//! regression guard against the previous single-shared-breaker design where one
//! bad backend tripped all traffic.

use scry::config::CircuitBreakerConfig;
use scry::observability::{HealthConfig, ProxyMetrics};
use scry::resilience::CircuitBreaker;
use std::sync::Arc;

fn breaker_config(failure_threshold: u32) -> CircuitBreakerConfig {
    CircuitBreakerConfig {
        enabled: true,
        failure_threshold,
        success_threshold: 2,
        window_secs: 30,
        open_timeout_secs: 60,
        use_health_monitor: false,
    }
}

#[test]
fn one_backend_failing_does_not_open_another_backends_breaker() {
    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));

    // Two independent per-backend breakers.
    let backend_a = Arc::new(CircuitBreaker::new(breaker_config(3), None));
    let backend_b = Arc::new(CircuitBreaker::new(breaker_config(3), None));
    metrics.register_circuit_breaker("backend_a".to_string(), Arc::clone(&backend_a));
    metrics.register_circuit_breaker("backend_b".to_string(), Arc::clone(&backend_b));

    // Backend A fails repeatedly and trips its breaker open.
    for _ in 0..3 {
        backend_a.record_failure();
    }
    assert!(backend_a.allow_request().is_err(), "backend_a breaker should be OPEN");

    // Backend B is healthy — its breaker must remain closed and serving.
    assert!(backend_b.allow_request().is_ok(), "backend_b breaker must stay CLOSED (isolated)");

    // Per-backend observability reflects the divergent states.
    let all = metrics.circuit_breaker_metrics_all();
    let a = all.iter().find(|(n, _)| n == "backend_a").expect("backend_a metrics");
    let b = all.iter().find(|(n, _)| n == "backend_b").expect("backend_b metrics");
    assert_eq!(a.1.state, "open", "backend_a state should be open: {:?}", a.1);
    assert_eq!(b.1.state, "closed", "backend_b state should be closed: {:?}", b.1);
}
