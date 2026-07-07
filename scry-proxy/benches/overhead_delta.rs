//! Non-blocking proxy-overhead perf signal (WP-5 Task 5.4, P5 §4.3).
//!
//! Runs a representative point-select workload directly against Postgres and
//! then through the proxy, computes the per-percentile *added* latency
//! (proxy − direct), and prints it against the configured [`LatencyBudget`].
//!
//! This is a `harness = false` benchmark (a plain `main`) so it can be run as a
//! single, self-contained CI job. It is a **baseline signal only**: it always
//! exits 0, never failing the build. The gate flips to blocking on a stable
//! runner in WP-13.

use scry::config::{Config, LatencyBudget, PoolingStrategy};
use scry::observability::{HealthConfig, ProxyMetrics};
use scry::proxy::{EventBatcher, ProxyServer};
use scry::publisher::{EventPublisher, QueryEvent};
use std::sync::Arc;
use std::time::{Duration, Instant};
use testcontainers::{clients::Cli, RunnableImage};
use testcontainers_modules::postgres::Postgres;
use tokio::runtime::Runtime;

/// Publisher that drops everything — we want to measure proxy transport
/// overhead, not publishing.
#[derive(Debug)]
struct NoOpPublisher;

#[async_trait::async_trait]
impl EventPublisher for NoOpPublisher {
    async fn publish_batch(&self, _events: Vec<QueryEvent>) -> anyhow::Result<()> {
        Ok(())
    }
    async fn shutdown(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

fn build_config(backend_port: u16) -> Config {
    let mut c = Config::default();
    c.proxy.listen_address = "127.0.0.1:0".to_string();
    c.backend.host = "127.0.0.1".to_string();
    c.backend.port = backend_port;
    c.backend.user = "postgres".to_string();
    c.backend.password = "postgres".to_string();
    c.backend.database = "postgres".to_string();
    c.publisher.enabled = false;
    c.publisher.anonymize = false;
    c.performance.connection_pooling = PoolingStrategy::Disabled;
    c.resilience.healthcheck.active_enabled = false;
    c
}

async fn start_proxy(config: Config) -> u16 {
    let batcher = EventBatcher::new(
        Arc::new(NoOpPublisher),
        config.publisher.batch_size,
        config.publisher.flush_interval_ms,
        config.publisher.max_queue_size,
    );
    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));
    let server = ProxyServer::new(config.clone(), batcher, metrics).await.expect("proxy start");
    let port = server.local_addr().expect("local addr").port();
    tokio::spawn(async move {
        let _ = server.run().await;
    });
    port
}

/// Run `warmup` + `samples` point-select queries against `port`, returning the
/// per-query latencies in microseconds.
async fn measure(port: u16, warmup: usize, samples: usize) -> Vec<u64> {
    const QUERY: &str = "SELECT 1";
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=postgres password=postgres dbname=postgres"),
        tokio_postgres::NoTls,
    )
    .await
    .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    for _ in 0..warmup {
        let _ = client.execute(QUERY, &[]).await;
    }
    let mut latencies = Vec::with_capacity(samples);
    for _ in 0..samples {
        let t = Instant::now();
        client.execute(QUERY, &[]).await.expect("query");
        latencies.push(t.elapsed().as_micros() as u64);
    }
    latencies
}

fn percentile(sorted: &[u64], q: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 * q) as usize).min(sorted.len() - 1);
    sorted[idx]
}

fn report_line(name: &str, direct: u64, proxy: u64, budget: u64) -> (String, bool) {
    let delta = proxy.saturating_sub(direct);
    let ok = delta <= budget;
    let verdict = if ok { "OK" } else { "OVER (non-blocking)" };
    (
        format!(
            "{name}: direct={direct}us proxy={proxy}us added={delta}us budget={budget}us -> {verdict}"
        ),
        ok,
    )
}

fn main() {
    let rt = Runtime::new().unwrap();

    let docker = Cli::default();
    let postgres = docker.run(RunnableImage::from(Postgres::default()).with_tag("16-alpine"));
    let postgres_port = postgres.get_host_port_ipv4(5432);
    std::thread::sleep(Duration::from_secs(3));

    let config = build_config(postgres_port);
    let proxy_port = rt.block_on(start_proxy(config));
    std::thread::sleep(Duration::from_millis(300));

    // Tail-adequate sample size for a point-select reference workload.
    const WARMUP: usize = 200;
    const SAMPLES: usize = 2000;

    let mut direct = rt.block_on(measure(postgres_port, WARMUP, SAMPLES));
    let mut proxied = rt.block_on(measure(proxy_port, WARMUP, SAMPLES));
    direct.sort_unstable();
    proxied.sort_unstable();

    let budget = LatencyBudget::default();
    println!("=== proxy added-latency signal (workload: {}) ===", budget.reference_workload);
    println!("samples: {SAMPLES} (per side), warmup: {WARMUP}");

    let (l50, _) = report_line(
        "p50",
        percentile(&direct, 0.50),
        percentile(&proxied, 0.50),
        budget.overhead_p50_micros,
    );
    let (l95, _) = report_line(
        "p95",
        percentile(&direct, 0.95),
        percentile(&proxied, 0.95),
        budget.overhead_p95_micros,
    );
    let (l99, p99_ok) = report_line(
        "p99",
        percentile(&direct, 0.99),
        percentile(&proxied, 0.99),
        budget.overhead_p99_micros,
    );
    println!("{l50}");
    println!("{l95}");
    println!("{l99}");

    if !p99_ok {
        // Surface it, but do NOT fail: this is a non-blocking baseline signal.
        println!(
            "NOTE: p99 added latency exceeds the budget. This signal is non-blocking; \
             the gate becomes blocking on a stable runner in WP-13."
        );
    }
    // Always succeed.
}
