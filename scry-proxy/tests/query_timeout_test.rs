//! Query-timeout fault-injection test (WP-8, P3 §4.3).
//!
//! A query that runs longer than `query_timeout_secs` must be cancelled by the
//! proxy — the client receives an error promptly instead of blocking for the
//! full backend duration, and the connection (now in an unknown state) is not
//! reused.

use scry::config::{Config, PoolingStrategy};
use scry::observability::{HealthConfig, ProxyMetrics};
use scry::proxy::{EventBatcher, ProxyServer};
use scry::publisher::{EventPublisher, QueryEvent};
use std::sync::Arc;
use std::time::{Duration, Instant};
use testcontainers::{clients::Cli, RunnableImage};
use testcontainers_modules::postgres::Postgres;
use tokio::time::sleep;

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
    // Use a pooled strategy so the managed-connection path (which enforces the
    // query deadline) is exercised.
    c.performance.connection_pooling = PoolingStrategy::Session;
    c.performance.query_timeout_secs = 1;
    c.resilience.healthcheck.active_enabled = false;
    c
}

#[tokio::test]
async fn slow_query_is_cancelled_at_query_timeout() {
    let docker = Cli::default();
    let postgres = docker.run(RunnableImage::from(Postgres::default()).with_tag("16-alpine"));
    let pg_port = postgres.get_host_port_ipv4(5432);
    sleep(Duration::from_secs(2)).await;

    let config = build_config(pg_port);
    let batcher = EventBatcher::new(Arc::new(NoOpPublisher), 10, 100, 1000);
    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));
    let server = ProxyServer::new(config.clone(), batcher, metrics).await.expect("server");
    let proxy_port = server.local_addr().unwrap().port();
    tokio::spawn(async move {
        let _ = server.run().await;
    });
    sleep(Duration::from_millis(200)).await;

    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={proxy_port} user=postgres password=postgres dbname=postgres"
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    // A 10s query against a 1s query_timeout must be cancelled well before 10s.
    let start = Instant::now();
    let result = client.simple_query("SELECT pg_sleep(10)").await;
    let elapsed = start.elapsed();

    assert!(result.is_err(), "slow query should be cancelled, got: {result:?}");
    assert!(
        elapsed < Duration::from_secs(5),
        "cancellation should happen near the 1s timeout, took {elapsed:?}"
    );
    println!("slow query cancelled after {elapsed:?} (budget 1s)");
}
