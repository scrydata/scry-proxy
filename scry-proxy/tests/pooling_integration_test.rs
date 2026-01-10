/// Integration tests for connection pooling with transaction-based release
///
/// These tests verify that connections are properly released back to the pool
/// after transactions complete, enabling efficient connection reuse.
use scry::{config::*, observability::*, proxy::*, publisher::*};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use testcontainers::{clients::Cli, RunnableImage};
use testcontainers_modules::postgres::Postgres;
use tokio::time::sleep;

/// Test publisher that captures events for verification
#[derive(Debug, Clone)]
struct TestPublisher {
    events: Arc<Mutex<Vec<QueryEvent>>>,
}

impl TestPublisher {
    fn new() -> Self {
        Self { events: Arc::new(Mutex::new(Vec::new())) }
    }

    #[allow(dead_code)]
    fn events(&self) -> Vec<QueryEvent> {
        self.events.lock().unwrap().clone()
    }

    #[allow(dead_code)]
    fn event_count(&self) -> usize {
        self.events.lock().unwrap().len()
    }
}

#[async_trait::async_trait]
impl EventPublisher for TestPublisher {
    async fn publish_batch(&self, events: Vec<QueryEvent>) -> anyhow::Result<()> {
        let mut captured = self.events.lock().unwrap();
        captured.extend(events);
        Ok(())
    }

    async fn shutdown(&self) -> anyhow::Result<()> {
        Ok(())
    }
}

/// Helper to create a test config with transaction pooling
fn create_test_config(backend_host: String, backend_port: u16) -> Config {
    Config {
        proxy: ProxyConfig {
            listen_address: "127.0.0.1:0".to_string(),
            max_connections: 10,
            shutdown_timeout_secs: 30,
        },
        backend: BackendConfig {
            protocol: scry::config::DatabaseProtocol::Postgres,
            host: backend_host,
            port: backend_port,
            database: "postgres".to_string(),
            user: "postgres".to_string(),
            password: "postgres".to_string(),
            pool_size: 5,
            connection_timeout_ms: 5000,
        },
        observability: ObservabilityConfig {
            enable_tracing: false,
            otlp_endpoint: None,
            service_name: "scry-test".to_string(),
            enable_metrics_server: false,
            metrics_server_address: "127.0.0.1:9090".to_string(),
        },
        protocol: ProtocolConfig { max_prepared_statements: 1000 },
        publisher: PublisherConfig {
            enabled: true,
            batch_size: 10,
            flush_interval_ms: 100,
            anonymize: false,
            publisher_type: "debug".to_string(),
            max_queue_size: 1000,
            http_endpoint: None,
            http_timeout_ms: 500,
            http_max_retries: 2,
            http_api_key: None,
            http_compression: true,
            shadow_id: None,
        },
        performance: PerformanceConfig {
            target_latency_ms: 1,
            connection_pooling: PoolingStrategy::Transaction,
            pool_size: 5,
            pool_min_idle: 0,
            pool_timeout_secs: 30,
            pool_recycle_secs: 3600,
            pool_aggressive_unpinning: false,
            buffer_size: 8192,
            pool_queue_depth: 50,
            pool_idle_unpin_secs: 60,
            pool_lifo: true,
        },
        resilience: ResilienceConfig {
            circuit_breaker: CircuitBreakerConfig {
                enabled: false,
                failure_threshold: 5,
                success_threshold: 2,
                window_secs: 30,
                open_timeout_secs: 60,
                use_health_monitor: false,
            },
            connection_retry: ConnectionRetryConfig {
                enabled: false,
                max_attempts: 3,
                initial_backoff_ms: 50,
                max_backoff_ms: 5000,
                backoff_multiplier: 2.0,
                jitter_factor: 0.1,
            },
            healthcheck: HealthcheckConfig {
                active_enabled: false,
                interval_secs: 30,
                timeout_ms: 1000,
                failure_threshold: 3,
            },
        },
        tls: TlsConfig::default(),
    }
}

/// Start the proxy server and return its listen port
async fn start_test_proxy(
    config: Config,
    publisher: Arc<dyn EventPublisher>,
) -> anyhow::Result<u16> {
    let batcher = EventBatcher::new(
        publisher,
        config.publisher.batch_size,
        config.publisher.flush_interval_ms,
        config.publisher.max_queue_size,
    );

    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));
    let server = ProxyServer::new(config.clone(), batcher, metrics).await?;
    let port = server.local_addr()?.port();

    tokio::spawn(async move {
        let _ = server.run().await;
    });

    Ok(port)
}

/// Test that a single client can execute multiple transactions with transaction pooling
///
/// This verifies that after COMMIT, the client can start a new transaction.
/// With transaction pooling, the backend connection may be different for each transaction.
#[tokio::test]
async fn test_transaction_pooling_multiple_transactions() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let mut config = create_test_config(postgres_host, postgres_port);
    config.performance.connection_pooling = PoolingStrategy::Transaction;
    config.performance.pool_size = 2;

    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port =
        start_test_proxy(config.clone(), publisher).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(200)).await;

    // Connect client
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user={} password={} dbname={}",
            proxy_port, config.backend.user, config.backend.password, config.backend.database
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("Failed to connect to proxy");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    // First transaction
    client.execute("BEGIN", &[]).await.expect("BEGIN 1 failed");
    let rows = client.query("SELECT 1 as num", &[]).await.expect("SELECT 1 failed");
    assert_eq!(rows.len(), 1);
    let num: i32 = rows[0].get(0);
    assert_eq!(num, 1);
    client.execute("COMMIT", &[]).await.expect("COMMIT 1 failed");

    // Brief pause to allow connection release
    sleep(Duration::from_millis(50)).await;

    // Second transaction (may use different backend connection)
    client.execute("BEGIN", &[]).await.expect("BEGIN 2 failed");
    let rows = client.query("SELECT 2 as num", &[]).await.expect("SELECT 2 failed");
    assert_eq!(rows.len(), 1);
    let num: i32 = rows[0].get(0);
    assert_eq!(num, 2);
    client.execute("COMMIT", &[]).await.expect("COMMIT 2 failed");

    // Third transaction with ROLLBACK
    client.execute("BEGIN", &[]).await.expect("BEGIN 3 failed");
    let rows = client.query("SELECT 3 as num", &[]).await.expect("SELECT 3 failed");
    assert_eq!(rows.len(), 1);
    let num: i32 = rows[0].get(0);
    assert_eq!(num, 3);
    client.execute("ROLLBACK", &[]).await.expect("ROLLBACK 3 failed");

    // Verify we can still query after all transactions
    let rows = client.query("SELECT 'done' as status", &[]).await.expect("Final SELECT failed");
    assert_eq!(rows.len(), 1);
    let status: &str = rows[0].get(0);
    assert_eq!(status, "done");
}

/// Test that session mode works (connections NOT released between transactions)
#[tokio::test]
async fn test_session_mode_multiple_transactions() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let mut config = create_test_config(postgres_host, postgres_port);
    config.performance.connection_pooling = PoolingStrategy::Session;
    config.performance.pool_size = 2;

    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port =
        start_test_proxy(config.clone(), publisher).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(200)).await;

    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user={} password={} dbname={}",
            proxy_port, config.backend.user, config.backend.password, config.backend.database
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("Failed to connect to proxy");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    // First transaction
    client.execute("BEGIN", &[]).await.expect("BEGIN 1 failed");
    client.execute("SELECT 1", &[]).await.expect("SELECT 1 failed");
    client.execute("COMMIT", &[]).await.expect("COMMIT 1 failed");

    // Second transaction
    client.execute("BEGIN", &[]).await.expect("BEGIN 2 failed");
    client.execute("SELECT 2", &[]).await.expect("SELECT 2 failed");
    client.execute("COMMIT", &[]).await.expect("COMMIT 2 failed");

    // Simple query to verify all is well
    let rows = client.query("SELECT 'session_ok' as status", &[]).await.expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    let status: &str = rows[0].get(0);
    assert_eq!(status, "session_ok");
}

/// Test that hybrid mode works (connections released when not pinned)
#[tokio::test]
async fn test_hybrid_mode_multiple_transactions() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let mut config = create_test_config(postgres_host, postgres_port);
    config.performance.connection_pooling = PoolingStrategy::Hybrid;
    config.performance.pool_size = 2;

    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port =
        start_test_proxy(config.clone(), publisher).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(200)).await;

    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user={} password={} dbname={}",
            proxy_port, config.backend.user, config.backend.password, config.backend.database
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("Failed to connect to proxy");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    // Transactions without pinning state - should release
    client.execute("BEGIN", &[]).await.expect("BEGIN 1 failed");
    client.execute("SELECT 1", &[]).await.expect("SELECT 1 failed");
    client.execute("COMMIT", &[]).await.expect("COMMIT 1 failed");

    sleep(Duration::from_millis(50)).await;

    client.execute("BEGIN", &[]).await.expect("BEGIN 2 failed");
    client.execute("SELECT 2", &[]).await.expect("SELECT 2 failed");
    client.execute("COMMIT", &[]).await.expect("COMMIT 2 failed");

    let rows = client.query("SELECT 'hybrid_ok' as status", &[]).await.expect("SELECT failed");
    assert_eq!(rows.len(), 1);
    let status: &str = rows[0].get(0);
    assert_eq!(status, "hybrid_ok");
}
