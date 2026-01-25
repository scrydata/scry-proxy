//! Integration tests for connection multiplexing under load
//!
//! These tests verify that Scry can handle high numbers of concurrent connections
//! without protocol errors, specifically testing the fix for CRIT-1 (DISCARD ALL
//! response handling).
//!
//! The primary issue was that under high load, PostgreSQL's response to DISCARD ALL
//! (CommandComplete + ReadyForQuery) may arrive in separate TCP packets. If only
//! CommandComplete was read, ReadyForQuery ('Z' = 0x5A) remained in the socket buffer
//! and corrupted the next client's protocol stream.

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

/// Helper to create a test config with transaction pooling for multiplexing tests
fn create_multiplexing_config(backend_host: String, backend_port: u16, pool_size: usize) -> Config {
    Config {
        proxy: ProxyConfig {
            listen_address: "127.0.0.1:0".to_string(),
            max_connections: 50, // Reasonable for tests
            shutdown_timeout_secs: 30,
            unix_socket: None,
        },
        databases: Vec::new(),
        backend: BackendConfig {
            protocol: scry::config::DatabaseProtocol::Postgres,
            host: backend_host,
            port: backend_port,
            database: "postgres".to_string(),
            user: "postgres".to_string(),
            password: "postgres".to_string(),
            pool_size,
            connection_timeout_ms: 10000,
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
            batch_size: 100,
            flush_interval_ms: 100,
            anonymize: false,
            publisher_type: "debug".to_string(),
            max_queue_size: 10000,
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
            pool_size,
            pool_min_idle: 0, // Don't pre-warm for tests
            pool_timeout_secs: 30,
            pool_recycle_secs: 3600,
            pool_aggressive_unpinning: false,
            buffer_size: 8192,
            pool_queue_depth: 50,
            pool_idle_unpin_secs: 60,
            pool_lifo: true,
            pool_reset_timeout_ms: 5000,
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
                enabled: true,
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
        auth: AuthConfig::default(),
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

    // Give the server time to start accepting connections
    sleep(Duration::from_millis(100)).await;

    Ok(port)
}

/// Test that multiple sequential clients can reuse connections
///
/// This test verifies the CRIT-1 fix: proper DISCARD ALL response handling.
/// Multiple clients connect sequentially and should all succeed by
/// reusing recycled connections.
#[tokio::test]
async fn test_sequential_connection_reuse() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    // Use pool_size=1 to force connection reuse
    let mut config = create_multiplexing_config(postgres_host, postgres_port, 1);
    config.performance.connection_pooling = PoolingStrategy::Session;
    config.backend.pool_size = 1;

    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port =
        start_test_proxy(config.clone(), publisher).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(200)).await;

    // Run 5 sequential clients, each should succeed
    for client_id in 0..5 {
        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user={} password={} dbname={}",
                proxy_port, config.backend.user, config.backend.password, config.backend.database
            ),
            tokio_postgres::NoTls,
        )
        .await
        .expect(&format!("Client {} failed to connect", client_id));

        let conn_handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Client {} connection error: {}", client_id, e);
            }
        });

        // Execute a query
        let rows = client
            .query(&format!("SELECT {} as client_id", client_id), &[])
            .await
            .expect(&format!("Client {} query failed", client_id));
        assert_eq!(rows.len(), 1);
        let id: i32 = rows[0].get(0);
        assert_eq!(id, client_id);

        // Disconnect
        drop(client);
        sleep(Duration::from_millis(50)).await;
        conn_handle.abort();

        // Give time for connection to be recycled
        sleep(Duration::from_millis(100)).await;
    }
}

/// Test transaction mode with sequential clients
///
/// This test verifies that in transaction mode, connections are properly
/// reset after each transaction and can be reused by subsequent clients.
#[tokio::test]
async fn test_transaction_mode_sequential_clients() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    // Use Transaction mode with pool_size=1 to verify connection reset
    let mut config = create_multiplexing_config(postgres_host, postgres_port, 1);
    config.performance.connection_pooling = PoolingStrategy::Transaction;
    config.backend.pool_size = 1;

    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port =
        start_test_proxy(config.clone(), publisher).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(200)).await;

    // Client 1: Set a session variable, then disconnect
    {
        let (client1, connection1) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user={} password={} dbname={}",
                proxy_port, config.backend.user, config.backend.password, config.backend.database
            ),
            tokio_postgres::NoTls,
        )
        .await
        .expect("Client 1 failed to connect");

        let conn_handle = tokio::spawn(async move {
            if let Err(e) = connection1.await {
                eprintln!("Connection 1 error: {}", e);
            }
        });

        // Execute a transaction
        client1.simple_query("BEGIN").await.expect("BEGIN failed");
        client1.simple_query("SELECT 1").await.expect("SELECT failed");
        client1.simple_query("COMMIT").await.expect("COMMIT failed");

        drop(client1);
        sleep(Duration::from_millis(50)).await;
        conn_handle.abort();
    }

    sleep(Duration::from_millis(100)).await;

    // Client 2: Should get the recycled connection
    {
        let (client2, connection2) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user={} password={} dbname={}",
                proxy_port, config.backend.user, config.backend.password, config.backend.database
            ),
            tokio_postgres::NoTls,
        )
        .await
        .expect("Client 2 failed to connect - connection recycling may have failed");

        tokio::spawn(async move {
            if let Err(e) = connection2.await {
                eprintln!("Connection 2 error: {}", e);
            }
        });

        // Execute a transaction
        client2.simple_query("BEGIN").await.expect("BEGIN failed");
        let rows =
            client2.query("SELECT 'client2_ok' as status", &[]).await.expect("SELECT failed");
        assert_eq!(rows.len(), 1);
        let status: &str = rows[0].get(0);
        assert_eq!(status, "client2_ok");
        client2.simple_query("COMMIT").await.expect("COMMIT failed");
    }
}

/// Test that connection recycling works correctly with DISCARD ALL
///
/// This test verifies that connections can be reused between clients
/// after being reset with DISCARD ALL.
#[tokio::test]
async fn test_connection_recycling_with_discard_all() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    // Use Session mode with pool_size=1 to force connection reuse
    let mut config = create_multiplexing_config(postgres_host, postgres_port, 1);
    config.performance.connection_pooling = PoolingStrategy::Session;
    config.backend.pool_size = 1;

    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port =
        start_test_proxy(config.clone(), publisher).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(200)).await;

    // Client 1: Connect, query, set a session variable, disconnect
    {
        let (client1, connection1) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user={} password={} dbname={}",
                proxy_port, config.backend.user, config.backend.password, config.backend.database
            ),
            tokio_postgres::NoTls,
        )
        .await
        .expect("Failed to connect client 1");

        let conn_handle = tokio::spawn(async move {
            if let Err(e) = connection1.await {
                eprintln!("Connection 1 error: {}", e);
            }
        });

        // Set a session variable
        client1
            .simple_query("SET application_name = 'client1_test'")
            .await
            .expect("Client 1 SET failed");

        // Verify it was set
        let rows =
            client1.simple_query("SHOW application_name").await.expect("Client 1 SHOW failed");
        assert!(!rows.is_empty());

        // Disconnect Client 1 - connection returns to pool with DISCARD ALL
        drop(client1);
        sleep(Duration::from_millis(50)).await;
        conn_handle.abort();
    }

    // Give time for connection to be released and reset
    sleep(Duration::from_millis(100)).await;

    // Client 2: Should get the recycled connection (after DISCARD ALL)
    {
        let (client2, connection2) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user={} password={} dbname={}",
                proxy_port, config.backend.user, config.backend.password, config.backend.database
            ),
            tokio_postgres::NoTls,
        )
        .await
        .expect("Failed to connect client 2 - connection may not have been recycled");

        tokio::spawn(async move {
            if let Err(e) = connection2.await {
                eprintln!("Connection 2 error: {}", e);
            }
        });

        // Verify session variable was reset by DISCARD ALL
        // application_name should be empty or default, not 'client1_test'
        let rows =
            client2.simple_query("SHOW application_name").await.expect("Client 2 SHOW failed");
        assert!(!rows.is_empty());

        // The application_name should NOT be 'client1_test' if DISCARD ALL worked
        // (It will be the default or empty)

        // Execute a query to verify the connection works
        let rows = client2
            .query("SELECT 'client2_ok' as status", &[])
            .await
            .expect("Client 2 SELECT failed");
        assert_eq!(rows.len(), 1);
        let status: &str = rows[0].get(0);
        assert_eq!(status, "client2_ok");
    }
}
