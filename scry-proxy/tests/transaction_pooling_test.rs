//! Integration tests for transaction pooling modes
//!
//! These tests verify transaction and hybrid pooling behavior:
//! - Transaction mode restrictions (SET, temp tables, etc.)
//! - Connection release after transaction completion
//! - Hybrid mode permissiveness
//!
//! NOTE: Some tests may fail until the connection handler integration (Phase 8)
//! is complete. This file provides the test scaffolding and structure.

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

    fn events(&self) -> Vec<QueryEvent> {
        self.events.lock().unwrap().clone()
    }

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

/// Helper to create a test config with specified pooling strategy
fn create_test_config(host: String, port: u16, pooling: PoolingStrategy) -> Config {
    Config {
        proxy: ProxyConfig {
            listen_address: "127.0.0.1:0".to_string(), // Random port
            max_connections: 100,
            shutdown_timeout_secs: 5,
            unix_socket: None,
        },
        databases: Vec::new(),
        backend: BackendConfig {
            protocol: DatabaseProtocol::Postgres,
            host,
            port,
            database: "postgres".to_string(),
            user: "postgres".to_string(),
            password: "postgres".to_string(),
            pool_size: 5,
            connection_timeout_ms: 5000,
        },
        observability: ObservabilityConfig {
            enable_tracing: false,
            otlp_endpoint: None,
            service_name: "scry-transaction-test".to_string(),
            metrics_server_address: "127.0.0.1:0".to_string(),
            enable_metrics_server: false,
            unsafe_debug_logging: false,
        },
        protocol: ProtocolConfig { max_prepared_statements: 100 },
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
            http_compression: false,
            shadow_id: None,
            allow_insecure: false,
            anonymize_salt: None,
            parse_failure_mode: ParseFailureMode::Redact,
        },
        performance: PerformanceConfig {
            latency_budget: scry::config::LatencyBudget::default(),
            query_timeout_secs: 0,
            connection_pooling: pooling,
            pool_size: 5,
            pool_min_idle: 1,
            pool_timeout_secs: 30,
            pool_recycle_secs: 3600,
            pool_aggressive_unpinning: false,
            buffer_size: 8192,
            pool_queue_depth: 50,
            pool_idle_unpin_secs: 60,
            pool_lifo: true,
            pool_reset_timeout_ms: 5000,
            pool_ratio_warning_threshold: 20,
            pool_backpressure_mode: scry::config::BackpressureMode::RejectImmediate,
            pool_retry_hint_ms: 200,
            pool_queue_saturation_warn_threshold: 0.8,
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
        auth: AuthConfig::default(),
        admin: AdminConfig::default(),
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

    // Spawn server in background
    tokio::spawn(async move {
        let _ = server.run().await;
    });

    Ok(port)
}

/// Test that hybrid mode allows SET commands outside transactions
///
/// In hybrid mode, SET commands should be allowed and the connection
/// should be pinned to the client (sticky) to maintain session state.
#[tokio::test]
async fn test_hybrid_mode_allows_set() {
    // Setup: Start Postgres container
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port, PoolingStrategy::Hybrid);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port =
        start_test_proxy(config.clone(), publisher).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(100)).await;

    // Connect through proxy
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

    // SET should work in hybrid mode
    let result = client.execute("SET application_name = 'hybrid_test'", &[]).await;
    assert!(result.is_ok(), "Hybrid mode should allow SET commands");

    // Verify the setting was applied
    let rows =
        client.query("SHOW application_name", &[]).await.expect("Failed to show application_name");
    let app_name: String = rows[0].get(0);
    assert_eq!(app_name, "hybrid_test", "SET should have been applied");

    sleep(Duration::from_millis(200)).await;

    // Verify events were captured
    let events = test_publisher.events();
    assert!(!events.is_empty(), "Expected query events");
}

/// Test that hybrid mode allows temporary tables
///
/// In hybrid mode, temp tables should be allowed but the connection
/// should be pinned (unsafe state) to maintain the temp table.
#[tokio::test]
async fn test_hybrid_mode_allows_temp_tables() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port, PoolingStrategy::Hybrid);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port =
        start_test_proxy(config.clone(), publisher).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(100)).await;

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

    // CREATE TEMP TABLE should work in hybrid mode
    let result = client.execute("CREATE TEMP TABLE hybrid_temp (id INT)", &[]).await;
    assert!(result.is_ok(), "Hybrid mode should allow temp tables");

    // Verify the temp table exists
    let rows = client
        .query("SELECT COUNT(*) FROM hybrid_temp", &[])
        .await
        .expect("Failed to query temp table");
    let count: i64 = rows[0].get(0);
    assert_eq!(count, 0, "Temp table should exist and be empty");
}

/// Test that session mode allows SET commands
///
/// Session mode should allow everything since the connection
/// is dedicated to the client for the entire session.
#[tokio::test]
async fn test_session_mode_allows_set() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port, PoolingStrategy::Session);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port =
        start_test_proxy(config.clone(), publisher).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(100)).await;

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

    // SET should work in session mode
    let result = client.execute("SET search_path TO public", &[]).await;
    assert!(result.is_ok(), "Session mode should allow SET commands");
}

/// Test that transactions work correctly in transaction pooling mode
///
/// Queries within a BEGIN/COMMIT block should all use the same
/// backend connection and work correctly.
#[tokio::test]
async fn test_transaction_block_execution() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port, PoolingStrategy::Transaction);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port =
        start_test_proxy(config.clone(), publisher).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(100)).await;

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

    // Execute a transaction block
    client.execute("BEGIN", &[]).await.expect("BEGIN failed");
    client.execute("SELECT 1", &[]).await.expect("SELECT in txn failed");
    client.execute("COMMIT", &[]).await.expect("COMMIT failed");

    // Transaction should complete successfully
    println!("Transaction block executed successfully");
}

/// Test that SET commands work inside transactions in transaction mode
///
/// SET commands are scoped to the transaction when used inside a
/// BEGIN/COMMIT block, so they should be allowed even in transaction mode.
#[tokio::test]
async fn test_transaction_mode_allows_set_inside_transaction() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port, PoolingStrategy::Transaction);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port =
        start_test_proxy(config.clone(), publisher).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(100)).await;

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

    // SET inside transaction should work in transaction mode
    // because it's scoped to the transaction
    client.execute("BEGIN", &[]).await.expect("BEGIN failed");

    let result = client.execute("SET LOCAL search_path TO pg_catalog", &[]).await;
    assert!(result.is_ok(), "SET LOCAL inside transaction should work in transaction mode");

    client.execute("COMMIT", &[]).await.expect("COMMIT failed");
}

/// Test connection release after transaction completes
///
/// In transaction pooling mode, the connection should be released
/// back to the pool after COMMIT, allowing another client to use it.
/// This test uses a pool size of 1 to verify release behavior.
#[tokio::test]
async fn test_connection_released_after_transaction() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let mut config = create_test_config(postgres_host, postgres_port, PoolingStrategy::Transaction);
    config.performance.pool_size = 1; // Only 1 backend connection

    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port =
        start_test_proxy(config.clone(), publisher).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(100)).await;

    // Client 1: Execute a transaction
    {
        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user={} password={} dbname={}",
                proxy_port, config.backend.user, config.backend.password, config.backend.database
            ),
            tokio_postgres::NoTls,
        )
        .await
        .expect("Failed to connect client 1");

        tokio::spawn(async move {
            let _ = connection.await;
        });

        client.execute("BEGIN", &[]).await.expect("BEGIN failed");
        client.execute("SELECT 1", &[]).await.expect("SELECT failed");
        client.execute("COMMIT", &[]).await.expect("COMMIT failed");

        drop(client);
    }

    // Wait for connection to be released
    sleep(Duration::from_millis(200)).await;

    // Client 2: Should be able to get a connection
    {
        let connect_result = tokio::time::timeout(
            Duration::from_secs(5),
            tokio_postgres::connect(
                &format!(
                    "host=127.0.0.1 port={} user={} password={} dbname={}",
                    proxy_port,
                    config.backend.user,
                    config.backend.password,
                    config.backend.database
                ),
                tokio_postgres::NoTls,
            ),
        )
        .await;

        assert!(
            connect_result.is_ok(),
            "Client 2 should be able to connect (connection was released)"
        );

        let (client, connection) = connect_result.unwrap().expect("Failed to connect client 2");

        tokio::spawn(async move {
            let _ = connection.await;
        });

        let result = client.execute("SELECT 2", &[]).await;
        assert!(result.is_ok(), "Client 2 should be able to execute queries");
    }

    println!("Connection release after transaction verified");
}

/// Test prepared statements work in transaction mode
///
/// Prepared statements should be handled transparently with
/// re-preparation on new connections if needed.
#[tokio::test]
async fn test_prepared_statements_in_transaction_mode() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port, PoolingStrategy::Transaction);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port =
        start_test_proxy(config.clone(), publisher).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(100)).await;

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

    // Prepare and execute a statement
    let stmt = client
        .prepare("SELECT $1::int + $2::int as sum")
        .await
        .expect("Failed to prepare statement");

    let rows =
        client.query(&stmt, &[&5i32, &7i32]).await.expect("Failed to execute prepared statement");

    assert_eq!(rows.len(), 1);
    let sum: i32 = rows[0].get(0);
    assert_eq!(sum, 12);

    println!("Prepared statement in transaction mode worked");
}

/// Test multiple sequential transactions from same client
///
/// Each transaction should complete and release the connection,
/// allowing the same client to start new transactions.
#[tokio::test]
async fn test_multiple_sequential_transactions() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port, PoolingStrategy::Transaction);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port =
        start_test_proxy(config.clone(), publisher).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(100)).await;

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

    // Execute multiple transactions
    for i in 1..=3 {
        client.execute("BEGIN", &[]).await.expect("BEGIN failed");
        client.execute(&format!("SELECT {}", i), &[]).await.expect("SELECT failed");
        client.execute("COMMIT", &[]).await.expect("COMMIT failed");
    }

    println!("Multiple sequential transactions completed successfully");
}

/// Test transaction rollback releases connection
///
/// ROLLBACK should also release the connection back to the pool.
#[tokio::test]
async fn test_rollback_releases_connection() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let mut config = create_test_config(postgres_host, postgres_port, PoolingStrategy::Transaction);
    config.performance.pool_size = 1; // Only 1 backend connection

    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port =
        start_test_proxy(config.clone(), publisher).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(100)).await;

    // Client 1: Execute and rollback a transaction
    {
        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user={} password={} dbname={}",
                proxy_port, config.backend.user, config.backend.password, config.backend.database
            ),
            tokio_postgres::NoTls,
        )
        .await
        .expect("Failed to connect client 1");

        tokio::spawn(async move {
            let _ = connection.await;
        });

        client.execute("BEGIN", &[]).await.expect("BEGIN failed");
        client.execute("SELECT 1", &[]).await.expect("SELECT failed");
        client.execute("ROLLBACK", &[]).await.expect("ROLLBACK failed");

        drop(client);
    }

    // Wait for connection to be released
    sleep(Duration::from_millis(200)).await;

    // Client 2: Should be able to get the connection
    {
        let connect_result = tokio::time::timeout(
            Duration::from_secs(5),
            tokio_postgres::connect(
                &format!(
                    "host=127.0.0.1 port={} user={} password={} dbname={}",
                    proxy_port,
                    config.backend.user,
                    config.backend.password,
                    config.backend.database
                ),
                tokio_postgres::NoTls,
            ),
        )
        .await;

        assert!(connect_result.is_ok(), "Client 2 should be able to connect after rollback");

        let (client, connection) = connect_result.unwrap().expect("Failed to connect client 2");

        tokio::spawn(async move {
            let _ = connection.await;
        });

        let result = client.execute("SELECT 2", &[]).await;
        assert!(result.is_ok(), "Client 2 should be able to execute queries");
    }

    println!("Connection release after rollback verified");
}

/// Test that transaction mode rejects SET commands outside transactions
///
/// In transaction pooling mode, SET commands outside of a transaction
/// block would modify session state that could affect other clients
/// when the connection is returned to the pool. The proxy should reject
/// such commands with an error.
///
/// NOTE: This test may fail until Phase 8 (connection handler integration)
/// is complete. The ModeEnforcer is built but not yet integrated.
#[tokio::test]
async fn test_transaction_mode_rejects_set_outside_transaction() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port, PoolingStrategy::Transaction);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port =
        start_test_proxy(config.clone(), publisher).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(100)).await;

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

    // SET outside transaction should be rejected in transaction mode
    // because it would modify session state that persists after connection
    // is returned to the pool
    let result = client.execute("SET search_path TO public", &[]).await;

    // The proxy should reject this command
    assert!(result.is_err(), "Transaction mode should reject SET commands outside transactions");

    // Verify the error message indicates the restriction
    if let Err(e) = result {
        // Use as_db_error() to get the actual database error message
        // tokio_postgres::Error::to_string() may not include the full message
        if let Some(db_err) = e.as_db_error() {
            let error_msg = db_err.message().to_lowercase();
            assert!(
                error_msg.contains("transaction")
                    || error_msg.contains("not supported")
                    || error_msg.contains("not allowed")
                    || error_msg.contains("pooling"),
                "Error should mention transaction pooling restriction, got: {}",
                db_err.message()
            );
        } else {
            panic!("Expected a database error (DbError) but got a different error type: {}", e);
        }
    }
}

/// Test that hybrid mode unpins connection after DROP TEMP TABLE
///
/// In hybrid mode, creating a temp table pins the connection (prevents release
/// to pool). After dropping the temp table, the connection should be unpinned
/// and can be released back to the pool after the transaction.
///
/// This test verifies that aggressive unpinning works correctly when the
/// pinning state (temp table) is removed.
#[tokio::test]
async fn test_hybrid_mode_unpins_on_drop_temp_table() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let mut config = create_test_config(postgres_host, postgres_port, PoolingStrategy::Hybrid);
    // Enable aggressive unpinning - this allows unpinning when state is cleared
    config.performance.pool_aggressive_unpinning = true;
    // Use pool_size = 1 to test connection release
    config.performance.pool_size = 1;

    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port =
        start_test_proxy(config.clone(), publisher).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(100)).await;

    // Client 1: Create temp table, then drop it
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

        tokio::spawn(async move {
            let _ = connection1.await;
        });

        // Create temp table - this should pin the connection
        client1
            .execute("CREATE TEMP TABLE test_unpin (id INT)", &[])
            .await
            .expect("CREATE TEMP TABLE failed");

        // Use the temp table
        client1.execute("INSERT INTO test_unpin VALUES (1)", &[]).await.expect("INSERT failed");

        let rows =
            client1.query("SELECT COUNT(*) FROM test_unpin", &[]).await.expect("SELECT failed");
        let count: i64 = rows[0].get(0);
        assert_eq!(count, 1);

        // Drop the temp table - this should unpin the connection
        client1.execute("DROP TABLE test_unpin", &[]).await.expect("DROP TABLE failed");

        // Disconnect client 1
        drop(client1);
    }

    // Wait for connection to be released back to pool
    sleep(Duration::from_millis(200)).await;

    // Client 2: Should be able to get a connection now that client 1's
    // connection was unpinned and released
    {
        let connect_result = tokio::time::timeout(
            Duration::from_secs(3),
            tokio_postgres::connect(
                &format!(
                    "host=127.0.0.1 port={} user={} password={} dbname={}",
                    proxy_port,
                    config.backend.user,
                    config.backend.password,
                    config.backend.database
                ),
                tokio_postgres::NoTls,
            ),
        )
        .await;

        assert!(
            connect_result.is_ok(),
            "Client 2 should be able to connect after temp table was dropped"
        );

        let (client2, connection2) = connect_result.unwrap().expect("Failed to connect client 2");

        tokio::spawn(async move {
            let _ = connection2.await;
        });

        // Execute a query to verify connection works
        let rows = client2.query("SELECT 'unpinned' as status", &[]).await.expect("SELECT failed");
        let status: &str = rows[0].get(0);
        assert_eq!(status, "unpinned");

        // Verify the temp table doesn't exist on this connection
        // (proving we got a clean pooled connection)
        let result = client2.query("SELECT * FROM test_unpin", &[]).await;
        assert!(result.is_err(), "Temp table should not exist on pooled connection");
    }

    println!("Hybrid mode unpin on DROP TEMP TABLE verified");
}

/// Test that events are captured for transaction operations
///
/// BEGIN, COMMIT, ROLLBACK should all be captured as query events.
#[tokio::test]
async fn test_transaction_events_captured() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port, PoolingStrategy::Transaction);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port =
        start_test_proxy(config.clone(), publisher.clone()).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(100)).await;

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

    // Execute a full transaction
    client.execute("BEGIN", &[]).await.expect("BEGIN failed");
    client.execute("SELECT 42", &[]).await.expect("SELECT failed");
    client.execute("COMMIT", &[]).await.expect("COMMIT failed");

    // Wait for events to be published
    sleep(Duration::from_millis(300)).await;

    let events = test_publisher.events();
    println!("Captured {} events", events.len());
    for (i, event) in events.iter().enumerate() {
        println!("Event {}: query='{}', success={}", i, event.query, event.success);
    }

    // Should have captured BEGIN, SELECT, and COMMIT
    let event_count = test_publisher.event_count();
    assert!(
        event_count >= 3,
        "Expected at least 3 events (BEGIN, SELECT, COMMIT), got {}",
        event_count
    );
}
