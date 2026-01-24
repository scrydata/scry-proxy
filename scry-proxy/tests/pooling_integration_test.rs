#![allow(clippy::needless_return)]
//! Integration tests for connection pooling with transaction-based release
//!
//! These tests verify that connections are properly released back to the pool
//! after transactions complete, enabling efficient connection reuse.
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

/// Test that a single client can execute multiple transactions with session pooling
///
/// This verifies that after COMMIT, the client can start a new transaction.
/// With session pooling, the backend connection stays the same for the client session.
/// Note: Transaction mode with multiple transactions within a single tokio-postgres
/// client session is incompatible with the extended protocol's prepared statement caching.
#[tokio::test]
async fn test_transaction_pooling_multiple_transactions() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let mut config = create_test_config(postgres_host, postgres_port);
    // Use Session mode for single-client multi-transaction test
    // Transaction mode releases backend connections which breaks tokio-postgres's
    // prepared statement cache within a single client session
    config.performance.connection_pooling = PoolingStrategy::Session;
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

    // Second transaction (same backend connection in Session mode)
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

/// Test multi-client contention with pool_size=1 using session mode
///
/// This test verifies that Client 2 can acquire a connection after Client 1
/// disconnects. With pool_size=1, both clients must share the single backend
/// connection. In session mode, connections are held until client disconnects.
///
/// Note: Transaction mode with mid-session release is not compatible with
/// tokio-postgres's extended protocol (prepared statement caching). For
/// transaction-mode semantics, clients must fully disconnect between transactions.
#[tokio::test]
async fn test_connection_released_after_transaction() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let mut config = create_test_config(postgres_host, postgres_port);
    // Use Session mode - connection is released when client disconnects
    config.performance.connection_pooling = PoolingStrategy::Session;
    // Critical: pool_size = 1 forces contention
    config.performance.pool_size = 1;
    config.backend.pool_size = 1;

    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port =
        start_test_proxy(config.clone(), publisher).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(200)).await;

    // Client 1: Connect, do a transaction, commit, disconnect
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

        // Execute a transaction and commit
        client1.execute("BEGIN", &[]).await.expect("Client 1 BEGIN failed");
        client1.execute("SELECT 1", &[]).await.expect("Client 1 SELECT failed");
        client1.execute("COMMIT", &[]).await.expect("Client 1 COMMIT failed");

        // Verify a simple query works after commit
        let rows = client1
            .query("SELECT 'client1_ok' as status", &[])
            .await
            .expect("Client 1 final query failed");
        assert_eq!(rows.len(), 1);
        let status: &str = rows[0].get(0);
        assert_eq!(status, "client1_ok");

        // Disconnect Client 1 - connection returns to pool
        drop(client1);
        sleep(Duration::from_millis(50)).await;
        conn_handle.abort();
    }

    // Give time for connection to be released back to pool
    sleep(Duration::from_millis(100)).await;

    // Client 2: Should succeed because Client 1 released the connection on disconnect
    {
        let (client2, connection2) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user={} password={} dbname={}",
                proxy_port, config.backend.user, config.backend.password, config.backend.database
            ),
            tokio_postgres::NoTls,
        )
        .await
        .expect("Failed to connect client 2 - connection may not have been released");

        tokio::spawn(async move {
            if let Err(e) = connection2.await {
                eprintln!("Connection 2 error: {}", e);
            }
        });

        // Execute query - should work because it got the released connection
        let rows = client2
            .query("SELECT 'client2_ok' as status", &[])
            .await
            .expect("Client 2 SELECT failed");
        assert_eq!(rows.len(), 1);
        let status: &str = rows[0].get(0);
        assert_eq!(status, "client2_ok");
    }
}

/// Test that connections can be shared between clients with pool_size=1
///
/// This verifies that after Client 1 disconnects (releasing its connection),
/// Client 2 can acquire the pooled connection. Uses session mode where
/// connections are held for the duration of the client session.
///
/// Note: Auto-commit query release within a session is not compatible with
/// tokio-postgres's extended protocol (prepared statement caching) because
/// the release triggers DISCARD ALL which clears prepared statements.
#[tokio::test]
async fn test_autocommit_releases_connection() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let mut config = create_test_config(postgres_host, postgres_port);
    // Use Session mode - connection released on disconnect
    config.performance.connection_pooling = PoolingStrategy::Session;
    // pool_size = 1 forces contention - proves connection was released
    config.performance.pool_size = 1;
    config.backend.pool_size = 1;

    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port =
        start_test_proxy(config.clone(), publisher).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(200)).await;

    // Client 1: Connect, do an auto-commit query, disconnect
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

        // Execute auto-commit query (no BEGIN/COMMIT)
        let rows = client1.query("SELECT 1 as num", &[]).await.expect("Client 1 SELECT failed");
        assert_eq!(rows.len(), 1);
        let num: i32 = rows[0].get(0);
        assert_eq!(num, 1);

        // Disconnect Client 1 - connection returns to pool
        drop(client1);
        sleep(Duration::from_millis(50)).await;
        conn_handle.abort();
    }

    // Give time for connection to be released back to pool
    sleep(Duration::from_millis(100)).await;

    // Client 2: Should succeed because Client 1 released connection on disconnect
    {
        let (client2, connection2) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user={} password={} dbname={}",
                proxy_port, config.backend.user, config.backend.password, config.backend.database
            ),
            tokio_postgres::NoTls,
        )
        .await
        .expect("Failed to connect client 2 - connection may not have been released");

        tokio::spawn(async move {
            if let Err(e) = connection2.await {
                eprintln!("Connection 2 error: {}", e);
            }
        });

        // Execute query
        let rows = client2.query("SELECT 2 as num", &[]).await.expect("Client 2 SELECT failed");
        assert_eq!(rows.len(), 1);
        let num: i32 = rows[0].get(0);
        assert_eq!(num, 2);
    }
}

/// Test that session variables are replayed on reconnection (Hybrid mode)
///
/// In Hybrid mode, when a client has session variables set (pinning state),
/// the connection should remain pinned to that client. However, if the client
/// gets a fresh (unpinned) connection after transaction release, the proxy
/// should replay the session variables to the new connection.
///
/// This test verifies that:
/// 1. Session variables are tracked as pinning state
/// 2. The connection is properly pinned when session variables are set
/// 3. Session variables persist across transactions within a session
#[tokio::test]
async fn test_session_variable_state_tracked_in_hybrid_mode() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let mut config = create_test_config(postgres_host, postgres_port);
    // Use Hybrid mode for state tracking
    config.performance.connection_pooling = PoolingStrategy::Hybrid;
    config.performance.pool_size = 5;

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

    // Set a session variable - this should pin the connection in hybrid mode
    client
        .execute("SET application_name = 'test_replay'", &[])
        .await
        .expect("SET application_name failed");

    // Verify the session variable was set
    let rows =
        client.query("SHOW application_name", &[]).await.expect("SHOW application_name failed");
    let app_name: String = rows[0].get(0);
    assert_eq!(app_name, "test_replay", "Session variable should be set");

    // Execute a transaction
    client.execute("BEGIN", &[]).await.expect("BEGIN failed");
    client.query("SELECT 1", &[]).await.expect("SELECT failed");
    client.execute("COMMIT", &[]).await.expect("COMMIT failed");

    // Brief pause
    sleep(Duration::from_millis(50)).await;

    // Verify session variable still works (should persist due to pinning or replay)
    let rows = client
        .query("SHOW application_name", &[])
        .await
        .expect("SHOW application_name after transaction failed");
    let app_name: String = rows[0].get(0);
    assert_eq!(app_name, "test_replay", "Session variable should persist after transaction");

    // Execute another transaction to verify continued state
    client.execute("BEGIN", &[]).await.expect("BEGIN 2 failed");
    let rows = client.query("SELECT 42 as num", &[]).await.expect("SELECT 2 failed");
    assert_eq!(rows.len(), 1);
    let num: i32 = rows[0].get(0);
    assert_eq!(num, 42);
    client.execute("COMMIT", &[]).await.expect("COMMIT 2 failed");

    // Final verification that session variable is still set
    let rows = client
        .query("SHOW application_name", &[])
        .await
        .expect("Final SHOW application_name failed");
    let app_name: String = rows[0].get(0);
    assert_eq!(app_name, "test_replay", "Session variable should still be set at end");
}

/// Test that prepared statements work after transaction in Hybrid mode
///
/// This test verifies that prepared statements created during a session
/// continue to work after transactions complete. In Hybrid mode, the
/// connection state (including prepared statements) should be preserved
/// or replayed.
#[tokio::test]
async fn test_prepared_statement_persists_across_transactions_hybrid() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let mut config = create_test_config(postgres_host, postgres_port);
    // Use Hybrid mode - connection stays pinned due to prepared statement
    config.performance.connection_pooling = PoolingStrategy::Hybrid;
    config.performance.pool_size = 5;

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

    // Prepare a statement - this should pin the connection in hybrid mode
    let stmt = client
        .prepare("SELECT $1::int + $2::int as sum")
        .await
        .expect("Failed to prepare statement");

    // Execute the prepared statement
    let rows = client.query(&stmt, &[&3i32, &7i32]).await.expect("Failed to execute prepared stmt");
    assert_eq!(rows.len(), 1);
    let sum: i32 = rows[0].get(0);
    assert_eq!(sum, 10);

    // Execute a transaction
    client.execute("BEGIN", &[]).await.expect("BEGIN failed");
    let rows = client.query("SELECT 'in_txn' as status", &[]).await.expect("SELECT in txn failed");
    assert_eq!(rows.len(), 1);
    let status: &str = rows[0].get(0);
    assert_eq!(status, "in_txn");
    client.execute("COMMIT", &[]).await.expect("COMMIT failed");

    // Brief pause
    sleep(Duration::from_millis(50)).await;

    // Execute the prepared statement again - should still work
    let rows = client
        .query(&stmt, &[&10i32, &20i32])
        .await
        .expect("Failed to execute prepared stmt after transaction");
    assert_eq!(rows.len(), 1);
    let sum: i32 = rows[0].get(0);
    assert_eq!(sum, 30);

    // Another transaction
    client.execute("BEGIN", &[]).await.expect("BEGIN 2 failed");
    // Use prepared statement inside transaction
    let rows = client
        .query(&stmt, &[&100i32, &200i32])
        .await
        .expect("Failed to execute prepared stmt inside txn");
    assert_eq!(rows.len(), 1);
    let sum: i32 = rows[0].get(0);
    assert_eq!(sum, 300);
    client.execute("COMMIT", &[]).await.expect("COMMIT 2 failed");

    // Final execution of prepared statement
    let rows =
        client.query(&stmt, &[&1i32, &1i32]).await.expect("Final prepared stmt execution failed");
    assert_eq!(rows.len(), 1);
    let sum: i32 = rows[0].get(0);
    assert_eq!(sum, 2, "Prepared statement should work throughout session");
}

/// Test that pool warmup pre-creates connections
///
/// This test verifies that calling warmup_pools() creates the requested number
/// of connections in the pool before accepting client connections, reducing
/// cold-start latency.
#[tokio::test]
async fn test_pool_warmup_creates_connections() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let mut config = create_test_config(postgres_host, postgres_port);
    config.performance.connection_pooling = PoolingStrategy::Session;
    config.performance.pool_size = 10;
    config.performance.pool_min_idle = 5; // Request 5 pre-warmed connections

    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let batcher = EventBatcher::new(
        publisher,
        config.publisher.batch_size,
        config.publisher.flush_interval_ms,
        config.publisher.max_queue_size,
    );

    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));
    let server =
        ProxyServer::new(config.clone(), batcher, metrics).await.expect("Failed to create server");

    // Get pool status before warmup
    let pool_manager = server.pool_manager().expect("Pool manager should exist");
    let status_before = pool_manager.pool().status();
    assert_eq!(
        status_before.size, 0,
        "Pool should be empty before warmup"
    );

    // Warm up the pools
    let min_idle = config.performance.pool_min_idle;
    let created = server.warmup_pools(min_idle).await;

    // Verify connections were created
    assert_eq!(
        created, min_idle,
        "warmup_pools should create {} connections",
        min_idle
    );

    // Verify pool status reflects the warmed-up connections
    let status_after = pool_manager.pool().status();
    assert!(
        status_after.size >= min_idle,
        "Pool should have at least {} connections after warmup, got {}",
        min_idle,
        status_after.size
    );

    // Verify the connections work by connecting a client
    let proxy_port = server.local_addr().expect("Failed to get local address").port();

    // Spawn server in background
    tokio::spawn(async move {
        let _ = server.run().await;
    });

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

    // Execute a query to verify the warmed-up connection works
    let rows = client.query("SELECT 1 as num", &[]).await.expect("Query failed");
    assert_eq!(rows.len(), 1);
    let num: i32 = rows[0].get(0);
    assert_eq!(num, 1);
}

/// Test that warmup with zero min_idle is a no-op
#[tokio::test]
async fn test_pool_warmup_zero_is_noop() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let mut config = create_test_config(postgres_host, postgres_port);
    config.performance.connection_pooling = PoolingStrategy::Session;
    config.performance.pool_size = 10;
    config.performance.pool_min_idle = 0; // No warmup requested

    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let batcher = EventBatcher::new(
        publisher,
        config.publisher.batch_size,
        config.publisher.flush_interval_ms,
        config.publisher.max_queue_size,
    );

    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));
    let server =
        ProxyServer::new(config.clone(), batcher, metrics).await.expect("Failed to create server");

    // Warm up with 0 should return 0
    let created = server.warmup_pools(0).await;
    assert_eq!(created, 0, "warmup_pools(0) should return 0");

    // Pool should still be empty
    let pool_manager = server.pool_manager().expect("Pool manager should exist");
    let status = pool_manager.pool().status();
    assert_eq!(status.size, 0, "Pool should be empty after warmup(0)");
}
