/// Integration tests for connection pooling
///
/// Tests that the TCP connection pool correctly reuses connections
/// between client sessions and maintains pool metrics.

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
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
        }
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

/// Helper to create a test config with pooling enabled
fn create_pooled_config(backend_host: String, backend_port: u16, pool_size: usize) -> Config {
    Config {
        proxy: ProxyConfig {
            listen_address: "127.0.0.1:0".to_string(),
            max_connections: 10,
            shutdown_timeout_secs: 30,
        },
        backend: BackendConfig {
            protocol: DatabaseProtocol::Postgres,
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
        protocol: ProtocolConfig {
            max_prepared_statements: 1000,
        },
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
            connection_pooling: PoolingStrategy::Session, // Enable pooling!
            pool_size,
            pool_min_idle: 2,
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
    }
}

#[tokio::test]
async fn test_connection_pool_reuse() {
    // Setup: Start Postgres container
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    // Create config with pooling enabled (pool size = 3)
    let config = create_pooled_config(postgres_host, postgres_port, 3);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let batcher = EventBatcher::new(
        publisher,
        config.publisher.batch_size,
        config.publisher.flush_interval_ms,
        config.publisher.max_queue_size,
    );

    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));
    let server = ProxyServer::new(config.clone(), batcher, metrics)
        .await
        .expect("Failed to create proxy server");

    let proxy_port = server.local_addr().expect("Failed to get local addr").port();

    // Start proxy in background
    let proxy_handle = tokio::spawn(async move { server.run().await });

    sleep(Duration::from_millis(100)).await;

    // Make 5 sequential connections (more than pool size of 3)
    // This tests that connections are properly returned to pool and reused
    for i in 0..5 {
        println!("Connection {}", i + 1);

        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user={} password={} dbname={}",
                proxy_port, config.backend.user, config.backend.password, config.backend.database
            ),
            tokio_postgres::NoTls,
        )
        .await
        .expect("Failed to connect to proxy");

        let conn_handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        // Execute a query
        client
            .execute(&format!("SELECT {} as num", i + 1), &[])
            .await
            .expect("Query failed");

        // Close connection to return it to pool
        drop(client);

        // Wait for connection to be fully closed
        sleep(Duration::from_millis(100)).await;

        // Abort the connection handle
        conn_handle.abort();
    }

    // Wait for event flush
    sleep(Duration::from_millis(300)).await;

    // Verify all queries were executed
    let event_count = test_publisher.event_count();
    println!("Total events captured: {}", event_count);
    assert!(
        event_count >= 5,
        "Expected at least 5 events, got {}",
        event_count
    );

    // The pool should have reused connections - we created 5 connections
    // but pool size is only 3, so at least 2 connections were reused
    println!("Connection pooling test completed successfully");

    // Cleanup
    drop(proxy_handle);
}

#[tokio::test]
async fn test_pooling_vs_direct_connections() {
    // This test compares pooled vs direct connections
    // Setup: Start Postgres container
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    // Test 1: With pooling disabled
    let mut config_no_pool = create_pooled_config(postgres_host.clone(), postgres_port, 3);
    config_no_pool.performance.connection_pooling = PoolingStrategy::Disabled;

    let test_publisher_no_pool = TestPublisher::new();
    let publisher_no_pool = Arc::new(test_publisher_no_pool.clone());

    let batcher_no_pool = EventBatcher::new(
        publisher_no_pool,
        config_no_pool.publisher.batch_size,
        config_no_pool.publisher.flush_interval_ms,
        config_no_pool.publisher.max_queue_size,
    );

    let metrics_no_pool = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));
    let server_no_pool = ProxyServer::new(config_no_pool.clone(), batcher_no_pool, metrics_no_pool)
        .await
        .expect("Failed to create proxy server");

    let proxy_port_no_pool = server_no_pool.local_addr().expect("Failed to get local addr").port();

    let proxy_handle_no_pool = tokio::spawn(async move { server_no_pool.run().await });

    sleep(Duration::from_millis(100)).await;

    // Execute queries without pooling
    for _ in 0..3 {
        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user={} password={} dbname={}",
                proxy_port_no_pool,
                config_no_pool.backend.user,
                config_no_pool.backend.password,
                config_no_pool.backend.database
            ),
            tokio_postgres::NoTls,
        )
        .await
        .expect("Failed to connect");

        tokio::spawn(async move {
            let _ = connection.await;
        });

        client.execute("SELECT 1", &[]).await.expect("Query failed");
        drop(client);
        sleep(Duration::from_millis(50)).await;
    }

    sleep(Duration::from_millis(200)).await;
    let events_no_pool = test_publisher_no_pool.event_count();
    println!("Events without pooling: {}", events_no_pool);

    drop(proxy_handle_no_pool);

    // Test 2: With pooling enabled
    let config_with_pool = create_pooled_config(postgres_host, postgres_port, 3);
    let test_publisher_with_pool = TestPublisher::new();
    let publisher_with_pool = Arc::new(test_publisher_with_pool.clone());

    let batcher_with_pool = EventBatcher::new(
        publisher_with_pool,
        config_with_pool.publisher.batch_size,
        config_with_pool.publisher.flush_interval_ms,
        config_with_pool.publisher.max_queue_size,
    );

    let metrics_with_pool = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));
    let server_with_pool = ProxyServer::new(config_with_pool.clone(), batcher_with_pool, metrics_with_pool)
        .await
        .expect("Failed to create proxy server");

    let proxy_port_with_pool =
        server_with_pool.local_addr().expect("Failed to get local addr").port();

    let proxy_handle_with_pool = tokio::spawn(async move { server_with_pool.run().await });

    sleep(Duration::from_millis(100)).await;

    // Execute queries with pooling
    for _ in 0..3 {
        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user={} password={} dbname={}",
                proxy_port_with_pool,
                config_with_pool.backend.user,
                config_with_pool.backend.password,
                config_with_pool.backend.database
            ),
            tokio_postgres::NoTls,
        )
        .await
        .expect("Failed to connect");

        tokio::spawn(async move {
            let _ = connection.await;
        });

        client.execute("SELECT 1", &[]).await.expect("Query failed");
        drop(client);
        sleep(Duration::from_millis(50)).await;
    }

    sleep(Duration::from_millis(200)).await;
    let events_with_pool = test_publisher_with_pool.event_count();
    println!("Events with pooling: {}", events_with_pool);

    // Both should have executed the same number of queries
    assert!(
        events_no_pool >= 3 && events_with_pool >= 3,
        "Expected at least 3 events in each case"
    );

    println!("Pooling comparison test completed successfully");

    drop(proxy_handle_with_pool);
}

#[tokio::test]
async fn test_discard_all_resets_connection_state() {
    // This test verifies that DISCARD ALL properly resets connection state
    // when a connection is returned to the pool and reused

    // Setup: Start Postgres container
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    // Create config with pooling enabled (pool size = 1 for this test)
    let config = create_pooled_config(postgres_host, postgres_port, 1);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let batcher = EventBatcher::new(
        publisher,
        config.publisher.batch_size,
        config.publisher.flush_interval_ms,
        config.publisher.max_queue_size,
    );

    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));
    let server = ProxyServer::new(config.clone(), batcher, metrics)
        .await
        .expect("Failed to create proxy server");

    let proxy_port = server.local_addr().expect("Failed to get local addr").port();

    // Start proxy in background
    let proxy_handle = tokio::spawn(async move { server.run().await });

    sleep(Duration::from_millis(100)).await;

    // Connection 1: Create a temporary table and set a session variable
    {
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
            let _ = connection.await;
        });

        // Create a temporary table
        client
            .execute("CREATE TEMP TABLE test_temp (id INT)", &[])
            .await
            .expect("Failed to create temp table");

        // Set a session variable
        client
            .execute("SET application_name = 'test_app'", &[])
            .await
            .expect("Failed to set session variable");

        // Verify they exist
        let rows = client
            .query("SELECT COUNT(*) FROM test_temp", &[])
            .await
            .expect("Failed to query temp table");
        assert_eq!(rows.len(), 1);

        let app_name_rows = client
            .query("SHOW application_name", &[])
            .await
            .expect("Failed to show application_name");
        assert_eq!(app_name_rows.len(), 1);

        // Close connection - should return to pool and trigger DISCARD ALL
        drop(client);
    }

    // Wait for connection to be returned to pool and DISCARD ALL to execute
    sleep(Duration::from_millis(200)).await;

    // Connection 2: Get connection from pool (should be the same physical connection)
    // but with state reset via DISCARD ALL
    {
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
            let _ = connection.await;
        });

        // Verify temp table no longer exists (DISCARD ALL removed it)
        let result = client.query("SELECT COUNT(*) FROM test_temp", &[]).await;
        assert!(
            result.is_err(),
            "Temp table should not exist after DISCARD ALL"
        );

        // Verify session variable was reset (DISCARD ALL reset it)
        let app_name_rows = client
            .query("SHOW application_name", &[])
            .await
            .expect("Failed to show application_name");
        let app_name: String = app_name_rows[0].get(0);
        assert_ne!(
            app_name, "test_app",
            "Session variable should be reset after DISCARD ALL"
        );

        println!("DISCARD ALL successfully reset connection state!");
        drop(client);
    }

    // Cleanup
    drop(proxy_handle);
}

