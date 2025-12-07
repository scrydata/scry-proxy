/// Integration tests for the Scry proxy with real Postgres
///
/// These tests spin up a real Postgres instance using testcontainers,
/// start the proxy, and verify end-to-end query execution and event publishing.
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

    fn clear(&self) {
        self.events.lock().unwrap().clear();
    }

    fn find_query(&self, pattern: &str) -> Option<QueryEvent> {
        self.events.lock().unwrap().iter().find(|e| e.query.contains(pattern)).cloned()
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

/// Helper to create a test config pointing to the given backend
fn create_test_config(backend_host: String, backend_port: u16) -> Config {
    Config {
        proxy: ProxyConfig {
            listen_address: "127.0.0.1:0".to_string(), // Let OS assign port
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
            enable_tracing: false, // Disable for tests
            otlp_endpoint: None,
            service_name: "scry-test".to_string(),
            enable_metrics_server: false,
            metrics_server_address: "127.0.0.1:9090".to_string(),
        },
        publisher: PublisherConfig {
            enabled: true,
            batch_size: 10,
            flush_interval_ms: 100, // Fast flush for tests
            anonymize: false,
            publisher_type: "debug".to_string(),
            max_queue_size: 1000,
            http_endpoint: None,
            http_timeout_ms: 500,
            http_max_retries: 2,
            http_api_key: None,
            http_compression: true,
        },
        performance: PerformanceConfig {
            target_latency_ms: 1,
            connection_pooling: PoolingStrategy::Disabled,
            pool_size: 100,
            pool_min_idle: 10,
            pool_timeout_secs: 30,
            pool_recycle_secs: 3600,
            pool_aggressive_unpinning: false,
            buffer_size: 8192,
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

/// Start the proxy server and return its listen address and port
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

#[tokio::test]
async fn test_basic_query_proxying() {
    // Setup: Start Postgres container
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    // Wait for Postgres to be ready
    sleep(Duration::from_secs(2)).await;

    // Create test config pointing to the container
    let config = create_test_config(postgres_host, postgres_port);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    // Start proxy
    let proxy_port =
        start_test_proxy(config.clone(), publisher).await.expect("Failed to start proxy");

    // Wait for proxy to be ready
    sleep(Duration::from_millis(100)).await;

    // Connect client to proxy
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user={} password={} dbname={}",
            proxy_port, config.backend.user, config.backend.password, config.backend.database
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("Failed to connect to proxy");

    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    // Execute a simple query
    let rows = client.query("SELECT 1 as num, 'hello' as text", &[]).await.expect("Query failed");

    assert_eq!(rows.len(), 1);
    let num: i32 = rows[0].get(0);
    let text: &str = rows[0].get(1);
    assert_eq!(num, 1);
    assert_eq!(text, "hello");

    // Wait for events to be published
    sleep(Duration::from_millis(200)).await;

    // Verify event was captured
    let events = test_publisher.events();
    assert!(!events.is_empty(), "Expected at least one query event");

    let found = test_publisher.find_query("SELECT");
    assert!(found.is_some(), "Expected to find SELECT query in events");
}

#[tokio::test]
async fn test_multiple_queries() {
    // Setup: Start Postgres container
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port =
        start_test_proxy(config.clone(), publisher).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(100)).await;

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

    // Execute multiple queries
    let queries = vec!["SELECT 1", "SELECT 2 + 2", "SELECT 'test'", "SELECT NOW()"];

    for query in &queries {
        client.execute(*query, &[]).await.expect("Query failed");
    }

    // Wait for events to be published
    sleep(Duration::from_millis(300)).await;

    // Verify all queries were captured
    let event_count = test_publisher.event_count();
    assert!(
        event_count >= queries.len(),
        "Expected at least {} events, got {}",
        queries.len(),
        event_count
    );
}

#[tokio::test]
async fn test_prepared_statements() {
    // Setup: Start Postgres container
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port =
        start_test_proxy(config.clone(), publisher).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(100)).await;

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

    // Use prepared statements (extended protocol)
    let stmt = client
        .prepare("SELECT $1::int + $2::int as sum")
        .await
        .expect("Failed to prepare statement");

    let rows =
        client.query(&stmt, &[&5i32, &7i32]).await.expect("Failed to execute prepared statement");

    assert_eq!(rows.len(), 1);
    let sum: i32 = rows[0].get(0);
    assert_eq!(sum, 12);

    // Wait for events
    sleep(Duration::from_millis(200)).await;

    // Verify prepared statement was captured
    let event_count = test_publisher.event_count();
    assert!(event_count > 0, "Expected query events from prepared statement");
}

#[tokio::test]
async fn test_syntax_error_captured() {
    // Setup: Start Postgres container
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port =
        start_test_proxy(config.clone(), publisher).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(100)).await;

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

    // Execute query with syntax error
    let result = client.execute("SELEC 1", &[]).await;
    assert!(result.is_err(), "Expected syntax error");

    // Wait for events to be published
    sleep(Duration::from_millis(200)).await;

    // Verify error event was captured
    let events = test_publisher.events();
    assert!(!events.is_empty(), "Expected at least one error event");

    // Find the error event
    let error_event = events.iter().find(|e| !e.success);
    assert!(error_event.is_some(), "Expected to find error event");

    let error_event = error_event.unwrap();
    assert_eq!(error_event.success, false);
    assert!(error_event.error.is_some(), "Expected error message");

    let error_msg = error_event.error.as_ref().unwrap();
    assert!(
        error_msg.contains("syntax error") || error_msg.contains("SELEC"),
        "Expected error message to mention syntax error, got: {}",
        error_msg
    );
}

#[tokio::test]
async fn test_table_not_found_error() {
    // Setup: Start Postgres container
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port =
        start_test_proxy(config.clone(), publisher).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(100)).await;

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

    // Execute query on non-existent table
    let result = client.execute("SELECT * FROM nonexistent_table", &[]).await;
    assert!(result.is_err(), "Expected table not found error");

    // Wait for events to be published
    sleep(Duration::from_millis(200)).await;

    // Verify error event was captured
    let events = test_publisher.events();
    let error_events: Vec<_> = events.iter().filter(|e| !e.success).collect();
    assert!(!error_events.is_empty(), "Expected at least one error event");

    let error_event = &error_events[0];
    assert!(error_event.error.is_some(), "Expected error message");

    let error_msg = error_event.error.as_ref().unwrap();
    assert!(
        error_msg.contains("does not exist") || error_msg.contains("nonexistent_table"),
        "Expected error message about missing table, got: {}",
        error_msg
    );
}

#[tokio::test]
async fn test_mixed_success_and_error_queries() {
    // Setup: Start Postgres container
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port =
        start_test_proxy(config.clone(), publisher).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(100)).await;

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

    // Execute mix of successful and failing queries
    let _ = client.execute("SELECT 1", &[]).await;
    sleep(Duration::from_millis(50)).await;

    let _ = client.execute("SELEC 1", &[]).await; // Syntax error
    sleep(Duration::from_millis(50)).await;

    let _ = client.execute("SELECT 2", &[]).await;
    sleep(Duration::from_millis(50)).await;

    let _ = client.execute("SELECT * FROM nonexistent", &[]).await; // Table error
    sleep(Duration::from_millis(50)).await;

    // Wait for events to be published
    sleep(Duration::from_millis(300)).await;

    // Verify both success and error events captured
    let events = test_publisher.events();
    assert!(events.len() >= 3, "Expected at least 3 events, got {}", events.len());

    let success_count = events.iter().filter(|e| e.success).count();
    let error_count = events.iter().filter(|e| !e.success).count();

    // Print debug info
    println!("Total events: {}", events.len());
    println!("Success count: {}", success_count);
    println!("Error count: {}", error_count);
    for (i, event) in events.iter().enumerate() {
        println!(
            "Event {}: query='{}', success={}, error={:?}",
            i, event.query, event.success, event.error
        );
    }

    assert!(success_count >= 1, "Expected at least 1 successful query");
    assert!(error_count >= 1, "Expected at least 1 failed query");
}
