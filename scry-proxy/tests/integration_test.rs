/// Integration tests for the Scry proxy with real Postgres
///
/// These tests spin up a real Postgres instance using testcontainers,
/// start the proxy, and verify end-to-end query execution and event publishing.
use scry::{config::DatabaseConfig, config::*, observability::*, proxy::*, publisher::*};
use scry_protocol::ParamValue;
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

    #[allow(dead_code)]
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
            enable_tracing: false, // Disable for tests
            otlp_endpoint: None,
            service_name: "scry-test".to_string(),
            enable_metrics_server: false,
            metrics_server_address: "127.0.0.1:9090".to_string(),
        },
        protocol: ProtocolConfig { max_prepared_statements: 1000 },
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
            shadow_id: None,
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
            pool_queue_depth: 50,
            pool_idle_unpin_secs: 60,
            pool_lifo: true,
            pool_reset_timeout_ms: 5000,
            pool_ratio_warning_threshold: 20,
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

/// Start the proxy server and return its listen address and port
async fn start_test_proxy(
    config: Config,
    publisher: Arc<dyn EventPublisher>,
) -> anyhow::Result<u16> {
    println!("DEBUG: Starting proxy with backend {}:{}", config.backend.host, config.backend.port);
    println!("DEBUG: Listen address: {}", config.proxy.listen_address);
    println!("DEBUG: Pooling: {:?}", config.performance.connection_pooling);

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
    println!("DEBUG: Postgres container on {}:{}", postgres_host, postgres_port);

    // Wait for Postgres to be ready
    sleep(Duration::from_secs(2)).await;

    // Create test config pointing to the container
    let config = create_test_config(postgres_host, postgres_port);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    // Start proxy
    let proxy_port =
        start_test_proxy(config.clone(), publisher).await.expect("Failed to start proxy");
    println!("DEBUG: Proxy listening on port {}", proxy_port);

    // Wait for proxy to be ready
    sleep(Duration::from_millis(500)).await;
    println!("DEBUG: Attempting to connect to proxy at 127.0.0.1:{}", proxy_port);

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
    assert!(!error_event.success);
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

#[tokio::test]
async fn test_prepared_statement_params_captured() {
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

    // Execute prepared statement with various parameter types
    let stmt = client
        .prepare("SELECT $1::int4 as num, $2::text as txt, $3::bool as flag")
        .await
        .expect("Failed to prepare statement");

    let rows = client
        .query(&stmt, &[&42i32, &"hello", &true])
        .await
        .expect("Failed to execute prepared statement");

    assert_eq!(rows.len(), 1);
    let num: i32 = rows[0].get(0);
    let txt: &str = rows[0].get(1);
    let flag: bool = rows[0].get(2);
    assert_eq!(num, 42);
    assert_eq!(txt, "hello");
    assert!(flag);

    // Wait for events to be published
    sleep(Duration::from_millis(300)).await;

    // Verify captured event has correct params
    let events = test_publisher.events();

    // Print all events for debugging
    println!("All events:");
    for (i, e) in events.iter().enumerate() {
        println!(
            "  Event {}: query='{}', params={:?}, success={}",
            i, e.query, e.params, e.success
        );
    }

    // Find the event with our query AND non-empty params
    // (The prepare() call also emits an event, but with empty params)
    let event = events
        .iter()
        .find(|e| e.query.contains("SELECT $1") && !e.params.is_empty())
        .expect("Expected to find query event with params");

    // Print debug info
    println!("Found event with params: query='{}', params={:?}", event.query, event.params);

    // Verify params were captured
    assert!(!event.params.is_empty(), "Expected params to be captured, but got empty params");

    // Verify we got 3 params
    assert_eq!(
        event.params.len(),
        3,
        "Expected 3 params, got {}: {:?}",
        event.params.len(),
        event.params
    );

    // Verify param values
    // Note: tokio_postgres sends Parse with num_params=0 (server infers types),
    // so we get Unknown variants with raw binary data.
    // The raw data is still correct and can be decoded by the replay engine
    // which will have the schema information.

    match &event.params[0] {
        ParamValue::Int32(n) => assert_eq!(*n, 42, "First param should be 42"),
        ParamValue::Unknown { oid: _, data } => {
            // Binary format: 4 bytes big-endian i32
            assert_eq!(data.len(), 4, "Int32 should be 4 bytes");
            let val = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
            assert_eq!(val, 42, "First param should be 42");
        }
        other => panic!("Expected Int32(42) or Unknown, got {:?}", other),
    }

    match &event.params[1] {
        ParamValue::Text(s) => assert_eq!(s, "hello", "Second param should be 'hello'"),
        ParamValue::Unknown { oid: _, data } => {
            // Binary format: raw bytes
            let val = String::from_utf8(data.clone()).expect("Should be valid UTF-8");
            assert_eq!(val, "hello", "Second param should be 'hello'");
        }
        other => panic!("Expected Text('hello') or Unknown, got {:?}", other),
    }

    match &event.params[2] {
        ParamValue::Bool(b) => assert!(*b, "Third param should be true"),
        ParamValue::Unknown { oid: _, data } => {
            // Binary format: 1 byte, non-zero = true
            assert_eq!(data.len(), 1, "Bool should be 1 byte");
            assert_eq!(data[0], 1, "Third param should be true (1)");
        }
        other => panic!("Expected Bool(true) or Unknown, got {:?}", other),
    }

    // params_incomplete may be true if OIDs weren't in Parse message
    // (tokio_postgres uses server-inferred types)
    println!("params_incomplete: {}", event.params_incomplete);
}

/// Test multi-database routing with different database names
///
/// This test verifies that the proxy correctly routes connections based on
/// the database name specified in the PostgreSQL startup message. Clients
/// connecting with different database names should be routed to their
/// respective backend configurations.
#[tokio::test]
async fn test_multi_database_routing() {
    // Setup: Start Postgres container
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    // Create config with multiple database routes
    // All routes point to the same backend (for test simplicity) but have
    // different logical names - proving the routing mechanism works
    let mut config = create_test_config(postgres_host.clone(), postgres_port);

    // Add database routing entries
    config.databases = vec![
        DatabaseConfig {
            name: "app_db".to_string(),
            host: postgres_host.clone(),
            port: postgres_port,
            database: "postgres".to_string(), // Same actual DB
            user: "postgres".to_string(),
            password: "postgres".to_string(),
            pool_size: Some(5),
        },
        DatabaseConfig {
            name: "analytics_db".to_string(),
            host: postgres_host.clone(),
            port: postgres_port,
            database: "postgres".to_string(), // Same actual DB
            user: "postgres".to_string(),
            password: "postgres".to_string(),
            pool_size: Some(3),
        },
    ];

    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port =
        start_test_proxy(config.clone(), publisher).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(100)).await;

    // Test 1: Connect using "app_db" logical name
    {
        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user=postgres password=postgres dbname=app_db",
                proxy_port
            ),
            tokio_postgres::NoTls,
        )
        .await
        .expect("Failed to connect to app_db");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        // Verify connection works
        let rows = client
            .query("SELECT 'app_db_test' as source", &[])
            .await
            .expect("Query on app_db failed");
        assert_eq!(rows.len(), 1);
        let source: &str = rows[0].get(0);
        assert_eq!(source, "app_db_test");

        println!("Successfully connected and queried via app_db route");
    }

    // Test 2: Connect using "analytics_db" logical name
    {
        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user=postgres password=postgres dbname=analytics_db",
                proxy_port
            ),
            tokio_postgres::NoTls,
        )
        .await
        .expect("Failed to connect to analytics_db");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        // Verify connection works
        let rows = client
            .query("SELECT 'analytics_db_test' as source", &[])
            .await
            .expect("Query on analytics_db failed");
        assert_eq!(rows.len(), 1);
        let source: &str = rows[0].get(0);
        assert_eq!(source, "analytics_db_test");

        println!("Successfully connected and queried via analytics_db route");
    }

    // Test 3: Connect using default backend (postgres - the default database)
    {
        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
                proxy_port
            ),
            tokio_postgres::NoTls,
        )
        .await
        .expect("Failed to connect to default database");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        // Verify connection works
        let rows = client
            .query("SELECT 'default_test' as source", &[])
            .await
            .expect("Query on default database failed");
        assert_eq!(rows.len(), 1);
        let source: &str = rows[0].get(0);
        assert_eq!(source, "default_test");

        println!("Successfully connected and queried via default route");
    }

    // Wait for events to be published
    sleep(Duration::from_millis(300)).await;

    // Verify events were captured from all connections
    let events = test_publisher.events();
    println!("Captured {} events from multi-database routing test", events.len());

    // Should have at least 3 SELECT queries (one from each connection)
    let select_events: Vec<_> = events.iter().filter(|e| e.query.contains("SELECT")).collect();
    assert!(
        select_events.len() >= 3,
        "Expected at least 3 SELECT events, got {}",
        select_events.len()
    );

    // Verify we can find events from each database route
    let app_db_event = events.iter().find(|e| e.query.contains("app_db_test"));
    assert!(app_db_event.is_some(), "Expected event from app_db connection");

    let analytics_event = events.iter().find(|e| e.query.contains("analytics_db_test"));
    assert!(analytics_event.is_some(), "Expected event from analytics_db connection");

    let default_event = events.iter().find(|e| e.query.contains("default_test"));
    assert!(default_event.is_some(), "Expected event from default connection");

    println!("Multi-database routing integration test passed");
}
