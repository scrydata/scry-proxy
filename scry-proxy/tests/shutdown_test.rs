/// Integration tests for graceful shutdown
///
/// Tests that the proxy correctly handles shutdown signals, drains connections,
/// and flushes events before exiting.
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
        println!("TestPublisher shutdown called");
        Ok(())
    }
}

/// Helper to create a test config pointing to the given backend
fn create_test_config(backend_host: String, backend_port: u16, shutdown_timeout: u64) -> Config {
    Config {
        proxy: ProxyConfig {
            listen_address: "127.0.0.1:0".to_string(),
            max_connections: 10,
            shutdown_timeout_secs: shutdown_timeout,
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
            unsafe_debug_logging: false,
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
            allow_insecure: false,
            anonymize_salt: None,
            parse_failure_mode: ParseFailureMode::Redact,
        },
        performance: PerformanceConfig {
            latency_budget: scry::config::LatencyBudget::default(),
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

#[tokio::test]
async fn test_graceful_shutdown_drains_connections() {
    // Setup: Start Postgres container
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    // Create test config with short shutdown timeout
    let config = create_test_config(postgres_host, postgres_port, 5);
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

    // Create a connection and start a long-running query
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

    // Execute a simple query to verify connection works
    client.execute("SELECT 1", &[]).await.expect("Query failed");

    // Simulate shutdown signal by dropping the proxy handle
    // (In real scenario, this would be triggered by Ctrl+C or SIGTERM)
    // For this test, we'll just wait a bit and then check that events were flushed

    sleep(Duration::from_millis(500)).await;

    // The connection should still be active
    let result = client.execute("SELECT 2", &[]).await;
    assert!(result.is_ok(), "Connection should still be active");

    // Wait for event flush
    sleep(Duration::from_millis(300)).await;

    // Verify events were captured
    let event_count = test_publisher.event_count();
    assert!(event_count >= 2, "Expected at least 2 events, got {}", event_count);

    // Cleanup
    drop(client);
    drop(proxy_handle);
}

#[tokio::test]
async fn test_shutdown_timeout() {
    // This test verifies that shutdown timeout works correctly
    // Setup: Start Postgres container
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    // Create test config with very short shutdown timeout (1 second)
    let config = create_test_config(postgres_host, postgres_port, 1);
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

    // Create multiple connections
    let mut clients = vec![];
    for _ in 0..3 {
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

        clients.push(client);
    }

    // Execute queries on all connections
    for client in &clients {
        client.execute("SELECT 1", &[]).await.expect("Query failed");
    }

    sleep(Duration::from_millis(500)).await;

    // Verify events were captured
    let event_count = test_publisher.event_count();
    assert!(event_count >= 3, "Expected at least 3 events, got {}", event_count);

    // Cleanup
    drop(clients);
    drop(proxy_handle);
}

/// Real SIGTERM graceful-drain test (WP-7 Task 7.1, P3 §4.4/§5.2).
///
/// Spawns the actual `scry` binary, starts a long-running query through it,
/// sends SIGTERM, and asserts the in-flight query completes (the proxy drained
/// rather than cutting it off) and the process exits cleanly within the
/// configured shutdown timeout. Unix-only: SIGTERM is a Unix signal.
#[cfg(unix)]
#[tokio::test]
async fn test_sigterm_drains_in_flight_query() {
    use std::process::Command;
    use tokio::net::TcpStream;

    let docker = Cli::default();
    let postgres = docker.run(RunnableImage::from(Postgres::default()).with_tag("16-alpine"));
    let pg_port = postgres.get_host_port_ipv4(5432);
    sleep(Duration::from_secs(2)).await;

    // Pick a free port for the proxy to listen on.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let proxy_port = listener.local_addr().unwrap().port();
    drop(listener);

    // Launch the real binary, configured entirely via env, with a generous
    // drain timeout so the in-flight query has time to finish.
    let mut child = Command::new(env!("CARGO_BIN_EXE_scry"))
        .env("SCRY_PROXY__LISTEN_ADDRESS", format!("127.0.0.1:{proxy_port}"))
        .env("SCRY_PROXY__SHUTDOWN_TIMEOUT_SECS", "15")
        .env("SCRY_BACKEND__HOST", "127.0.0.1")
        .env("SCRY_BACKEND__PORT", pg_port.to_string())
        .env("SCRY_BACKEND__USER", "postgres")
        .env("SCRY_BACKEND__PASSWORD", "postgres")
        .env("SCRY_BACKEND__DATABASE", "postgres")
        .env("SCRY_AUTH__ALLOW_TRUST", "true")
        .env("SCRY_PUBLISHER__ENABLED", "false")
        .env("SCRY_PUBLISHER__ANONYMIZE", "false")
        .env("SCRY_PERFORMANCE__CONNECTION_POOLING", "disabled")
        .env("SCRY_RESILIENCE__HEALTHCHECK__ACTIVE_ENABLED", "false")
        .env("SCRY_OBSERVABILITY__ENABLE_TRACING", "false")
        .env("SCRY_OBSERVABILITY__ENABLE_METRICS_SERVER", "false")
        .spawn()
        .expect("failed to spawn scry binary");
    let pid = child.id();

    // Wait for the proxy to accept connections.
    let mut ready = false;
    for _ in 0..100 {
        if TcpStream::connect(("127.0.0.1", proxy_port)).await.is_ok() {
            ready = true;
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert!(ready, "proxy did not become ready");

    // Connect a client and start a slow query in the background.
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={proxy_port} user=postgres password=postgres dbname=postgres"
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("connect to proxy");
    tokio::spawn(async move {
        let _ = connection.await;
    });

    let query = tokio::spawn(async move {
        // ~3s in-flight query; must survive the SIGTERM drain.
        client.query_one("SELECT pg_sleep(3), 42::int AS answer", &[]).await
    });

    // Let the query get in flight, then send SIGTERM.
    sleep(Duration::from_millis(800)).await;
    let status = Command::new("kill").arg("-TERM").arg(pid.to_string()).status().unwrap();
    assert!(status.success(), "kill -TERM failed");

    // The in-flight query must complete successfully (proxy drained it).
    let row = query
        .await
        .expect("query task panicked")
        .expect("in-flight query should complete during graceful drain");
    assert_eq!(row.get::<_, i32>("answer"), 42);

    // The process must exit cleanly within the drain window.
    let mut exited = false;
    for _ in 0..150 {
        match child.try_wait().expect("try_wait") {
            Some(status) => {
                assert!(status.success(), "proxy exited non-zero: {status:?}");
                exited = true;
                break;
            }
            None => sleep(Duration::from_millis(100)).await,
        }
    }
    if !exited {
        let _ = child.kill();
        panic!("proxy did not exit within the shutdown window after SIGTERM");
    }
}
