/// Integration tests for authentication
///
/// These tests verify the proxy's authentication functionality:
/// - Trust mode (no authentication required)
/// - MD5 password authentication with auth_file
/// - Authentication failure handling
use scry::{config::*, observability::*, proxy::*, publisher::*};
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use tempfile::NamedTempFile;
use testcontainers::{clients::Cli, RunnableImage};
use testcontainers_modules::postgres::Postgres;
use tokio::time::sleep;

/// Helper to create a test config pointing to the given backend
fn create_test_config(backend_host: String, backend_port: u16) -> Config {
    Config {
        proxy: ProxyConfig {
            listen_address: "127.0.0.1:0".to_string(),
            max_connections: 10,
            shutdown_timeout_secs: 5,
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
        auth: AuthConfig::default(), // Trust mode by default
    }
}

/// Start the proxy server and return its listen port
async fn start_test_proxy(config: Config) -> anyhow::Result<u16> {
    let publisher = Arc::new(DebugLoggerPublisher::new());
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

#[tokio::test]
async fn test_trust_mode_no_auth_required() {
    // Setup: Start Postgres container
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);

    // Wait for Postgres to be ready
    sleep(Duration::from_secs(2)).await;

    // Create config with trust mode (default)
    let config = create_test_config("127.0.0.1".to_string(), postgres_port);

    // Start proxy
    let proxy_port = start_test_proxy(config.clone()).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(100)).await;

    // Connect without any special credentials - should work in trust mode
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=testuser password=anypassword dbname=postgres",
            proxy_port
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("Failed to connect to proxy in trust mode");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    // Execute a simple query
    let rows = client.query("SELECT 1 as num", &[]).await.expect("Query failed");
    assert_eq!(rows.len(), 1);
    let num: i32 = rows[0].get(0);
    assert_eq!(num, 1);
}

#[tokio::test]
async fn test_md5_auth_success() {
    // Setup: Start Postgres container
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);

    // Wait for Postgres to be ready
    sleep(Duration::from_secs(2)).await;

    // Create auth file with test credentials
    let mut auth_file = NamedTempFile::new().expect("Failed to create temp file");
    writeln!(auth_file, "\"testuser\" \"testpass123\"").unwrap();
    auth_file.flush().unwrap();

    // Create config with MD5 auth
    let mut config = create_test_config("127.0.0.1".to_string(), postgres_port);
    config.auth.auth_type = AuthType::Md5;
    config.auth.auth_file = Some(auth_file.path().to_string_lossy().to_string());

    // Start proxy
    let proxy_port = start_test_proxy(config.clone()).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(100)).await;

    // Connect with correct credentials
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=testuser password=testpass123 dbname=postgres",
            proxy_port
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("Failed to connect with correct credentials");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    // Execute a simple query
    let rows = client.query("SELECT 1 as num", &[]).await.expect("Query failed");
    assert_eq!(rows.len(), 1);
    let num: i32 = rows[0].get(0);
    assert_eq!(num, 1);
}

#[tokio::test]
async fn test_md5_auth_failure_wrong_password() {
    // Setup: Start Postgres container
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);

    // Wait for Postgres to be ready
    sleep(Duration::from_secs(2)).await;

    // Create auth file with test credentials
    let mut auth_file = NamedTempFile::new().expect("Failed to create temp file");
    writeln!(auth_file, "\"testuser\" \"correctpassword\"").unwrap();
    auth_file.flush().unwrap();

    // Create config with MD5 auth
    let mut config = create_test_config("127.0.0.1".to_string(), postgres_port);
    config.auth.auth_type = AuthType::Md5;
    config.auth.auth_file = Some(auth_file.path().to_string_lossy().to_string());

    // Start proxy
    let proxy_port = start_test_proxy(config.clone()).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(100)).await;

    // Connect with wrong password - should fail
    let result = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=testuser password=wrongpassword dbname=postgres",
            proxy_port
        ),
        tokio_postgres::NoTls,
    )
    .await;

    assert!(result.is_err(), "Expected connection to fail with wrong password");
}

#[tokio::test]
async fn test_md5_auth_failure_unknown_user() {
    // Setup: Start Postgres container
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);

    // Wait for Postgres to be ready
    sleep(Duration::from_secs(2)).await;

    // Create auth file with only one user
    let mut auth_file = NamedTempFile::new().expect("Failed to create temp file");
    writeln!(auth_file, "\"knownuser\" \"password\"").unwrap();
    auth_file.flush().unwrap();

    // Create config with MD5 auth
    let mut config = create_test_config("127.0.0.1".to_string(), postgres_port);
    config.auth.auth_type = AuthType::Md5;
    config.auth.auth_file = Some(auth_file.path().to_string_lossy().to_string());

    // Start proxy
    let proxy_port = start_test_proxy(config.clone()).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(100)).await;

    // Connect with unknown user - should fail
    let result = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=unknownuser password=password dbname=postgres",
            proxy_port
        ),
        tokio_postgres::NoTls,
    )
    .await;

    assert!(result.is_err(), "Expected connection to fail with unknown user");
}

#[tokio::test]
async fn test_md5_auth_multiple_users() {
    // Setup: Start Postgres container
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);

    // Wait for Postgres to be ready
    sleep(Duration::from_secs(2)).await;

    // Create auth file with multiple users
    let mut auth_file = NamedTempFile::new().expect("Failed to create temp file");
    writeln!(auth_file, "\"user1\" \"pass1\"").unwrap();
    writeln!(auth_file, "\"user2\" \"pass2\"").unwrap();
    writeln!(auth_file, "; this is a comment").unwrap();
    writeln!(auth_file, "\"user3\" \"pass3\"").unwrap();
    auth_file.flush().unwrap();

    // Create config with MD5 auth
    let mut config = create_test_config("127.0.0.1".to_string(), postgres_port);
    config.auth.auth_type = AuthType::Md5;
    config.auth.auth_file = Some(auth_file.path().to_string_lossy().to_string());

    // Start proxy
    let proxy_port = start_test_proxy(config.clone()).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(100)).await;

    // Test user1
    let (client1, conn1) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=user1 password=pass1 dbname=postgres",
            proxy_port
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("Failed to connect as user1");

    tokio::spawn(async move {
        let _ = conn1.await;
    });

    let rows = client1.query("SELECT 1", &[]).await.expect("Query failed for user1");
    assert_eq!(rows.len(), 1);

    // Test user2
    let (client2, conn2) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=user2 password=pass2 dbname=postgres",
            proxy_port
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("Failed to connect as user2");

    tokio::spawn(async move {
        let _ = conn2.await;
    });

    let rows = client2.query("SELECT 2", &[]).await.expect("Query failed for user2");
    assert_eq!(rows.len(), 1);

    // Test user3
    let (client3, conn3) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=user3 password=pass3 dbname=postgres",
            proxy_port
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("Failed to connect as user3");

    tokio::spawn(async move {
        let _ = conn3.await;
    });

    let rows = client3.query("SELECT 3", &[]).await.expect("Query failed for user3");
    assert_eq!(rows.len(), 1);
}

/// Test that the proxy can authenticate to a backend that requires MD5 password
///
/// This test verifies the BackendAuthenticator works correctly:
/// 1. Starts Postgres with md5 auth (default in testcontainers)
/// 2. Starts the proxy pointing to Postgres with backend credentials
/// 3. Connects a client through the proxy (trust auth to proxy)
/// 4. Verifies queries work (proving proxy authenticated to backend)
#[tokio::test]
async fn test_proxy_with_md5_backend() {
    // Setup: Start Postgres container (uses MD5 or SCRAM by default)
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);

    // Wait for Postgres to be ready
    sleep(Duration::from_secs(2)).await;

    // Create config - client uses trust auth to proxy, proxy uses MD5 to backend
    let mut config = create_test_config("127.0.0.1".to_string(), postgres_port);
    config.auth.auth_type = AuthType::Trust;
    // Backend credentials are already set in create_test_config (postgres/postgres)

    // Start proxy
    let proxy_port = start_test_proxy(config).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(100)).await;

    // Connect through proxy - no password needed (trust mode to proxy)
    // Proxy will authenticate to backend using configured credentials
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user=testuser dbname=postgres",
            proxy_port
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("Failed to connect through proxy");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    // Verify query works - this proves proxy authenticated to backend successfully
    let row = client.query_one("SELECT 1 as value", &[]).await.expect("Query failed");
    let value: i32 = row.get("value");
    assert_eq!(value, 1);

    // Run additional query to verify connection is fully working
    let rows = client
        .query("SELECT current_user, current_database()", &[])
        .await
        .expect("Query failed");
    assert_eq!(rows.len(), 1);
}
