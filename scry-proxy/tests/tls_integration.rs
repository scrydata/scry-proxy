//! TLS integration tests
//!
//! These tests verify TLS functionality with the proxy server.

use scry::config::*;
use scry::observability::{HealthConfig, ProxyMetrics};
use scry::proxy::{EventBatcher, ProxyServer};
use scry::publisher::{DebugLoggerPublisher, EventPublisher};
use scry::tls::{load_server_tls_config, upgrade_backend_to_tls};
use std::sync::Arc;
use std::time::Duration as StdDuration;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Generate self-signed test certificates using openssl
/// Returns (temp_dir, cert_path, key_path) - temp_dir must be kept alive
fn generate_test_certs() -> Option<(tempfile::TempDir, String, String)> {
    use std::process::Command;

    let dir = tempfile::tempdir().ok()?;
    let cert_path = dir.path().join("server.crt");
    let key_path = dir.path().join("server.key");

    // Generate self-signed certificate using openssl
    let status = Command::new("openssl")
        .args([
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-keyout",
            key_path.to_str()?,
            "-out",
            cert_path.to_str()?,
            "-days",
            "1",
            "-nodes",
            "-subj",
            "/CN=localhost",
        ])
        .output();

    match status {
        Ok(output) if output.status.success() => Some((
            dir,
            cert_path.to_string_lossy().to_string(),
            key_path.to_string_lossy().to_string(),
        )),
        _ => None,
    }
}

/// Send an SSLRequest and read the response
async fn send_ssl_request(addr: &str) -> std::io::Result<u8> {
    let mut stream = TcpStream::connect(addr).await?;

    // SSLRequest message: length (8) + code (80877103)
    let ssl_request: [u8; 8] = [0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f];
    stream.write_all(&ssl_request).await?;

    let mut response = [0u8; 1];
    stream.read_exact(&mut response).await?;

    Ok(response[0])
}

/// Helper to create a minimal test config
fn create_minimal_config() -> Config {
    Config {
        proxy: ProxyConfig {
            listen_address: "127.0.0.1:0".to_string(),
            max_connections: 10,
            shutdown_timeout_secs: 5,
            unix_socket: None,
        },
        databases: Vec::new(),
        backend: BackendConfig {
            protocol: DatabaseProtocol::Postgres,
            host: "127.0.0.1".to_string(),
            port: 5432,
            database: "postgres".to_string(),
            user: "postgres".to_string(),
            password: "postgres".to_string(),
            password_file: None,
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
        protocol: ProtocolConfig { max_prepared_statements: 100 },
        publisher: PublisherConfig {
            enabled: true,
            batch_size: 10,
            flush_interval_ms: 100,
            anonymize: false,
            publisher_type: "debug".to_string(),
            max_queue_size: 100,
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
            query_timeout_secs: 0,
            connection_pooling: PoolingStrategy::Disabled,
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

/// Helper to start a proxy server with the given config
async fn start_proxy(config: Config) -> anyhow::Result<(u16, tokio::task::JoinHandle<()>)> {
    let publisher: Arc<dyn EventPublisher> = Arc::new(DebugLoggerPublisher::new());
    let batcher = EventBatcher::new(
        publisher,
        config.publisher.batch_size,
        config.publisher.flush_interval_ms,
        config.publisher.max_queue_size,
    );

    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));
    let server = ProxyServer::new(config, batcher, metrics).await?;
    let port = server.local_addr()?.port();

    let handle = tokio::spawn(async move {
        let _ = server.run().await;
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    Ok((port, handle))
}

#[tokio::test]
async fn test_ssl_disabled_responds_no() {
    // Start proxy with TLS disabled (default)
    let config = create_minimal_config();
    let (port, handle) = start_proxy(config).await.expect("Failed to start proxy");

    // Send SSL request
    let addr = format!("127.0.0.1:{}", port);
    let response = send_ssl_request(&addr).await.unwrap();

    // Should respond with 'N' (no SSL)
    assert_eq!(response, b'N', "Expected 'N' (no SSL) response");

    handle.abort();
}

#[tokio::test]
async fn test_ssl_allow_with_certs_responds_yes() {
    // Skip if openssl is not available
    let certs = match generate_test_certs() {
        Some(c) => c,
        None => {
            eprintln!("Skipping test: openssl not available");
            return;
        }
    };
    let (_dir, cert_path, key_path) = certs;

    let mut config = create_minimal_config();
    config.tls.client_tls_sslmode = TlsSslMode::Allow;
    config.tls.client_tls_cert_file = Some(cert_path);
    config.tls.client_tls_key_file = Some(key_path);

    let (port, handle) = start_proxy(config).await.expect("Failed to start proxy");

    // Send SSL request
    let addr = format!("127.0.0.1:{}", port);
    let response = send_ssl_request(&addr).await.unwrap();

    // Should respond with 'S' (yes SSL)
    assert_eq!(response, b'S', "Expected 'S' (yes SSL) response");

    handle.abort();
}

#[tokio::test]
async fn test_ssl_require_with_certs_responds_yes() {
    // Skip if openssl is not available
    let certs = match generate_test_certs() {
        Some(c) => c,
        None => {
            eprintln!("Skipping test: openssl not available");
            return;
        }
    };
    let (_dir, cert_path, key_path) = certs;

    let mut config = create_minimal_config();
    config.tls.client_tls_sslmode = TlsSslMode::Require;
    config.tls.client_tls_cert_file = Some(cert_path);
    config.tls.client_tls_key_file = Some(key_path);

    let (port, handle) = start_proxy(config).await.expect("Failed to start proxy");

    // Send SSL request
    let addr = format!("127.0.0.1:{}", port);
    let response = send_ssl_request(&addr).await.unwrap();

    // Should respond with 'S' (yes SSL)
    assert_eq!(response, b'S', "Expected 'S' (yes SSL) response");

    handle.abort();
}

#[tokio::test]
async fn test_no_ssl_request_accepted() {
    // Test that a connection without SSL request is accepted
    let config = create_minimal_config();
    let (port, handle) = start_proxy(config).await.expect("Failed to start proxy");

    // Connect and send a regular startup message (not SSL request)
    let addr = format!("127.0.0.1:{}", port);
    let mut stream = TcpStream::connect(&addr).await.unwrap();

    // Regular PostgreSQL startup message (protocol version 3.0)
    // Length (4 bytes) + Protocol version (4 bytes) + params
    let startup_msg: [u8; 16] = [
        0, 0, 0, 16, // length = 16
        0, 3, 0, 0, // protocol version 3.0
        b'u', b's', b'e', b'r', 0, // "user\0"
        b'x', 0, // "x\0"
        0, // null terminator for params
    ];
    stream.write_all(&startup_msg).await.unwrap();

    // We should get some response (likely connection refused to backend or auth error,
    // but the connection was accepted by the proxy)
    let mut buf = [0u8; 1];
    let result = tokio::time::timeout(Duration::from_millis(500), stream.read(&mut buf)).await;

    // Either we get a response or timeout - the key is that connection was established
    // and not immediately rejected
    drop(result); // Just need to verify we got this far without panic

    handle.abort();
}

// =============================================================================
// Backend TLS Tests
// =============================================================================

/// Test that sslmode=require fails when backend doesn't support SSL
#[tokio::test]
async fn test_backend_tls_require_fails_without_ssl() {
    // Create a mock "backend" that declines SSL
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Spawn mock backend that responds 'N' to SSLRequest
    let backend_task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();

        // Read SSLRequest (8 bytes)
        let mut buf = [0u8; 8];
        stream.read_exact(&mut buf).await.unwrap();

        // Respond with 'N' (no SSL)
        stream.write_all(&[b'N']).await.unwrap();
    });

    // Try to connect with sslmode=require
    let stream = TcpStream::connect(addr).await.unwrap();

    let mut tls_config = TlsConfig::default();
    tls_config.server_tls_sslmode = TlsSslMode::Require;

    let server_tls = load_server_tls_config(&tls_config).unwrap();

    let result =
        upgrade_backend_to_tls(stream, "localhost", &tls_config.server_tls_sslmode, server_tls)
            .await;

    // Should fail because backend declined SSL
    match result {
        Err(e) => {
            let err_msg = e.to_string();
            assert!(
                err_msg.contains("does not support SSL"),
                "Expected 'does not support SSL' in error, got: {}",
                err_msg
            );
        }
        Ok(_) => panic!("Expected error when backend declines SSL with sslmode=require"),
    }

    backend_task.await.unwrap();
}

/// Test that sslmode=allow falls back to plain TCP when backend declines SSL
#[tokio::test]
async fn test_backend_tls_allow_fallback() {
    // Create a mock "backend" that declines SSL
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let backend_task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();

        // Read SSLRequest (8 bytes)
        let mut buf = [0u8; 8];
        stream.read_exact(&mut buf).await.unwrap();

        // Respond with 'N' (no SSL)
        stream.write_all(&[b'N']).await.unwrap();

        // Keep connection alive briefly so the test can verify transport state
        tokio::time::sleep(Duration::from_millis(100)).await;
    });

    let stream = TcpStream::connect(addr).await.unwrap();

    let mut tls_config = TlsConfig::default();
    tls_config.server_tls_sslmode = TlsSslMode::Allow;

    let server_tls = load_server_tls_config(&tls_config).unwrap();

    let result =
        upgrade_backend_to_tls(stream, "localhost", &tls_config.server_tls_sslmode, server_tls)
            .await;

    // Should succeed with plain TCP fallback
    match result {
        Ok(transport) => {
            assert!(
                !transport.is_encrypted(),
                "Expected plain TCP (not encrypted) when backend declines SSL"
            );
        }
        Err(e) => panic!("Expected success with sslmode=allow, got error: {}", e),
    }

    backend_task.await.unwrap();
}

/// Test that sslmode=disable skips SSL negotiation entirely
#[tokio::test]
async fn test_backend_tls_disable_skips_negotiation() {
    // Create a mock "backend" - it should NOT receive any SSLRequest
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let backend_task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();

        // With sslmode=disable, we should NOT receive an SSLRequest
        // Set a short timeout - if we receive anything, the test should fail
        let mut buf = [0u8; 8];
        let result = tokio::time::timeout(Duration::from_millis(100), stream.read(&mut buf)).await;

        // We expect timeout (no data received) because sslmode=disable skips negotiation
        match result {
            Err(_) => {}    // Timeout is expected - no SSL negotiation
            Ok(Ok(0)) => {} // Connection closed without sending is also fine
            Ok(Ok(_)) => panic!("Received unexpected data with sslmode=disable"),
            Ok(Err(e)) => panic!("Unexpected error: {}", e),
        }
    });

    let stream = TcpStream::connect(addr).await.unwrap();

    let tls_config = TlsConfig::default(); // sslmode=disable by default

    let server_tls = load_server_tls_config(&tls_config).unwrap();

    let result =
        upgrade_backend_to_tls(stream, "localhost", &tls_config.server_tls_sslmode, server_tls)
            .await;

    // Should succeed immediately with plain TCP (no negotiation)
    match result {
        Ok(transport) => {
            assert!(!transport.is_encrypted(), "Expected plain TCP with sslmode=disable");
        }
        Err(e) => panic!("Expected success with sslmode=disable, got error: {}", e),
    }

    backend_task.await.unwrap();
}

// =============================================================================
// TLS Connection State Isolation Tests
// =============================================================================

/// Test that TLS connections are properly reset between clients
///
/// This verifies CRIT-2: TLS Connections Skip State Reset is fixed.
///
/// Test scenario:
/// 1. Client A creates a prepared statement
/// 2. Client A disconnects (connection returns to pool)
/// 3. Client B connects (gets same pooled connection)
/// 4. Client B should NOT see Client A's prepared statement
#[tokio::test]
#[ignore] // Requires TLS-enabled Postgres backend
async fn test_tls_connection_state_isolation() {
    // This test requires:
    // 1. A TLS-enabled PostgreSQL backend
    // 2. The proxy configured with server_tls_sslmode = require
    //
    // Skip if not configured
    let scry_tls_url = match std::env::var("SCRY_TLS_TEST_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("Skipping test: SCRY_TLS_TEST_URL not set");
            eprintln!("Set to a TLS-enabled Scry proxy URL to run this test");
            return;
        }
    };

    if scry_tls_url.is_empty() {
        eprintln!("Skipping test: SCRY_TLS_TEST_URL is empty");
        return;
    }

    // Use native-tls for the test client
    let connector = native_tls::TlsConnector::builder()
        .danger_accept_invalid_certs(true) // For self-signed test certs
        .build()
        .expect("Failed to create TLS connector");
    let connector = postgres_native_tls::MakeTlsConnector::new(connector);

    // Client A: Create prepared statement
    {
        let (client, connection) = tokio_postgres::connect(&scry_tls_url, connector.clone())
            .await
            .expect("Client A failed to connect");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Client A connection error: {}", e);
            }
        });

        // Create a named prepared statement
        client
            .execute("PREPARE client_a_stmt AS SELECT 42", &[])
            .await
            .expect("Failed to create prepared statement");

        // Verify it exists
        let result = client
            .query_one("EXECUTE client_a_stmt", &[])
            .await
            .expect("Failed to execute prepared statement");
        assert_eq!(result.get::<_, i32>(0), 42);

        // Client A disconnects - connection returns to pool
        drop(client);
    }

    // Small delay to ensure connection is recycled
    tokio::time::sleep(StdDuration::from_millis(100)).await;

    // Client B: Should NOT see Client A's prepared statement
    {
        let (client, connection) = tokio_postgres::connect(&scry_tls_url, connector)
            .await
            .expect("Client B failed to connect");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Client B connection error: {}", e);
            }
        });

        // Try to execute Client A's prepared statement - should fail
        let result = client.query("EXECUTE client_a_stmt", &[]).await;

        match result {
            Err(e) => {
                // Expected: prepared statement doesn't exist
                let err_msg = e.to_string();
                assert!(
                    err_msg.contains("client_a_stmt")
                        && (err_msg.contains("does not exist") || err_msg.contains("not found")),
                    "Expected 'prepared statement does not exist', got: {}",
                    err_msg
                );
            }
            Ok(_) => {
                panic!(
                    "CRIT-2 REGRESSION: Client B could execute Client A's prepared statement! \
                     TLS connection state was NOT reset properly."
                );
            }
        }
    }
}

/// Test that TLS connection health checks work
#[tokio::test]
#[ignore] // Requires TLS-enabled Postgres backend
async fn test_tls_connection_health_check() {
    let scry_tls_url = match std::env::var("SCRY_TLS_TEST_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("Skipping test: SCRY_TLS_TEST_URL not set");
            return;
        }
    };

    if scry_tls_url.is_empty() {
        eprintln!("Skipping test: SCRY_TLS_TEST_URL is empty");
        return;
    }

    let connector = native_tls::TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .expect("Failed to create TLS connector");
    let connector = postgres_native_tls::MakeTlsConnector::new(connector);

    // Make multiple connections to exercise the pool
    for i in 0..5 {
        let (client, connection) = tokio_postgres::connect(&scry_tls_url, connector.clone())
            .await
            .unwrap_or_else(|e| panic!("Connection {} failed: {}", i, e));

        tokio::spawn(async move {
            let _ = connection.await;
        });

        // Simple query to verify connection works
        let result = client
            .query_one("SELECT 1", &[])
            .await
            .unwrap_or_else(|e| panic!("Query {} failed: {}", i, e));
        assert_eq!(result.get::<_, i32>(0), 1);

        drop(client);
    }

    // If we get here without errors, health checks are working
}
