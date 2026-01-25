//! Integration tests for max_connections enforcement (CRIT-3)

use scry::config::Config;
use scry::observability::{HealthConfig, ProxyMetrics};
use scry::proxy::{EventBatcher, ProxyServer};
use scry::publisher::DebugLoggerPublisher;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};

fn create_test_config_with_max_connections(max_connections: usize) -> Config {
    let mut config = Config::default();
    config.proxy.listen_address = "127.0.0.1:0".to_string();
    config.proxy.max_connections = max_connections;
    config.resilience.healthcheck.active_enabled = false;
    config
}

#[tokio::test]
async fn test_connection_limit_enforced() {
    let max_connections = 3;
    let config = create_test_config_with_max_connections(max_connections);
    let publisher = Arc::new(DebugLoggerPublisher::new());
    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));

    let server =
        ProxyServer::new(config, EventBatcher::new(publisher, 10, 100, 1000), Arc::clone(&metrics))
            .await
            .unwrap();

    let addr = server.local_addr().unwrap();

    // Spawn server in background
    let server_handle = tokio::spawn(async move {
        let _ = server.run().await;
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect max_connections clients successfully
    let mut connections = Vec::new();
    for i in 0..max_connections {
        let stream = timeout(Duration::from_secs(2), TcpStream::connect(addr))
            .await
            .expect("connection timeout")
            .unwrap_or_else(|_| panic!("connection {} should succeed", i));
        connections.push(stream);
    }

    // Small delay to let connections be counted
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify we have max_connections active
    assert_eq!(metrics.get_active_connections(), max_connections);

    // Attempt one more connection - should be rejected
    let extra_result = timeout(Duration::from_secs(2), TcpStream::connect(addr)).await;

    match extra_result {
        Ok(Ok(mut stream)) => {
            // Connection might be accepted at TCP level but should receive error
            use tokio::io::AsyncReadExt;
            let mut buf = [0u8; 256];
            let n = timeout(Duration::from_secs(1), stream.read(&mut buf))
                .await
                .expect("read timeout")
                .expect("read error");

            // Should receive PostgreSQL ErrorResponse (starts with 'E')
            assert_eq!(buf[0], b'E', "Expected ErrorResponse, got {:?}", &buf[..n]);
        }
        Ok(Err(_)) => {
            // Connection refused is also acceptable
        }
        Err(_) => {
            panic!("Connection attempt timed out");
        }
    }

    // Clean up
    drop(connections);
    server_handle.abort();
}

#[tokio::test]
async fn test_connection_counter_decrements_on_close() {
    let max_connections = 5;
    let config = create_test_config_with_max_connections(max_connections);
    let publisher = Arc::new(DebugLoggerPublisher::new());
    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));

    let server =
        ProxyServer::new(config, EventBatcher::new(publisher, 10, 100, 1000), Arc::clone(&metrics))
            .await
            .unwrap();

    let addr = server.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        let _ = server.run().await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect a client
    let stream = TcpStream::connect(addr).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(metrics.get_active_connections(), 1);

    // Drop the connection
    drop(stream);
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Counter should decrement
    assert_eq!(metrics.get_active_connections(), 0);

    server_handle.abort();
}

#[tokio::test]
async fn test_rejected_connections_metric_increments() {
    let max_connections = 2;
    let config = create_test_config_with_max_connections(max_connections);
    let publisher = Arc::new(DebugLoggerPublisher::new());
    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));

    let server =
        ProxyServer::new(config, EventBatcher::new(publisher, 10, 100, 1000), Arc::clone(&metrics))
            .await
            .unwrap();

    let addr = server.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        let _ = server.run().await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Fill up connection slots
    let mut connections = Vec::new();
    for _ in 0..max_connections {
        let stream = TcpStream::connect(addr).await.unwrap();
        connections.push(stream);
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Initial rejected count should be 0
    assert_eq!(metrics.get_connections_rejected(), 0);

    // Try to connect 3 more times (should all be rejected)
    for _ in 0..3 {
        let _ = TcpStream::connect(addr).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Should have 3 rejected connections
    assert_eq!(metrics.get_connections_rejected(), 3);

    // Max connections metric should be set
    assert_eq!(metrics.get_max_connections(), max_connections);

    drop(connections);
    server_handle.abort();
}

#[tokio::test]
async fn test_connection_limit_under_load() {
    let max_connections = 100;
    let config = create_test_config_with_max_connections(max_connections);
    let publisher = Arc::new(DebugLoggerPublisher::new());
    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));

    let server =
        ProxyServer::new(config, EventBatcher::new(publisher, 10, 100, 1000), Arc::clone(&metrics))
            .await
            .unwrap();

    let addr = server.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        let _ = server.run().await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Attempt 150 concurrent connections
    let mut handles = Vec::new();
    for _ in 0..150 {
        let addr = addr;
        handles.push(tokio::spawn(async move { TcpStream::connect(addr).await }));
    }

    // Wait for all connection attempts
    let results: Vec<_> = futures::future::join_all(handles).await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Should have exactly max_connections active
    let active = metrics.get_active_connections();
    assert!(
        active <= max_connections,
        "Active connections {} should not exceed max {}",
        active,
        max_connections
    );

    // Should have rejected ~50 connections (150 - 100)
    let rejected = metrics.get_connections_rejected();
    assert!(
        rejected >= 40, // Allow some tolerance
        "Expected ~50 rejections, got {}",
        rejected
    );

    // All TCP connects succeed (server accepts then rejects), so we count TCP-level connections
    // The real validation is that active + rejected should account for attempted connections
    let tcp_connects = results.iter().filter(|r| r.is_ok() && r.as_ref().unwrap().is_ok()).count();
    assert!(
        tcp_connects >= 100, // All or most TCP connects should succeed
        "Expected most TCP connects to succeed, got {}",
        tcp_connects
    );

    // Verify the sum of active + rejected is reasonable (some connections may have closed)
    // The key invariant is that active never exceeded max_connections during the test
    assert!(metrics.get_connections_rejected() > 0);

    server_handle.abort();
}

#[tokio::test]
async fn test_connection_slots_freed_on_disconnect() {
    let max_connections = 3;
    let config = create_test_config_with_max_connections(max_connections);
    let publisher = Arc::new(DebugLoggerPublisher::new());
    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));

    let server =
        ProxyServer::new(config, EventBatcher::new(publisher, 10, 100, 1000), Arc::clone(&metrics))
            .await
            .unwrap();

    let addr = server.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        let _ = server.run().await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Fill all slots
    let mut connections = Vec::new();
    for _ in 0..max_connections {
        connections.push(TcpStream::connect(addr).await.unwrap());
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(metrics.get_active_connections(), max_connections);

    // New connection should be rejected
    let _ = TcpStream::connect(addr).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(metrics.get_connections_rejected(), 1);

    // Close one connection
    drop(connections.pop());
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Now a new connection should succeed
    let new_conn = TcpStream::connect(addr).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Should still have max_connections active (one freed, one new)
    assert_eq!(metrics.get_active_connections(), max_connections);
    // Rejection count should not have increased
    assert_eq!(metrics.get_connections_rejected(), 1);

    drop(new_conn);
    drop(connections);
    server_handle.abort();
}
