/// Integration tests for stateful connection features
///
/// Tests transaction management, cursors, session variables, and other
/// connection-scoped state to ensure the proxy correctly maintains state
/// across multiple queries on the same connection.

use scry::{config::*, proxy::*, publisher::*};
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

    fn events(&self) -> Vec<QueryEvent> {
        self.events.lock().unwrap().clone()
    }

    fn clear(&self) {
        self.events.lock().unwrap().clear();
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
            listen_address: "127.0.0.1:0".to_string(),
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
            enable_tracing: false,
            otlp_endpoint: None,
            service_name: "scry-test".to_string(),
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

    let server = ProxyServer::new(config.clone(), batcher).await?;
    let port = server.local_addr()?.port();

    tokio::spawn(async move {
        let _ = server.run().await;
    });

    Ok(port)
}

// ============================================================================
// TRANSACTION TESTS
// ============================================================================

#[tokio::test]
async fn test_transaction_commit() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port = start_test_proxy(config.clone(), publisher)
        .await
        .expect("Failed to start proxy");

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

    // Create test table
    client
        .execute("CREATE TEMP TABLE tx_test (id INT, value TEXT)", &[])
        .await
        .expect("Failed to create table");

    // Begin transaction
    client.execute("BEGIN", &[]).await.expect("BEGIN failed");

    // Insert data
    client
        .execute("INSERT INTO tx_test VALUES (1, 'first')", &[])
        .await
        .expect("INSERT failed");

    client
        .execute("INSERT INTO tx_test VALUES (2, 'second')", &[])
        .await
        .expect("INSERT failed");

    // Commit transaction
    client.execute("COMMIT", &[]).await.expect("COMMIT failed");

    // Verify data persisted
    let rows = client
        .query("SELECT COUNT(*) FROM tx_test", &[])
        .await
        .expect("SELECT failed");

    let count: i64 = rows[0].get(0);
    assert_eq!(count, 2, "Expected 2 rows after COMMIT");

    sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
async fn test_transaction_rollback() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port = start_test_proxy(config.clone(), publisher)
        .await
        .expect("Failed to start proxy");

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

    // Create test table
    client
        .execute("CREATE TEMP TABLE tx_test (id INT, value TEXT)", &[])
        .await
        .expect("Failed to create table");

    // Begin transaction
    client.execute("BEGIN", &[]).await.expect("BEGIN failed");

    // Insert data
    client
        .execute("INSERT INTO tx_test VALUES (1, 'first')", &[])
        .await
        .expect("INSERT failed");

    client
        .execute("INSERT INTO tx_test VALUES (2, 'second')", &[])
        .await
        .expect("INSERT failed");

    // Rollback transaction
    client.execute("ROLLBACK", &[]).await.expect("ROLLBACK failed");

    // Verify data was NOT persisted
    let rows = client
        .query("SELECT COUNT(*) FROM tx_test", &[])
        .await
        .expect("SELECT failed");

    let count: i64 = rows[0].get(0);
    assert_eq!(count, 0, "Expected 0 rows after ROLLBACK");

    sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
async fn test_transaction_with_savepoints() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port = start_test_proxy(config.clone(), publisher)
        .await
        .expect("Failed to start proxy");

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

    // Create test table
    client
        .execute("CREATE TEMP TABLE tx_test (id INT, value TEXT)", &[])
        .await
        .expect("Failed to create table");

    // Begin transaction
    client.execute("BEGIN", &[]).await.expect("BEGIN failed");

    // Insert first row
    client
        .execute("INSERT INTO tx_test VALUES (1, 'first')", &[])
        .await
        .expect("INSERT failed");

    // Create savepoint
    client.execute("SAVEPOINT sp1", &[]).await.expect("SAVEPOINT failed");

    // Insert second row
    client
        .execute("INSERT INTO tx_test VALUES (2, 'second')", &[])
        .await
        .expect("INSERT failed");

    // Create another savepoint
    client.execute("SAVEPOINT sp2", &[]).await.expect("SAVEPOINT failed");

    // Insert third row
    client
        .execute("INSERT INTO tx_test VALUES (3, 'third')", &[])
        .await
        .expect("INSERT failed");

    // Rollback to sp2 (removes third row)
    client
        .execute("ROLLBACK TO SAVEPOINT sp2", &[])
        .await
        .expect("ROLLBACK TO SAVEPOINT failed");

    // Commit transaction
    client.execute("COMMIT", &[]).await.expect("COMMIT failed");

    // Verify we have exactly 2 rows (first and second)
    let rows = client
        .query("SELECT id FROM tx_test ORDER BY id", &[])
        .await
        .expect("SELECT failed");

    assert_eq!(rows.len(), 2, "Expected 2 rows after partial rollback");
    let id1: i32 = rows[0].get(0);
    let id2: i32 = rows[1].get(0);
    assert_eq!(id1, 1);
    assert_eq!(id2, 2);

    sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
async fn test_transaction_isolation_levels() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port = start_test_proxy(config.clone(), publisher)
        .await
        .expect("Failed to start proxy");

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

    // Test different isolation levels
    let isolation_levels = vec![
        "READ UNCOMMITTED",
        "READ COMMITTED",
        "REPEATABLE READ",
        "SERIALIZABLE",
    ];

    for level in isolation_levels {
        client.execute("BEGIN", &[]).await.expect("BEGIN failed");

        client
            .execute(&format!("SET TRANSACTION ISOLATION LEVEL {}", level), &[])
            .await
            .expect(&format!("Failed to set isolation level {}", level));

        // Verify we can execute a query in this transaction
        let rows = client
            .query("SELECT 1", &[])
            .await
            .expect("SELECT failed");
        assert_eq!(rows.len(), 1);

        client.execute("COMMIT", &[]).await.expect("COMMIT failed");
    }

    sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
async fn test_nested_transactions_with_release() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port = start_test_proxy(config.clone(), publisher)
        .await
        .expect("Failed to start proxy");

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

    // Create test table
    client
        .execute("CREATE TEMP TABLE tx_test (id INT, value TEXT)", &[])
        .await
        .expect("Failed to create table");

    client.execute("BEGIN", &[]).await.expect("BEGIN failed");

    client
        .execute("INSERT INTO tx_test VALUES (1, 'first')", &[])
        .await
        .expect("INSERT failed");

    client.execute("SAVEPOINT sp1", &[]).await.expect("SAVEPOINT failed");

    client
        .execute("INSERT INTO tx_test VALUES (2, 'second')", &[])
        .await
        .expect("INSERT failed");

    // Release savepoint (makes changes part of parent transaction)
    client
        .execute("RELEASE SAVEPOINT sp1", &[])
        .await
        .expect("RELEASE SAVEPOINT failed");

    // Commit - should include both inserts
    client.execute("COMMIT", &[]).await.expect("COMMIT failed");

    let rows = client
        .query("SELECT COUNT(*) FROM tx_test", &[])
        .await
        .expect("SELECT failed");

    let count: i64 = rows[0].get(0);
    assert_eq!(count, 2, "Expected 2 rows after RELEASE and COMMIT");

    sleep(Duration::from_millis(200)).await;
}

// ============================================================================
// CURSOR TESTS
// ============================================================================

#[tokio::test]
async fn test_cursor_basic_declare_fetch_close() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port = start_test_proxy(config.clone(), publisher)
        .await
        .expect("Failed to start proxy");

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

    // Create test table with data
    client
        .execute("CREATE TEMP TABLE cursor_test (id INT, value TEXT)", &[])
        .await
        .expect("Failed to create table");

    for i in 1..=10 {
        client
            .execute(
                "INSERT INTO cursor_test VALUES ($1, $2)",
                &[&i, &format!("value_{}", i)],
            )
            .await
            .expect("INSERT failed");
    }

    // Begin transaction (required for cursors)
    client.execute("BEGIN", &[]).await.expect("BEGIN failed");

    // Declare cursor
    client
        .execute("DECLARE test_cursor CURSOR FOR SELECT id, value FROM cursor_test ORDER BY id", &[])
        .await
        .expect("DECLARE CURSOR failed");

    // Fetch first 3 rows
    let rows = client
        .query("FETCH 3 FROM test_cursor", &[])
        .await
        .expect("FETCH failed");

    assert_eq!(rows.len(), 3, "Expected 3 rows from FETCH");
    let id1: i32 = rows[0].get(0);
    let id2: i32 = rows[1].get(0);
    let id3: i32 = rows[2].get(0);
    assert_eq!(id1, 1);
    assert_eq!(id2, 2);
    assert_eq!(id3, 3);

    // Fetch next 3 rows
    let rows = client
        .query("FETCH 3 FROM test_cursor", &[])
        .await
        .expect("FETCH failed");

    assert_eq!(rows.len(), 3, "Expected 3 rows from second FETCH");
    let id4: i32 = rows[0].get(0);
    assert_eq!(id4, 4);

    // Close cursor
    client
        .execute("CLOSE test_cursor", &[])
        .await
        .expect("CLOSE failed");

    // Commit transaction
    client.execute("COMMIT", &[]).await.expect("COMMIT failed");

    sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
async fn test_cursor_fetch_directions() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port = start_test_proxy(config.clone(), publisher)
        .await
        .expect("Failed to start proxy");

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

    // Create test table
    client
        .execute("CREATE TEMP TABLE cursor_test (id INT)", &[])
        .await
        .expect("Failed to create table");

    for i in 1..=10 {
        client
            .execute("INSERT INTO cursor_test VALUES ($1)", &[&i])
            .await
            .expect("INSERT failed");
    }

    client.execute("BEGIN", &[]).await.expect("BEGIN failed");

    // Declare scrollable cursor
    client
        .execute("DECLARE test_cursor SCROLL CURSOR FOR SELECT id FROM cursor_test ORDER BY id", &[])
        .await
        .expect("DECLARE CURSOR failed");

    // Fetch forward
    let rows = client
        .query("FETCH FORWARD 3 FROM test_cursor", &[])
        .await
        .expect("FETCH FORWARD failed");
    assert_eq!(rows.len(), 3);
    let id: i32 = rows[2].get(0);
    assert_eq!(id, 3);

    // Fetch backward
    let rows = client
        .query("FETCH BACKWARD 1 FROM test_cursor", &[])
        .await
        .expect("FETCH BACKWARD failed");
    assert_eq!(rows.len(), 1);
    let id: i32 = rows[0].get(0);
    assert_eq!(id, 2);

    // Fetch absolute position
    let rows = client
        .query("FETCH ABSOLUTE 7 FROM test_cursor", &[])
        .await
        .expect("FETCH ABSOLUTE failed");
    assert_eq!(rows.len(), 1);
    let id: i32 = rows[0].get(0);
    assert_eq!(id, 7);

    // Fetch relative
    let rows = client
        .query("FETCH RELATIVE 2 FROM test_cursor", &[])
        .await
        .expect("FETCH RELATIVE failed");
    assert_eq!(rows.len(), 1);
    let id: i32 = rows[0].get(0);
    assert_eq!(id, 9);

    client.execute("CLOSE test_cursor", &[]).await.expect("CLOSE failed");
    client.execute("COMMIT", &[]).await.expect("COMMIT failed");

    sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
async fn test_cursor_rollback_invalidates() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port = start_test_proxy(config.clone(), publisher)
        .await
        .expect("Failed to start proxy");

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

    // Create test table
    client
        .execute("CREATE TEMP TABLE cursor_test (id INT)", &[])
        .await
        .expect("Failed to create table");

    for i in 1..=5 {
        client
            .execute("INSERT INTO cursor_test VALUES ($1)", &[&i])
            .await
            .expect("INSERT failed");
    }

    client.execute("BEGIN", &[]).await.expect("BEGIN failed");

    client
        .execute("DECLARE test_cursor CURSOR FOR SELECT id FROM cursor_test ORDER BY id", &[])
        .await
        .expect("DECLARE CURSOR failed");

    // Fetch some data
    let rows = client
        .query("FETCH 2 FROM test_cursor", &[])
        .await
        .expect("FETCH failed");
    assert_eq!(rows.len(), 2);

    // Rollback transaction
    client.execute("ROLLBACK", &[]).await.expect("ROLLBACK failed");

    // Cursor should now be invalid - attempting to fetch should fail
    let result = client.query("FETCH 1 FROM test_cursor", &[]).await;
    assert!(
        result.is_err(),
        "Expected cursor to be invalid after ROLLBACK"
    );

    sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
async fn test_multiple_concurrent_cursors() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port = start_test_proxy(config.clone(), publisher)
        .await
        .expect("Failed to start proxy");

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

    // Create test tables
    client
        .execute("CREATE TEMP TABLE table1 (id INT)", &[])
        .await
        .expect("Failed to create table1");

    client
        .execute("CREATE TEMP TABLE table2 (id INT)", &[])
        .await
        .expect("Failed to create table2");

    for i in 1..=5 {
        client
            .execute("INSERT INTO table1 VALUES ($1)", &[&i])
            .await
            .expect("INSERT failed");
        client
            .execute("INSERT INTO table2 VALUES ($1)", &[&(i * 10)])
            .await
            .expect("INSERT failed");
    }

    client.execute("BEGIN", &[]).await.expect("BEGIN failed");

    // Declare two cursors
    client
        .execute("DECLARE cursor1 CURSOR FOR SELECT id FROM table1 ORDER BY id", &[])
        .await
        .expect("DECLARE cursor1 failed");

    client
        .execute("DECLARE cursor2 CURSOR FOR SELECT id FROM table2 ORDER BY id", &[])
        .await
        .expect("DECLARE cursor2 failed");

    // Interleave fetches from both cursors
    let rows1 = client
        .query("FETCH 2 FROM cursor1", &[])
        .await
        .expect("FETCH from cursor1 failed");
    assert_eq!(rows1.len(), 2);
    let id: i32 = rows1[0].get(0);
    assert_eq!(id, 1);

    let rows2 = client
        .query("FETCH 2 FROM cursor2", &[])
        .await
        .expect("FETCH from cursor2 failed");
    assert_eq!(rows2.len(), 2);
    let id: i32 = rows2[0].get(0);
    assert_eq!(id, 10);

    let rows1 = client
        .query("FETCH 2 FROM cursor1", &[])
        .await
        .expect("FETCH from cursor1 failed");
    assert_eq!(rows1.len(), 2);
    let id: i32 = rows1[0].get(0);
    assert_eq!(id, 3);

    // Close both cursors
    client.execute("CLOSE cursor1", &[]).await.expect("CLOSE cursor1 failed");
    client.execute("CLOSE cursor2", &[]).await.expect("CLOSE cursor2 failed");

    client.execute("COMMIT", &[]).await.expect("COMMIT failed");

    sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
async fn test_cursor_with_hold() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port = start_test_proxy(config.clone(), publisher)
        .await
        .expect("Failed to start proxy");

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

    // Create test table
    client
        .execute("CREATE TEMP TABLE cursor_test (id INT)", &[])
        .await
        .expect("Failed to create table");

    for i in 1..=5 {
        client
            .execute("INSERT INTO cursor_test VALUES ($1)", &[&i])
            .await
            .expect("INSERT failed");
    }

    client.execute("BEGIN", &[]).await.expect("BEGIN failed");

    // Declare cursor WITH HOLD - survives transaction commit
    client
        .execute("DECLARE test_cursor CURSOR WITH HOLD FOR SELECT id FROM cursor_test ORDER BY id", &[])
        .await
        .expect("DECLARE CURSOR WITH HOLD failed");

    // Fetch some rows
    let rows = client
        .query("FETCH 2 FROM test_cursor", &[])
        .await
        .expect("FETCH failed");
    assert_eq!(rows.len(), 2);

    // Commit transaction
    client.execute("COMMIT", &[]).await.expect("COMMIT failed");

    // Cursor should still be valid and positioned correctly
    let rows = client
        .query("FETCH 2 FROM test_cursor", &[])
        .await
        .expect("FETCH after COMMIT failed");
    assert_eq!(rows.len(), 2);
    let id: i32 = rows[0].get(0);
    assert_eq!(id, 3, "Cursor should continue from position 3");

    // Clean up
    client.execute("CLOSE test_cursor", &[]).await.expect("CLOSE failed");

    sleep(Duration::from_millis(200)).await;
}

// ============================================================================
// SESSION VARIABLES TESTS
// ============================================================================

#[tokio::test]
async fn test_session_variables_persist() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port = start_test_proxy(config.clone(), publisher)
        .await
        .expect("Failed to start proxy");

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

    // Set application_name
    client
        .execute("SET application_name = 'scry_test_app'", &[])
        .await
        .expect("SET application_name failed");

    // Verify it persists
    let rows = client
        .query("SHOW application_name", &[])
        .await
        .expect("SHOW application_name failed");
    let app_name: String = rows[0].get(0);
    assert_eq!(app_name, "scry_test_app");

    // Set statement_timeout
    client
        .execute("SET statement_timeout = '5s'", &[])
        .await
        .expect("SET statement_timeout failed");

    let rows = client
        .query("SHOW statement_timeout", &[])
        .await
        .expect("SHOW statement_timeout failed");
    let timeout: String = rows[0].get(0);
    assert_eq!(timeout, "5s");

    // Set search_path
    client
        .execute("SET search_path = public, pg_catalog", &[])
        .await
        .expect("SET search_path failed");

    let rows = client
        .query("SHOW search_path", &[])
        .await
        .expect("SHOW search_path failed");
    let search_path: String = rows[0].get(0);
    assert!(search_path.contains("public"));

    sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
async fn test_multiple_session_variables() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port = start_test_proxy(config.clone(), publisher)
        .await
        .expect("Failed to start proxy");

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

    // Set multiple variables
    client
        .execute("SET application_name = 'test1'", &[])
        .await
        .expect("SET failed");
    client
        .execute("SET work_mem = '16MB'", &[])
        .await
        .expect("SET failed");
    client
        .execute("SET timezone = 'UTC'", &[])
        .await
        .expect("SET failed");

    // Execute some queries
    client
        .query("SELECT 1", &[])
        .await
        .expect("SELECT failed");

    // Verify all variables still set
    let rows = client.query("SHOW application_name", &[]).await.unwrap();
    let val: String = rows[0].get(0);
    assert_eq!(val, "test1");

    let rows = client.query("SHOW work_mem", &[]).await.unwrap();
    let val: String = rows[0].get(0);
    assert_eq!(val, "16MB");

    let rows = client.query("SHOW timezone", &[]).await.unwrap();
    let val: String = rows[0].get(0);
    assert_eq!(val, "UTC");

    sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
async fn test_session_variable_in_transaction() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port = start_test_proxy(config.clone(), publisher)
        .await
        .expect("Failed to start proxy");

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

    // Set variable outside transaction
    client
        .execute("SET application_name = 'before_tx'", &[])
        .await
        .expect("SET failed");

    // Begin transaction
    client.execute("BEGIN", &[]).await.expect("BEGIN failed");

    // Set LOCAL variable (transaction-scoped)
    client
        .execute("SET LOCAL application_name = 'in_tx'", &[])
        .await
        .expect("SET LOCAL failed");

    let rows = client.query("SHOW application_name", &[]).await.unwrap();
    let val: String = rows[0].get(0);
    assert_eq!(val, "in_tx");

    // Commit
    client.execute("COMMIT", &[]).await.expect("COMMIT failed");

    // Variable should revert to pre-transaction value
    let rows = client.query("SHOW application_name", &[]).await.unwrap();
    let val: String = rows[0].get(0);
    assert_eq!(val, "before_tx");

    sleep(Duration::from_millis(200)).await;
}

// ============================================================================
// TEMPORARY TABLES TESTS
// ============================================================================

#[tokio::test]
async fn test_temp_table_lifecycle() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port = start_test_proxy(config.clone(), publisher)
        .await
        .expect("Failed to start proxy");

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

    // Create temp table
    client
        .execute("CREATE TEMP TABLE my_temp (id INT, data TEXT)", &[])
        .await
        .expect("CREATE TEMP TABLE failed");

    // Insert data
    client
        .execute("INSERT INTO my_temp VALUES (1, 'test')", &[])
        .await
        .expect("INSERT failed");

    // Query in another statement (verifies table persists)
    let rows = client
        .query("SELECT * FROM my_temp", &[])
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);

    // Update data
    client
        .execute("UPDATE my_temp SET data = 'updated' WHERE id = 1", &[])
        .await
        .expect("UPDATE failed");

    let rows = client
        .query("SELECT data FROM my_temp WHERE id = 1", &[])
        .await
        .expect("SELECT failed");
    let data: String = rows[0].get(0);
    assert_eq!(data, "updated");

    // Drop temp table
    client
        .execute("DROP TABLE my_temp", &[])
        .await
        .expect("DROP TABLE failed");

    // Verify table no longer exists
    let result = client.query("SELECT * FROM my_temp", &[]).await;
    assert!(result.is_err(), "Expected table to not exist");

    sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
async fn test_temp_table_on_commit_delete_rows() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port = start_test_proxy(config.clone(), publisher)
        .await
        .expect("Failed to start proxy");

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

    // Create temp table with ON COMMIT DELETE ROWS
    client
        .execute(
            "CREATE TEMP TABLE my_temp (id INT) ON COMMIT DELETE ROWS",
            &[],
        )
        .await
        .expect("CREATE TEMP TABLE failed");

    // Begin transaction
    client.execute("BEGIN", &[]).await.expect("BEGIN failed");

    // Insert data
    client
        .execute("INSERT INTO my_temp VALUES (1), (2), (3)", &[])
        .await
        .expect("INSERT failed");

    let rows = client
        .query("SELECT COUNT(*) FROM my_temp", &[])
        .await
        .expect("SELECT failed");
    let count: i64 = rows[0].get(0);
    assert_eq!(count, 3);

    // Commit - should delete rows
    client.execute("COMMIT", &[]).await.expect("COMMIT failed");

    // Verify rows deleted
    let rows = client
        .query("SELECT COUNT(*) FROM my_temp", &[])
        .await
        .expect("SELECT failed");
    let count: i64 = rows[0].get(0);
    assert_eq!(count, 0);

    sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
async fn test_temp_table_on_commit_drop() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port = start_test_proxy(config.clone(), publisher)
        .await
        .expect("Failed to start proxy");

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

    // Begin transaction
    client.execute("BEGIN", &[]).await.expect("BEGIN failed");

    // Create temp table with ON COMMIT DROP (must be in transaction)
    client
        .execute("CREATE TEMP TABLE my_temp (id INT) ON COMMIT DROP", &[])
        .await
        .expect("CREATE TEMP TABLE failed");

    // Insert data
    client
        .execute("INSERT INTO my_temp VALUES (1)", &[])
        .await
        .expect("INSERT failed");

    // Commit - should drop table
    client.execute("COMMIT", &[]).await.expect("COMMIT failed");

    // Verify table dropped
    let result = client.query("SELECT * FROM my_temp", &[]).await;
    assert!(result.is_err(), "Expected table to be dropped");

    sleep(Duration::from_millis(200)).await;
}

// ============================================================================
// ADVISORY LOCKS TESTS
// ============================================================================

#[tokio::test]
async fn test_advisory_lock_basic() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port = start_test_proxy(config.clone(), publisher)
        .await
        .expect("Failed to start proxy");

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

    // Acquire advisory lock
    let rows = client
        .query("SELECT pg_advisory_lock(12345)", &[])
        .await
        .expect("pg_advisory_lock failed");
    assert_eq!(rows.len(), 1);

    // Execute query while holding lock
    let rows = client
        .query("SELECT 1", &[])
        .await
        .expect("SELECT failed");
    assert_eq!(rows.len(), 1);

    // Unlock
    let rows = client
        .query("SELECT pg_advisory_unlock(12345)", &[])
        .await
        .expect("pg_advisory_unlock failed");
    let unlocked: bool = rows[0].get(0);
    assert!(unlocked, "Expected successful unlock");

    // Try to lock again after unlocking - should succeed
    let rows = client
        .query("SELECT pg_try_advisory_lock(12345)", &[])
        .await
        .expect("pg_try_advisory_lock failed");
    let locked: bool = rows[0].get(0);
    assert!(locked, "Should successfully acquire lock after unlocking");

    // Clean up
    client
        .query("SELECT pg_advisory_unlock(12345)", &[])
        .await
        .expect("pg_advisory_unlock failed");

    sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
async fn test_advisory_lock_conflict() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port = start_test_proxy(config.clone(), publisher)
        .await
        .expect("Failed to start proxy");

    sleep(Duration::from_millis(100)).await;

    // Create two separate connections
    let (client1, connection1) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user={} password={} dbname={}",
            proxy_port, config.backend.user, config.backend.password, config.backend.database
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("Failed to connect client1");

    tokio::spawn(async move {
        if let Err(e) = connection1.await {
            eprintln!("Connection1 error: {}", e);
        }
    });

    let (client2, connection2) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user={} password={} dbname={}",
            proxy_port, config.backend.user, config.backend.password, config.backend.database
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("Failed to connect client2");

    tokio::spawn(async move {
        if let Err(e) = connection2.await {
            eprintln!("Connection2 error: {}", e);
        }
    });

    // Client1 acquires lock
    client1
        .query("SELECT pg_advisory_lock(99999)", &[])
        .await
        .expect("pg_advisory_lock failed");

    // Client2 tries to acquire same lock (should fail with try variant)
    let rows = client2
        .query("SELECT pg_try_advisory_lock(99999)", &[])
        .await
        .expect("pg_try_advisory_lock failed");
    let locked: bool = rows[0].get(0);
    assert!(!locked, "Client2 should not acquire lock held by client1");

    // Client1 unlocks
    client1
        .query("SELECT pg_advisory_unlock(99999)", &[])
        .await
        .expect("pg_advisory_unlock failed");

    // Now client2 can acquire
    let rows = client2
        .query("SELECT pg_try_advisory_lock(99999)", &[])
        .await
        .expect("pg_try_advisory_lock failed");
    let locked: bool = rows[0].get(0);
    assert!(locked, "Client2 should acquire lock after client1 unlocks");

    // Cleanup
    client2
        .query("SELECT pg_advisory_unlock(99999)", &[])
        .await
        .expect("pg_advisory_unlock failed");

    sleep(Duration::from_millis(200)).await;
}

#[tokio::test]
async fn test_advisory_lock_transaction_scoped() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port = start_test_proxy(config.clone(), publisher)
        .await
        .expect("Failed to start proxy");

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

    // Begin transaction
    client.execute("BEGIN", &[]).await.expect("BEGIN failed");

    // Use transaction-level lock (automatically released on commit/rollback)
    client
        .query("SELECT pg_advisory_xact_lock(55555)", &[])
        .await
        .expect("pg_advisory_xact_lock failed");

    // Do some work
    client
        .query("SELECT 1", &[])
        .await
        .expect("SELECT failed");

    // Commit - lock should auto-release
    client.execute("COMMIT", &[]).await.expect("COMMIT failed");

    // Start new transaction and try to acquire same lock - should succeed
    client.execute("BEGIN", &[]).await.expect("BEGIN failed");

    let rows = client
        .query("SELECT pg_try_advisory_xact_lock(55555)", &[])
        .await
        .expect("pg_try_advisory_xact_lock failed");
    let locked: bool = rows[0].get(0);
    assert!(locked, "Lock should be available after previous transaction");

    client.execute("COMMIT", &[]).await.expect("COMMIT failed");

    sleep(Duration::from_millis(200)).await;
}

// ============================================================================
// LISTEN/NOTIFY TESTS
// ============================================================================

#[tokio::test]
async fn test_listen_notify_commands() {
    // This test verifies that LISTEN/NOTIFY commands execute successfully through the proxy
    // Full notification delivery testing would require complex async message handling
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(postgres_host, postgres_port);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port = start_test_proxy(config.clone(), publisher)
        .await
        .expect("Failed to start proxy");

    sleep(Duration::from_millis(100)).await;

    // Connection 1: Listener
    let (listener_client, listener_connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user={} password={} dbname={}",
            proxy_port, config.backend.user, config.backend.password, config.backend.database
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("Failed to connect listener");

    tokio::spawn(async move {
        if let Err(e) = listener_connection.await {
            eprintln!("Listener connection error: {}", e);
        }
    });

    // Connection 2: Notifier
    let (notifier_client, notifier_connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user={} password={} dbname={}",
            proxy_port, config.backend.user, config.backend.password, config.backend.database
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("Failed to connect notifier");

    tokio::spawn(async move {
        if let Err(e) = notifier_connection.await {
            eprintln!("Notifier connection error: {}", e);
        }
    });

    // Test LISTEN command
    listener_client
        .execute("LISTEN test_channel", &[])
        .await
        .expect("LISTEN command failed");

    // Test LISTEN on multiple channels
    listener_client
        .execute("LISTEN channel1", &[])
        .await
        .expect("LISTEN channel1 failed");

    listener_client
        .execute("LISTEN channel2", &[])
        .await
        .expect("LISTEN channel2 failed");

    // Test NOTIFY command
    notifier_client
        .execute("NOTIFY test_channel, 'test message'", &[])
        .await
        .expect("NOTIFY failed");

    // Test UNLISTEN command
    listener_client
        .execute("UNLISTEN test_channel", &[])
        .await
        .expect("UNLISTEN failed");

    // Test UNLISTEN *
    listener_client
        .execute("UNLISTEN *", &[])
        .await
        .expect("UNLISTEN * failed");

    sleep(Duration::from_millis(200)).await;
    // If we got here, all LISTEN/NOTIFY commands executed successfully through the proxy
}

// Note: Full LISTEN/NOTIFY notification delivery testing would require more complex
// async message handling with tokio-postgres. The test above verifies that the proxy
// correctly forwards LISTEN/NOTIFY protocol messages. Since the proxy uses transparent
// message forwarding, notifications should be delivered correctly to clients.
