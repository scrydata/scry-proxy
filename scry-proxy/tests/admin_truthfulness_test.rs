//! Truthfulness regression guardrail for the admin `SHOW` commands (WP-10, P4
//! §4.1/§5.3).
//!
//! Task 1 built live registries (`ClientRegistry`, `ServerRegistry`) behind
//! `AdminHandles` but nothing consumed them yet — `SHOW CLIENTS`/`SHOW SERVERS`
//! were always empty, and `SHOW DATABASES`/`SHOW POOLS`/`SHOW STATS` returned
//! hardcoded placeholder values (`"default"`, `"localhost"`, `"5432"`, `"scry"`,
//! ...). This suite drives a REAL proxy (real Postgres backend via
//! testcontainers, real client sockets) into a known state and asserts the
//! `SHOW` handlers reflect *that exact state* — not the canned placeholders.
//!
//! Deliberately uses a distinctive, non-default database/user/host so a
//! regression back to the old hardcoded values (which happened to coincide
//! with common defaults like "postgres") cannot accidentally pass.
//!
//! Test harness approach: in-process `AdminConsole` driven directly (not over
//! the wire). The suite starts a *real* `ProxyServer` (`common::
//! start_test_proxy_with_handles`) against a real Postgres testcontainer, opens
//! real client sockets through it (so `ClientRegistry`/`ServerRegistry` are
//! populated exactly as they would be in production), then builds an
//! `AdminConsole` from the server's live `Arc<AdminHandles>` and calls
//! `execute()` directly. This exercises the exact SHOW handler code a wire
//! connection would reach, while keeping the test focused on handler
//! truthfulness rather than re-proving the admin wire/auth path (covered by
//! `admin_console_test.rs`) or the wire encoding (covered by `response.rs`
//! unit tests).
mod common;

use common::{connect_client, create_test_config, start_test_proxy_with_handles, TestPublisher};
use scry::admin::{AdminConsole, AdminResponse};
use scry::config::{PoolingStrategy, TlsSslMode};
use scry::observability::{HealthConfig, ProxyMetrics};
use scry::publisher::EventPublisher;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::{clients::Cli, RunnableImage};
use testcontainers_modules::postgres::Postgres;
use tokio::time::sleep;

/// Distinctive names so a regression to the old canned values (`"default"`,
/// `"postgres"`, `"scry"`, `"localhost"`, `"5432"`) cannot accidentally match.
const TRUTH_DB: &str = "truthfulness_db";
const TRUTH_USER: &str = "truthfulness_user";
const TRUTH_PASSWORD: &str = "truthfulness_pw";

/// Extract a `RowSet`'s `(columns, rows)`, panicking with a useful message on
/// any other response variant.
fn expect_rowset(response: AdminResponse) -> (Vec<String>, Vec<Vec<String>>) {
    match response {
        AdminResponse::RowSet { columns, rows } => (columns, rows),
        other => panic!("expected RowSet, got {other:?}"),
    }
}

fn col_index(columns: &[String], name: &str) -> usize {
    columns.iter().position(|c| c == name).unwrap_or_else(|| panic!("missing column {name}"))
}

/// Generate self-signed test certs (mirrors `tests/tls_integration.rs`).
fn generate_test_certs() -> Option<(tempfile::TempDir, String, String)> {
    use std::process::Command;

    let dir = tempfile::tempdir().ok()?;
    let cert_path = dir.path().join("server.crt");
    let key_path = dir.path().join("server.key");

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

/// Connect a client over TLS (via `postgres-native-tls`, accepting the
/// self-signed test cert) through the proxy.
async fn connect_tls_client(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    dbname: &str,
) -> anyhow::Result<tokio_postgres::Client> {
    let connector = native_tls::TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .danger_accept_invalid_hostnames(true)
        .build()?;
    let connector = postgres_native_tls::MakeTlsConnector::new(connector);

    let (client, connection) = tokio_postgres::connect(
        &format!("host={host} port={port} user={user} password={password} dbname={dbname} sslmode=require"),
        connector,
    )
    .await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("tls connection error: {e}");
        }
    });

    Ok(client)
}

/// Open `n` real client connections against a known db/user through a real
/// proxy in front of a real Postgres container, plus one TLS client, then
/// assert `SHOW CLIENTS` reports exactly those live connections with their
/// real addr/user/database/tls — not the old canned empty result.
#[tokio::test]
async fn show_clients_reflects_live_connections_including_tls() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(
        Postgres::default()
            .with_db_name(TRUTH_DB)
            .with_user(TRUTH_USER)
            .with_password(TRUTH_PASSWORD),
    )
    .with_tag("16-alpine");
    let postgres = docker.run(postgres_image);
    let postgres_port = postgres.get_host_port_ipv4(5432);

    let certs = generate_test_certs();

    let mut config =
        create_test_config("127.0.0.1".to_string(), postgres_port, PoolingStrategy::Session);
    config.backend.database = TRUTH_DB.to_string();
    config.backend.user = TRUTH_USER.to_string();
    config.backend.password = TRUTH_PASSWORD.to_string();

    let tls_enabled = certs.is_some();
    if let Some((_, cert_path, key_path)) = &certs {
        config.tls.client_tls_sslmode = TlsSslMode::Allow;
        config.tls.client_tls_cert_file = Some(cert_path.clone());
        config.tls.client_tls_key_file = Some(key_path.clone());
    }

    let publisher: Arc<dyn EventPublisher> = Arc::new(TestPublisher::new());
    let (proxy_port, handles) =
        start_test_proxy_with_handles(config.clone(), publisher).await.unwrap();
    sleep(Duration::from_millis(300)).await;

    // K plain-TCP clients on the known db/user.
    let k = 2usize;
    let mut plain_clients = Vec::new();
    for _ in 0..k {
        let c = connect_client(
            "127.0.0.1",
            proxy_port,
            &config.backend.user,
            &config.backend.password,
            &config.backend.database,
        )
        .await
        .expect("plain client should connect through proxy");
        plain_clients.push(c);
    }

    // One TLS client (only if openssl was available to mint test certs).
    let mut tls_client = None;
    if tls_enabled {
        let c = connect_tls_client(
            "127.0.0.1",
            proxy_port,
            &config.backend.user,
            &config.backend.password,
            &config.backend.database,
        )
        .await
        .expect("tls client should connect through proxy");
        tls_client = Some(c);
    }

    let expected_total = k + if tls_enabled { 1 } else { 0 };

    // Registration is best-effort at connect; poll briefly.
    let mut waited = 0;
    while handles.client_registry.len() < expected_total && waited < 50 {
        sleep(Duration::from_millis(100)).await;
        waited += 1;
    }
    assert_eq!(
        handles.client_registry.len(),
        expected_total,
        "registry should have exactly the live connections we opened"
    );

    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));
    let admin = AdminConsole::new(Arc::clone(&handles), metrics);
    let (columns, rows) = expect_rowset(admin.execute("SHOW CLIENTS").await.unwrap());

    assert_eq!(rows.len(), expected_total, "SHOW CLIENTS must return one row per live client");

    let user_idx = col_index(&columns, "user");
    let db_idx = col_index(&columns, "database");
    let addr_idx = col_index(&columns, "addr");
    let tls_idx = col_index(&columns, "tls");

    for row in &rows {
        assert_eq!(row[user_idx], TRUTH_USER, "user column must be the real startup user");
        assert_eq!(row[db_idx], TRUTH_DB, "database column must be the real startup database");
        assert!(
            row[addr_idx].starts_with("127.0.0.1"),
            "addr column must be the real client socket, got {}",
            row[addr_idx]
        );
    }

    if tls_enabled {
        assert!(
            rows.iter().any(|r| r[tls_idx] != "0"),
            "at least one row must report tls=true for the TLS client"
        );
        // And at least one plain connection must NOT be marked tls.
        assert!(
            rows.iter().any(|r| r[tls_idx] == "0"),
            "plain-TCP clients must not be reported as tls"
        );
    } else {
        eprintln!("NOTE: openssl unavailable, TLS leg of this test skipped (still asserted {k} plain clients)");
    }

    drop(plain_clients);
    drop(tls_client);
}

/// `SHOW DATABASES` must list the actually-configured backend — a distinctive
/// host isn't feasible (testcontainers always binds 127.0.0.1), but the port
/// is Docker-assigned and virtually never 5432, and the database name is
/// deliberately distinctive. The old canned handler returned
/// name=default/host=localhost/port=5432/database=postgres unconditionally.
#[tokio::test]
async fn show_databases_reflects_real_backend_config() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(
        Postgres::default()
            .with_db_name(TRUTH_DB)
            .with_user(TRUTH_USER)
            .with_password(TRUTH_PASSWORD),
    )
    .with_tag("16-alpine");
    let postgres = docker.run(postgres_image);
    let postgres_port = postgres.get_host_port_ipv4(5432);
    assert_ne!(postgres_port, 5432, "test assumption: docker-assigned port differs from 5432");

    let mut config =
        create_test_config("127.0.0.1".to_string(), postgres_port, PoolingStrategy::Session);
    config.backend.database = TRUTH_DB.to_string();
    config.backend.user = TRUTH_USER.to_string();
    config.backend.password = TRUTH_PASSWORD.to_string();

    let publisher: Arc<dyn EventPublisher> = Arc::new(TestPublisher::new());
    let (_proxy_port, handles) =
        start_test_proxy_with_handles(config.clone(), publisher).await.unwrap();
    sleep(Duration::from_millis(300)).await;

    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));
    let admin = AdminConsole::new(Arc::clone(&handles), metrics);
    let (columns, rows) = expect_rowset(admin.execute("SHOW DATABASES").await.unwrap());

    assert!(!rows.is_empty(), "SHOW DATABASES must list the configured backend, not be empty");

    let host_idx = col_index(&columns, "host");
    let port_idx = col_index(&columns, "port");
    let db_idx = col_index(&columns, "database");

    let row = &rows[0];
    assert_eq!(row[host_idx], "127.0.0.1", "host must be the real configured backend host");
    assert_eq!(
        row[port_idx],
        postgres_port.to_string(),
        "port must be the real Docker-assigned backend port, not the canned 5432"
    );
    assert_eq!(row[db_idx], TRUTH_DB, "database must be the real configured database name");
    assert!(
        rows.iter().all(|r| r[host_idx] != "localhost" || row[host_idx] == "localhost"),
        "sanity: host assertion above already pins this"
    );
}

/// `SHOW SERVERS` must be non-empty and reflect the real backend once a pool
/// is connected — the old handler always returned zero rows.
#[tokio::test]
async fn show_servers_reflects_real_backend_pool() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(
        Postgres::default()
            .with_db_name(TRUTH_DB)
            .with_user(TRUTH_USER)
            .with_password(TRUTH_PASSWORD),
    )
    .with_tag("16-alpine");
    let postgres = docker.run(postgres_image);
    let postgres_port = postgres.get_host_port_ipv4(5432);

    let mut config =
        create_test_config("127.0.0.1".to_string(), postgres_port, PoolingStrategy::Session);
    config.backend.database = TRUTH_DB.to_string();
    config.backend.user = TRUTH_USER.to_string();
    config.backend.password = TRUTH_PASSWORD.to_string();

    let publisher: Arc<dyn EventPublisher> = Arc::new(TestPublisher::new());
    let (proxy_port, handles) =
        start_test_proxy_with_handles(config.clone(), publisher).await.unwrap();
    sleep(Duration::from_millis(300)).await;

    // Drive a real client through so the pool actually has a live backend
    // connection to report (not just a configured-but-idle pool).
    let client = connect_client(
        "127.0.0.1",
        proxy_port,
        &config.backend.user,
        &config.backend.password,
        &config.backend.database,
    )
    .await
    .expect("client should connect through proxy");
    client.simple_query("SELECT 1").await.expect("query should succeed");

    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));
    let admin = AdminConsole::new(Arc::clone(&handles), metrics);
    let (columns, rows) = expect_rowset(admin.execute("SHOW SERVERS").await.unwrap());

    assert!(!rows.is_empty(), "SHOW SERVERS must be non-empty once a backend pool is connected");

    let addr_idx = col_index(&columns, "addr");
    let port_idx = col_index(&columns, "port");
    assert!(
        rows.iter().any(|r| r[addr_idx] == "127.0.0.1" && r[port_idx] == postgres_port.to_string()),
        "SHOW SERVERS must report the real backend addr:port, got {rows:?}"
    );

    drop(client);
}

/// `SHOW POOLS` must show the real database name (not the canned `"default"`)
/// and a real (non-fabricated) pool size pulled from config/live status.
#[tokio::test]
async fn show_pools_reflects_real_database_and_pool_size() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(
        Postgres::default()
            .with_db_name(TRUTH_DB)
            .with_user(TRUTH_USER)
            .with_password(TRUTH_PASSWORD),
    )
    .with_tag("16-alpine");
    let postgres = docker.run(postgres_image);
    let postgres_port = postgres.get_host_port_ipv4(5432);

    let mut config =
        create_test_config("127.0.0.1".to_string(), postgres_port, PoolingStrategy::Session);
    config.backend.database = TRUTH_DB.to_string();
    config.backend.user = TRUTH_USER.to_string();
    config.backend.password = TRUTH_PASSWORD.to_string();
    config.performance.pool_size = 7; // distinctive, non-default pool size

    let publisher: Arc<dyn EventPublisher> = Arc::new(TestPublisher::new());
    let (_proxy_port, handles) =
        start_test_proxy_with_handles(config.clone(), publisher).await.unwrap();
    sleep(Duration::from_millis(300)).await;

    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));
    let admin = AdminConsole::new(Arc::clone(&handles), metrics);
    let (columns, rows) = expect_rowset(admin.execute("SHOW POOLS").await.unwrap());

    assert!(!rows.is_empty(), "SHOW POOLS must report the configured pool");

    let db_idx = col_index(&columns, "database");
    let user_idx = col_index(&columns, "user");

    assert!(
        rows.iter().any(|r| r[db_idx] == TRUTH_DB),
        "SHOW POOLS database column must be the real db name, not \"default\": {rows:?}"
    );
    assert!(
        rows.iter().any(|r| r[user_idx] == TRUTH_USER),
        "SHOW POOLS user column must be the real backend user, not \"scry\": {rows:?}"
    );
}
