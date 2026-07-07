//! Command-effect tests for the PAUSE / RESUME / RELOAD admin commands
//! (WP-10, P4 §4.2/§5.4).
//!
//! These are the §5.4 guardrail: they assert the REAL effect of each command,
//! not the returned tag string. A no-op stub that returns
//! `CommandComplete{tag:"PAUSE"}` while doing nothing must FAIL these tests.
//!
//!  - PAUSE gates NEW backend acquisition (a fresh client cannot get a working
//!    session) while an already-established, in-flight session keeps working;
//!    RESUME restores new acquisition.
//!  - RELOAD reports the real outcome: a broken `auth_file` yields an
//!    `AdminResponse::Error`, never a false `CommandComplete`.
//!  - PAUSE of an unknown database returns an honest error.
mod common;

use common::{
    connect_client, create_test_config, start_test_proxy_capturing_task,
    start_test_proxy_with_handles, TestPublisher,
};
use scry::admin::{AdminConsole, AdminResponse};
use scry::config::{BackpressureMode, Config, DatabaseConfig, PoolingStrategy};
use scry::observability::{HealthConfig, ProxyMetrics};
use scry::proxy::AdminHandles;
use scry::publisher::EventPublisher;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::{clients::Cli, RunnableImage};
use testcontainers_modules::postgres::Postgres;
use tokio::sync::watch;
use tokio::time::{sleep, timeout};

/// Build an `AdminConsole` over the same shared handles the running proxy uses,
/// so `PAUSE`/`RESUME`/`RELOAD` executed here hit the exact `PoolManager`/
/// reload path the proxy is serving through. `metrics` is only consulted by
/// `SHOW STATS`, so a throwaway instance is fine for these commands.
fn admin_console(handles: Arc<scry::proxy::AdminHandles>) -> AdminConsole {
    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));
    AdminConsole::new(handles, metrics)
}

/// Attempt to establish a NEW working session through the proxy and run a
/// trivial query. Returns `true` if that fully succeeds, `false` if either the
/// connect handshake or the query is rejected. While a pool is paused, backend
/// acquisition is gated BEFORE the client finishes authenticating, so a new
/// client is rejected during connect; this helper tolerates either failure
/// point.
async fn new_session_works(port: u16, user: &str, password: &str, dbname: &str) -> bool {
    match connect_client("127.0.0.1", port, user, password, dbname).await {
        Err(_) => false,
        Ok(client) => client.simple_query("SELECT 1").await.is_ok(),
    }
}

#[tokio::test]
async fn pause_stops_new_acquisition_and_resume_restores() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);
    let postgres_port = postgres.get_host_port_ipv4(5432);

    let mut config =
        create_test_config("127.0.0.1".to_string(), postgres_port, PoolingStrategy::Session);
    // Small pool so behavior is crisp, and reject-immediate so a gated acquire
    // fails fast instead of waiting on the queue.
    config.performance.pool_size = 2;
    config.performance.pool_min_idle = 0;
    config.performance.pool_backpressure_mode = BackpressureMode::RejectImmediate;

    let publisher: Arc<dyn EventPublisher> = Arc::new(TestPublisher::new());
    let (proxy_port, handles) =
        start_test_proxy_with_handles(config.clone(), publisher).await.unwrap();
    sleep(Duration::from_millis(300)).await;

    let admin = admin_console(handles.clone());
    let (user, pass, db) = (
        config.backend.user.clone(),
        config.backend.password.clone(),
        config.backend.database.clone(),
    );

    // Baseline: a new session works before pausing.
    assert!(
        new_session_works(proxy_port, &user, &pass, &db).await,
        "a new session must work before PAUSE"
    );

    // Client A opens an in-flight transaction BEFORE the pause; it holds its
    // already-acquired backend for the life of the (session-pooled) session.
    let client_a = connect_client("127.0.0.1", proxy_port, &user, &pass, &db)
        .await
        .expect("client A should connect before pause");
    client_a.simple_query("BEGIN").await.expect("BEGIN before pause");
    client_a.simple_query("SELECT 42").await.expect("first query before pause");

    // PAUSE (all pools).
    let resp = admin.execute("PAUSE").await.unwrap();
    assert!(
        matches!(resp, AdminResponse::CommandComplete { .. }),
        "PAUSE should return CommandComplete on success, got {resp:?}"
    );

    // EFFECT 1: a NEW client cannot establish a working session while paused.
    assert!(
        !new_session_works(proxy_port, &user, &pass, &db).await,
        "a new session MUST be rejected while the pool is paused"
    );

    // EFFECT 2: the in-flight session opened before the pause still completes.
    client_a.simple_query("SELECT 43").await.expect("in-flight query must still work while paused");
    client_a.simple_query("COMMIT").await.expect("in-flight COMMIT must still work while paused");

    // RESUME (all pools).
    let resp = admin.execute("RESUME").await.unwrap();
    assert!(
        matches!(resp, AdminResponse::CommandComplete { .. }),
        "RESUME should return CommandComplete on success, got {resp:?}"
    );

    // EFFECT 3: a new session works again after RESUME.
    assert!(
        new_session_works(proxy_port, &user, &pass, &db).await,
        "a new session MUST work again after RESUME"
    );
}

#[tokio::test]
async fn pause_unknown_database_returns_honest_error() {
    // No backend traffic is exercised, so no container is needed; point the
    // backend at an unused port. `ProxyServer::new` does not dial the backend.
    let config = create_test_config("127.0.0.1".to_string(), 1, PoolingStrategy::Session);
    let publisher: Arc<dyn EventPublisher> = Arc::new(TestPublisher::new());
    let (_proxy_port, handles) = start_test_proxy_with_handles(config, publisher).await.unwrap();

    let admin = admin_console(handles);
    let resp = admin.execute("PAUSE definitely_not_a_configured_db").await.unwrap();
    assert!(
        matches!(resp, AdminResponse::Error { .. }),
        "PAUSE of an unknown database must return an ErrorResponse, got {resp:?}"
    );
}

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

/// SHOW DATABASES must reflect LIVE pause state (WP-10 whole-branch review,
/// Fix 2), not a hardcoded `"0"`. Before `PAUSE`, `paused=0`; after
/// `PAUSE <db>`, that db's row shows `paused=1`; after `RESUME <db>`, back to
/// `0`. This is the RED case for the pre-fix hardcoded column: it always
/// reports `0`, so the post-PAUSE assertion below fails against it. No
/// backend container is needed — `ProxyServer::new` never dials the backend,
/// and PAUSE/RESUME/SHOW DATABASES only touch pool-manager/config state.
#[tokio::test]
async fn pause_reflected_in_show_databases_paused_column() {
    let config = create_test_config("127.0.0.1".to_string(), 1, PoolingStrategy::Session);
    let publisher: Arc<dyn EventPublisher> = Arc::new(TestPublisher::new());
    let (_proxy_port, handles) =
        start_test_proxy_with_handles(config.clone(), publisher).await.unwrap();

    let admin = admin_console(handles.clone());
    let db = config.backend.database.clone();

    let (columns, rows) = expect_rowset(admin.execute("SHOW DATABASES").await.unwrap());
    let name_idx = col_index(&columns, "name");
    let paused_idx = col_index(&columns, "paused");
    let row =
        rows.iter().find(|r| r[name_idx] == db).unwrap_or_else(|| panic!("no row for db {db}"));
    assert_eq!(row[paused_idx], "0", "expected paused=0 before PAUSE, got row {row:?}");

    let resp = admin.execute(&format!("PAUSE {db}")).await.unwrap();
    assert!(
        matches!(resp, AdminResponse::CommandComplete { .. }),
        "PAUSE {db} should return CommandComplete, got {resp:?}"
    );

    let (columns, rows) = expect_rowset(admin.execute("SHOW DATABASES").await.unwrap());
    let paused_idx = col_index(&columns, "paused");
    let row =
        rows.iter().find(|r| r[name_idx] == db).unwrap_or_else(|| panic!("no row for db {db}"));
    assert_eq!(
        row[paused_idx], "1",
        "SHOW DATABASES MUST report paused=1 for {db} after PAUSE, got row {row:?}"
    );

    let resp = admin.execute(&format!("RESUME {db}")).await.unwrap();
    assert!(
        matches!(resp, AdminResponse::CommandComplete { .. }),
        "RESUME {db} should return CommandComplete, got {resp:?}"
    );

    let (columns, rows) = expect_rowset(admin.execute("SHOW DATABASES").await.unwrap());
    let paused_idx = col_index(&columns, "paused");
    let row =
        rows.iter().find(|r| r[name_idx] == db).unwrap_or_else(|| panic!("no row for db {db}"));
    assert_eq!(
        row[paused_idx], "0",
        "SHOW DATABASES MUST report paused=0 for {db} after RESUME, got row {row:?}"
    );
}

/// A `config.databases` entry named `name`, routed to the same real backend
/// the default pool uses. Two of these give us two DISTINCT logical databases
/// (`db1`, `db2`) whose live clients we can tell apart in the registry, both
/// backed by one Postgres container.
fn db_entry(name: &str, backend_port: u16) -> DatabaseConfig {
    DatabaseConfig {
        name: name.to_string(),
        host: "127.0.0.1".to_string(),
        port: backend_port,
        database: "postgres".to_string(),
        user: "postgres".to_string(),
        password: "postgres".to_string(),
        pool_size: None,
    }
}

/// KILL [db] must forcibly disconnect exactly the targeted database's live
/// clients (their sockets close, their registry entries vanish) while leaving
/// every other database's clients untouched. A no-op stub that returns
/// `CommandComplete{tag:"KILL"}` while doing nothing FAILS this test (the db1
/// clients stay connected and stay in the registry).
#[tokio::test]
async fn kill_disconnects_targeted_database_only() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);
    let postgres_port = postgres.get_host_port_ipv4(5432);

    // Two logical databases, both backed by the one container.
    let mut config: Config =
        create_test_config("127.0.0.1".to_string(), postgres_port, PoolingStrategy::Session);
    config.databases = vec![db_entry("db1", postgres_port), db_entry("db2", postgres_port)];

    let publisher: Arc<dyn EventPublisher> = Arc::new(TestPublisher::new());
    let (proxy_port, handles) =
        start_test_proxy_with_handles(config.clone(), publisher).await.unwrap();
    sleep(Duration::from_millis(300)).await;

    let (user, pass) = (config.backend.user.clone(), config.backend.password.clone());

    // Open 2 clients on db1 and 2 on db2.
    let mut db1_clients = Vec::new();
    for _ in 0..2 {
        db1_clients.push(
            connect_client("127.0.0.1", proxy_port, &user, &pass, "db1")
                .await
                .expect("db1 client should connect"),
        );
    }
    let mut db2_clients = Vec::new();
    for _ in 0..2 {
        db2_clients.push(
            connect_client("127.0.0.1", proxy_port, &user, &pass, "db2")
                .await
                .expect("db2 client should connect"),
        );
    }

    // Everyone works, and the registry lists all four.
    for c in db1_clients.iter().chain(db2_clients.iter()) {
        c.simple_query("SELECT 1").await.expect("query before KILL");
    }
    let snap = handles.client_registry.snapshot();
    assert_eq!(
        snap.iter().filter(|e| e.database == "db1").count(),
        2,
        "expected 2 db1 clients registered before KILL, got {snap:?}"
    );
    assert_eq!(
        snap.iter().filter(|e| e.database == "db2").count(),
        2,
        "expected 2 db2 clients registered before KILL"
    );

    // KILL db1.
    let resp = admin_console(handles.clone()).execute("KILL db1").await.unwrap();
    assert!(
        matches!(resp, AdminResponse::CommandComplete { .. }),
        "KILL db1 should return CommandComplete, got {resp:?}"
    );

    // Give the aborts a moment to propagate to the client sockets.
    sleep(Duration::from_millis(300)).await;

    // EFFECT 1: every db1 client is now disconnected (its next op errors).
    for (i, c) in db1_clients.iter().enumerate() {
        assert!(
            c.simple_query("SELECT 1").await.is_err(),
            "db1 client #{i} MUST be disconnected after KILL db1"
        );
    }

    // EFFECT 2: db2 clients are untouched and still work.
    for (i, c) in db2_clients.iter().enumerate() {
        c.simple_query("SELECT 1")
            .await
            .unwrap_or_else(|e| panic!("db2 client #{i} must still work after KILL db1: {e}"));
    }

    // EFFECT 3: the registry no longer lists any db1 client; db2 remain.
    let snap = handles.client_registry.snapshot();
    assert_eq!(
        snap.iter().filter(|e| e.database == "db1").count(),
        0,
        "registry MUST NOT list any killed db1 client after KILL, got {snap:?}"
    );
    assert_eq!(
        snap.iter().filter(|e| e.database == "db2").count(),
        2,
        "registry MUST still list the untouched db2 clients"
    );
}

/// KILL of an unknown database returns an honest error, never a false success.
#[tokio::test]
async fn kill_unknown_database_returns_honest_error() {
    let config = create_test_config("127.0.0.1".to_string(), 1, PoolingStrategy::Session);
    let publisher: Arc<dyn EventPublisher> = Arc::new(TestPublisher::new());
    let (_proxy_port, handles) = start_test_proxy_with_handles(config, publisher).await.unwrap();

    let resp = admin_console(handles).execute("KILL definitely_not_a_configured_db").await.unwrap();
    assert!(
        matches!(resp, AdminResponse::Error { .. }),
        "KILL of an unknown database must return an ErrorResponse, got {resp:?}"
    );
}

/// SHUTDOWN must actually drain and stop the proxy — the `run()` task must
/// complete within the shutdown timeout. A no-op stub that returns
/// `CommandComplete{tag:"SHUTDOWN"}` without triggering the drain leaves
/// `run()` running forever and FAILS this test (the join times out).
#[tokio::test]
async fn shutdown_actually_drains_dedicated_proxy() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);
    let postgres_port = postgres.get_host_port_ipv4(5432);

    // Dedicated proxy with a short drain window so the (idle) client is
    // force-drained quickly and the test stays time-bounded.
    let mut config =
        create_test_config("127.0.0.1".to_string(), postgres_port, PoolingStrategy::Session);
    config.proxy.shutdown_timeout_secs = 2;

    let publisher: Arc<dyn EventPublisher> = Arc::new(TestPublisher::new());
    let (proxy_port, handles, run_task) =
        start_test_proxy_capturing_task(config.clone(), publisher).await.unwrap();
    sleep(Duration::from_millis(300)).await;

    let (user, pass, db) = (
        config.backend.user.clone(),
        config.backend.password.clone(),
        config.backend.database.clone(),
    );
    let client = connect_client("127.0.0.1", proxy_port, &user, &pass, &db)
        .await
        .expect("client should connect before shutdown");
    client.simple_query("SELECT 1").await.expect("query before shutdown");

    // Trigger a real shutdown via the admin console.
    let resp = admin_console(handles.clone()).execute("SHUTDOWN").await.unwrap();
    assert!(
        matches!(resp, AdminResponse::CommandComplete { .. }),
        "SHUTDOWN should return CommandComplete, got {resp:?}"
    );

    // EFFECT: run() must complete (proxy drained) within the drain window + slack.
    let bound = Duration::from_secs(config.proxy.shutdown_timeout_secs + 6);
    timeout(bound, run_task)
        .await
        .expect("run() MUST complete after SHUTDOWN (proxy actually drained)")
        .expect("run() task should not panic");
}

/// SHUTDOWN WAIT semantics, proven hermetically (no container, no self-drain
/// deadlock): the command must (a) actually fire the shutdown trigger and
/// (b) BLOCK until the drain-completion signal flips, then return
/// CommandComplete. The stub ignores `wait` and never triggers, so the
/// "trigger fired" assertion FAILS against it.
#[tokio::test]
async fn shutdown_wait_blocks_until_drain_completes() {
    // Build handles with a LIVE shutdown receiver so `trigger_shutdown()`
    // succeeds (mirrors what `run()`'s subscription provides in production).
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut observe_rx = shutdown_tx.subscribe();
    let (reload_tx, _reload_rx) = watch::channel(());
    let reload_fn: Arc<dyn Fn() -> anyhow::Result<()> + Send + Sync> = Arc::new(|| Ok(()));
    let handles = AdminHandles::new(
        Arc::new(config_for_wait_test()),
        HashMap::new(),
        reload_tx,
        shutdown_tx,
        reload_fn,
    );
    // Keep the seed receiver alive for the whole test.
    let _shutdown_rx = shutdown_rx;

    let admin = admin_console(handles.clone());

    // Signal drain completion shortly after the command starts blocking.
    let h2 = handles.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(150)).await;
        h2.signal_drain_complete();
    });

    // WAIT blocks until the drain signal fires, then returns CommandComplete.
    // Record elapsed time around the call: this is the load-bearing part of
    // the test. A stubbed/no-op WAIT that skips `.wait_for(...)` and returns
    // immediately would still produce a `CommandComplete` here, so the tag
    // check alone can't tell a real block from a stub. Requiring elapsed time
    // to approach (but stay comfortably under) the background task's 150ms
    // delay proves `execute` genuinely blocked on the completion signal rather
    // than returning early.
    let started = std::time::Instant::now();
    let resp = timeout(Duration::from_secs(3), admin.execute("SHUTDOWN WAIT"))
        .await
        .expect("SHUTDOWN WAIT must return once drain completes (bounded)")
        .unwrap();
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(140),
        "SHUTDOWN WAIT returned after {elapsed:?}, which is too fast to have actually blocked on \
         the drain-completion signal (background signal fires at ~150ms) — WAIT appears to have \
         degenerated into a no-op that returns immediately"
    );
    assert!(
        matches!(resp, AdminResponse::CommandComplete { .. }),
        "SHUTDOWN WAIT should return CommandComplete after drain, got {resp:?}"
    );

    // The command genuinely fired the shutdown trigger (the stub does not).
    assert!(
        observe_rx.has_changed().unwrap() && *observe_rx.borrow_and_update(),
        "SHUTDOWN WAIT MUST fire the shutdown trigger"
    );
}

/// Minimal config for the hermetic WAIT test (no backend is dialed).
fn config_for_wait_test() -> Config {
    let mut config = create_test_config("127.0.0.1".to_string(), 1, PoolingStrategy::Session);
    // Bound the WAIT fallback timeout tightly; the drain signal fires first.
    config.proxy.shutdown_timeout_secs = 2;
    config
}

#[tokio::test]
async fn reload_reports_honest_error_on_broken_auth_file() {
    use scry::config::AuthType;
    use std::io::Write as _;

    // Start with a VALID auth file so `ProxyServer::new` succeeds.
    let mut auth_file = tempfile::NamedTempFile::new().unwrap();
    writeln!(auth_file, "\"user1\" \"pass1\"").unwrap();
    auth_file.flush().unwrap();
    let auth_path = auth_file.path().to_string_lossy().to_string();

    let mut config = create_test_config("127.0.0.1".to_string(), 1, PoolingStrategy::Session);
    config.auth.auth_type = AuthType::Md5;
    config.auth.auth_file = Some(auth_path.clone());

    let publisher: Arc<dyn EventPublisher> = Arc::new(TestPublisher::new());
    let (_proxy_port, handles) = start_test_proxy_with_handles(config, publisher).await.unwrap();

    let admin = admin_console(handles);

    // A reload of the still-valid file succeeds.
    let resp = admin.execute("RELOAD").await.unwrap();
    assert!(
        matches!(resp, AdminResponse::CommandComplete { .. }),
        "RELOAD of a valid auth file must return CommandComplete, got {resp:?}"
    );

    // Now make the reload fail deterministically: remove the auth file.
    drop(auth_file); // deletes the temp file

    let resp = admin.execute("RELOAD").await.unwrap();
    assert!(
        matches!(resp, AdminResponse::Error { .. }),
        "RELOAD with a broken auth_file MUST return an ErrorResponse (never a false \
         CommandComplete), got {resp:?}"
    );
}
