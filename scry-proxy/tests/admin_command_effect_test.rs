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

use common::{connect_client, create_test_config, start_test_proxy_with_handles, TestPublisher};
use scry::admin::{AdminConsole, AdminResponse};
use scry::config::{BackpressureMode, PoolingStrategy};
use scry::observability::{HealthConfig, ProxyMetrics};
use scry::publisher::EventPublisher;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::{clients::Cli, RunnableImage};
use testcontainers_modules::postgres::Postgres;
use tokio::time::sleep;

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
