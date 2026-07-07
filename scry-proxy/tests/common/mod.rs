//! Shared differential-transparency test harness (WP-9, P2 §5.1).
//!
//! This module is included via `mod common;` from individual `tests/*.rs` binaries
//! (Rust convention: a file at `tests/common/mod.rs`, rather than `tests/common.rs`,
//! is not itself compiled as a standalone test binary). It factors the config/
//! publisher/proxy-bringup boilerplate that the 5 pre-existing container suites
//! each copy-pasted, and adds the direct-vs-proxy comparison primitives that none
//! of them had: `paired_clients`, `all_modes`, and the result comparator.
//!
//! Not every test binary uses every item here, so allow dead_code at the module
//! level rather than sprinkling `#[allow(dead_code)]` per item.
#![allow(dead_code)]

use scry::{config::*, observability::*, proxy::*, publisher::*};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::sleep;
use tokio_postgres::{Client, SimpleQueryMessage};

// ---------------------------------------------------------------------------
// Publisher
// ---------------------------------------------------------------------------

/// Test publisher that captures events for verification (copied verbatim from
/// the pattern in `integration_test.rs` / `transaction_pooling_test.rs`).
#[derive(Debug, Clone)]
pub struct TestPublisher {
    events: Arc<Mutex<Vec<QueryEvent>>>,
}

impl Default for TestPublisher {
    fn default() -> Self {
        Self::new()
    }
}

impl TestPublisher {
    pub fn new() -> Self {
        Self { events: Arc::new(Mutex::new(Vec::new())) }
    }

    pub fn events(&self) -> Vec<QueryEvent> {
        self.events.lock().unwrap().clone()
    }

    pub fn event_count(&self) -> usize {
        self.events.lock().unwrap().len()
    }

    pub fn clear(&self) {
        self.events.lock().unwrap().clear();
    }

    pub fn find_query(&self, pattern: &str) -> Option<QueryEvent> {
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

// ---------------------------------------------------------------------------
// Config + proxy bring-up
// ---------------------------------------------------------------------------

/// Shared config builder, parametrized by backend address and pooling mode
/// (mirrors `transaction_pooling_test.rs:52`).
pub fn create_test_config(host: String, port: u16, pooling: PoolingStrategy) -> Config {
    Config {
        proxy: ProxyConfig {
            listen_address: "127.0.0.1:0".to_string(), // Let OS assign port
            max_connections: 100,
            shutdown_timeout_secs: 5,
            unix_socket: None,
        },
        databases: Vec::new(),
        backend: BackendConfig {
            protocol: DatabaseProtocol::Postgres,
            host,
            port,
            database: "postgres".to_string(),
            user: "postgres".to_string(),
            password: "postgres".to_string(),
            pool_size: 5,
            connection_timeout_ms: 5000,
        },
        observability: ObservabilityConfig {
            enable_tracing: false,
            otlp_endpoint: None,
            service_name: "scry-differential-test".to_string(),
            enable_metrics_server: false,
            metrics_server_address: "127.0.0.1:0".to_string(),
            unsafe_debug_logging: false,
        },
        protocol: ProtocolConfig { max_prepared_statements: 100 },
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
            http_compression: false,
            shadow_id: None,
            allow_insecure: false,
            anonymize_salt: None,
            parse_failure_mode: ParseFailureMode::Redact,
        },
        performance: PerformanceConfig {
            latency_budget: scry::config::LatencyBudget::default(),
            query_timeout_secs: 0,
            connection_pooling: pooling,
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

/// Start the proxy server and return its listen port (mirrors
/// `integration_test.rs:151`).
pub async fn start_test_proxy(
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

    tokio::spawn(async move {
        let _ = server.run().await;
    });

    Ok(port)
}

/// Variant that also returns the metrics handle (mirrors
/// `integration_test.rs:872`), for tasks that need to inspect histograms.
pub async fn start_test_proxy_with_metrics(
    config: Config,
    publisher: Arc<dyn EventPublisher>,
) -> anyhow::Result<(u16, Arc<ProxyMetrics>)> {
    let batcher = EventBatcher::new(
        publisher,
        config.publisher.batch_size,
        config.publisher.flush_interval_ms,
        config.publisher.max_queue_size,
    );

    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));
    let server = ProxyServer::new(config.clone(), batcher, Arc::clone(&metrics)).await?;
    let port = server.local_addr()?.port();

    tokio::spawn(async move {
        let _ = server.run().await;
    });

    Ok((port, metrics))
}

/// Variant that also returns the shared [`AdminHandles`] so a test can inspect
/// live registry state (client/server registries) or drive a programmatic
/// shutdown. Grabs the handles before the server is moved into `run()`.
pub async fn start_test_proxy_with_handles(
    config: Config,
    publisher: Arc<dyn EventPublisher>,
) -> anyhow::Result<(u16, Arc<AdminHandles>)> {
    let batcher = EventBatcher::new(
        publisher,
        config.publisher.batch_size,
        config.publisher.flush_interval_ms,
        config.publisher.max_queue_size,
    );

    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));
    let server = ProxyServer::new(config.clone(), batcher, metrics).await?;
    let port = server.local_addr()?.port();
    let handles = server.admin_handles();

    tokio::spawn(async move {
        let _ = server.run().await;
    });

    Ok((port, handles))
}

/// Like [`start_test_proxy_with_handles`] but ALSO returns the `JoinHandle` of
/// the spawned `run()` task, so a test can observe the proxy actually shutting
/// down (the task completing) — used by the `SHUTDOWN` command-effect test to
/// prove a real drain rather than a false `CommandComplete`.
pub async fn start_test_proxy_capturing_task(
    config: Config,
    publisher: Arc<dyn EventPublisher>,
) -> anyhow::Result<(u16, Arc<AdminHandles>, tokio::task::JoinHandle<()>)> {
    let batcher = EventBatcher::new(
        publisher,
        config.publisher.batch_size,
        config.publisher.flush_interval_ms,
        config.publisher.max_queue_size,
    );

    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));
    let server = ProxyServer::new(config.clone(), batcher, metrics).await?;
    let port = server.local_addr()?.port();
    let handles = server.admin_handles();

    let task = tokio::spawn(async move {
        let _ = server.run().await;
    });

    Ok((port, handles, task))
}

/// All four pooling modes, for matrix loops.
pub fn all_modes() -> [PoolingStrategy; 4] {
    [
        PoolingStrategy::Disabled,
        PoolingStrategy::Session,
        PoolingStrategy::Transaction,
        PoolingStrategy::Hybrid,
    ]
}

// ---------------------------------------------------------------------------
// Client connection helpers
// ---------------------------------------------------------------------------

/// Connects a `tokio_postgres` client to `host:port` and spawns its connection
/// driver in the background, returning the ready-to-use client.
pub async fn connect_client(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    dbname: &str,
) -> anyhow::Result<Client> {
    let (client, connection) = tokio_postgres::connect(
        &format!("host={host} port={port} user={user} password={password} dbname={dbname}"),
        tokio_postgres::NoTls,
    )
    .await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    Ok(client)
}

/// A proxy-connected client paired with a client connected directly to the
/// same backend container, for the same pooling mode. This is the core
/// primitive of the differential suite: run the same operation on both and
/// compare via [`assert_outcomes_equivalent`].
pub struct PairedClients {
    pub proxy_port: u16,
    pub proxy: Client,
    pub direct: Client,
}

/// Starts a proxy in front of `backend_host:backend_port` with the given
/// pooling mode, and returns both a client connected through the proxy and a
/// client connected directly to the backend container.
///
/// The direct client is the "ground truth": a real client talking to
/// unmodified Postgres. The proxy client is what we're validating. If the
/// proxy is truly transparent, every observable outcome should match.
pub async fn paired_clients(
    backend_host: &str,
    backend_port: u16,
    pooling: PoolingStrategy,
) -> anyhow::Result<PairedClients> {
    let config = create_test_config(backend_host.to_string(), backend_port, pooling);
    let publisher: Arc<dyn EventPublisher> = Arc::new(TestPublisher::new());
    let proxy_port = start_test_proxy(config.clone(), publisher).await?;

    // Give the proxy listener a moment to come up before connecting.
    sleep(Duration::from_millis(300)).await;

    let proxy = connect_client(
        "127.0.0.1",
        proxy_port,
        &config.backend.user,
        &config.backend.password,
        &config.backend.database,
    )
    .await?;
    let direct = connect_client(
        backend_host,
        backend_port,
        &config.backend.user,
        &config.backend.password,
        &config.backend.database,
    )
    .await?;

    Ok(PairedClients { proxy_port, proxy, direct })
}

/// Starts a proxy in front of `backend_host:backend_port` with the given
/// pooling mode, capping the pool to exactly `pool_size` physical backend
/// connections — the CRIT-1 recycle pattern from `connection_multiplexing.rs`
/// (`test_sequential_connection_reuse` et al.). Returns only the proxy's
/// listen port (no direct/ground-truth client): callers of this are probing
/// pooled-connection hygiene (WP-9 Task 8, P2 §5.3), not direct-vs-proxy
/// transparency, so they connect their own clients via [`connect_client`].
///
/// With `pool_size` connections capped, a second client's connect is FORCED
/// to wait for and reuse the exact physical connection(s) a prior client
/// released — genuine reuse, not merely likely reuse on a shared Docker
/// container. Callers should confirm this landed on the intended physical
/// connection via `SELECT pg_backend_pid()` (recycle resets session state
/// with `DISCARD ALL` but never changes the backend PID), rather than
/// trusting pool sizing alone.
///
/// Only `performance.pool_size` is actually wired into deadpool's
/// `Pool::max_size` (`server.rs` routes the default database's pool size from
/// `config.performance.pool_size` via `DatabaseRouter`, never from
/// `config.backend.pool_size`). Both are set here for clarity/parity with
/// `connection_multiplexing.rs`'s pattern, but `performance.pool_size` is the
/// one that's load-bearing. `pool_min_idle` is forced to 0 so pre-warming
/// can't race the single slot at proxy startup.
pub async fn start_pool_capped_proxy(
    backend_host: &str,
    backend_port: u16,
    pooling: PoolingStrategy,
    pool_size: usize,
) -> anyhow::Result<u16> {
    let mut config = create_test_config(backend_host.to_string(), backend_port, pooling);
    config.backend.pool_size = pool_size;
    config.performance.pool_size = pool_size;
    config.performance.pool_min_idle = 0;
    let publisher: Arc<dyn EventPublisher> = Arc::new(TestPublisher::new());
    let proxy_port = start_test_proxy(config, publisher).await?;

    // Give the proxy listener a moment to come up before connecting.
    sleep(Duration::from_millis(300)).await;

    Ok(proxy_port)
}

// ---------------------------------------------------------------------------
// Result comparator
// ---------------------------------------------------------------------------

/// A single statement's outcome from the simple query protocol: the row data
/// (as text — simple protocol never returns binary), and the rows-affected /
/// command-complete count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
    pub rows_affected: u64,
}

/// The full outcome of a (possibly multi-statement) simple-query call: one
/// [`StatementResult`] per statement, in order.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct QuerySnapshot {
    pub statements: Vec<StatementResult>,
}

/// Normalizes the raw `SimpleQueryMessage` stream into a [`QuerySnapshot`].
/// Rows accumulate into the statement they belong to; a `CommandComplete`
/// closes out the current statement.
fn snapshot_from_messages(msgs: Vec<SimpleQueryMessage>) -> QuerySnapshot {
    let mut statements = Vec::new();
    let mut columns: Vec<String> = Vec::new();
    let mut rows: Vec<Vec<Option<String>>> = Vec::new();

    for msg in msgs {
        match msg {
            SimpleQueryMessage::RowDescription(cols) => {
                columns = cols.iter().map(|c| c.name().to_string()).collect();
            }
            SimpleQueryMessage::Row(row) => {
                let mut values = Vec::with_capacity(row.len());
                for i in 0..row.len() {
                    values.push(row.get(i).map(|s| s.to_string()));
                }
                rows.push(values);
            }
            SimpleQueryMessage::CommandComplete(rows_affected) => {
                statements.push(StatementResult {
                    columns: std::mem::take(&mut columns),
                    rows: std::mem::take(&mut rows),
                    rows_affected,
                });
            }
            // SimpleQueryMessage is #[non_exhaustive] upstream in newer versions;
            // treat anything else as a no-op rather than fail to compile.
            #[allow(unreachable_patterns)]
            _ => {}
        }
    }

    QuerySnapshot { statements }
}

/// The outcome of running one operation against one client: either a
/// normalized result snapshot, or an error identified by its SQLSTATE code
/// (never by message text, which can differ between direct and proxied
/// connections even when the underlying error is identical).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunOutcome {
    Ok(QuerySnapshot),
    Err { sqlstate: Option<String> },
}

/// Runs `sql` via the simple query protocol and captures the outcome.
///
/// Uses `simple_query` (not `.query()`/`.execute()`, which always use the
/// extended protocol in `tokio_postgres` even with zero parameters) so the
/// baseline matrix genuinely exercises the simple-protocol path end to end,
/// including multi-statement batches like `BEGIN; ...; COMMIT;`.
pub async fn run_simple(client: &Client, sql: &str) -> RunOutcome {
    match client.simple_query(sql).await {
        Ok(msgs) => RunOutcome::Ok(snapshot_from_messages(msgs)),
        Err(e) => RunOutcome::Err { sqlstate: e.code().map(|c| c.code().to_string()) },
    }
}

/// Runs `sql` via the EXTENDED query protocol (`.execute()`, which — unlike
/// `.simple_query()` — always issues Parse/Bind/Execute in `tokio_postgres`,
/// even with zero bind parameters, per its own doc comment) and captures the
/// outcome as a [`RunOutcome`] so the same comparator
/// ([`assert_outcomes_equivalent`]) used by the simple-protocol baseline
/// matrix works unchanged. The statements this is used for (`BEGIN`, `SET`,
/// `CREATE TEMP TABLE`, `DECLARE CURSOR`, `COMMIT`, `pg_advisory_lock`) are
/// session/DDL commands with no meaningful row shape to compare, so the
/// affected-row count is the sole `QuerySnapshot` signal — column/row lists
/// are always empty.
pub async fn run_extended(client: &Client, sql: &str) -> RunOutcome {
    match client.execute(sql, &[]).await {
        Ok(rows_affected) => RunOutcome::Ok(QuerySnapshot {
            statements: vec![StatementResult { columns: vec![], rows: vec![], rows_affected }],
        }),
        Err(e) => RunOutcome::Err { sqlstate: e.code().map(|c| c.code().to_string()) },
    }
}

/// Hybrid-mode-only proof (WP-9 Task 4, P2 §4.2): a stateful command issued
/// via the EXTENDED protocol must pin the connection exactly as the simple
/// protocol does, so a completed transaction does not cause Hybrid mode to
/// silently recycle the backend connection out from under the client.
///
/// The `SET` itself is sent via `.execute()` (Parse/Bind/Execute) — the
/// thing under test. The transaction boundary and cleanup
/// (`BEGIN`/`DEALLOCATE ALL`/`COMMIT`) deliberately go over `simple_query`
/// instead, for test isolation, not because it doesn't matter which protocol
/// they use:
///
/// `Message::Parse`'s PRE-EXISTING (untouched by this task)
/// `add_prepared_statement` bookkeeping runs for *every* Parse, and sets its
/// own `PinReason::PreparedStatement` — a real, separate pin unrelated to
/// classification. If `BEGIN`/`COMMIT` were also sent via `.execute()`,
/// *their own* Parse would add a fresh prepared-statement pin (or, if the
/// SET's still-open prepared statement hasn't been `Close`d client-side yet,
/// *that* pin) right before the release check runs — masking whether the
/// `SET`'s session-variable classification (what this task actually adds)
/// is doing anything at all, since `is_pinned()` would already be true for
/// an unrelated reason either way. Sending `BEGIN`/`COMMIT` via simple query
/// (which never touches `add_prepared_statement`) and running an explicit
/// `DEALLOCATE ALL` (also simple query) right after the `SET` — which clears
/// only `prepared_statements`, per `ConnectionState::apply_query`'s
/// `DeallocateAll` arm, leaving `session_variables` untouched — deterministically
/// strips that incidental pin, isolating the one pin this task is
/// responsible for.
///
/// `connection.rs` only evaluates `should_release_connection` when a
/// transaction completes (auto-commit statements never trigger release —
/// see the comment in `connection.rs` near `just_finished_transaction`), so
/// wrapping in BEGIN/COMMIT is what exercises the Hybrid release decision
/// this test is probing. Then:
///   (a) drives further activity on the SAME client and confirms the GUC is
///       still visible (proof the connection wasn't silently swapped/reset
///       out from under it), and
///   (b) opens a FRESH client against the same pooled proxy port and
///       confirms it does NOT see the value (proof any pin didn't leak to a
///       different logical session).
///
/// Before the Parse-arm fix: `ConnectionState` never observes the SET (the
/// `Message::Parse` arm didn't classify the SQL it carried), so — once the
/// incidental prepared-statement pin is stripped by `DEALLOCATE ALL` — it is
/// unpinned, Hybrid mode releases the connection at COMMIT, and the pool's
/// recycle step (`DISCARD ALL` via `protocol::postgres::reset_connection`)
/// wipes the GUC before the next checkout — check (a) fails deterministically
/// (recycle always runs `DISCARD ALL` on checkout, regardless of whether the
/// same or a different physical backend connection is handed back).
pub async fn assert_extended_state_survives_hybrid_recycle(
    proxy: &Client,
    proxy_port: u16,
    user: &str,
    password: &str,
    dbname: &str,
    guc_name: &str,
    guc_value: &str,
) -> anyhow::Result<()> {
    proxy.simple_query("BEGIN").await?;
    proxy.execute(&format!("SET {guc_name} = '{guc_value}'"), &[]).await?; // EXTENDED protocol — the thing under test
    proxy.simple_query("DEALLOCATE ALL").await?; // strips the incidental PreparedStatement pin only
    proxy.simple_query("COMMIT").await?; // transaction completes -> Hybrid release check runs

    // Enough further activity that an un-pinned connection would already
    // have been released back to the pool (and DISCARD-ALL reset) by now.
    for _ in 0..5 {
        proxy.simple_query("SELECT 1").await?;
    }

    let rows = proxy.query(&format!("SHOW {guc_name}"), &[]).await?;
    let observed: String = rows[0].get(0);
    anyhow::ensure!(
        observed == guc_value,
        "extended-protocol SET was lost on the SAME client — Hybrid mode recycled the connection \
         instead of pinning it (expected '{guc_value}', got '{observed}')"
    );

    let fresh = connect_client("127.0.0.1", proxy_port, user, password, dbname).await?;
    let fresh_rows = fresh.query(&format!("SHOW {guc_name}"), &[]).await?;
    let fresh_value: String = fresh_rows[0].get(0);
    anyhow::ensure!(
        fresh_value != guc_value,
        "extended-protocol SET leaked to a fresh pooled client (expected != '{guc_value}', got '{fresh_value}')"
    );

    Ok(())
}

/// Hybrid-mode-only proof (WP-9 Task 5, P2 §4.3) that a `LISTEN` registration
/// pins its connection: the mirror of [`assert_extended_state_survives_hybrid_recycle`]
/// above, but for `PinReason::Listen` instead of `PinReason::SessionVariable`.
///
/// There's no `SHOW`-able GUC for "am I still listening" the way there is for a
/// session variable, so this uses Postgres's own `pg_listening_channels()` builtin
/// (returns the set of channels the *current backend session* is subscribed to) as
/// the observable signal in exactly the same two-part shape Task 4 used:
///   (a) on the SAME proxy client, after enough further activity that an unpinned
///       connection would already have been released and recycled, `channel` must
///       still appear in `pg_listening_channels()` — proof the backend wasn't
///       swapped out from under the LISTEN.
///   (b) a FRESH client against the same pooled proxy port must NOT see `channel` —
///       proof the registration didn't leak onto some other pooled session.
///
/// As with Task 4, `LISTEN` is sent via the EXTENDED protocol (`.execute()`) —
/// wrapped in a simple-query `BEGIN`/`DEALLOCATE ALL`/`COMMIT` for the same isolation
/// reason documented on `assert_extended_state_survives_hybrid_recycle`: only an
/// explicit transaction boundary makes `connection.rs` evaluate
/// `should_release_connection` at all (autocommit statements never trigger a release
/// check — see the comment in `connection.rs` near `just_finished_transaction`), and
/// `DEALLOCATE ALL` strips the incidental `PreparedStatement` pin that every `Parse`
/// adds, so the only pin left standing when the release check runs is the one this
/// task added: `PinReason::Listen`.
///
/// Note this can only meaningfully prove non-recycle in Hybrid mode. Under strict
/// Transaction pooling, `should_release_connection` unconditionally returns `true`
/// regardless of any pin (`PoolingStrategy::Transaction => true`) — matching real
/// PgBouncer transaction-pooling mode, where LISTEN/NOTIFY is documented as
/// unsupported across an explicit transaction boundary. So this assertion is
/// intentionally Hybrid-only; asserting it under Transaction mode would fail not
/// because Listen pinning is broken, but because Transaction mode deliberately never
/// honors pins at all.
pub async fn assert_listen_survives_hybrid_recycle(
    proxy: &Client,
    proxy_port: u16,
    user: &str,
    password: &str,
    dbname: &str,
    channel: &str,
) -> anyhow::Result<()> {
    proxy.simple_query("BEGIN").await?;
    proxy.execute(&format!("LISTEN {channel}"), &[]).await?; // EXTENDED protocol — the thing under test
    proxy.simple_query("DEALLOCATE ALL").await?; // strips the incidental PreparedStatement pin only
    proxy.simple_query("COMMIT").await?; // transaction completes -> Hybrid release check runs

    // Enough further activity that an un-pinned connection would already
    // have been released back to the pool (and DISCARD-ALL reset) by now.
    for _ in 0..5 {
        proxy.simple_query("SELECT 1").await?;
    }

    let rows = proxy.query("SELECT pg_listening_channels()", &[]).await?;
    let channels: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    anyhow::ensure!(
        channels.iter().any(|c| c == channel),
        "LISTEN registration was lost on the SAME client — Hybrid mode recycled the connection \
         instead of pinning it (expected '{channel}' among {channels:?})"
    );

    let fresh = connect_client("127.0.0.1", proxy_port, user, password, dbname).await?;
    let fresh_rows = fresh.query("SELECT pg_listening_channels()", &[]).await?;
    let fresh_channels: Vec<String> = fresh_rows.iter().map(|r| r.get(0)).collect();
    anyhow::ensure!(
        !fresh_channels.iter().any(|c| c == channel),
        "LISTEN registration leaked to a fresh pooled client (found '{channel}' among {fresh_channels:?})"
    );

    Ok(())
}

/// The comparator: asserts that the direct and proxied outcomes of the same
/// operation are equivalent — same rows (values + column names/count), same
/// command tag / rows-affected, and (for errors) the same SQLSTATE code.
/// `context` is prefixed to failure messages to identify which case failed.
///
/// This is the load-bearing assertion for the whole differential suite: if
/// it ever degenerated to always passing, every test built on it would be
/// worthless. See the self-proof test in
/// `differential_transparency_test.rs` for the check that it actually
/// discriminates.
pub fn assert_outcomes_equivalent(direct: &RunOutcome, proxy: &RunOutcome, context: &str) {
    match (direct, proxy) {
        (RunOutcome::Ok(d), RunOutcome::Ok(p)) => {
            assert_eq!(
                d, p,
                "{context}: direct and proxied results diverged (rows/columns/command-tag)"
            );
        }
        (RunOutcome::Err { sqlstate: d }, RunOutcome::Err { sqlstate: p }) => {
            assert_eq!(d, p, "{context}: direct and proxied SQLSTATE diverged");
        }
        (direct, proxy) => {
            panic!(
                "{context}: one side errored and the other did not — direct={direct:?} proxy={proxy:?}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Pool-cleanliness probe
// ---------------------------------------------------------------------------

/// Extracts the first column of the first row from a simple-query message
/// stream, if any (used by [`assert_session_state_clean`]).
fn simple_first_value(msgs: &[SimpleQueryMessage]) -> Option<String> {
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::Row(row) if row.len() > 0 => row.get(0).map(|s| s.to_string()),
        _ => None,
    })
}

/// Opens a fresh connection to `host:port` and asserts no session-local state
/// leaked from a previous client sharing this proxy's pool: `guc_name` must
/// still report `expected_guc_value` (its default, if the caller never sets
/// it), and `temp_table_name` (a previously-created temp table, schema-
/// qualified e.g. `"pg_temp.leak_probe"`) must not be visible via
/// `to_regclass`.
///
/// This is the proof that pooling doesn't leak state: Postgres temp tables
/// and session GUCs live on the *backend* connection, not the logical client
/// session, so if the proxy hands the same backend connection to a new
/// client without resetting it, this probe catches it.
pub async fn assert_session_state_clean(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    dbname: &str,
    guc_name: &str,
    expected_guc_value: &str,
    temp_table_name: &str,
) -> anyhow::Result<()> {
    let client = connect_client(host, port, user, password, dbname).await?;

    let msgs = client.simple_query(&format!("SHOW {guc_name}")).await?;
    let actual_guc = simple_first_value(&msgs).unwrap_or_default();
    anyhow::ensure!(
        actual_guc.eq_ignore_ascii_case(expected_guc_value),
        "GUC {guc_name} leaked across pooled sessions: expected '{expected_guc_value}', got '{actual_guc}'"
    );

    let msgs =
        client.simple_query(&format!("SELECT to_regclass('{temp_table_name}')::text")).await?;
    let visible = simple_first_value(&msgs);
    anyhow::ensure!(
        visible.is_none(),
        "temp table {temp_table_name} leaked across pooled sessions (to_regclass returned {visible:?})"
    );

    Ok(())
}
