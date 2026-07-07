//! Differential transparency baseline matrix (WP-9, P2 §5.1).
//!
//! For every pooling mode, runs the same simple-protocol operations directly
//! against Postgres and through the proxy, and asserts the two are
//! indistinguishable to a client: same rows, same command tag / rows-affected,
//! same SQLSTATE on error, and no pooled-state leakage between sessions.
//!
//! These are baseline ops that already work today. Later WP-9 tasks extend
//! this matrix with trickier cases (extended protocol, COPY, LISTEN/NOTIFY,
//! prepared statements across pooled connections, etc.) using the same
//! `tests/common` harness.
mod common;

use common::*;
use std::time::Duration;
use testcontainers::{clients::Cli, RunnableImage};
use testcontainers_modules::postgres::Postgres;
use tokio::time::sleep;

#[tokio::test]
async fn baseline_matrix_all_modes() {
    // Single container reused across all four pooling modes: each mode
    // iteration restarts only the proxy (a fresh `paired_clients` call), not
    // Postgres itself, keeping the container count (and test runtime) down.
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);
    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1";

    sleep(Duration::from_secs(2)).await;

    for mode in all_modes() {
        println!("=== baseline matrix: pooling mode {mode:?} ===");

        let PairedClients { proxy_port, proxy, direct } =
            paired_clients(postgres_host, postgres_port, mode.clone())
                .await
                .unwrap_or_else(|e| panic!("failed to start paired clients for {mode:?}: {e}"));

        // 1. SELECT 1
        let d = run_simple(&direct, "SELECT 1").await;
        let p = run_simple(&proxy, "SELECT 1").await;
        assert_outcomes_equivalent(&d, &p, &format!("[{mode:?}] SELECT 1"));

        // 2. Multi-row SELECT.
        let d = run_simple(&direct, "SELECT * FROM generate_series(1, 5)").await;
        let p = run_simple(&proxy, "SELECT * FROM generate_series(1, 5)").await;
        assert_outcomes_equivalent(&d, &p, &format!("[{mode:?}] generate_series"));

        // 3. INSERT ... RETURNING. Uses a real (non-temp) table, dropped and
        // recreated in the *same* simple-query round trip so direct vs. proxy
        // (and repeated mode iterations against the same reused container)
        // never collide. A temp table would be a cleaner probe of pooling
        // state, but `CREATE TEMP TABLE` is deliberately rejected by the
        // proxy's `ModeEnforcer` under strict Transaction pooling (P3-era
        // PgBouncer-compatible restriction, `mode_enforcer.rs`) — that's
        // intentional, non-transparent-by-design behavior, not something this
        // "ops that already work" baseline should paper over, so it can't be
        // the vehicle for the basic INSERT/RETURNING equivalence check.
        let insert_sql = "DROP TABLE IF EXISTS wp9_insert_probe; \
             CREATE TABLE wp9_insert_probe (id int, value text); \
             INSERT INTO wp9_insert_probe (id, value) VALUES (1, 'x') RETURNING id, value;";
        let d = run_simple(&direct, insert_sql).await;
        let p = run_simple(&proxy, insert_sql).await;
        assert_outcomes_equivalent(&d, &p, &format!("[{mode:?}] INSERT ... RETURNING"));

        // 4. BEGIN; ...; COMMIT transaction.
        let txn_sql = "BEGIN; SELECT 1; COMMIT;";
        let d = run_simple(&direct, txn_sql).await;
        let p = run_simple(&proxy, txn_sql).await;
        assert_outcomes_equivalent(&d, &p, &format!("[{mode:?}] BEGIN/COMMIT"));

        // 5. Error case: SQLSTATE parity (message text is allowed to differ).
        let d = run_simple(&direct, "SELECT 1/0").await;
        let p = run_simple(&proxy, "SELECT 1/0").await;
        assert_outcomes_equivalent(&d, &p, &format!("[{mode:?}] division by zero"));

        // Pool cleanliness: a brand-new connection through the same (pooled)
        // proxy port must not see a temp table the previous proxy client
        // created, and `statement_timeout` (never touched by our test config
        // or by the proxy's client-side timeout enforcement) must still
        // report its default. This is the "post-op session-observable state"
        // check called out in the task brief.
        //
        // Skipped under strict Transaction pooling: `CREATE TEMP TABLE` is
        // deliberately rejected there by `ModeEnforcer` (see comment above),
        // so there is no temp table to probe for in that mode — the proxy
        // prevents the leak vector outright rather than requiring cleanup.
        if mode != scry::config::PoolingStrategy::Transaction {
            let probe_table = "wp9_pool_cleanliness_probe";
            let create =
                run_simple(&proxy, &format!("CREATE TEMP TABLE {probe_table} (x int)")).await;
            assert!(
                matches!(create, RunOutcome::Ok(_)),
                "[{mode:?}] expected CREATE TEMP TABLE to succeed on the proxy client so the \
                 cleanliness probe has something to probe for, got {create:?}"
            );

            assert_session_state_clean(
                "127.0.0.1",
                proxy_port,
                "postgres",
                "postgres",
                "postgres",
                "statement_timeout",
                "0",
                probe_table,
            )
            .await
            .unwrap_or_else(|e| panic!("[{mode:?}] pool cleanliness probe failed: {e}"));
        }

        println!("=== baseline matrix: pooling mode {mode:?} OK ===");
    }
}

/// Extended-protocol matrix (WP-9 Task 4, P2 §4.2): the same stateful ops as
/// the simple-protocol baseline above, but driven via `.execute()`/`.query()`
/// — Parse/Bind/Execute, the protocol every modern driver (including
/// `tokio_postgres`'s own `.query()`/`.execute()`) actually uses for normal
/// operation. Before this task, `connection.rs`'s `Message::Parse` arm never
/// classified the SQL it carried, so none of these commands set any pin/
/// state on `ConnectionState` — this suite is what catches that gap; see
/// especially the Hybrid-only non-recycle check at the end of the loop.
#[tokio::test]
async fn extended_protocol_matrix_all_modes() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);
    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1";

    sleep(Duration::from_secs(2)).await;

    for mode in all_modes() {
        println!("=== extended-protocol matrix: pooling mode {mode:?} ===");

        let PairedClients { proxy_port, proxy, direct } =
            paired_clients(postgres_host, postgres_port, mode.clone())
                .await
                .unwrap_or_else(|e| panic!("failed to start paired clients for {mode:?}: {e}"));

        // 1. SET a session GUC, wrapped in an explicit transaction, via the
        // EXTENDED protocol. Wrapping in BEGIN/COMMIT is required regardless
        // of mode: `connection.rs` only evaluates its Hybrid release
        // decision when a transaction completes (never for autocommit
        // statements), and a bare SET outside a transaction is rejected by
        // ModeEnforcer under strict Transaction pooling (same scoping as the
        // Task 2 baseline) — SET *inside* a transaction is allowed in every
        // mode.
        {
            let d1 = run_extended(&direct, "BEGIN").await;
            let d2 = run_extended(&direct, "SET application_name = 'wp9_ext_set'").await;
            let d3 = run_extended(&direct, "COMMIT").await;
            let p1 = run_extended(&proxy, "BEGIN").await;
            let p2 = run_extended(&proxy, "SET application_name = 'wp9_ext_set'").await;
            let p3 = run_extended(&proxy, "COMMIT").await;
            assert_outcomes_equivalent(&d1, &p1, &format!("[{mode:?}] extended BEGIN"));
            assert_outcomes_equivalent(&d2, &p2, &format!("[{mode:?}] extended SET"));
            assert_outcomes_equivalent(&d3, &p3, &format!("[{mode:?}] extended COMMIT"));
        }

        // 2. CREATE TEMP TABLE via extended protocol — skipped under
        // Transaction mode, which ModeEnforcer unconditionally rejects (same
        // scoping as the simple-protocol baseline above).
        if mode != scry::config::PoolingStrategy::Transaction {
            let table = "wp9_ext_temp_probe";
            let d = run_extended(&direct, &format!("CREATE TEMP TABLE {table} (id int)")).await;
            let p = run_extended(&proxy, &format!("CREATE TEMP TABLE {table} (id int)")).await;
            assert_outcomes_equivalent(&d, &p, &format!("[{mode:?}] extended CREATE TEMP TABLE"));
            let _ = run_extended(&direct, &format!("DROP TABLE {table}")).await;
            let _ = run_extended(&proxy, &format!("DROP TABLE {table}")).await;
        }

        // 3. DECLARE CURSOR (must be inside a transaction — a Postgres
        // requirement independent of pooling mode) via extended protocol.
        {
            let _ = run_extended(&direct, "BEGIN").await;
            let d = run_extended(&direct, "DECLARE wp9_ext_cursor CURSOR FOR SELECT 1").await;
            let _ = run_extended(&direct, "COMMIT").await;

            let _ = run_extended(&proxy, "BEGIN").await;
            let p = run_extended(&proxy, "DECLARE wp9_ext_cursor CURSOR FOR SELECT 1").await;
            let _ = run_extended(&proxy, "COMMIT").await;

            assert_outcomes_equivalent(&d, &p, &format!("[{mode:?}] extended DECLARE CURSOR"));
        }

        // 4. pg_advisory_lock via extended protocol — skipped under
        // Transaction mode (unconditionally rejected by ModeEnforcer).
        // Direct and proxy use DISTINCT lock keys: `pg_advisory_lock` blocks
        // across sessions for the same key, and direct/proxy are separate
        // backend sessions here, so sharing a key would deadlock the second
        // acquire against the first (which isn't released until after both
        // calls return).
        if mode != scry::config::PoolingStrategy::Transaction {
            let d = run_extended(&direct, "SELECT pg_advisory_lock(424242)").await;
            let p = run_extended(&proxy, "SELECT pg_advisory_lock(424243)").await;
            assert_outcomes_equivalent(&d, &p, &format!("[{mode:?}] extended pg_advisory_lock"));
            let _ = run_extended(&direct, "SELECT pg_advisory_unlock(424242)").await;
            let _ = run_extended(&proxy, "SELECT pg_advisory_unlock(424243)").await;
        }

        // 5. Hybrid-only: the connection carrying extended-protocol state
        // must NOT be recycled underneath the client. This is the assertion
        // that FAILS before the Parse-arm fix (Hybrid releases the un-pinned
        // connection at COMMIT, and pool recycle's DISCARD ALL wipes the
        // GUC) and PASSES after (Parse-arm classification pins it).
        if mode == scry::config::PoolingStrategy::Hybrid {
            assert_extended_state_survives_hybrid_recycle(
                &proxy,
                proxy_port,
                "postgres",
                "postgres",
                "postgres",
                "application_name",
                "wp9_ext_hybrid_probe",
            )
            .await
            .unwrap_or_else(|e| {
                panic!("[{mode:?}] Hybrid extended-protocol pin check failed: {e}")
            });
        }

        // 6. LISTEN/UNLISTEN via extended protocol (WP-9 Task 5, P2 §4.3):
        // outcome equivalence for every mode (the command itself must
        // execute identically direct vs. proxied), plus — Hybrid-only, same
        // shape as check 5 above — a positive proof that the typed
        // `PinReason::Listen` keeps the connection from being recycled out
        // from under an active registration. `UNLISTEN *` cleans up so the
        // registration doesn't outlive this block on either client.
        {
            let d = run_extended(&direct, "LISTEN wp9_ext_listen_probe").await;
            let p = run_extended(&proxy, "LISTEN wp9_ext_listen_probe").await;
            assert_outcomes_equivalent(&d, &p, &format!("[{mode:?}] extended LISTEN"));
            let _ = run_extended(&direct, "UNLISTEN *").await;
            let _ = run_extended(&proxy, "UNLISTEN *").await;
        }

        if mode == scry::config::PoolingStrategy::Hybrid {
            assert_listen_survives_hybrid_recycle(
                &proxy,
                proxy_port,
                "postgres",
                "postgres",
                "postgres",
                "wp9_ext_listen_hybrid_probe",
            )
            .await
            .unwrap_or_else(|e| panic!("[{mode:?}] Hybrid LISTEN pin check failed: {e}"));
        }

        println!("=== extended-protocol matrix: pooling mode {mode:?} OK ===");
    }
}

/// Multi-statement fail-closed pinning under Hybrid pooling (WP-9 Task 9, P2
/// §4.6, priority correctness item).
///
/// # The bug
/// `CommandDetector::classify` (and, before this task, `ConnectionState::
/// apply_query`) classified a whole simple-`Query` string by its LEADING
/// keyword only. `"SELECT 1; SET application_name = 'wp9_multi_probe'"` leads
/// with `SELECT`, a known-clean keyword — so the entire batch, trailing `SET`
/// included, classified as `Clean`, and `apply_query` set NO pin. In Hybrid
/// mode a clean connection is released back to the pool at the next
/// transaction boundary, so the `SET`'s GUC leaked onto whichever pooled
/// backend the next client happened to land on. This is a real transparency/
/// isolation correctness bug, not merely an event-attribution gap.
///
/// # Why this must be wrapped in an explicit transaction
/// `connection.rs` only evaluates its Hybrid release-vs-pin decision
/// (`should_release_connection`) when a `ReadyForQuery` transitions from
/// `InTransaction` back to `Idle` — i.e. only across an EXPLICIT `BEGIN`/
/// `COMMIT` boundary that spans separate round trips (see
/// `TransactionTracker`). A bare autocommit multi-statement batch (no
/// surrounding `BEGIN`/`COMMIT` in a separate round trip) never produces that
/// transition — Postgres auto-commits the implicit block and replies with a
/// single `ReadyForQuery('I')`, which the proxy cannot distinguish from "was
/// already idle". So `BEGIN` and `COMMIT` are sent as their OWN
/// `simple_query` calls (separate round trips), with the multi-statement
/// probe itself as a THIRD, separate `simple_query` call in between — exactly
/// the shape `assert_extended_state_survives_hybrid_recycle` /
/// `dirty_connection_invariant_hybrid_mode`'s client C block use for the same
/// reason. No `Parse` is ever sent here (pure simple-protocol), so — unlike
/// the extended-protocol tests — there is no incidental `PreparedStatement`
/// pin to strip first; the only pin possible is the `SessionVariable` one
/// this task adds.
///
/// # RED before the fix / GREEN after
/// Before Task 9: the multi-statement batch classifies `Clean` (leading-
/// keyword-only classification), so `should_release_connection` releases the
/// connection at `COMMIT`; deadpool's `recycle()` then `DISCARD ALL`s it
/// before anyone else can observe it, so this test's own client would already
/// see the GUC gone by the "same client" check below — but at pool sizes > 1,
/// or via a genuine mid-flight race, a fresh client for the same pooled proxy
/// port getting this exact connection while it is still marked "released, not
/// yet recycled" is exactly how a `SessionVariable` GUC set by one logical
/// client becomes visible to another: the connection carries live, un-pinned
/// state back to the pool. Manually verified RED against the pre-fix
/// `ConnectionState::apply_query` (single-statement classification only): the
/// "same client still sees the value" assertion below fails first (COMMIT
/// silently drops the GUC), which is the direct, deterministic signature of
/// the leading-keyword-only bug — see the task report for the exact
/// pre-fix/post-fix `cargo test` transcript.
#[tokio::test]
async fn multi_statement_simple_query_fail_closed_hybrid_mode() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);
    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1";

    sleep(Duration::from_secs(2)).await;

    let PairedClients { proxy_port, proxy, direct: _direct } =
        paired_clients(postgres_host, postgres_port, scry::config::PoolingStrategy::Hybrid)
            .await
            .expect("failed to start paired clients for Hybrid mode");

    let probe_value = "wp9_multi_probe";

    proxy.simple_query("BEGIN").await.expect("BEGIN");
    // The thing under test: a single simple-Query message carrying TWO
    // `;`-separated statements, the second of which is state-changing. Only
    // the multi-statement-aware `apply_query` (this task's fix) observes the
    // trailing SET; leading-keyword-only classification sees just `SELECT`.
    proxy
        .simple_query(&format!("SELECT 1; SET application_name = '{probe_value}'"))
        .await
        .expect("multi-statement simple query should succeed in Hybrid mode");
    proxy.simple_query("COMMIT").await.expect("COMMIT"); // transaction completes -> Hybrid release check runs

    // Enough further activity that an un-pinned connection would already have
    // been released back to the pool (and DISCARD-ALL reset) by now.
    for _ in 0..5 {
        proxy.simple_query("SELECT 1").await.expect("keepalive SELECT 1");
    }

    // Same client: the GUC must still be visible — proof the connection was
    // pinned (not silently released+recycled) rather than the SET having been
    // dropped some other way. (Uses the extended protocol purely to read the
    // value back; by this point the release-vs-pin decision at COMMIT has
    // already run, so a Parse here adds no incidental pin relevant to the
    // check.)
    let same_client_value: String = proxy
        .query_one("SHOW application_name", &[])
        .await
        .expect("SHOW application_name on same client")
        .get(0);
    assert_eq!(
        same_client_value, probe_value,
        "multi-statement SET was silently dropped on the SAME client — the trailing SET in \
         \"SELECT 1; SET application_name = '...'\" was never classified/applied, so Hybrid \
         mode recycled the connection instead of pinning it"
    );

    // Fresh client, same pooled proxy port: must NOT see the value — proof
    // the pin kept this connection from being handed to another client dirty.
    assert_session_state_clean(
        "127.0.0.1",
        proxy_port,
        "postgres",
        "postgres",
        "postgres",
        "application_name",
        "",
        "wp9_multistmt_nonexistent_probe",
    )
    .await
    .unwrap_or_else(|e| panic!("Hybrid multi-statement fail-closed pin check failed: {e}"));
}

/// Multi-statement event attribution (WP-9 Task 9, P2 §4.6, observability item
/// 1).
///
/// Before this task, a multi-statement simple-Query batch (one `Query`
/// message, N `CommandComplete`s on the wire) was attributed to a SINGLE
/// `QueryEvent` under the first `CommandComplete` found — `pending.query` held
/// the *entire* `;`-joined batch text as one event. This asserts the fix:
/// `connection.rs`'s backend-response handler now re-uses
/// `CommandDetector::split_statements` (the exact same fail-closed split the
/// priority-item fix above uses for pinning) to emit one event PER statement.
/// Best-effort observability only — this test never inspects the bytes on the
/// wire, only the published `QueryEvent`s; the byte-level transparency
/// guarantee is what the rest of this suite's `assert_outcomes_equivalent`
/// calls already prove.
#[tokio::test]
async fn multi_statement_event_attribution() {
    use scry::publisher::EventPublisher;
    use std::sync::Arc;

    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);
    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1";

    sleep(Duration::from_secs(2)).await;

    let config = create_test_config(
        postgres_host.to_string(),
        postgres_port,
        scry::config::PoolingStrategy::Session,
    );
    let test_publisher = TestPublisher::new();
    let publisher: Arc<dyn EventPublisher> = Arc::new(test_publisher.clone());
    let proxy_port =
        start_test_proxy(config.clone(), publisher).await.expect("failed to start proxy");
    sleep(Duration::from_millis(300)).await;

    let client = connect_client("127.0.0.1", proxy_port, "postgres", "postgres", "postgres")
        .await
        .expect("client connect");

    client
        .simple_query("SELECT 1; SELECT 2")
        .await
        .expect("multi-statement simple query should succeed");

    // Give the batcher a moment to flush (config's flush_interval_ms is 100).
    sleep(Duration::from_millis(500)).await;

    let events = test_publisher.events();
    let queries: Vec<&str> = events.iter().map(|e| e.query.as_str()).collect();

    assert!(
        queries.iter().any(|q| q.trim() == "SELECT 1"),
        "expected an event attributed to \"SELECT 1\" alone, got: {queries:?}"
    );
    assert!(
        queries.iter().any(|q| q.trim() == "SELECT 2"),
        "expected an event attributed to \"SELECT 2\" alone, got: {queries:?}"
    );
    assert!(
        !queries.iter().any(|q| q.contains(';')),
        "expected no single merged multi-statement event (containing ';'); got: {queries:?}"
    );
}

/// COPY passthrough-unchanged assertion (WP-9 Task 9, observability item 2,
/// P2 §9.4).
///
/// COPY stays passthrough-only for 1.0 by design: `CommandDetector`
/// classifies `COPY ... FROM/TO STDIN/STDOUT` as `Unknown` (see
/// `pooling_safety_test.rs`'s `MUST_PIN_UNKNOWN` list), which fail-closed PINS
/// the connection rather than attempting to model/replay COPY's own
/// sub-protocol (no state modeling is added here, or ever planned for 1.0).
/// This test is the promised deliverable for that scoping decision: proof the
/// COPY data itself is forwarded byte-for-byte, direct vs. proxied — both
/// directions (`FROM STDIN` and `TO STDOUT`) — exactly like every other
/// operation this differential suite already covers for ordinary queries.
#[tokio::test]
async fn copy_passthrough_byte_identical() {
    use bytes::{Bytes, BytesMut};
    use futures::{SinkExt, TryStreamExt};
    use std::pin::pin;

    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);
    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1";

    sleep(Duration::from_secs(2)).await;

    // Hybrid mode: COPY's `Unknown` classification is exactly what should
    // fail-closed pin the connection for the duration of the COPY, so this
    // exercises the interaction (pinned connection, passthrough bytes) rather
    // than a mode where pinning is moot.
    let PairedClients { proxy_port: _, proxy, direct } =
        paired_clients(postgres_host, postgres_port, scry::config::PoolingStrategy::Hybrid)
            .await
            .expect("failed to start paired clients for Hybrid mode");

    for (client, table) in [(&direct, "wp9_copy_direct"), (&proxy, "wp9_copy_proxy")] {
        client.simple_query(&format!("DROP TABLE IF EXISTS {table}")).await.unwrap();
        client.simple_query(&format!("CREATE TABLE {table} (id int, value text)")).await.unwrap();
    }

    // COPY ... FROM STDIN: stream the identical payload through direct and
    // proxy, via the extended protocol (as tokio_postgres's `copy_in` always
    // uses) — the same path a real client library uses.
    let payload = Bytes::from_static(b"1\thello\n2\tworld\n3\tembedded space\n");

    let mut sink = pin!(direct.copy_in("COPY wp9_copy_direct FROM STDIN").await.unwrap());
    sink.send(payload.clone()).await.expect("direct COPY FROM STDIN send");
    sink.finish().await.expect("direct COPY FROM STDIN finish");

    let mut sink = pin!(proxy.copy_in("COPY wp9_copy_proxy FROM STDIN").await.unwrap());
    sink.send(payload.clone()).await.expect("proxied COPY FROM STDIN send");
    sink.finish().await.expect("proxied COPY FROM STDIN finish");

    // COPY ... TO STDOUT: read the raw COPY byte stream back out (not via
    // SELECT, which would go through a different code path) on both sides and
    // assert byte-for-byte equality between direct and proxied.
    let direct_out = direct
        .copy_out("COPY wp9_copy_direct TO STDOUT")
        .await
        .expect("direct COPY TO STDOUT")
        .try_fold(BytesMut::new(), |mut buf, chunk| async move {
            buf.extend_from_slice(&chunk);
            Ok(buf)
        })
        .await
        .expect("direct COPY TO STDOUT collect");

    let proxy_out = proxy
        .copy_out("COPY wp9_copy_proxy TO STDOUT")
        .await
        .expect("proxied COPY TO STDOUT")
        .try_fold(BytesMut::new(), |mut buf, chunk| async move {
            buf.extend_from_slice(&chunk);
            Ok(buf)
        })
        .await
        .expect("proxied COPY TO STDOUT collect");

    assert_eq!(
        &direct_out[..],
        &proxy_out[..],
        "COPY TO STDOUT bytes diverged between direct and proxied connections — the proxy \
         must forward COPY data byte-for-byte (passthrough-only by design, P2 §9.4)"
    );
    assert_eq!(
        &direct_out[..],
        &payload[..],
        "COPY round trip altered the data even relative to what was sent (sanity check on the \
         test itself, not the proxy)"
    );
}

/// Comparator self-proof (task brief deliverable 3): feeds the comparator two
/// deliberately different results and confirms it reports divergence, rather
/// than trivially passing. Container-free — this is pure logic over
/// hand-built `RunOutcome`/`QuerySnapshot` values.
///
/// Without this test, a comparator that always returned "equal" would make
/// the entire differential suite worthless. This was manually verified to
/// fail when `assert_outcomes_equivalent` was temporarily stubbed to a no-op
/// (see task report for details); it is restored to the real implementation
/// here.
#[test]
fn comparator_discriminates_different_results() {
    let row_1 = RunOutcome::Ok(QuerySnapshot {
        statements: vec![StatementResult {
            columns: vec!["n".to_string()],
            rows: vec![vec![Some("1".to_string())]],
            rows_affected: 1,
        }],
    });
    let row_2 = RunOutcome::Ok(QuerySnapshot {
        statements: vec![StatementResult {
            columns: vec!["n".to_string()],
            rows: vec![vec![Some("2".to_string())]], // deliberately different value
            rows_affected: 1,
        }],
    });

    let diverged_rows = std::panic::catch_unwind(|| {
        assert_outcomes_equivalent(&row_1, &row_2, "self-proof: deliberately different rows");
    });
    assert!(
        diverged_rows.is_err(),
        "comparator failed to detect divergent row values — it is not discriminating"
    );

    // Ok vs. Err must diverge.
    let an_error = RunOutcome::Err { sqlstate: Some("42601".to_string()) };
    let ok_vs_err = std::panic::catch_unwind(|| {
        assert_outcomes_equivalent(&row_1, &an_error, "self-proof: ok vs err must diverge");
    });
    assert!(ok_vs_err.is_err(), "comparator failed to detect an Ok-vs-Err divergence");

    // Two different SQLSTATE codes must diverge.
    let a_different_error = RunOutcome::Err { sqlstate: Some("22012".to_string()) };
    let diverged_sqlstate = std::panic::catch_unwind(|| {
        assert_outcomes_equivalent(
            &an_error,
            &a_different_error,
            "self-proof: different sqlstate must diverge",
        );
    });
    assert!(diverged_sqlstate.is_err(), "comparator failed to detect different SQLSTATE codes");

    // Sanity: identical results must NOT diverge (would panic if they did).
    assert_outcomes_equivalent(
        &row_1,
        &row_1.clone(),
        "self-proof: identical results must be equal",
    );
}

/// Prepared-statement honesty under strict Transaction pooling (WP-9 Task 6,
/// P2 §4.5). This is the guardrail for the restrict-by-pinning resolution: a
/// connection carrying a client-cached prepared statement must be PINNED (not
/// released) in Transaction mode, so the statement keeps working across a
/// transaction boundary. There must be no silent "prepared statement does not
/// exist" failure.
///
/// # Why this is RED before the fix and GREEN after (load-bearing on the
/// release-predicate change)
///
/// Before restrict-by-pinning, `should_release_connection` returned `true`
/// unconditionally in Transaction mode, so at the `COMMIT` below the proxy
/// released the connection and re-acquired one from the pool. deadpool's
/// `recycle` runs `DISCARD ALL` on checkout, which wipes the backend's
/// prepared statement; the proxy then attempted to restore it via a
/// SQL-level `PREPARE "<name>" AS <query>` replay that **drops the
/// client-supplied parameter OIDs**. This test deliberately prepares
/// `SELECT $1` with the parameter type supplied by the client (`int8`): it
/// round-trips fine over the extended protocol (the client sends the OID),
/// but the OID-less `PREPARE ... AS SELECT $1` replay re-resolves `$1` to a
/// different type, so the client's cached binary parameter no longer matches
/// the re-prepared statement. The reuse below therefore fails with a
/// client-visible error (observed: `22021` invalid byte sequence — the binary
/// `int8` bytes reinterpreted as text — but the general failure is "the
/// statement the client cached no longer behaves as prepared") that the client
/// gets NO proxy explanation for. That silent corruption is the bug.
///
/// After restrict-by-pinning (`Transaction => !conn_state.is_pinned()`), the
/// prepared statement keeps the connection pinned, it is never released at the
/// `COMMIT`, the same backend keeps the statement, and the reuse succeeds.
/// Reverting the predicate change re-breaks this assertion.
#[tokio::test]
async fn transaction_mode_prepared_statement_honesty() {
    use tokio_postgres::types::Type;

    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);
    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1";

    sleep(Duration::from_secs(2)).await;

    let PairedClients { proxy, .. } =
        paired_clients(postgres_host, postgres_port, scry::config::PoolingStrategy::Transaction)
            .await
            .expect("failed to start Transaction-mode paired clients");

    // Prepare a statement whose parameter type CANNOT be inferred from the
    // query text — the client supplies the OID (int8). This round-trips fine
    // through the proxy (autocommit; pinned by the prepared statement), but
    // defeats the old OID-less SQL-PREPARE replay path on purpose.
    let stmt = proxy
        .prepare_typed("SELECT $1", &[Type::INT8])
        .await
        .expect("prepare_typed should succeed through the proxy");

    // Cross an explicit transaction boundary — the ONLY thing that makes
    // connection.rs evaluate its release decision (autocommit statements never
    // trigger a release check). Sent via the simple protocol (batch_execute) as
    // separate round trips so the transaction tracker observes ReadyForQuery(T)
    // after BEGIN and ReadyForQuery(I) after COMMIT — the exact transition the
    // old code released on.
    proxy.batch_execute("BEGIN").await.expect("BEGIN");
    proxy.batch_execute("SELECT 1").await.expect("SELECT 1 within transaction");
    proxy.batch_execute("COMMIT").await.expect("COMMIT");

    // Reuse the cached prepared statement AFTER the release boundary. Under
    // restrict-by-pinning the connection was never released, so this works.
    let rows = proxy.query(&stmt, &[&7i64]).await.expect(
        "cached prepared statement must still work across a Transaction-mode release \
         boundary (restrict-by-pinning); a failure here is the silent prepared-statement \
         destruction this test guards against",
    );
    let value: i64 = rows[0].get(0);
    assert_eq!(value, 7, "prepared statement returned the wrong value after the txn boundary");
}

/// §5.3 dirty-connection invariant auditor (WP-9 Task 8, P2 §5.3) — Hybrid mode.
///
/// Proves BOTH halves of the named invariant on a connection that is
/// GENUINELY recycled between clients, not merely assumed to be:
///
/// (a) **Cleanliness**: once client A's logical session ends (disconnects),
///     NONE of the state it left on the backend connection is observable to
///     a fresh client B that gets handed the exact same physical connection.
/// (b) **No silent drop**: state a client is still actively using (mid-
///     session, well past the point an unpinned connection would have been
///     recycled out from under it) MUST survive — the fail-closed guarantee
///     Task 4/6 established, asserted here explicitly as the §5.3 invariant.
///
/// # Forcing genuine reuse (not a vacuous pass)
/// Uses [`start_pool_capped_proxy`] with `pool_size = 1` (the CRIT-1 pattern
/// from `connection_multiplexing.rs`): with exactly one physical backend
/// connection possible, client B's `connect()` cannot proceed until client
/// A's connection is returned to the pool, and there is no other connection
/// it could possibly receive. This is confirmed, not just argued, by reading
/// `SELECT pg_backend_pid()` from each client and asserting they match:
/// `DISCARD ALL` resets session state but never changes the backend process,
/// so identical PIDs are direct proof of physical-connection reuse. Client C
/// (invariant (b)) reuses the same single connection again after client B
/// disconnects, so its pid is checked too.
///
/// # Proves genuine warm cross-client reuse (GREEN, WP-9 Task 8, P2 §5.3)
/// This anti-vacuity guardrail (comparing `pg_backend_pid()`) confirms client
/// B lands on client A's exact backend pid, not a fresh one. `connection.rs`'s
/// client-read loop intercepts the client's graceful-disconnect `Terminate`
/// ('X') — sent by `tokio_postgres`'s own connection driver on disconnect,
/// standard spec-compliant client behavior (see
/// `tokio-postgres-0.7.18/src/connection.rs`, the `"at eof, terminating"`
/// branch) — instead of forwarding it to the real backend. The physical
/// Postgres session therefore stays alive and is released to the pool warm;
/// deadpool's `recycle()` health-check finds it healthy and `DISCARD ALL`-
/// resets it before handing it to the next client, so reuse is both warm AND
/// clean. This holds for any well-behaved client's normal disconnect, across
/// pooling strategies — a regression here (e.g. a future edit that goes back
/// to forwarding Terminate) would silently defeat cross-client backend reuse
/// for the most common real-world disconnect pattern.
#[tokio::test]
async fn dirty_connection_invariant_hybrid_mode() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);
    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1";

    sleep(Duration::from_secs(2)).await;

    let proxy_port = start_pool_capped_proxy(
        postgres_host,
        postgres_port,
        scry::config::PoolingStrategy::Hybrid,
        1,
    )
    .await
    .expect("failed to start pool-capped Hybrid proxy");

    // --- Client A: dirty the connection via BOTH protocol variants, then disconnect. ---
    let client_a = connect_client("127.0.0.1", proxy_port, "postgres", "postgres", "postgres")
        .await
        .expect("client A failed to connect");

    let pid_a: i32 = client_a
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .expect("client A pid query")
        .get(0);

    client_a
        .simple_query("SET application_name = 'wp9_dirty_probe_simple'")
        .await
        .expect("simple-protocol SET should succeed in Hybrid mode");
    client_a
        .execute("SET statement_timeout = '424242'", &[])
        .await
        .expect("extended-protocol SET should succeed in Hybrid mode");
    client_a
        .simple_query("CREATE TEMP TABLE wp9_dirty_temp (x int)")
        .await
        .expect("CREATE TEMP TABLE should succeed in Hybrid mode");
    client_a
        .execute("SELECT pg_advisory_lock(918273)", &[])
        .await
        .expect("advisory lock should succeed in Hybrid mode");
    client_a
        .simple_query("PREPARE wp9_dirty_stmt AS SELECT 1")
        .await
        .expect("PREPARE should succeed");

    // Client A's logical session ends. Disconnect entirely — the connection
    // handler's final `pool_manager.release(managed_conn)` runs on this path
    // unconditionally (regardless of any pin), returning the physical
    // connection to deadpool, whose `recycle()` hook runs `DISCARD ALL`
    // before it is handed to anyone else.
    drop(client_a);
    sleep(Duration::from_millis(300)).await;

    // --- Client B: a fresh logical session on the same pooled proxy port. ---
    let client_b = connect_client("127.0.0.1", proxy_port, "postgres", "postgres", "postgres")
        .await
        .expect("client B failed to connect");

    let pid_b: i32 = client_b
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .expect("client B pid query")
        .get(0);
    assert_eq!(
        pid_a, pid_b,
        "test did not exercise genuine connection reuse (different backend pids) — \
         cleanliness would be proven vacuously on a fresh connection, not a recycled one. \
         This would mean a regression in the WP-9 Task 8 (P2 §5.3) warm-reuse fix: the \
         client's graceful-disconnect Terminate ('X') is no longer being intercepted by \
         connection.rs's client-read loop and is instead being forwarded verbatim to the \
         real backend (or should_forward incorrectly stayed true), closing the actual \
         Postgres session before the pool can return it warm"
    );

    // Invariant (a) probe 1: simple-protocol SET must not leak.
    let app_name: String = client_b
        .query_one("SHOW application_name", &[])
        .await
        .expect("SHOW application_name")
        .get(0);
    assert_ne!(
        app_name, "wp9_dirty_probe_simple",
        "simple-protocol SET leaked across a genuinely recycled pooled connection"
    );

    // Invariant (a) probe 2: extended-protocol SET must not leak.
    let stmt_timeout: String = client_b
        .query_one("SHOW statement_timeout", &[])
        .await
        .expect("SHOW statement_timeout")
        .get(0);
    assert_eq!(
        stmt_timeout, "0",
        "extended-protocol SET leaked across a genuinely recycled pooled connection \
         (expected default '0', got '{stmt_timeout}')"
    );

    // Invariant (a) probe 3: temp table must not leak.
    let visible: Option<String> = client_b
        .query_one("SELECT to_regclass('pg_temp.wp9_dirty_temp')::text", &[])
        .await
        .expect("to_regclass query")
        .get(0);
    assert!(
        visible.is_none(),
        "temp table leaked across a genuinely recycled pooled connection (to_regclass returned {visible:?})"
    );

    // Invariant (a) probe 4: advisory lock must not leak (a fresh try-lock on
    // the same key must succeed, proving nothing on this backend still holds it).
    let acquired: bool = client_b
        .query_one("SELECT pg_try_advisory_lock(918273)", &[])
        .await
        .expect("pg_try_advisory_lock query")
        .get(0);
    assert!(
        acquired,
        "advisory lock leaked across a genuinely recycled pooled connection \
         (pg_try_advisory_lock returned false — still held)"
    );
    let _ = client_b.execute("SELECT pg_advisory_unlock(918273)", &[]).await;

    // Invariant (a) probe 5: prepared statement must not leak — EXECUTE by
    // name on the fresh client must fail as unprepared, not succeed.
    match client_b.simple_query("EXECUTE wp9_dirty_stmt").await {
        Err(e) => {
            assert_eq!(
                e.code().map(|c| c.code()),
                Some("26000"),
                "expected undefined_pstmt (26000) executing a leaked-name prepared statement, got {:?}",
                e.code()
            );
        }
        Ok(_) => panic!(
            "prepared statement wp9_dirty_stmt LEAKED to a fresh, genuinely-recycled pooled client"
        ),
    }

    drop(client_b);
    sleep(Duration::from_millis(300)).await;

    // --- Client C: invariant (b) — state this client still needs must survive. ---
    let client_c = connect_client("127.0.0.1", proxy_port, "postgres", "postgres", "postgres")
        .await
        .expect("client C failed to connect");

    let pid_c: i32 = client_c
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .expect("client C pid query")
        .get(0);
    assert_eq!(pid_a, pid_c, "client C did not land on the same genuinely-recycled connection");

    // Simple-protocol GUC + temp table, wrapped in an explicit transaction:
    // `connection.rs` only evaluates its Hybrid release decision when a
    // transaction completes (never for autocommit statements), so this is
    // what exercises the release-vs-pin decision at all.
    client_c.simple_query("BEGIN").await.expect("BEGIN");
    client_c
        .simple_query("SET application_name = 'wp9_survive_probe'")
        .await
        .expect("SET inside txn");
    client_c
        .simple_query("CREATE TEMP TABLE wp9_survive_temp (y int)")
        .await
        .expect("CREATE TEMP TABLE inside txn");
    client_c.simple_query("COMMIT").await.expect("COMMIT");

    // Enough further activity that an un-pinned connection would already
    // have been released back to the pool (and DISCARD-ALL reset) by now.
    for _ in 0..5 {
        client_c.simple_query("SELECT 1").await.expect("keepalive SELECT 1");
    }

    let app_name: String = client_c
        .query_one("SHOW application_name", &[])
        .await
        .expect("SHOW application_name")
        .get(0);
    assert_eq!(
        app_name, "wp9_survive_probe",
        "GUC set via SIMPLE protocol was silently dropped — Hybrid mode recycled the \
         connection instead of pinning it (§5.3 fail-closed violation)"
    );

    let visible: Option<String> = client_c
        .query_one("SELECT to_regclass('pg_temp.wp9_survive_temp')::text", &[])
        .await
        .expect("to_regclass query")
        .get(0);
    assert!(
        visible.is_some(),
        "temp table created via SIMPLE protocol was silently dropped — Hybrid mode recycled \
         the connection instead of pinning it (§5.3 fail-closed violation)"
    );

    // Same invariant (b), extended protocol — Task 4/6's own guardrail, asserted
    // here explicitly as the named §5.3 fail-closed invariant, and entirely on
    // client C (its CONTINUING logical session), with NO second concurrent
    // client. The cross-client NON-leak half of §5.3 is invariant (a), already
    // proven above with client B on a genuinely warm-reused backend; opening a
    // second client here would be unsatisfiable anyway — this proxy is capped at
    // `pool_size = 1` and client C's connection is (correctly) PINNED because it
    // carries live session state, so no second physical backend can exist for a
    // concurrent client to land on.
    //
    // A dotted custom GUC name (`wp9.survive_ext_guc`) is required: Postgres
    // rejects `SET` of a bare, non-namespaced unknown parameter ("unrecognized
    // configuration parameter"), but accepts any dotted name as a session
    // placeholder GUC — exactly the leakable extended-protocol session state this
    // invariant probes. A distinct namespace from the simple-protocol
    // `application_name` probe above keeps the two checks independent.
    //
    // The `SET` goes over the EXTENDED protocol (`.execute()`), which is the pin
    // trigger under test (Task 4's Parse-arm classification). `DEALLOCATE ALL`
    // (simple) strips the incidental prepared-statement pin so this asserts the
    // session-variable pin specifically; BEGIN/COMMIT (simple) are what make
    // Hybrid evaluate its release-vs-pin decision at all.
    client_c.batch_execute("BEGIN").await.expect("BEGIN (ext survive)");
    client_c
        .execute("SET wp9.survive_ext_guc = 'wp9_survive_ext_value'", &[])
        .await
        .expect("extended-protocol SET should succeed");
    client_c.batch_execute("DEALLOCATE ALL").await.expect("DEALLOCATE ALL");
    client_c.batch_execute("COMMIT").await.expect("COMMIT (ext survive)");
    for _ in 0..5 {
        client_c.simple_query("SELECT 1").await.expect("keepalive SELECT 1 (ext survive)");
    }
    let ext_guc: String = client_c
        .query_one("SHOW wp9.survive_ext_guc", &[])
        .await
        .expect("SHOW wp9.survive_ext_guc")
        .get(0);
    assert_eq!(
        ext_guc, "wp9_survive_ext_value",
        "GUC set via EXTENDED protocol was silently dropped — Hybrid mode recycled the \
         connection instead of pinning it (§5.3 fail-closed violation)"
    );
}

/// §5.3 dirty-connection invariant auditor (WP-9 Task 8, P2 §5.3) — Transaction mode.
///
/// Same two invariants as [`dirty_connection_invariant_hybrid_mode`], scoped to
/// what strict Transaction pooling actually allows: `SET`/temp-table/cursor/
/// advisory-lock ops are rejected outside a transaction (0A000) and CREATE TEMP
/// TABLE/cursors/advisory locks are rejected unconditionally (`mode_enforcer.rs`),
/// so they cannot establish leakable state in this mode at all — asserting their
/// rejection is already covered elsewhere. What CAN carry state here: a `SET`
/// executed *inside* an explicit transaction (allowed even in Transaction mode),
/// and a prepared statement (`PREPARE`, allowed unconditionally). Both are probed
/// for non-leakage; the prepared statement is also probed for fail-closed
/// persistence (invariant (b)), mirroring `transaction_mode_prepared_statement_honesty`
/// but framed explicitly as the §5.3 invariant and run on a connection whose
/// physical reuse is confirmed via matching `pg_backend_pid()`, exactly as in the
/// Hybrid-mode test above.
///
/// # Proves genuine warm cross-client reuse (GREEN)
/// Same anti-vacuity guardrail as [`dirty_connection_invariant_hybrid_mode`], and
/// it passes for the identical, pooling-strategy-independent reason: the client's
/// graceful-disconnect `Terminate` is intercepted rather than forwarded to the
/// real backend, so the physical connection survives and is returned to the pool
/// warm. See that test's doc comment for the full mechanism.
#[tokio::test]
async fn dirty_connection_invariant_transaction_mode() {
    use tokio_postgres::types::Type;

    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);
    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1";

    sleep(Duration::from_secs(2)).await;

    let proxy_port = start_pool_capped_proxy(
        postgres_host,
        postgres_port,
        scry::config::PoolingStrategy::Transaction,
        1,
    )
    .await
    .expect("failed to start pool-capped Transaction-mode proxy");

    // --- Client A: establish state, then disconnect. ---
    let client_a = connect_client("127.0.0.1", proxy_port, "postgres", "postgres", "postgres")
        .await
        .expect("client A failed to connect");

    let pid_a: i32 = client_a
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .expect("client A pid query")
        .get(0);

    // SET is rejected outside a transaction in strict Transaction mode, but
    // allowed inside one (scoped to the transaction) — this is the only
    // GUC-leak vector available in this mode.
    client_a.simple_query("BEGIN").await.expect("BEGIN");
    client_a
        .simple_query("SET application_name = 'wp9_txn_dirty_probe'")
        .await
        .expect("SET inside txn should be allowed even in Transaction mode");
    client_a.simple_query("COMMIT").await.expect("COMMIT");

    client_a
        .simple_query("PREPARE wp9_txn_dirty_stmt AS SELECT 1")
        .await
        .expect("PREPARE should be allowed in Transaction mode");

    drop(client_a);
    sleep(Duration::from_millis(300)).await;

    // --- Client B: fresh session, must observe none of A's state. ---
    let client_b = connect_client("127.0.0.1", proxy_port, "postgres", "postgres", "postgres")
        .await
        .expect("client B failed to connect");

    let pid_b: i32 = client_b
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .expect("client B pid query")
        .get(0);
    assert_eq!(
        pid_a, pid_b,
        "test did not exercise genuine connection reuse (different backend pids) — \
         cleanliness would be proven vacuously on a fresh connection, not a recycled one. \
         This would mean a regression in the WP-9 Task 8 (P2 §5.3) warm-reuse fix: the \
         client's graceful-disconnect Terminate ('X') is no longer being intercepted by \
         connection.rs's client-read loop and is instead being forwarded verbatim to the \
         real backend (or should_forward incorrectly stayed true), closing the actual \
         Postgres session before the pool can return it warm"
    );

    let app_name: String = client_b
        .query_one("SHOW application_name", &[])
        .await
        .expect("SHOW application_name")
        .get(0);
    assert_eq!(
        app_name, "",
        "GUC set inside a transaction leaked across a genuinely recycled Transaction-mode \
         connection (expected default '', got '{app_name}')"
    );

    match client_b.simple_query("EXECUTE wp9_txn_dirty_stmt").await {
        Err(e) => {
            assert_eq!(
                e.code().map(|c| c.code()),
                Some("26000"),
                "expected undefined_pstmt (26000) executing a leaked-name prepared statement, got {:?}",
                e.code()
            );
        }
        Ok(_) => panic!(
            "prepared statement wp9_txn_dirty_stmt LEAKED to a fresh, genuinely-recycled \
             Transaction-mode client"
        ),
    }

    drop(client_b);
    sleep(Duration::from_millis(300)).await;

    // --- Client C: invariant (b) — a prepared statement client C still needs
    // must survive a completed transaction boundary (restrict-by-pinning),
    // not be silently destroyed by an unpinned release + DISCARD ALL.
    let client_c = connect_client("127.0.0.1", proxy_port, "postgres", "postgres", "postgres")
        .await
        .expect("client C failed to connect");

    let pid_c: i32 = client_c
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .expect("client C pid query")
        .get(0);
    assert_eq!(pid_a, pid_c, "client C did not land on the same genuinely-recycled connection");

    let stmt = client_c
        .prepare_typed("SELECT $1", &[Type::INT8])
        .await
        .expect("prepare_typed should succeed through the proxy in Transaction mode");

    client_c.batch_execute("BEGIN").await.expect("BEGIN");
    client_c.batch_execute("SELECT 1").await.expect("SELECT 1 within transaction");
    client_c.batch_execute("COMMIT").await.expect("COMMIT");

    let rows = client_c.query(&stmt, &[&7i64]).await.expect(
        "cached prepared statement must survive a Transaction-mode release boundary \
         (restrict-by-pinning, §5.3 fail-closed invariant) — a failure here is the silent \
         prepared-statement destruction this test guards against",
    );
    let value: i64 = rows[0].get(0);
    assert_eq!(value, 7, "prepared statement returned the wrong value after the txn boundary");
}
