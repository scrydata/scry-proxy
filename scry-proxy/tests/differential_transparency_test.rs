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
