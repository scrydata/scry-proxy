//! Fail-closed pooling-safety property test (P2 §4.1, §5.4).
//!
//! The core invariant: a backend connection may only be released back to the
//! pool when every command run on it was *positively* classified. Any command
//! the detector cannot classify (`Unknown`) must leave the connection pinned —
//! the blast radius of a detection gap is a performance cost (an over-pinned
//! connection), never a correctness bug (leaked session state).

use proptest::prelude::*;
use scry::protocol::{CommandClass, CommandDetector};
use scry::proxy::ConnectionState;

/// Recognized state-changing commands — must be `Stateful`, never `Clean` or
/// `Unknown`.
const STATEFUL: &[&str] = &[
    "SET search_path TO x",
    "RESET ALL",
    "CREATE TEMP TABLE t (id int)",
    "DECLARE c CURSOR FOR SELECT 1",
    "DISCARD ALL",
    "DEALLOCATE ALL",
    "SELECT pg_advisory_lock(42)",
];

/// Recognized commands that *add* residual state — must pin a fresh connection.
const STATE_ADDING: &[&str] = &[
    "SET statement_timeout = 100",
    "CREATE TEMP TABLE t (id int)",
    "DECLARE c CURSOR FOR SELECT 1",
    "SELECT pg_advisory_lock(42)",
];

/// Recognized cleanup commands that *remove* state — on a fresh (already-clean)
/// connection they must NOT pin.
const CLEANUP: &[&str] = &["RESET ALL", "DISCARD ALL", "DEALLOCATE ALL"];

/// Commands that cannot be proven clean and must therefore pin (`Unknown`).
const MUST_PIN_UNKNOWN: &[&str] = &[
    "LISTEN channel_name",
    "NOTIFY channel_name",
    "CALL some_procedure()",
    "DO $$ BEGIN PERFORM 1; END $$",
    "COPY t FROM STDIN",
    "LOCK TABLE t IN ACCESS EXCLUSIVE MODE",
    "PREPARE p AS SELECT 1",
    "SELECT set_config('search_path', 'evil', false)", // read with a side effect
    "GRANT ALL ON t TO bob",
    "VACUUM FULL t",
    "\u{0007}garbage-not-sql",
];

/// Commands that are positively safe to multiplex.
const KNOWN_CLEAN: &[&str] = &[
    "SELECT 1",
    "SELECT * FROM users WHERE id = $1",
    "INSERT INTO t (a) VALUES (1)",
    "UPDATE t SET a = 1 WHERE id = 2",
    "DELETE FROM t WHERE id = 3",
    "WITH cte AS (SELECT 1) SELECT * FROM cte",
    "VALUES (1),(2)",
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "SAVEPOINT s1",
    "SHOW search_path",
    "EXPLAIN SELECT 1",
];

#[test]
fn recognized_stateful_commands_are_not_clean() {
    for sql in STATEFUL {
        let class = CommandDetector::classify(sql);
        assert!(
            matches!(class, CommandClass::Stateful(_)),
            "stateful command should be Stateful, not {class:?}: {sql:?}"
        );
    }
}

#[test]
fn state_adding_commands_pin() {
    for sql in STATE_ADDING {
        let mut state = ConnectionState::new(100);
        state.apply_query(sql);
        assert!(state.is_pinned(), "state-adding command did not pin: {sql:?}");
    }
}

#[test]
fn cleanup_commands_do_not_pin_a_fresh_connection() {
    for sql in CLEANUP {
        let mut state = ConnectionState::new(100);
        state.apply_query(sql);
        assert!(!state.is_pinned(), "cleanup command wrongly pinned a clean connection: {sql:?}");
    }
}

#[test]
fn unclassifiable_commands_pin_the_connection() {
    for sql in MUST_PIN_UNKNOWN {
        assert_eq!(
            CommandDetector::classify(sql),
            CommandClass::Unknown,
            "command should be Unknown (fail closed): {sql:?}"
        );
        let mut state = ConnectionState::new(100);
        state.apply_query(sql);
        assert!(state.is_pinned(), "unknown command did not pin (fail-open!): {sql:?}");
        assert!(state.has_unsafe_state(), "unknown command must count as unsafe state: {sql:?}");
    }
}

#[test]
fn known_clean_commands_do_not_pin() {
    for sql in KNOWN_CLEAN {
        assert_eq!(
            CommandDetector::classify(sql),
            CommandClass::Clean,
            "known-clean command misclassified: {sql:?}"
        );
        let mut state = ConnectionState::new(100);
        state.apply_query(sql);
        assert!(!state.is_pinned(), "clean command wrongly pinned: {sql:?}");
    }
}

proptest! {
    /// The central fail-closed property: for ANY input string, if the classifier
    /// could not positively classify it (`Unknown`), then applying it to a fresh
    /// connection state leaves the connection pinned — never releasable.
    #[test]
    fn unknown_implies_pinned(sql in ".*") {
        if CommandDetector::classify(&sql) == CommandClass::Unknown {
            let mut state = ConnectionState::new(100);
            state.apply_query(&sql);
            prop_assert!(
                state.is_pinned(),
                "unclassifiable command left connection releasable (fail-open): {sql:?}"
            );
        }
    }
}
