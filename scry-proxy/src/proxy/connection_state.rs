// scry-proxy/src/proxy/connection_state.rs

use std::collections::{HashMap, HashSet, VecDeque};

/// Reasons why a connection is pinned to a client
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PinReason {
    PreparedStatement,
    SessionVariable,
    TempTable,
    Cursor,
    AdvisoryLock,
    /// An active `LISTEN` registration exists on this connection (P2 §4.3).
    /// Non-replayable: there is no way to re-establish the exact set of
    /// channel subscriptions on a different backend connection without
    /// risking a missed notification in the gap, so the connection must stay
    /// pinned for as long as any registration is active.
    Listen,
    /// A command that could not be positively classified as safe to multiplex
    /// was observed (P2 §4.1). Fail closed: pin, because we cannot prove the
    /// connection is clean.
    UnknownCommand,
}

impl PinReason {
    /// Check if this pin reason represents unsafe state that cannot be replayed
    pub fn is_unsafe(&self) -> bool {
        matches!(
            self,
            PinReason::TempTable
                | PinReason::Cursor
                | PinReason::AdvisoryLock
                | PinReason::Listen
                | PinReason::UnknownCommand
        )
    }
}

/// Prepared statement info for replay
#[derive(Debug, Clone)]
pub struct PreparedStatementInfo {
    pub name: String,
    pub query: String,
    pub param_oids: Vec<u32>,
}

/// State that can be replayed on a new connection
#[derive(Debug, Clone, Default)]
pub struct ReplayableState {
    pub prepared_statements: Vec<PreparedStatementInfo>,
    pub session_variables: HashMap<String, String>,
}

/// Tracks all state on a backend connection for pinning decisions
#[derive(Debug)]
pub struct ConnectionState {
    /// Prepared statements (name -> query, param_oids)
    prepared_statements: HashMap<String, (String, Vec<u32>)>,
    /// Session variables (name -> value)
    session_variables: HashMap<String, String>,
    /// Temporary tables
    temp_tables: HashSet<String>,
    /// Open cursors
    cursors: HashSet<String>,
    /// Advisory locks held
    advisory_locks: HashSet<i64>,
    /// Active LISTEN channel registrations (P2 §4.3). Tracked as a set (not a
    /// single flag) so `UNLISTEN <chan>` can precisely drop one registration
    /// while leaving the pin in place if other channels are still active —
    /// mirroring how `temp_tables`/`cursors` are tracked, rather than
    /// conservatively over-pinning until `UNLISTEN *`/`DISCARD ALL`.
    listen_channels: HashSet<String>,
    /// A command that could not be positively classified as clean was seen, so
    /// the connection must stay pinned (fail closed, P2 §4.1).
    unknown_command: bool,
    /// Maximum prepared statements (LRU eviction). Clamped to at least 1 (see
    /// [`ConnectionState::new`]) so a misconfigured/unset `0` can never mean
    /// "evict everything" — that would let a prepared statement exist on the
    /// backend while the map (and therefore `is_pinned()`) reports clean.
    max_prepared_statements: usize,
    /// Insertion/use order for `prepared_statements`, oldest at the front.
    /// A plain `HashMap` has no order, so this tracks LRU order alongside it;
    /// kept in sync on every insert/remove/clear.
    prepared_statements_lru: VecDeque<String>,
}

impl ConnectionState {
    pub fn new(max_prepared_statements: usize) -> Self {
        Self {
            prepared_statements: HashMap::new(),
            session_variables: HashMap::new(),
            temp_tables: HashSet::new(),
            cursors: HashSet::new(),
            advisory_locks: HashSet::new(),
            listen_channels: HashSet::new(),
            unknown_command: false,
            // Fail-closed: a bound of 0 must not silently evict every
            // statement immediately after insertion (which would leave the
            // map empty and `is_pinned()` reporting clean despite a live
            // prepared statement on the backend). Clamp to a minimum of 1.
            max_prepared_statements: max_prepared_statements.max(1),
            prepared_statements_lru: VecDeque::new(),
        }
    }

    /// Check if connection is pinned (has any state)
    pub fn is_pinned(&self) -> bool {
        self.unknown_command
            || !self.prepared_statements.is_empty()
            || !self.session_variables.is_empty()
            || !self.temp_tables.is_empty()
            || !self.cursors.is_empty()
            || !self.advisory_locks.is_empty()
            || !self.listen_channels.is_empty()
    }

    /// Check if connection has unsafe state that cannot be replayed
    pub fn has_unsafe_state(&self) -> bool {
        self.unknown_command
            || !self.temp_tables.is_empty()
            || !self.cursors.is_empty()
            || !self.advisory_locks.is_empty()
            || !self.listen_channels.is_empty()
    }

    /// Record that a command which could not be positively classified as clean
    /// was observed. The connection will stay pinned until state is cleared.
    pub fn mark_unknown_command(&mut self) {
        self.unknown_command = true;
    }

    /// Apply a client query to the tracked state, fail-closed (P2 §4.1): a
    /// recognized state-changing command updates the corresponding state; a
    /// command that cannot be positively classified as clean pins the
    /// connection via [`mark_unknown_command`].
    pub fn apply_query(&mut self, query: &str) {
        use crate::protocol::{CommandClass, CommandDetector, DetectedCommand};
        match CommandDetector::classify(query) {
            CommandClass::Clean => {}
            CommandClass::Unknown => self.mark_unknown_command(),
            CommandClass::Stateful(cmd) => match cmd {
                DetectedCommand::Set { name, value } => self.add_session_variable(name, value),
                DetectedCommand::Reset { name } => self.remove_session_variable(&name),
                DetectedCommand::ResetAll => self.clear_session_variables(),
                DetectedCommand::CreateTempTable { name } => self.add_temp_table(name),
                DetectedCommand::DropTable { name } => self.remove_temp_table(&name),
                DetectedCommand::DeclareCursor { name, .. } => self.add_cursor(name),
                DetectedCommand::CloseCursor { name } => self.remove_cursor(&name),
                DetectedCommand::AdvisoryLock { key } => {
                    if let Some(k) = key {
                        self.add_advisory_lock(k);
                    }
                }
                DetectedCommand::AdvisoryUnlock { key } => {
                    if let Some(k) = key {
                        self.remove_advisory_lock(k);
                    }
                }
                DetectedCommand::DiscardAll => self.clear_all(),
                DetectedCommand::Deallocate { name } => self.remove_prepared_statement(&name),
                DetectedCommand::DeallocateAll => self.clear_prepared_statements(),
                DetectedCommand::Listen { channel } => self.add_listen_channel(channel),
                DetectedCommand::Unlisten { channel } => match channel {
                    Some(name) => self.remove_listen_channel(&name),
                    None => self.clear_listen_channels(),
                },
                // NOTIFY is fire-and-forget: it never registers a
                // subscription on this connection, so — unlike LISTEN — it
                // must not pin. It is still classified (not folded into
                // `is_known_clean`) purely so callers can identify/attribute
                // it as a NOTIFY if needed; the classification itself is a
                // no-op against `ConnectionState`.
                DetectedCommand::Notify { .. } => {}
            },
        }
    }

    /// Get all current pin reasons
    pub fn pin_reasons(&self) -> HashSet<PinReason> {
        let mut reasons = HashSet::new();
        if !self.prepared_statements.is_empty() {
            reasons.insert(PinReason::PreparedStatement);
        }
        if !self.session_variables.is_empty() {
            reasons.insert(PinReason::SessionVariable);
        }
        if !self.temp_tables.is_empty() {
            reasons.insert(PinReason::TempTable);
        }
        if !self.cursors.is_empty() {
            reasons.insert(PinReason::Cursor);
        }
        if !self.advisory_locks.is_empty() {
            reasons.insert(PinReason::AdvisoryLock);
        }
        if !self.listen_channels.is_empty() {
            reasons.insert(PinReason::Listen);
        }
        if self.unknown_command {
            reasons.insert(PinReason::UnknownCommand);
        }
        reasons
    }

    // Prepared statements
    ///
    /// Enforces `max_prepared_statements` with LRU eviction (closes
    /// `TODO(tracked): WP-9 Task 7`): if the name is already tracked, its LRU
    /// position is refreshed; otherwise, once the map is at the bound, the
    /// least-recently-used statement is evicted (from both the map and the
    /// order tracker) before the new one is inserted. Because eviction only
    /// ever runs immediately before an insert, the map never goes empty as a
    /// side effect of eviction — it stays at the bound (>= 1, see `new`),
    /// non-empty, so `is_pinned()` correctly remains `true`.
    pub fn add_prepared_statement(&mut self, name: String, query: String, param_oids: Vec<u32>) {
        if self.prepared_statements.contains_key(&name) {
            // Re-inserting an existing name: refresh its LRU position rather
            // than double-counting it in the order tracker.
            self.prepared_statements_lru.retain(|n| n != &name);
        } else {
            while self.prepared_statements.len() >= self.max_prepared_statements {
                if let Some(oldest) = self.prepared_statements_lru.pop_front() {
                    self.prepared_statements.remove(&oldest);
                } else {
                    // Order tracker empty but map at/over bound would mean
                    // they've desynced; break defensively rather than loop.
                    break;
                }
            }
        }

        self.prepared_statements.insert(name.clone(), (query, param_oids));
        self.prepared_statements_lru.push_back(name);
    }

    pub fn remove_prepared_statement(&mut self, name: &str) {
        self.prepared_statements.remove(name);
        self.prepared_statements_lru.retain(|n| n != name);
    }

    pub fn clear_prepared_statements(&mut self) {
        self.prepared_statements.clear();
        self.prepared_statements_lru.clear();
    }

    // Session variables
    pub fn add_session_variable(&mut self, name: String, value: String) {
        self.session_variables.insert(name, value);
    }

    pub fn remove_session_variable(&mut self, name: &str) {
        self.session_variables.remove(name);
    }

    pub fn clear_session_variables(&mut self) {
        self.session_variables.clear();
    }

    // Temp tables
    pub fn add_temp_table(&mut self, name: String) {
        self.temp_tables.insert(name);
    }

    pub fn remove_temp_table(&mut self, name: &str) {
        self.temp_tables.remove(name);
    }

    // Cursors
    pub fn add_cursor(&mut self, name: String) {
        self.cursors.insert(name);
    }

    pub fn remove_cursor(&mut self, name: &str) {
        self.cursors.remove(name);
    }

    // Advisory locks
    pub fn add_advisory_lock(&mut self, key: i64) {
        self.advisory_locks.insert(key);
    }

    pub fn remove_advisory_lock(&mut self, key: i64) {
        self.advisory_locks.remove(&key);
    }

    // LISTEN/NOTIFY channel registrations
    pub fn add_listen_channel(&mut self, channel: String) {
        self.listen_channels.insert(channel);
    }

    pub fn remove_listen_channel(&mut self, channel: &str) {
        self.listen_channels.remove(channel);
    }

    pub fn clear_listen_channels(&mut self) {
        self.listen_channels.clear();
    }

    /// Get state that can be replayed on reconnection
    pub fn get_replayable_state(&self) -> ReplayableState {
        ReplayableState {
            prepared_statements: self
                .prepared_statements
                .iter()
                .map(|(name, (query, oids))| PreparedStatementInfo {
                    name: name.clone(),
                    query: query.clone(),
                    param_oids: oids.clone(),
                })
                .collect(),
            session_variables: self.session_variables.clone(),
        }
    }

    /// Clear all state (for DISCARD ALL or connection reset)
    pub fn clear_all(&mut self) {
        self.prepared_statements.clear();
        self.prepared_statements_lru.clear();
        self.session_variables.clear();
        self.temp_tables.clear();
        self.cursors.clear();
        self.advisory_locks.clear();
        self.listen_channels.clear();
        // DISCARD ALL resets the session, so a prior unknown command no longer
        // keeps the connection pinned.
        self.unknown_command = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_state_unpinned() {
        let state = ConnectionState::new(1000);
        assert!(!state.is_pinned());
        assert!(state.pin_reasons().is_empty());
    }

    #[test]
    fn test_pin_on_prepared_statement() {
        let mut state = ConnectionState::new(1000);
        state.add_prepared_statement("stmt1".to_string(), "SELECT 1".to_string(), vec![]);

        assert!(state.is_pinned());
        assert!(state.pin_reasons().contains(&PinReason::PreparedStatement));
    }

    #[test]
    fn test_pin_on_set_variable() {
        let mut state = ConnectionState::new(1000);
        state.add_session_variable("search_path".to_string(), "public".to_string());

        assert!(state.is_pinned());
        assert!(state.pin_reasons().contains(&PinReason::SessionVariable));
    }

    #[test]
    fn test_pin_on_temp_table() {
        let mut state = ConnectionState::new(1000);
        state.add_temp_table("tmp_users".to_string());

        assert!(state.is_pinned());
        assert!(state.pin_reasons().contains(&PinReason::TempTable));
    }

    #[test]
    fn test_pin_on_cursor() {
        let mut state = ConnectionState::new(1000);
        state.add_cursor("my_cursor".to_string());

        assert!(state.is_pinned());
        assert!(state.pin_reasons().contains(&PinReason::Cursor));
    }

    #[test]
    fn test_pin_on_advisory_lock() {
        let mut state = ConnectionState::new(1000);
        state.add_advisory_lock(12345);

        assert!(state.is_pinned());
        assert!(state.pin_reasons().contains(&PinReason::AdvisoryLock));
    }

    #[test]
    fn test_unpin_on_deallocate() {
        let mut state = ConnectionState::new(1000);
        state.add_prepared_statement("stmt1".to_string(), "SELECT 1".to_string(), vec![]);
        state.remove_prepared_statement("stmt1");

        assert!(!state.is_pinned());
    }

    #[test]
    fn test_multiple_pins() {
        let mut state = ConnectionState::new(1000);
        state.add_prepared_statement("stmt1".to_string(), "SELECT 1".to_string(), vec![]);
        state.add_session_variable("tz".to_string(), "UTC".to_string());

        assert!(state.is_pinned());

        // Remove one, still pinned
        state.remove_prepared_statement("stmt1");
        assert!(state.is_pinned());

        // Remove other, unpinned
        state.remove_session_variable("tz");
        assert!(!state.is_pinned());
    }

    #[test]
    fn test_has_unsafe_state() {
        let mut state = ConnectionState::new(1000);

        // Prepared statements are safe
        state.add_prepared_statement("stmt1".to_string(), "SELECT 1".to_string(), vec![]);
        assert!(!state.has_unsafe_state());

        // Temp tables are unsafe
        state.add_temp_table("tmp".to_string());
        assert!(state.has_unsafe_state());
    }

    #[test]
    fn test_has_unsafe_state_cursor() {
        let mut state = ConnectionState::new(1000);
        state.add_cursor("cursor1".to_string());
        assert!(state.has_unsafe_state());
    }

    #[test]
    fn test_has_unsafe_state_advisory_lock() {
        let mut state = ConnectionState::new(1000);
        state.add_advisory_lock(99999);
        assert!(state.has_unsafe_state());
    }

    #[test]
    fn test_replayable_state() {
        let mut state = ConnectionState::new(1000);
        state.add_prepared_statement("stmt1".to_string(), "SELECT $1".to_string(), vec![23]);
        state.add_session_variable("tz".to_string(), "UTC".to_string());

        let replay = state.get_replayable_state();

        assert_eq!(replay.prepared_statements.len(), 1);
        assert_eq!(replay.session_variables.len(), 1);
        assert_eq!(replay.session_variables.get("tz"), Some(&"UTC".to_string()));
    }

    #[test]
    fn test_replayable_state_prepared_statement_details() {
        let mut state = ConnectionState::new(1000);
        state.add_prepared_statement(
            "my_stmt".to_string(),
            "SELECT $1, $2".to_string(),
            vec![23, 25],
        );

        let replay = state.get_replayable_state();

        assert_eq!(replay.prepared_statements.len(), 1);
        let stmt = &replay.prepared_statements[0];
        assert_eq!(stmt.name, "my_stmt");
        assert_eq!(stmt.query, "SELECT $1, $2");
        assert_eq!(stmt.param_oids, vec![23, 25]);
    }

    #[test]
    fn test_clear_all() {
        let mut state = ConnectionState::new(1000);
        state.add_prepared_statement("stmt1".to_string(), "SELECT 1".to_string(), vec![]);
        state.add_session_variable("tz".to_string(), "UTC".to_string());
        state.add_temp_table("tmp".to_string());
        state.add_cursor("cursor1".to_string());
        state.add_advisory_lock(12345);

        state.clear_all();

        assert!(!state.is_pinned());
        assert!(!state.has_unsafe_state());
    }

    #[test]
    fn test_clear_prepared_statements() {
        let mut state = ConnectionState::new(1000);
        state.add_prepared_statement("stmt1".to_string(), "SELECT 1".to_string(), vec![]);
        state.add_prepared_statement("stmt2".to_string(), "SELECT 2".to_string(), vec![]);
        state.add_session_variable("tz".to_string(), "UTC".to_string());

        state.clear_prepared_statements();

        // Should still be pinned due to session variable
        assert!(state.is_pinned());
        assert!(!state.pin_reasons().contains(&PinReason::PreparedStatement));
        assert!(state.pin_reasons().contains(&PinReason::SessionVariable));
    }

    #[test]
    fn test_clear_session_variables() {
        let mut state = ConnectionState::new(1000);
        state.add_session_variable("tz".to_string(), "UTC".to_string());
        state.add_session_variable("search_path".to_string(), "public".to_string());

        state.clear_session_variables();

        assert!(!state.is_pinned());
    }

    #[test]
    fn test_pin_reason_is_unsafe() {
        assert!(!PinReason::PreparedStatement.is_unsafe());
        assert!(!PinReason::SessionVariable.is_unsafe());
        assert!(PinReason::TempTable.is_unsafe());
        assert!(PinReason::Cursor.is_unsafe());
        assert!(PinReason::AdvisoryLock.is_unsafe());
    }

    #[test]
    fn test_remove_nonexistent_items() {
        let mut state = ConnectionState::new(1000);

        // These should not panic
        state.remove_prepared_statement("nonexistent");
        state.remove_session_variable("nonexistent");
        state.remove_temp_table("nonexistent");
        state.remove_cursor("nonexistent");
        state.remove_advisory_lock(99999);

        assert!(!state.is_pinned());
    }

    // -- LISTEN/NOTIFY (WP-9 Task 5, P2 §4.3) --------------------------------

    #[test]
    fn test_apply_query_listen_pins_typed_reason() {
        let mut state = ConnectionState::new(1000);
        state.apply_query("LISTEN foo");

        assert!(state.is_pinned());
        assert!(state.has_unsafe_state());
        assert!(state.pin_reasons().contains(&PinReason::Listen));
        // Fail-closed guarantee: typed classification must not be LESS pinned
        // than the old blanket Unknown fallback.
        assert!(!state.pin_reasons().contains(&PinReason::UnknownCommand));
    }

    #[test]
    fn test_apply_query_unlisten_star_clears_listen_pin() {
        let mut state = ConnectionState::new(1000);
        state.apply_query("LISTEN foo");
        state.apply_query("LISTEN bar");
        state.apply_query("UNLISTEN *");

        assert!(!state.pin_reasons().contains(&PinReason::Listen));
        assert!(!state.is_pinned());
    }

    #[test]
    fn test_apply_query_unlisten_specific_channel() {
        let mut state = ConnectionState::new(1000);
        state.apply_query("LISTEN foo");
        state.apply_query("LISTEN bar");
        state.apply_query("UNLISTEN foo");

        // "bar" is still an active registration, so the Listen pin remains.
        assert!(state.pin_reasons().contains(&PinReason::Listen));

        state.apply_query("UNLISTEN bar");
        assert!(!state.pin_reasons().contains(&PinReason::Listen));
        assert!(!state.is_pinned());
    }

    #[test]
    fn test_apply_query_discard_all_clears_listen_pin() {
        let mut state = ConnectionState::new(1000);
        state.apply_query("LISTEN foo");
        state.apply_query("DISCARD ALL");

        assert!(!state.pin_reasons().contains(&PinReason::Listen));
        assert!(!state.is_pinned());
    }

    #[test]
    fn test_clear_all_clears_listen_pin() {
        let mut state = ConnectionState::new(1000);
        state.apply_query("LISTEN foo");
        state.clear_all();

        assert!(!state.pin_reasons().contains(&PinReason::Listen));
        assert!(!state.is_pinned());
    }

    #[test]
    fn test_apply_query_bare_notify_does_not_pin() {
        let mut state = ConnectionState::new(1000);
        state.apply_query("NOTIFY foo");

        assert!(!state.is_pinned());
        assert!(state.pin_reasons().is_empty());
    }

    #[test]
    fn test_apply_query_notify_with_payload_does_not_pin() {
        let mut state = ConnectionState::new(1000);
        state.apply_query("NOTIFY foo, 'payload-42'");

        assert!(!state.is_pinned());
    }

    #[test]
    fn test_pin_reason_listen_is_unsafe() {
        assert!(PinReason::Listen.is_unsafe());
    }

    // -- LRU bound on prepared statements (WP-9 Task 7, P2 §4.7) -------------

    #[test]
    fn test_add_prepared_statement_beyond_bound_evicts_oldest() {
        let mut state = ConnectionState::new(2);
        state.add_prepared_statement("stmt1".to_string(), "SELECT 1".to_string(), vec![]);
        state.add_prepared_statement("stmt2".to_string(), "SELECT 2".to_string(), vec![]);
        // Over the bound of 2: stmt1 (oldest) must be evicted.
        state.add_prepared_statement("stmt3".to_string(), "SELECT 3".to_string(), vec![]);

        let replay = state.get_replayable_state();
        assert_eq!(replay.prepared_statements.len(), 2);
        let names: Vec<&str> = replay.prepared_statements.iter().map(|s| s.name.as_str()).collect();
        assert!(!names.contains(&"stmt1"), "oldest statement should have been evicted");
        assert!(names.contains(&"stmt2"));
        assert!(names.contains(&"stmt3"));
    }

    #[test]
    fn test_lru_eviction_keeps_connection_pinned() {
        let mut state = ConnectionState::new(1);
        state.add_prepared_statement("stmt1".to_string(), "SELECT 1".to_string(), vec![]);
        state.add_prepared_statement("stmt2".to_string(), "SELECT 2".to_string(), vec![]);

        // Map stays at the bound (non-empty), so pinning must never lapse due
        // to eviction alone.
        assert!(state.is_pinned());
        assert!(state.pin_reasons().contains(&PinReason::PreparedStatement));
    }

    #[test]
    fn test_lru_order_tracker_has_no_stale_entries_after_removal() {
        let mut state = ConnectionState::new(3);
        state.add_prepared_statement("stmt1".to_string(), "SELECT 1".to_string(), vec![]);
        state.add_prepared_statement("stmt2".to_string(), "SELECT 2".to_string(), vec![]);
        state.remove_prepared_statement("stmt1");

        // Fill back up to the bound; if the removed "stmt1" were still lurking
        // in the order tracker, it could be evicted a second time (harmless)
        // but must never resurface in the replayable state or be double-counted.
        state.add_prepared_statement("stmt3".to_string(), "SELECT 3".to_string(), vec![]);
        state.add_prepared_statement("stmt4".to_string(), "SELECT 4".to_string(), vec![]);

        let replay = state.get_replayable_state();
        assert_eq!(replay.prepared_statements.len(), 3);
        let names: Vec<&str> = replay.prepared_statements.iter().map(|s| s.name.as_str()).collect();
        assert!(!names.contains(&"stmt1"));
    }

    #[test]
    fn test_lru_order_tracker_synced_after_clear_prepared_statements() {
        let mut state = ConnectionState::new(2);
        state.add_prepared_statement("stmt1".to_string(), "SELECT 1".to_string(), vec![]);
        state.clear_prepared_statements();

        // After clearing, filling back up to the bound should evict purely on
        // FIFO order among the new entries, with no stale "stmt1" tracked.
        state.add_prepared_statement("stmt2".to_string(), "SELECT 2".to_string(), vec![]);
        state.add_prepared_statement("stmt3".to_string(), "SELECT 3".to_string(), vec![]);
        state.add_prepared_statement("stmt4".to_string(), "SELECT 4".to_string(), vec![]);

        let replay = state.get_replayable_state();
        assert_eq!(replay.prepared_statements.len(), 2);
        let names: Vec<&str> = replay.prepared_statements.iter().map(|s| s.name.as_str()).collect();
        assert!(!names.contains(&"stmt1"));
        assert!(!names.contains(&"stmt2"), "stmt2 should be the oldest of the new batch, evicted");
        assert!(names.contains(&"stmt3"));
        assert!(names.contains(&"stmt4"));
    }

    #[test]
    fn test_lru_order_tracker_synced_after_clear_all() {
        let mut state = ConnectionState::new(2);
        state.add_prepared_statement("stmt1".to_string(), "SELECT 1".to_string(), vec![]);
        state.clear_all();

        state.add_prepared_statement("stmt2".to_string(), "SELECT 2".to_string(), vec![]);
        state.add_prepared_statement("stmt3".to_string(), "SELECT 3".to_string(), vec![]);
        state.add_prepared_statement("stmt4".to_string(), "SELECT 4".to_string(), vec![]);

        let replay = state.get_replayable_state();
        assert_eq!(replay.prepared_statements.len(), 2);
        let names: Vec<&str> = replay.prepared_statements.iter().map(|s| s.name.as_str()).collect();
        assert!(!names.contains(&"stmt1"));
        assert!(!names.contains(&"stmt2"));
        assert!(names.contains(&"stmt3"));
        assert!(names.contains(&"stmt4"));
    }

    #[test]
    fn test_zero_max_prepared_statements_does_not_break_pinning() {
        // A misconfigured/unset bound of 0 must not mean "evict everything":
        // that would let a prepared statement exist on the backend while the
        // proxy believes the connection is clean (unsafe multiplexing). Guard
        // by clamping the effective bound to at least 1.
        let mut state = ConnectionState::new(0);
        state.add_prepared_statement("stmt1".to_string(), "SELECT 1".to_string(), vec![]);

        assert!(state.is_pinned());
        assert_eq!(state.get_replayable_state().prepared_statements.len(), 1);
    }
}
