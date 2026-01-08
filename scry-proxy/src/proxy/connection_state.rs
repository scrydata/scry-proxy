// scry-proxy/src/proxy/connection_state.rs

use std::collections::{HashMap, HashSet};

/// Reasons why a connection is pinned to a client
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PinReason {
    PreparedStatement,
    SessionVariable,
    TempTable,
    Cursor,
    AdvisoryLock,
}

impl PinReason {
    /// Check if this pin reason represents unsafe state that cannot be replayed
    pub fn is_unsafe(&self) -> bool {
        matches!(
            self,
            PinReason::TempTable | PinReason::Cursor | PinReason::AdvisoryLock
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
    /// Maximum prepared statements (LRU eviction)
    #[allow(dead_code)]
    max_prepared_statements: usize,
}

impl ConnectionState {
    pub fn new(max_prepared_statements: usize) -> Self {
        Self {
            prepared_statements: HashMap::new(),
            session_variables: HashMap::new(),
            temp_tables: HashSet::new(),
            cursors: HashSet::new(),
            advisory_locks: HashSet::new(),
            max_prepared_statements,
        }
    }

    /// Check if connection is pinned (has any state)
    pub fn is_pinned(&self) -> bool {
        !self.prepared_statements.is_empty()
            || !self.session_variables.is_empty()
            || !self.temp_tables.is_empty()
            || !self.cursors.is_empty()
            || !self.advisory_locks.is_empty()
    }

    /// Check if connection has unsafe state that cannot be replayed
    pub fn has_unsafe_state(&self) -> bool {
        !self.temp_tables.is_empty()
            || !self.cursors.is_empty()
            || !self.advisory_locks.is_empty()
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
        reasons
    }

    // Prepared statements
    pub fn add_prepared_statement(&mut self, name: String, query: String, param_oids: Vec<u32>) {
        // TODO: LRU eviction if over max
        self.prepared_statements.insert(name, (query, param_oids));
    }

    pub fn remove_prepared_statement(&mut self, name: &str) {
        self.prepared_statements.remove(name);
    }

    pub fn clear_prepared_statements(&mut self) {
        self.prepared_statements.clear();
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
        self.session_variables.clear();
        self.temp_tables.clear();
        self.cursors.clear();
        self.advisory_locks.clear();
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
        assert_eq!(
            replay.session_variables.get("tz"),
            Some(&"UTC".to_string())
        );
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
}
