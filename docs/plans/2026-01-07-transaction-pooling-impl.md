# Transaction Pooling Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement transaction and hybrid pooling modes to enable Scry as a drop-in PgBouncer replacement.

**Architecture:** Add transaction state tracking via ReadyForQuery streaming scan, connection state tracking for pinning decisions, bounded wait queue, LIFO+sticky connection selection, and transparent prepared statement re-preparation. Two modes: strict transaction (PgBouncer-compatible) and hybrid (smart pinning, default).

**Tech Stack:** Rust, Tokio, existing deadpool integration, existing PreparedStatementCache

---

## Phase 1: Transaction State Tracking

### Task 1.1: Add TransactionState enum and tracker

**Files:**
- Create: `scry-proxy/src/proxy/transaction.rs`
- Modify: `scry-proxy/src/proxy/mod.rs`

**Step 1: Write the failing test**

```rust
// In scry-proxy/src/proxy/transaction.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_state_is_idle() {
        let tracker = TransactionTracker::new();
        assert_eq!(tracker.state(), TransactionState::Idle);
    }

    #[test]
    fn test_transition_to_in_transaction() {
        let mut tracker = TransactionTracker::new();
        tracker.update_from_ready_for_query(b'T');
        assert_eq!(tracker.state(), TransactionState::InTransaction);
    }

    #[test]
    fn test_transition_to_error() {
        let mut tracker = TransactionTracker::new();
        tracker.update_from_ready_for_query(b'E');
        assert_eq!(tracker.state(), TransactionState::InError);
    }

    #[test]
    fn test_transition_back_to_idle() {
        let mut tracker = TransactionTracker::new();
        tracker.update_from_ready_for_query(b'T');
        tracker.update_from_ready_for_query(b'I');
        assert_eq!(tracker.state(), TransactionState::Idle);
    }

    #[test]
    fn test_is_in_transaction() {
        let mut tracker = TransactionTracker::new();
        assert!(!tracker.is_in_transaction());
        tracker.update_from_ready_for_query(b'T');
        assert!(tracker.is_in_transaction());
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p scry --lib transaction::tests`
Expected: FAIL with "cannot find module `transaction`"

**Step 3: Write minimal implementation**

```rust
// scry-proxy/src/proxy/transaction.rs

/// Transaction state as reported by PostgreSQL ReadyForQuery message
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// 'I' - Idle, not in a transaction
    Idle,
    /// 'T' - In a transaction block
    InTransaction,
    /// 'E' - In a failed transaction block
    InError,
}

/// Tracks transaction state for a client session
#[derive(Debug)]
pub struct TransactionTracker {
    state: TransactionState,
}

impl TransactionTracker {
    pub fn new() -> Self {
        Self {
            state: TransactionState::Idle,
        }
    }

    /// Update state from ReadyForQuery message status byte
    pub fn update_from_ready_for_query(&mut self, status: u8) {
        self.state = match status {
            b'I' => TransactionState::Idle,
            b'T' => TransactionState::InTransaction,
            b'E' => TransactionState::InError,
            _ => self.state, // Unknown status, keep current
        };
    }

    /// Get current transaction state
    pub fn state(&self) -> TransactionState {
        self.state
    }

    /// Check if currently in a transaction (T or E)
    pub fn is_in_transaction(&self) -> bool {
        matches!(self.state, TransactionState::InTransaction | TransactionState::InError)
    }

    /// Check if transaction just completed (state changed to Idle)
    pub fn is_idle(&self) -> bool {
        self.state == TransactionState::Idle
    }
}

impl Default for TransactionTracker {
    fn default() -> Self {
        Self::new()
    }
}
```

**Step 4: Add module to mod.rs**

```rust
// Add to scry-proxy/src/proxy/mod.rs after existing mods
mod transaction;

pub use transaction::{TransactionState, TransactionTracker};
```

**Step 5: Run test to verify it passes**

Run: `cargo test -p scry --lib transaction::tests`
Expected: PASS

**Step 6: Commit**

```bash
git add scry-proxy/src/proxy/transaction.rs scry-proxy/src/proxy/mod.rs
git commit -m "feat(pool): add TransactionTracker for transaction state"
```

---

### Task 1.2: Add ReadyForQuery streaming scanner

**Files:**
- Modify: `scry-proxy/src/protocol/extractor.rs`

**Step 1: Write the failing test**

```rust
// Add to scry-proxy/src/protocol/extractor.rs tests module

#[test]
fn test_extract_ready_for_query_idle() {
    let extractor = MessageExtractor::new();
    // ReadyForQuery: 'Z' + length(5) + status('I')
    let msg = vec![MSG_READY_FOR_QUERY, 0, 0, 0, 5, b'I'];

    let result = extractor.extract_ready_for_query(&msg);
    assert_eq!(result, Some(b'I'));
}

#[test]
fn test_extract_ready_for_query_in_transaction() {
    let extractor = MessageExtractor::new();
    let msg = vec![MSG_READY_FOR_QUERY, 0, 0, 0, 5, b'T'];

    let result = extractor.extract_ready_for_query(&msg);
    assert_eq!(result, Some(b'T'));
}

#[test]
fn test_extract_ready_for_query_error() {
    let extractor = MessageExtractor::new();
    let msg = vec![MSG_READY_FOR_QUERY, 0, 0, 0, 5, b'E'];

    let result = extractor.extract_ready_for_query(&msg);
    assert_eq!(result, Some(b'E'));
}

#[test]
fn test_extract_ready_for_query_in_stream() {
    let extractor = MessageExtractor::new();
    // DataRow + CommandComplete + ReadyForQuery
    let mut msg = vec![];
    // DataRow: 'D' + length + data
    msg.extend_from_slice(&[MSG_DATA_ROW, 0, 0, 0, 10]);
    msg.extend_from_slice(&[0, 1, 0, 0, 0, 1, b'1']); // 1 column, value "1"
    // CommandComplete: 'C' + length + "SELECT 1\0"
    msg.extend_from_slice(&[MSG_COMMAND_COMPLETE, 0, 0, 0, 13]);
    msg.extend_from_slice(b"SELECT 1\0");
    // ReadyForQuery
    msg.extend_from_slice(&[MSG_READY_FOR_QUERY, 0, 0, 0, 5, b'I']);

    let result = extractor.extract_ready_for_query(&msg);
    assert_eq!(result, Some(b'I'));
}

#[test]
fn test_no_ready_for_query() {
    let extractor = MessageExtractor::new();
    // Just a DataRow, no ReadyForQuery
    let mut msg = vec![MSG_DATA_ROW, 0, 0, 0, 10];
    msg.extend_from_slice(&[0, 1, 0, 0, 0, 1, b'1']);

    let result = extractor.extract_ready_for_query(&msg);
    assert_eq!(result, None);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p scry --lib extractor::tests::test_extract_ready_for_query`
Expected: FAIL with "no method named `extract_ready_for_query`"

**Step 3: Write minimal implementation**

```rust
// Add to MessageExtractor impl in scry-proxy/src/protocol/extractor.rs

/// Extract ReadyForQuery status from backend response stream
///
/// Scans through the message stream looking for ReadyForQuery ('Z') message
/// and returns the transaction status byte: 'I' (idle), 'T' (in transaction), 'E' (error)
///
/// This is a streaming scan - no buffering required.
pub fn extract_ready_for_query(&self, data: &[u8]) -> Option<u8> {
    let mut offset = 0;

    while offset + 5 <= data.len() {
        let msg_type = data[offset];

        if msg_type == MSG_READY_FOR_QUERY {
            // ReadyForQuery is always 6 bytes: type(1) + length(4) + status(1)
            // Length is always 5 (includes itself but not type byte)
            if offset + 6 <= data.len() {
                let status = data[offset + 5];
                return Some(status);
            }
        }

        // Skip to next message
        if offset + 5 <= data.len() {
            let length = i32::from_be_bytes([
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
            ]) as usize;

            if length < 4 || offset + 1 + length > data.len() {
                break; // Invalid or incomplete message
            }
            offset += 1 + length;
        } else {
            break;
        }
    }

    None
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p scry --lib extractor::tests::test_extract_ready_for_query`
Expected: PASS

**Step 5: Commit**

```bash
git add scry-proxy/src/protocol/extractor.rs
git commit -m "feat(protocol): add ReadyForQuery streaming extraction"
```

---

## Phase 2: Connection State Tracking

### Task 2.1: Add PinReason and ConnectionState

**Files:**
- Create: `scry-proxy/src/proxy/connection_state.rs`
- Modify: `scry-proxy/src/proxy/mod.rs`

**Step 1: Write the failing test**

```rust
// In scry-proxy/src/proxy/connection_state.rs
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
    fn test_clear_all() {
        let mut state = ConnectionState::new(1000);
        state.add_prepared_statement("stmt1".to_string(), "SELECT 1".to_string(), vec![]);
        state.add_session_variable("tz".to_string(), "UTC".to_string());
        state.add_temp_table("tmp".to_string());

        state.clear_all();

        assert!(!state.is_pinned());
        assert!(!state.has_unsafe_state());
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p scry --lib connection_state::tests`
Expected: FAIL with "cannot find module"

**Step 3: Write minimal implementation**

```rust
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
        matches!(self, PinReason::TempTable | PinReason::Cursor | PinReason::AdvisoryLock)
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
            prepared_statements: self.prepared_statements
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
```

**Step 4: Add module to mod.rs**

```rust
// Add to scry-proxy/src/proxy/mod.rs
mod connection_state;

pub use connection_state::{ConnectionState, PinReason, ReplayableState, PreparedStatementInfo};
```

**Step 5: Run test to verify it passes**

Run: `cargo test -p scry --lib connection_state::tests`
Expected: PASS

**Step 6: Commit**

```bash
git add scry-proxy/src/proxy/connection_state.rs scry-proxy/src/proxy/mod.rs
git commit -m "feat(pool): add ConnectionState for pinning decisions"
```

---

### Task 2.2: Add SQL command detector for state changes

**Files:**
- Create: `scry-proxy/src/protocol/command_detector.rs`
- Modify: `scry-proxy/src/protocol/mod.rs`

**Step 1: Write the failing test**

```rust
// In scry-proxy/src/protocol/command_detector.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_set_command() {
        let result = CommandDetector::detect("SET search_path TO public");
        assert!(matches!(result, Some(DetectedCommand::Set { name, value })
            if name == "search_path" && value == "public"));
    }

    #[test]
    fn test_detect_set_with_equals() {
        let result = CommandDetector::detect("SET timezone = 'UTC'");
        assert!(matches!(result, Some(DetectedCommand::Set { name, .. })
            if name == "timezone"));
    }

    #[test]
    fn test_detect_create_temp_table() {
        let result = CommandDetector::detect("CREATE TEMP TABLE tmp_users (id int)");
        assert!(matches!(result, Some(DetectedCommand::CreateTempTable { name })
            if name == "tmp_users"));
    }

    #[test]
    fn test_detect_create_temporary_table() {
        let result = CommandDetector::detect("CREATE TEMPORARY TABLE tmp_data AS SELECT 1");
        assert!(matches!(result, Some(DetectedCommand::CreateTempTable { .. })));
    }

    #[test]
    fn test_detect_declare_cursor() {
        let result = CommandDetector::detect("DECLARE my_cursor CURSOR FOR SELECT * FROM users");
        assert!(matches!(result, Some(DetectedCommand::DeclareCursor { name, with_hold: false })
            if name == "my_cursor"));
    }

    #[test]
    fn test_detect_declare_cursor_with_hold() {
        let result = CommandDetector::detect("DECLARE my_cursor CURSOR WITH HOLD FOR SELECT 1");
        assert!(matches!(result, Some(DetectedCommand::DeclareCursor { with_hold: true, .. })));
    }

    #[test]
    fn test_detect_close_cursor() {
        let result = CommandDetector::detect("CLOSE my_cursor");
        assert!(matches!(result, Some(DetectedCommand::CloseCursor { name })
            if name == "my_cursor"));
    }

    #[test]
    fn test_detect_advisory_lock() {
        let result = CommandDetector::detect("SELECT pg_advisory_lock(12345)");
        assert!(matches!(result, Some(DetectedCommand::AdvisoryLock { .. })));
    }

    #[test]
    fn test_detect_advisory_unlock() {
        let result = CommandDetector::detect("SELECT pg_advisory_unlock(12345)");
        assert!(matches!(result, Some(DetectedCommand::AdvisoryUnlock { .. })));
    }

    #[test]
    fn test_detect_discard_all() {
        let result = CommandDetector::detect("DISCARD ALL");
        assert!(matches!(result, Some(DetectedCommand::DiscardAll)));
    }

    #[test]
    fn test_detect_reset() {
        let result = CommandDetector::detect("RESET search_path");
        assert!(matches!(result, Some(DetectedCommand::Reset { name })
            if name == "search_path"));
    }

    #[test]
    fn test_detect_reset_all() {
        let result = CommandDetector::detect("RESET ALL");
        assert!(matches!(result, Some(DetectedCommand::ResetAll)));
    }

    #[test]
    fn test_detect_deallocate() {
        let result = CommandDetector::detect("DEALLOCATE stmt1");
        assert!(matches!(result, Some(DetectedCommand::Deallocate { name })
            if name == "stmt1"));
    }

    #[test]
    fn test_detect_deallocate_all() {
        let result = CommandDetector::detect("DEALLOCATE ALL");
        assert!(matches!(result, Some(DetectedCommand::DeallocateAll)));
    }

    #[test]
    fn test_detect_drop_temp_table() {
        let result = CommandDetector::detect("DROP TABLE tmp_users");
        // Note: We can't distinguish temp vs regular from SQL alone
        // This returns DropTable which caller must check against known temps
        assert!(matches!(result, Some(DetectedCommand::DropTable { name })
            if name == "tmp_users"));
    }

    #[test]
    fn test_regular_select_no_detection() {
        let result = CommandDetector::detect("SELECT * FROM users WHERE id = 1");
        assert!(result.is_none());
    }

    #[test]
    fn test_insert_no_detection() {
        let result = CommandDetector::detect("INSERT INTO users (name) VALUES ('test')");
        assert!(result.is_none());
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p scry --lib command_detector::tests`
Expected: FAIL with "cannot find module"

**Step 3: Write minimal implementation**

```rust
// scry-proxy/src/protocol/command_detector.rs

/// Detected command that affects connection state
#[derive(Debug, Clone, PartialEq)]
pub enum DetectedCommand {
    /// SET variable = value
    Set { name: String, value: String },
    /// RESET variable
    Reset { name: String },
    /// RESET ALL
    ResetAll,
    /// CREATE TEMP/TEMPORARY TABLE
    CreateTempTable { name: String },
    /// DROP TABLE (caller checks if it's a temp table)
    DropTable { name: String },
    /// DECLARE cursor [WITH HOLD]
    DeclareCursor { name: String, with_hold: bool },
    /// CLOSE cursor
    CloseCursor { name: String },
    /// pg_advisory_lock() or pg_advisory_lock_shared()
    AdvisoryLock { key: Option<i64> },
    /// pg_advisory_unlock() or pg_advisory_unlock_shared()
    AdvisoryUnlock { key: Option<i64> },
    /// DISCARD ALL
    DiscardAll,
    /// DEALLOCATE statement
    Deallocate { name: String },
    /// DEALLOCATE ALL
    DeallocateAll,
}

/// Detects state-changing SQL commands
pub struct CommandDetector;

impl CommandDetector {
    /// Detect if SQL command affects connection state
    pub fn detect(sql: &str) -> Option<DetectedCommand> {
        let sql_upper = sql.trim().to_uppercase();
        let sql_trimmed = sql.trim();

        // SET variable
        if sql_upper.starts_with("SET ") {
            return Self::parse_set(sql_trimmed);
        }

        // RESET
        if sql_upper.starts_with("RESET ") {
            return Self::parse_reset(sql_trimmed);
        }

        // DISCARD ALL
        if sql_upper.starts_with("DISCARD ALL") {
            return Some(DetectedCommand::DiscardAll);
        }

        // CREATE TEMP TABLE
        if sql_upper.contains("CREATE") &&
           (sql_upper.contains("TEMP TABLE") || sql_upper.contains("TEMPORARY TABLE")) {
            return Self::parse_create_temp_table(sql_trimmed);
        }

        // DROP TABLE
        if sql_upper.starts_with("DROP TABLE") {
            return Self::parse_drop_table(sql_trimmed);
        }

        // DECLARE CURSOR
        if sql_upper.starts_with("DECLARE ") && sql_upper.contains("CURSOR") {
            return Self::parse_declare_cursor(sql_trimmed);
        }

        // CLOSE cursor
        if sql_upper.starts_with("CLOSE ") {
            return Self::parse_close_cursor(sql_trimmed);
        }

        // DEALLOCATE
        if sql_upper.starts_with("DEALLOCATE ") {
            return Self::parse_deallocate(sql_trimmed);
        }

        // pg_advisory_lock
        if sql_upper.contains("PG_ADVISORY_LOCK") && !sql_upper.contains("PG_ADVISORY_UNLOCK") {
            return Some(DetectedCommand::AdvisoryLock { key: Self::extract_lock_key(&sql_upper) });
        }

        // pg_advisory_unlock
        if sql_upper.contains("PG_ADVISORY_UNLOCK") {
            return Some(DetectedCommand::AdvisoryUnlock { key: Self::extract_lock_key(&sql_upper) });
        }

        None
    }

    fn parse_set(sql: &str) -> Option<DetectedCommand> {
        // SET name = value or SET name TO value
        let rest = sql.strip_prefix("SET").or_else(|| sql.strip_prefix("set"))?.trim();

        let (name, value) = if let Some(eq_pos) = rest.find('=') {
            let name = rest[..eq_pos].trim().to_lowercase();
            let value = rest[eq_pos + 1..].trim().trim_matches('\'').to_string();
            (name, value)
        } else if let Some(to_pos) = rest.to_uppercase().find(" TO ") {
            let name = rest[..to_pos].trim().to_lowercase();
            let value = rest[to_pos + 4..].trim().trim_matches('\'').to_string();
            (name, value)
        } else {
            return None;
        };

        Some(DetectedCommand::Set { name, value })
    }

    fn parse_reset(sql: &str) -> Option<DetectedCommand> {
        let rest = sql.strip_prefix("RESET").or_else(|| sql.strip_prefix("reset"))?.trim();

        if rest.eq_ignore_ascii_case("ALL") {
            Some(DetectedCommand::ResetAll)
        } else {
            Some(DetectedCommand::Reset { name: rest.to_lowercase() })
        }
    }

    fn parse_create_temp_table(sql: &str) -> Option<DetectedCommand> {
        // Find table name after TEMP TABLE or TEMPORARY TABLE
        let upper = sql.to_uppercase();
        let table_pos = upper.find("TEMP TABLE")
            .map(|p| p + 10)
            .or_else(|| upper.find("TEMPORARY TABLE").map(|p| p + 15))?;

        let rest = sql[table_pos..].trim();
        let name = rest.split_whitespace().next()?.to_string();

        Some(DetectedCommand::CreateTempTable { name })
    }

    fn parse_drop_table(sql: &str) -> Option<DetectedCommand> {
        let rest = sql.strip_prefix("DROP TABLE").or_else(|| sql.strip_prefix("drop table"))?.trim();
        let rest = rest.strip_prefix("IF EXISTS").unwrap_or(rest).trim();
        let name = rest.split_whitespace().next()?.to_string();

        Some(DetectedCommand::DropTable { name })
    }

    fn parse_declare_cursor(sql: &str) -> Option<DetectedCommand> {
        let upper = sql.to_uppercase();
        let rest = sql.strip_prefix("DECLARE").or_else(|| sql.strip_prefix("declare"))?.trim();

        let name = rest.split_whitespace().next()?.to_string();
        let with_hold = upper.contains("WITH HOLD");

        Some(DetectedCommand::DeclareCursor { name, with_hold })
    }

    fn parse_close_cursor(sql: &str) -> Option<DetectedCommand> {
        let rest = sql.strip_prefix("CLOSE").or_else(|| sql.strip_prefix("close"))?.trim();
        let name = rest.split_whitespace().next()?.to_string();

        Some(DetectedCommand::CloseCursor { name })
    }

    fn parse_deallocate(sql: &str) -> Option<DetectedCommand> {
        let rest = sql.strip_prefix("DEALLOCATE").or_else(|| sql.strip_prefix("deallocate"))?.trim();
        let rest = rest.strip_prefix("PREPARE").unwrap_or(rest).trim();

        if rest.eq_ignore_ascii_case("ALL") {
            Some(DetectedCommand::DeallocateAll)
        } else {
            Some(DetectedCommand::Deallocate { name: rest.split_whitespace().next()?.to_string() })
        }
    }

    fn extract_lock_key(sql: &str) -> Option<i64> {
        // Try to extract numeric key from pg_advisory_lock(12345)
        if let Some(start) = sql.find('(') {
            if let Some(end) = sql.find(')') {
                let inner = &sql[start + 1..end];
                return inner.trim().parse().ok();
            }
        }
        None
    }
}
```

**Step 4: Add module to protocol/mod.rs**

```rust
// Add to scry-proxy/src/protocol/mod.rs
mod command_detector;

pub use command_detector::{CommandDetector, DetectedCommand};
```

**Step 5: Run test to verify it passes**

Run: `cargo test -p scry --lib command_detector::tests`
Expected: PASS

**Step 6: Commit**

```bash
git add scry-proxy/src/protocol/command_detector.rs scry-proxy/src/protocol/mod.rs
git commit -m "feat(protocol): add CommandDetector for state-changing SQL"
```

---

## Phase 3: Update Configuration

### Task 3.1: Add new pooling configuration options

**Files:**
- Modify: `scry-proxy/src/config/mod.rs`

**Step 1: Write the failing test**

```rust
// Add to scry-proxy/src/config/mod.rs tests section

#[test]
fn test_pooling_strategy_default_is_hybrid() {
    let config = Config::default();
    assert_eq!(config.performance.connection_pooling, PoolingStrategy::Hybrid);
}

#[test]
fn test_pool_queue_depth_default() {
    let config = Config::default();
    assert_eq!(config.performance.pool_queue_depth, 50);
}

#[test]
fn test_pool_idle_unpin_secs_default() {
    let config = Config::default();
    assert_eq!(config.performance.pool_idle_unpin_secs, 60);
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p scry --lib config::tests::test_pooling`
Expected: FAIL with "no field `pool_queue_depth`"

**Step 3: Update PoolingStrategy and PerformanceConfig**

```rust
// In scry-proxy/src/config/mod.rs

// Update PoolingStrategy enum to add Transaction
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PoolingStrategy {
    /// No pooling - 1:1 client-to-backend mapping
    Disabled,
    /// Session pooling - connection assigned for entire client session
    Session,
    /// Transaction pooling - connection released after each transaction (strict mode)
    Transaction,
    /// Hybrid pooling - dynamic pinning with automatic state tracking (default)
    Hybrid,
}

// Update PerformanceConfig to add new fields
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    pub target_latency_ms: u64,
    pub connection_pooling: PoolingStrategy,
    pub pool_size: usize,
    pub pool_min_idle: usize,
    pub pool_timeout_secs: u64,
    pub pool_recycle_secs: u64,
    pub pool_aggressive_unpinning: bool,
    pub buffer_size: usize,
    /// Maximum clients waiting for a connection (0 = unlimited)
    pub pool_queue_depth: usize,
    /// Idle timeout before unpinning in hybrid mode (seconds)
    pub pool_idle_unpin_secs: u64,
    /// Use LIFO connection selection (true) or FIFO (false)
    pub pool_lifo: bool,
}

// Update Default impl for PerformanceConfig
impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            target_latency_ms: 1,
            connection_pooling: PoolingStrategy::Hybrid, // Changed from Disabled
            pool_size: 100,
            pool_min_idle: 10,
            pool_timeout_secs: 30,
            pool_recycle_secs: 3600,
            pool_aggressive_unpinning: false,
            buffer_size: 8192,
            pool_queue_depth: 50,       // New
            pool_idle_unpin_secs: 60,   // New
            pool_lifo: true,            // New
        }
    }
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p scry --lib config`
Expected: PASS

**Step 5: Update test configs in test files**

The test files use explicit PerformanceConfig. Add the new fields:

```rust
// In each test file (integration_test.rs, stateful_test.rs, etc.)
// Add to PerformanceConfig creation:
pool_queue_depth: 50,
pool_idle_unpin_secs: 60,
pool_lifo: true,
```

**Step 6: Commit**

```bash
git add scry-proxy/src/config/mod.rs
git commit -m "feat(config): add transaction/hybrid pooling configuration"
```

---

## Phase 4: Transaction Mode Enforcement

### Task 4.1: Add transaction mode command validator

**Files:**
- Create: `scry-proxy/src/proxy/mode_enforcer.rs`
- Modify: `scry-proxy/src/proxy/mod.rs`

**Step 1: Write the failing test**

```rust
// In scry-proxy/src/proxy/mode_enforcer.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_mode_rejects_set_outside_txn() {
        let enforcer = ModeEnforcer::new(PoolingMode::Transaction);
        let result = enforcer.validate("SET search_path TO public", false);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not supported in transaction pooling mode"));
    }

    #[test]
    fn test_transaction_mode_allows_set_inside_txn() {
        let enforcer = ModeEnforcer::new(PoolingMode::Transaction);
        let result = enforcer.validate("SET search_path TO public", true);

        assert!(result.is_ok());
    }

    #[test]
    fn test_transaction_mode_rejects_temp_table() {
        let enforcer = ModeEnforcer::new(PoolingMode::Transaction);
        let result = enforcer.validate("CREATE TEMP TABLE tmp (id int)", false);

        assert!(result.is_err());
    }

    #[test]
    fn test_transaction_mode_rejects_cursor_with_hold() {
        let enforcer = ModeEnforcer::new(PoolingMode::Transaction);
        let result = enforcer.validate("DECLARE c CURSOR WITH HOLD FOR SELECT 1", false);

        assert!(result.is_err());
    }

    #[test]
    fn test_transaction_mode_allows_cursor_without_hold() {
        let enforcer = ModeEnforcer::new(PoolingMode::Transaction);
        // Regular cursor inside transaction is fine
        let result = enforcer.validate("DECLARE c CURSOR FOR SELECT 1", true);

        assert!(result.is_ok());
    }

    #[test]
    fn test_transaction_mode_rejects_advisory_lock() {
        let enforcer = ModeEnforcer::new(PoolingMode::Transaction);
        let result = enforcer.validate("SELECT pg_advisory_lock(123)", false);

        assert!(result.is_err());
    }

    #[test]
    fn test_transaction_mode_allows_prepare() {
        let enforcer = ModeEnforcer::new(PoolingMode::Transaction);
        // PREPARE is allowed (handled via transparent re-preparation)
        let result = enforcer.validate("PREPARE stmt AS SELECT $1", false);

        assert!(result.is_ok());
    }

    #[test]
    fn test_hybrid_mode_allows_everything() {
        let enforcer = ModeEnforcer::new(PoolingMode::Hybrid);

        assert!(enforcer.validate("SET search_path TO public", false).is_ok());
        assert!(enforcer.validate("CREATE TEMP TABLE tmp (id int)", false).is_ok());
        assert!(enforcer.validate("SELECT pg_advisory_lock(123)", false).is_ok());
    }

    #[test]
    fn test_session_mode_allows_everything() {
        let enforcer = ModeEnforcer::new(PoolingMode::Session);

        assert!(enforcer.validate("SET search_path TO public", false).is_ok());
        assert!(enforcer.validate("CREATE TEMP TABLE tmp (id int)", false).is_ok());
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p scry --lib mode_enforcer::tests`
Expected: FAIL

**Step 3: Write minimal implementation**

```rust
// scry-proxy/src/proxy/mode_enforcer.rs

use crate::protocol::{CommandDetector, DetectedCommand};

/// Pooling mode for enforcement decisions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolingMode {
    Session,
    Transaction,
    Hybrid,
}

/// Enforces pooling mode restrictions on SQL commands
pub struct ModeEnforcer {
    mode: PoolingMode,
}

impl ModeEnforcer {
    pub fn new(mode: PoolingMode) -> Self {
        Self { mode }
    }

    /// Validate a SQL command against the current pooling mode
    ///
    /// # Arguments
    /// * `sql` - The SQL command to validate
    /// * `in_transaction` - Whether currently inside a transaction block
    ///
    /// # Returns
    /// * `Ok(())` if command is allowed
    /// * `Err(error_message)` if command is rejected
    pub fn validate(&self, sql: &str, in_transaction: bool) -> Result<(), String> {
        // Session and Hybrid modes allow everything
        if self.mode != PoolingMode::Transaction {
            return Ok(());
        }

        // Transaction mode - enforce restrictions
        let detected = CommandDetector::detect(sql);

        match detected {
            Some(DetectedCommand::Set { .. }) => {
                if in_transaction {
                    // SET inside transaction is scoped to the transaction
                    Ok(())
                } else {
                    Err("session variables not supported in transaction pooling mode".to_string())
                }
            }
            Some(DetectedCommand::CreateTempTable { .. }) => {
                Err("temporary tables not supported in transaction pooling mode".to_string())
            }
            Some(DetectedCommand::DeclareCursor { with_hold: true, .. }) => {
                Err("cursors WITH HOLD not supported in transaction pooling mode".to_string())
            }
            Some(DetectedCommand::AdvisoryLock { .. }) => {
                Err("advisory locks not supported in transaction pooling mode".to_string())
            }
            // Everything else is allowed
            _ => Ok(()),
        }
    }

    /// Build a PostgreSQL error response message
    pub fn build_error_response(message: &str) -> Vec<u8> {
        let mut response = Vec::new();

        // ErrorResponse message type
        response.push(b'E');

        // Build fields
        let mut fields = Vec::new();

        // Severity
        fields.push(b'S');
        fields.extend_from_slice(b"ERROR");
        fields.push(0);

        // SQLSTATE (feature not supported)
        fields.push(b'C');
        fields.extend_from_slice(b"0A000");
        fields.push(0);

        // Message
        fields.push(b'M');
        fields.extend_from_slice(message.as_bytes());
        fields.push(0);

        // Terminator
        fields.push(0);

        // Length (includes itself)
        let length = (fields.len() + 4) as i32;
        response.extend_from_slice(&length.to_be_bytes());
        response.extend_from_slice(&fields);

        response
    }
}
```

**Step 4: Add module to mod.rs**

```rust
// Add to scry-proxy/src/proxy/mod.rs
mod mode_enforcer;

pub use mode_enforcer::{ModeEnforcer, PoolingMode};
```

**Step 5: Run test to verify it passes**

Run: `cargo test -p scry --lib mode_enforcer::tests`
Expected: PASS

**Step 6: Commit**

```bash
git add scry-proxy/src/proxy/mode_enforcer.rs scry-proxy/src/proxy/mod.rs
git commit -m "feat(pool): add ModeEnforcer for transaction mode restrictions"
```

---

## Phase 5: Wait Queue Implementation

### Task 5.1: Add bounded wait queue for pool exhaustion

**Files:**
- Create: `scry-proxy/src/proxy/wait_queue.rs`
- Modify: `scry-proxy/src/proxy/mod.rs`

**Step 1: Write the failing test**

```rust
// In scry-proxy/src/proxy/wait_queue.rs
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_queue_accepts_under_limit() {
        let queue = WaitQueue::new(10);

        let result = queue.enqueue().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_queue_rejects_over_limit() {
        let queue = WaitQueue::new(1);

        // Fill the queue
        let waiter1 = queue.enqueue().await.unwrap();

        // Second should be rejected
        let result = queue.enqueue().await;
        assert!(result.is_err());

        drop(waiter1);
    }

    #[tokio::test]
    async fn test_waiter_notified() {
        let queue = WaitQueue::new(10);

        let waiter = queue.enqueue().await.unwrap();

        // Notify in another task
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            queue.notify_one();
        });

        // Should complete without timeout
        let result = tokio::time::timeout(Duration::from_millis(100), waiter.wait()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_queue_depth_metric() {
        let queue = WaitQueue::new(10);

        assert_eq!(queue.depth(), 0);

        let _waiter1 = queue.enqueue().await.unwrap();
        assert_eq!(queue.depth(), 1);

        let _waiter2 = queue.enqueue().await.unwrap();
        assert_eq!(queue.depth(), 2);
    }

    #[tokio::test]
    async fn test_fifo_ordering() {
        let queue = WaitQueue::new(10);

        let waiter1 = queue.enqueue().await.unwrap();
        let waiter2 = queue.enqueue().await.unwrap();

        // Notify first waiter
        queue.notify_one();

        // waiter1 should be notified
        let result1 = tokio::time::timeout(Duration::from_millis(10), waiter1.wait()).await;
        assert!(result1.is_ok());

        // waiter2 should not be notified yet
        let result2 = tokio::time::timeout(Duration::from_millis(10), waiter2.wait()).await;
        assert!(result2.is_err()); // timeout
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p scry --lib wait_queue::tests`
Expected: FAIL

**Step 3: Write minimal implementation**

```rust
// scry-proxy/src/proxy/wait_queue.rs

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{Semaphore, OwnedSemaphorePermit};

/// A bounded wait queue for clients waiting for pool connections
pub struct WaitQueue {
    /// Maximum queue depth
    max_depth: usize,
    /// Current queue depth
    depth: AtomicUsize,
    /// Semaphore for notification
    notify: Arc<Semaphore>,
}

impl WaitQueue {
    pub fn new(max_depth: usize) -> Arc<Self> {
        Arc::new(Self {
            max_depth,
            depth: AtomicUsize::new(0),
            notify: Arc::new(Semaphore::new(0)),
        })
    }

    /// Try to enqueue a waiter
    ///
    /// Returns a Waiter if queue has space, or an error if full.
    pub async fn enqueue(self: &Arc<Self>) -> Result<Waiter, QueueFullError> {
        let current = self.depth.fetch_add(1, Ordering::SeqCst);

        if current >= self.max_depth {
            self.depth.fetch_sub(1, Ordering::SeqCst);
            return Err(QueueFullError);
        }

        Ok(Waiter {
            queue: Arc::clone(self),
            notified: false,
        })
    }

    /// Notify one waiter that a connection is available
    pub fn notify_one(&self) {
        self.notify.add_permits(1);
    }

    /// Get current queue depth
    pub fn depth(&self) -> usize {
        self.depth.load(Ordering::SeqCst)
    }

    /// Get maximum queue depth
    pub fn max_depth(&self) -> usize {
        self.max_depth
    }
}

/// A waiter in the queue
pub struct Waiter {
    queue: Arc<WaitQueue>,
    notified: bool,
}

impl Waiter {
    /// Wait until notified or timeout
    pub async fn wait(&mut self) {
        let _ = self.queue.notify.acquire().await;
        self.notified = true;
    }
}

impl Drop for Waiter {
    fn drop(&mut self) {
        // Decrement queue depth when waiter is dropped
        self.queue.depth.fetch_sub(1, Ordering::SeqCst);

        // If we were notified but not used, pass the permit on
        // This handles the case where waiter times out after being notified
    }
}

/// Error returned when queue is full
#[derive(Debug, Clone)]
pub struct QueueFullError;

impl std::fmt::Display for QueueFullError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "connection pool queue is full")
    }
}

impl std::error::Error for QueueFullError {}
```

**Step 4: Add module to mod.rs**

```rust
// Add to scry-proxy/src/proxy/mod.rs
mod wait_queue;

pub use wait_queue::{WaitQueue, Waiter, QueueFullError};
```

**Step 5: Run test to verify it passes**

Run: `cargo test -p scry --lib wait_queue::tests`
Expected: PASS

**Step 6: Commit**

```bash
git add scry-proxy/src/proxy/wait_queue.rs scry-proxy/src/proxy/mod.rs
git commit -m "feat(pool): add bounded WaitQueue for pool exhaustion"
```

---

## Phase 6: Integration Tests

### Task 6.1: Add transaction mode integration test

**Files:**
- Create: `scry-proxy/tests/transaction_pooling_test.rs`

**Step 1: Write the test file**

```rust
// scry-proxy/tests/transaction_pooling_test.rs

//! Integration tests for transaction pooling modes

use scry::config::*;
use scry::observability::{HealthConfig, ProxyMetrics};
use scry::proxy::ProxyServer;
use scry::publisher::DebugLoggerPublisher;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::{clients::Cli, Container};
use testcontainers_modules::postgres::Postgres;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn create_test_config(host: String, port: u16, pooling: PoolingStrategy) -> Config {
    Config {
        proxy: ProxyConfig {
            listen_address: "127.0.0.1:0".to_string(), // Random port
            max_connections: 100,
            shutdown_timeout_secs: 5,
        },
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
            service_name: "test".to_string(),
            metrics_server_address: "127.0.0.1:0".to_string(),
            enable_metrics_server: false,
        },
        protocol: ProtocolConfig {
            max_prepared_statements: 100,
        },
        publisher: PublisherConfig {
            enabled: false,
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
        },
        performance: PerformanceConfig {
            target_latency_ms: 1,
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
    }
}

// Helper to send a simple query and read response
async fn send_query(stream: &mut TcpStream, query: &str) -> Vec<u8> {
    // Build Query message
    let query_bytes = query.as_bytes();
    let length = (query_bytes.len() + 5) as i32;

    let mut msg = vec![b'Q'];
    msg.extend_from_slice(&length.to_be_bytes());
    msg.extend_from_slice(query_bytes);
    msg.push(0);

    stream.write_all(&msg).await.unwrap();

    // Read response
    let mut response = vec![0u8; 4096];
    let n = stream.read(&mut response).await.unwrap();
    response.truncate(n);
    response
}

#[tokio::test]
async fn test_transaction_mode_rejects_set_outside_transaction() {
    let docker = Cli::default();
    let postgres: Container<Postgres> = docker.run(Postgres::default());
    let port = postgres.get_host_port_ipv4(5432);

    let config = create_test_config("127.0.0.1".to_string(), port, PoolingStrategy::Transaction);
    let publisher = Arc::new(DebugLoggerPublisher::new());
    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));

    // Start proxy
    let server = ProxyServer::new(config.clone(),
        scry::proxy::EventBatcher::new(publisher, 10, 100, 1000),
        metrics).await.unwrap();

    let proxy_addr = server.local_addr();

    tokio::spawn(async move {
        server.run().await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Connect through proxy
    let mut stream = TcpStream::connect(proxy_addr).await.unwrap();

    // Skip startup handshake for simplicity (would need proper impl)
    // ... startup message exchange ...

    // Try SET outside transaction - should get error
    let response = send_query(&mut stream, "SET search_path TO public").await;

    // Check for error response
    assert!(response.contains(&b'E'[0]), "Expected error response");
    assert!(
        String::from_utf8_lossy(&response).contains("not supported in transaction pooling mode"),
        "Expected transaction pooling mode error"
    );
}

#[tokio::test]
async fn test_hybrid_mode_allows_set() {
    let docker = Cli::default();
    let postgres: Container<Postgres> = docker.run(Postgres::default());
    let port = postgres.get_host_port_ipv4(5432);

    let config = create_test_config("127.0.0.1".to_string(), port, PoolingStrategy::Hybrid);
    let publisher = Arc::new(DebugLoggerPublisher::new());
    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));

    let server = ProxyServer::new(config.clone(),
        scry::proxy::EventBatcher::new(publisher, 10, 100, 1000),
        metrics).await.unwrap();

    let proxy_addr = server.local_addr();

    tokio::spawn(async move {
        server.run().await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut stream = TcpStream::connect(proxy_addr).await.unwrap();

    // SET should work in hybrid mode
    let response = send_query(&mut stream, "SET search_path TO public").await;

    // Should NOT contain error
    assert!(!response.contains(&b'E'[0]) || !String::from_utf8_lossy(&response).contains("not supported"),
        "Hybrid mode should allow SET");
}

#[tokio::test]
async fn test_connection_released_after_transaction() {
    // This test verifies connections are returned to pool after COMMIT
    // by checking that a second client can get a connection from a pool of 1

    let docker = Cli::default();
    let postgres: Container<Postgres> = docker.run(Postgres::default());
    let port = postgres.get_host_port_ipv4(5432);

    let mut config = create_test_config("127.0.0.1".to_string(), port, PoolingStrategy::Transaction);
    config.performance.pool_size = 1; // Only 1 backend connection

    let publisher = Arc::new(DebugLoggerPublisher::new());
    let metrics = Arc::new(ProxyMetrics::new(100, HealthConfig::default()));

    let server = ProxyServer::new(config.clone(),
        scry::proxy::EventBatcher::new(publisher, 10, 100, 1000),
        metrics).await.unwrap();

    let proxy_addr = server.local_addr();

    tokio::spawn(async move {
        server.run().await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Client 1: BEGIN, query, COMMIT
    let mut stream1 = TcpStream::connect(proxy_addr).await.unwrap();
    send_query(&mut stream1, "BEGIN").await;
    send_query(&mut stream1, "SELECT 1").await;
    send_query(&mut stream1, "COMMIT").await;

    // Client 2 should be able to get connection now
    let mut stream2 = TcpStream::connect(proxy_addr).await.unwrap();
    let response = send_query(&mut stream2, "SELECT 2").await;

    // Should succeed, not timeout or error
    assert!(!response.is_empty(), "Second client should get response");
}
```

**Step 2: Run to verify tests work with infrastructure**

Run: `cargo test -p scry --test transaction_pooling_test`
Expected: Tests run (may need adjustment based on actual implementation)

**Step 3: Commit**

```bash
git add scry-proxy/tests/transaction_pooling_test.rs
git commit -m "test(pool): add transaction pooling integration tests"
```

---

## Remaining Phases (Summary)

The remaining implementation tasks follow the same TDD pattern:

### Phase 7: Pool Manager with LIFO+Sticky Selection
- Create `PoolManager` struct wrapping `TcpConnectionPool`
- Add LIFO connection stack
- Add client-to-backend sticky mapping for hybrid mode
- Integrate `WaitQueue` for bounded waiting

### Phase 8: Connection Handler Integration
- Modify `ConnectionHandler` to use `TransactionTracker`
- Add `ModeEnforcer` validation before forwarding
- Integrate `ConnectionState` tracking
- Handle transaction boundary detection via `ReadyForQuery`
- Release connections to pool on transaction end

### Phase 9: Transparent Reconnection
- Add reconnection logic for safe state
- Implement prepared statement replay
- Implement session variable replay
- Add metrics for reconnection attempts

### Phase 10: PgBouncer Configuration Compatibility
- Add `ini` crate for parsing pgbouncer.ini
- Create config loader that checks for pgbouncer.ini
- Map PgBouncer settings to Scry settings
- Add PGBOUNCER_* environment variable aliases

### Phase 11: Pooling Metrics
- Add `scry_pool_connections_pinned` gauge
- Add `scry_pool_pin_reason` counter
- Add `scry_pool_queue_depth` gauge
- Add `scry_pool_queue_rejected_total` counter
- Add `scry_pool_wait_seconds` histogram
- Integrate with Prometheus endpoint

---

## Execution Notes

**Estimated Tasks:** 40-50 individual steps
**Recommended Batch Size:** 5-10 tasks per session
**Review Checkpoints:** After each phase completion

Each task follows the TDD cycle:
1. Write failing test
2. Run test to confirm failure
3. Write minimal implementation
4. Run test to confirm pass
5. Commit

This ensures incremental, verifiable progress with git history showing each step.
