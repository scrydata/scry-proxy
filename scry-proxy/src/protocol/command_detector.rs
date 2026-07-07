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
    /// LISTEN channel — registers an async-notification subscription that
    /// must stay bound to this specific backend connection (P2 §4.3).
    Listen { channel: String },
    /// UNLISTEN channel, or UNLISTEN * (`channel: None`)
    Unlisten { channel: Option<String> },
    /// NOTIFY channel [, payload] — detected for attribution only; a bare
    /// NOTIFY does not itself register a subscription, so it never pins (see
    /// `ConnectionState::apply_query`).
    Notify { channel: String, payload: Option<String> },
}

/// Multiplexing-safety classification of a client command (P2 §4.1).
///
/// Used to decide, fail-closed, whether a connection may be released back to
/// the pool after a transaction: only `Clean` commands are positively safe.
#[derive(Debug, Clone, PartialEq)]
pub enum CommandClass {
    /// Positively safe to run on a pooled connection and release afterwards —
    /// standard DML, reads, and transaction control that leave no
    /// cross-transaction session state.
    Clean,
    /// A recognized state-changing command; the connection is pinned per its
    /// [`DetectedCommand`] reason.
    Stateful(DetectedCommand),
    /// Cannot be positively classified as clean (unknown command, a read with a
    /// session-mutating side effect, unusual/vendor syntax). Fail closed: pin.
    Unknown,
}

/// Whether `upper` (already upper-cased) begins with SQL keyword `kw` at a word
/// boundary — the keyword must be followed by whitespace, `(`, `;`, or the end
/// of the string, so `END` does not match `ENDPOINT`.
fn starts_with_keyword(upper: &str, kw: &str) -> bool {
    match upper.strip_prefix(kw) {
        Some(rest) => {
            rest.is_empty() || rest.starts_with(|c: char| c.is_whitespace() || c == '(' || c == ';')
        }
        None => false,
    }
}

/// Detects state-changing SQL commands
pub struct CommandDetector;

impl CommandDetector {
    /// Classify a command for pooling safety (P2 §4.1, §5.4).
    ///
    /// Fail-closed: a command is only `Clean` when it positively matches a
    /// known-safe shape. Anything else is `Stateful` (recognized) or `Unknown`
    /// (pin) — the blast radius of a detection gap is a performance cost (an
    /// over-pinned connection), never a correctness bug (leaked session state).
    ///
    /// `COPY ... FROM/TO STDIN/STDOUT` deliberately has no dedicated
    /// `DetectedCommand` variant and therefore always falls through to
    /// `Unknown` here (see `MUST_PIN_UNKNOWN` in `pooling_safety_test.rs`).
    /// COPY is passthrough-only by design for 1.0 (P2 §9.4): the proxy never
    /// models or replays COPY's own sub-protocol (`CopyInResponse`/
    /// `CopyData`/`CopyDone`/`CopyOutResponse`), it just forwards those bytes
    /// like any other unrecognized message — fail-closed `Unknown`
    /// classification simply keeps the connection pinned for the duration of
    /// the COPY rather than risking a mid-COPY release. See
    /// `copy_passthrough_byte_identical` in `differential_transparency_test.rs`
    /// for the byte-for-byte passthrough proof (WP-9 Task 9).
    pub fn classify(sql: &str) -> CommandClass {
        if let Some(cmd) = Self::detect(sql) {
            return CommandClass::Stateful(cmd);
        }
        if Self::is_known_clean(sql) {
            CommandClass::Clean
        } else {
            CommandClass::Unknown
        }
    }

    /// Whether `sql` is a positively-clean statement: pure DML, a read, or
    /// transaction control, with no session-mutating side effect. Conservative
    /// by design — when in doubt this returns `false` so the caller pins.
    pub fn is_known_clean(sql: &str) -> bool {
        let upper = sql.trim_start().to_uppercase();

        // Reject reads that carry a session-mutating side effect (e.g.
        // `SELECT set_config('x','y', false)` changes a GUC for the session).
        // `pg_advisory_*` is already caught by `detect()`.
        if upper.contains("SET_CONFIG") {
            return false;
        }

        const CLEAN_KEYWORDS: &[&str] = &[
            "SELECT",
            "INSERT",
            "UPDATE",
            "DELETE",
            "WITH",
            "VALUES",
            "TABLE",
            "EXPLAIN",
            "SHOW",
            "BEGIN",
            "START",
            "COMMIT",
            "ROLLBACK",
            "END",
            "ABORT",
            "SAVEPOINT",
            "RELEASE",
        ];
        CLEAN_KEYWORDS.iter().any(|kw| starts_with_keyword(&upper, kw))
    }

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
        if sql_upper.contains("CREATE")
            && (sql_upper.contains("TEMP TABLE") || sql_upper.contains("TEMPORARY TABLE"))
        {
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

        // LISTEN channel
        if sql_upper.starts_with("LISTEN ") {
            return Self::parse_listen(sql_trimmed);
        }

        // UNLISTEN channel / UNLISTEN *
        if sql_upper.starts_with("UNLISTEN ") || sql_upper == "UNLISTEN" {
            return Self::parse_unlisten(sql_trimmed);
        }

        // NOTIFY channel [, payload]
        if sql_upper.starts_with("NOTIFY ") {
            return Self::parse_notify(sql_trimmed);
        }

        // pg_advisory_lock
        if sql_upper.contains("PG_ADVISORY_LOCK") && !sql_upper.contains("PG_ADVISORY_UNLOCK") {
            return Some(DetectedCommand::AdvisoryLock { key: Self::extract_lock_key(&sql_upper) });
        }

        // pg_advisory_unlock
        if sql_upper.contains("PG_ADVISORY_UNLOCK") {
            return Some(DetectedCommand::AdvisoryUnlock {
                key: Self::extract_lock_key(&sql_upper),
            });
        }

        None
    }

    fn parse_set(sql: &str) -> Option<DetectedCommand> {
        // SET name = value or SET name TO value
        // We know sql starts with "SET " (case-insensitive) from detect(), so skip 3 chars for "SET"
        let rest = sql.get(3..)?.trim();

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
        // We know sql starts with "RESET " (case-insensitive) from detect(), so skip 5 chars for "RESET"
        let rest = sql.get(5..)?.trim();

        if rest.eq_ignore_ascii_case("ALL") {
            Some(DetectedCommand::ResetAll)
        } else {
            Some(DetectedCommand::Reset { name: rest.to_lowercase() })
        }
    }

    fn parse_create_temp_table(sql: &str) -> Option<DetectedCommand> {
        // Find table name after TEMP TABLE or TEMPORARY TABLE
        let upper = sql.to_uppercase();
        let table_pos = upper
            .find("TEMP TABLE")
            .map(|p| p + 10)
            .or_else(|| upper.find("TEMPORARY TABLE").map(|p| p + 15))?;

        let rest = sql[table_pos..].trim();
        let name = rest.split_whitespace().next()?.to_string();

        Some(DetectedCommand::CreateTempTable { name })
    }

    fn parse_drop_table(sql: &str) -> Option<DetectedCommand> {
        let upper = sql.to_uppercase();
        let rest = if upper.starts_with("DROP TABLE") {
            sql[10..].trim()
        } else {
            return None;
        };

        // Handle IF EXISTS
        let rest =
            if rest.to_uppercase().starts_with("IF EXISTS") { rest[9..].trim() } else { rest };

        let name = rest.split_whitespace().next()?.to_string();

        Some(DetectedCommand::DropTable { name })
    }

    fn parse_declare_cursor(sql: &str) -> Option<DetectedCommand> {
        let upper = sql.to_uppercase();
        let rest = if upper.starts_with("DECLARE ") {
            sql[8..].trim()
        } else {
            return None;
        };

        let name = rest.split_whitespace().next()?.to_string();
        let with_hold = upper.contains("WITH HOLD");

        Some(DetectedCommand::DeclareCursor { name, with_hold })
    }

    fn parse_close_cursor(sql: &str) -> Option<DetectedCommand> {
        let upper = sql.to_uppercase();
        let rest = if upper.starts_with("CLOSE ") {
            sql[6..].trim()
        } else {
            return None;
        };

        let name = rest.split_whitespace().next()?.to_string();

        Some(DetectedCommand::CloseCursor { name })
    }

    fn parse_deallocate(sql: &str) -> Option<DetectedCommand> {
        let upper = sql.to_uppercase();
        let rest = if upper.starts_with("DEALLOCATE ") {
            sql[11..].trim()
        } else {
            return None;
        };

        // Handle optional PREPARE keyword
        let rest =
            if rest.to_uppercase().starts_with("PREPARE ") { rest[8..].trim() } else { rest };

        if rest.eq_ignore_ascii_case("ALL") {
            Some(DetectedCommand::DeallocateAll)
        } else {
            Some(DetectedCommand::Deallocate { name: rest.split_whitespace().next()?.to_string() })
        }
    }

    fn parse_listen(sql: &str) -> Option<DetectedCommand> {
        // We know sql starts with "LISTEN " (case-insensitive) from detect().
        let rest = sql.get(7..)?.trim();
        let channel = rest.split_whitespace().next()?.trim_matches('"').to_string();
        Some(DetectedCommand::Listen { channel })
    }

    fn parse_unlisten(sql: &str) -> Option<DetectedCommand> {
        // We know sql starts with "UNLISTEN" (case-insensitive) from detect().
        let rest = sql.get(8..).unwrap_or("").trim();

        if rest.is_empty() || rest == "*" {
            Some(DetectedCommand::Unlisten { channel: None })
        } else {
            let channel = rest.split_whitespace().next()?.trim_matches('"').to_string();
            Some(DetectedCommand::Unlisten { channel: Some(channel) })
        }
    }

    fn parse_notify(sql: &str) -> Option<DetectedCommand> {
        // We know sql starts with "NOTIFY " (case-insensitive) from detect().
        let rest = sql.get(7..)?.trim();

        let (channel_part, payload) = match rest.find(',') {
            Some(idx) => {
                let payload = rest[idx + 1..].trim().trim_matches('\'').to_string();
                (&rest[..idx], Some(payload))
            }
            None => (rest, None),
        };

        let channel = channel_part.trim().trim_matches('"').to_string();
        Some(DetectedCommand::Notify { channel, payload })
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

    /// Naively split `sql` into non-empty, trimmed statements on `;` (WP-9 Task
    /// 9, P2 §4.6).
    ///
    /// Deliberately NOT a real SQL parser: it has no idea about string literals,
    /// dollar-quoting, or comments, so a `;` inside a literal (e.g.
    /// `SELECT 'a; SET x=1'`) produces a spurious extra fragment (here,
    /// `"SET x=1'"`). That is safe by construction for every caller of this
    /// function: `CommandDetector::classify` on a bogus fragment can only ever
    /// return `Unknown` or, at worst, a `Stateful` match on something that looks
    /// like a real command — both cases are fail-closed (over-pinning /
    /// over-attribution), never fail-open. A naive split can never make a real
    /// trailing state-changing statement (a genuine `SET`/temp-table/etc. after
    /// a real `;`) disappear from the output, so it can never cause
    /// under-pinning or a dropped attribution. A query with no interior `;`
    /// yields exactly one element (the trimmed original), so single-statement
    /// callers see no behavior change.
    ///
    /// Shared by [`ConnectionState::apply_query`](crate::proxy::ConnectionState::apply_query)
    /// (fail-closed pinning: classify+apply every split part) and the
    /// connection handler's event attribution (best-effort observability:
    /// attribute each part to its own `QueryEvent`) so both consumers agree on
    /// exactly one definition of "a statement" in a multi-statement batch.
    pub fn split_statements(sql: &str) -> Vec<&str> {
        sql.split(';').map(str::trim).filter(|s| !s.is_empty()).collect()
    }
}

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
    fn test_mixed_case_set_and_reset() {
        // Test mixed-case SET commands
        let result = CommandDetector::detect("Set timezone = 'UTC'");
        assert!(matches!(result, Some(DetectedCommand::Set { name, value })
            if name == "timezone" && value == "UTC"));

        let result = CommandDetector::detect("sEt search_path TO public");
        assert!(matches!(result, Some(DetectedCommand::Set { name, value })
            if name == "search_path" && value == "public"));

        // Test mixed-case RESET commands
        let result = CommandDetector::detect("Reset search_path");
        assert!(matches!(result, Some(DetectedCommand::Reset { name })
            if name == "search_path"));

        let result = CommandDetector::detect("rEsEt ALL");
        assert!(matches!(result, Some(DetectedCommand::ResetAll)));
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

    #[test]
    fn test_detect_listen() {
        let result = CommandDetector::detect("LISTEN scry_chan");
        assert!(matches!(result, Some(DetectedCommand::Listen { channel })
            if channel == "scry_chan"));
    }

    #[test]
    fn test_detect_listen_mixed_case() {
        let result = CommandDetector::detect("Listen scry_chan");
        assert!(matches!(result, Some(DetectedCommand::Listen { channel })
            if channel == "scry_chan"));
    }

    #[test]
    fn test_detect_unlisten_channel() {
        let result = CommandDetector::detect("UNLISTEN scry_chan");
        assert!(matches!(result, Some(DetectedCommand::Unlisten { channel: Some(channel) })
            if channel == "scry_chan"));
    }

    #[test]
    fn test_detect_unlisten_all() {
        let result = CommandDetector::detect("UNLISTEN *");
        assert!(matches!(result, Some(DetectedCommand::Unlisten { channel: None })));
    }

    #[test]
    fn test_detect_notify_bare() {
        let result = CommandDetector::detect("NOTIFY scry_chan");
        assert!(matches!(result, Some(DetectedCommand::Notify { channel, payload: None })
            if channel == "scry_chan"));
    }

    #[test]
    fn test_detect_notify_with_payload() {
        let result = CommandDetector::detect("NOTIFY scry_chan, 'payload-42'");
        assert!(matches!(result, Some(DetectedCommand::Notify { channel, payload: Some(payload) })
            if channel == "scry_chan" && payload == "payload-42"));
    }

    #[test]
    fn test_listen_is_stateful_via_classify() {
        assert!(matches!(
            CommandDetector::classify("LISTEN scry_chan"),
            CommandClass::Stateful(DetectedCommand::Listen { .. })
        ));
    }
}
