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

        // pg_advisory_lock
        if sql_upper.contains("PG_ADVISORY_LOCK") && !sql_upper.contains("PG_ADVISORY_UNLOCK") {
            return Some(DetectedCommand::AdvisoryLock {
                key: Self::extract_lock_key(&sql_upper),
            });
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
        let rest = sql
            .strip_prefix("SET")
            .or_else(|| sql.strip_prefix("set"))?
            .trim();

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
        let rest = sql
            .strip_prefix("RESET")
            .or_else(|| sql.strip_prefix("reset"))?
            .trim();

        if rest.eq_ignore_ascii_case("ALL") {
            Some(DetectedCommand::ResetAll)
        } else {
            Some(DetectedCommand::Reset {
                name: rest.to_lowercase(),
            })
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
        let rest = if rest.to_uppercase().starts_with("IF EXISTS") {
            rest[9..].trim()
        } else {
            rest
        };

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
        let rest = if rest.to_uppercase().starts_with("PREPARE ") {
            rest[8..].trim()
        } else {
            rest
        };

        if rest.eq_ignore_ascii_case("ALL") {
            Some(DetectedCommand::DeallocateAll)
        } else {
            Some(DetectedCommand::Deallocate {
                name: rest.split_whitespace().next()?.to_string(),
            })
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
