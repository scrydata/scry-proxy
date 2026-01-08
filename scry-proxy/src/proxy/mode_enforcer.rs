// scry-proxy/src/proxy/mode_enforcer.rs

//! ModeEnforcer for transaction pooling mode restrictions.
//!
//! Enforces rules based on the pooling mode:
//! - Session mode: allows everything (1:1 client-to-backend)
//! - Transaction mode: strict PgBouncer-compatible restrictions
//! - Hybrid mode: allows everything (uses smart pinning instead of rejection)

use crate::protocol::{CommandDetector, DetectedCommand};

/// Pooling mode for enforcement decisions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolingMode {
    /// Session pooling - connection assigned for entire client session
    Session,
    /// Transaction pooling - connection released after each transaction (strict mode)
    Transaction,
    /// Hybrid pooling - dynamic pinning with automatic state tracking
    Hybrid,
}

/// Enforces pooling mode restrictions on SQL commands
pub struct ModeEnforcer {
    mode: PoolingMode,
}

impl ModeEnforcer {
    /// Create a new ModeEnforcer for the specified pooling mode
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
            // Everything else is allowed (including PREPARE, regular cursors, etc.)
            _ => Ok(()),
        }
    }

    /// Build a PostgreSQL ErrorResponse message
    ///
    /// Creates a properly formatted PostgreSQL wire protocol error message
    /// with SQLSTATE 0A000 (feature_not_supported).
    pub fn build_error_response(message: &str) -> Vec<u8> {
        let mut response = Vec::new();

        // ErrorResponse message type
        response.push(b'E');

        // Build fields
        let mut fields = Vec::new();

        // Severity (required)
        fields.push(b'S');
        fields.extend_from_slice(b"ERROR");
        fields.push(0);

        // SQLSTATE code (0A000 = feature_not_supported)
        fields.push(b'C');
        fields.extend_from_slice(b"0A000");
        fields.push(0);

        // Message (required)
        fields.push(b'M');
        fields.extend_from_slice(message.as_bytes());
        fields.push(0);

        // Terminator
        fields.push(0);

        // Length (includes itself, 4 bytes)
        let length = (fields.len() + 4) as i32;
        response.extend_from_slice(&length.to_be_bytes());
        response.extend_from_slice(&fields);

        response
    }
}

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
        assert!(result.unwrap_err().contains("temporary tables not supported"));
    }

    #[test]
    fn test_transaction_mode_rejects_temp_table_inside_txn() {
        // Temp tables are rejected even inside transactions
        // because they persist beyond the transaction
        let enforcer = ModeEnforcer::new(PoolingMode::Transaction);
        let result = enforcer.validate("CREATE TEMP TABLE tmp (id int)", true);

        assert!(result.is_err());
    }

    #[test]
    fn test_transaction_mode_rejects_cursor_with_hold() {
        let enforcer = ModeEnforcer::new(PoolingMode::Transaction);
        let result = enforcer.validate("DECLARE c CURSOR WITH HOLD FOR SELECT 1", false);

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("WITH HOLD not supported"));
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
        assert!(result.unwrap_err().contains("advisory locks not supported"));
    }

    #[test]
    fn test_transaction_mode_rejects_advisory_lock_inside_txn() {
        // Advisory locks are rejected even inside transactions
        // because session-level locks persist beyond the transaction
        let enforcer = ModeEnforcer::new(PoolingMode::Transaction);
        let result = enforcer.validate("SELECT pg_advisory_lock(123)", true);

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
    fn test_transaction_mode_allows_regular_queries() {
        let enforcer = ModeEnforcer::new(PoolingMode::Transaction);

        assert!(enforcer.validate("SELECT * FROM users", false).is_ok());
        assert!(enforcer.validate("INSERT INTO users (name) VALUES ('test')", false).is_ok());
        assert!(enforcer.validate("UPDATE users SET name = 'foo'", false).is_ok());
        assert!(enforcer.validate("DELETE FROM users WHERE id = 1", false).is_ok());
        assert!(enforcer.validate("BEGIN", false).is_ok());
        assert!(enforcer.validate("COMMIT", false).is_ok());
        assert!(enforcer.validate("ROLLBACK", false).is_ok());
    }

    #[test]
    fn test_hybrid_mode_allows_everything() {
        let enforcer = ModeEnforcer::new(PoolingMode::Hybrid);

        assert!(enforcer.validate("SET search_path TO public", false).is_ok());
        assert!(enforcer.validate("CREATE TEMP TABLE tmp (id int)", false).is_ok());
        assert!(enforcer.validate("SELECT pg_advisory_lock(123)", false).is_ok());
        assert!(enforcer.validate("DECLARE c CURSOR WITH HOLD FOR SELECT 1", false).is_ok());
    }

    #[test]
    fn test_session_mode_allows_everything() {
        let enforcer = ModeEnforcer::new(PoolingMode::Session);

        assert!(enforcer.validate("SET search_path TO public", false).is_ok());
        assert!(enforcer.validate("CREATE TEMP TABLE tmp (id int)", false).is_ok());
        assert!(enforcer.validate("SELECT pg_advisory_lock(123)", false).is_ok());
        assert!(enforcer.validate("DECLARE c CURSOR WITH HOLD FOR SELECT 1", false).is_ok());
    }

    #[test]
    fn test_build_error_response_format() {
        let response = ModeEnforcer::build_error_response("test error message");

        // Should start with 'E' (ErrorResponse)
        assert_eq!(response[0], b'E');

        // Should have valid length (4 bytes after type)
        let length = i32::from_be_bytes([response[1], response[2], response[3], response[4]]);
        assert!(length > 0);
        assert_eq!(response.len(), 1 + length as usize);

        // Should contain the message
        let response_str = String::from_utf8_lossy(&response);
        assert!(response_str.contains("test error message"));

        // Should contain ERROR severity
        assert!(response_str.contains("ERROR"));

        // Should contain SQLSTATE 0A000
        assert!(response_str.contains("0A000"));
    }

    #[test]
    fn test_build_error_response_structure() {
        let response = ModeEnforcer::build_error_response("error");

        // Parse the message to verify structure
        assert_eq!(response[0], b'E'); // Type

        let length =
            i32::from_be_bytes([response[1], response[2], response[3], response[4]]) as usize;
        let body = &response[5..5 + length - 4]; // Body without length bytes

        // Should have fields: S (severity), C (code), M (message), and terminator
        assert_eq!(body[0], b'S'); // Severity field
        assert_eq!(body[body.len() - 1], 0); // Terminator
    }

    #[test]
    fn test_case_insensitive_validation() {
        let enforcer = ModeEnforcer::new(PoolingMode::Transaction);

        // Lower case
        assert!(enforcer.validate("set search_path to public", false).is_err());

        // Mixed case
        assert!(enforcer.validate("Set Search_Path TO public", false).is_err());

        // Upper case
        assert!(enforcer.validate("SET SEARCH_PATH TO PUBLIC", false).is_err());
    }
}
