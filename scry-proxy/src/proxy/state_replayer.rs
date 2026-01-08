// scry-proxy/src/proxy/state_replayer.rs

//! State replayer for transparent reconnection
//!
//! Replays safe connection state (prepared statements, session variables)
//! to a new backend connection after reconnection.

use crate::proxy::ReplayableState;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Errors that can occur during state replay
#[derive(Debug, Clone)]
pub enum ReplayError {
    PreparedStatement { name: String, error: String },
    SessionVariable { name: String, error: String },
}

/// Result of replaying state to a connection
#[derive(Debug, Default)]
pub struct ReplayResult {
    pub prepared_statements_replayed: usize,
    pub session_variables_replayed: usize,
    pub errors: Vec<ReplayError>,
}

impl ReplayResult {
    /// Returns true if all replay operations succeeded
    pub fn is_success(&self) -> bool {
        self.errors.is_empty()
    }

    /// Returns the total number of items replayed
    pub fn total_replayed(&self) -> usize {
        self.prepared_statements_replayed + self.session_variables_replayed
    }
}

/// Replays connection state to a new backend connection
pub struct StateReplayer {
    // Optional metrics handle (placeholder for future)
}

impl StateReplayer {
    pub fn new() -> Self {
        Self {}
    }

    /// Build a simple query message for PostgreSQL wire protocol
    /// Format: 'Q' + length (i32, includes self + query + null) + query + '\0'
    pub fn build_query_message(query: &str) -> Vec<u8> {
        // Length = 4 bytes for length field + query bytes + 1 byte for null terminator
        let length = 4 + query.len() + 1;
        let mut message = Vec::with_capacity(1 + length);

        // Message type: 'Q' for simple query
        message.push(b'Q');

        // Length (big-endian i32, includes itself + query + null)
        message.extend_from_slice(&(length as i32).to_be_bytes());

        // Query string
        message.extend_from_slice(query.as_bytes());

        // Null terminator
        message.push(0);

        message
    }

    /// Parse response from PostgreSQL to check for success or error
    /// Returns Ok(true) if command completed successfully (ends with ReadyForQuery)
    /// Returns Ok(false) if there was an error (contains ErrorResponse)
    /// Returns Err if the response is malformed or incomplete
    pub fn parse_response(data: &[u8]) -> Result<bool, String> {
        let mut offset = 0;
        let mut saw_error = false;
        let mut saw_ready = false;

        while offset < data.len() {
            // Need at least 5 bytes for message header (tag + length)
            if offset + 5 > data.len() {
                return Err("Incomplete message header".to_string());
            }

            let tag = data[offset];
            let length = i32::from_be_bytes([
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
            ]) as usize;

            // Length includes itself (4 bytes) but not the tag
            let total_msg_len = 1 + length;

            if offset + total_msg_len > data.len() {
                return Err("Incomplete message body".to_string());
            }

            match tag {
                b'E' => {
                    // ErrorResponse
                    saw_error = true;
                }
                b'Z' => {
                    // ReadyForQuery - end of response
                    saw_ready = true;
                    break;
                }
                _ => {
                    // Other messages (CommandComplete, RowDescription, etc.) - ignore
                }
            }

            offset += total_msg_len;
        }

        if !saw_ready {
            return Err("No ReadyForQuery received".to_string());
        }

        Ok(!saw_error)
    }

    /// Read response from stream until ReadyForQuery is received
    /// Returns true if successful, false if error response received
    pub async fn read_response(stream: &mut TcpStream) -> Result<bool, anyhow::Error> {
        let mut buffer = Vec::new();
        let mut temp = [0u8; 4096];

        loop {
            // Try to parse what we have
            if !buffer.is_empty() {
                match Self::parse_response(&buffer) {
                    Ok(success) => return Ok(success),
                    Err(_) => {
                        // Need more data - continue reading
                    }
                }
            }

            // Read more data from stream
            let n = stream.read(&mut temp).await?;
            if n == 0 {
                return Err(anyhow::anyhow!("Connection closed before ReadyForQuery"));
            }
            buffer.extend_from_slice(&temp[..n]);
        }
    }

    /// Build SQL for preparing a statement
    /// Uses PostgreSQL's PREPARE syntax: PREPARE name AS query
    pub fn build_prepare_sql(name: &str, query: &str) -> String {
        format!("PREPARE {} AS {}", name, query)
    }

    /// Build SQL for setting a session variable
    /// Handles special cases like search_path and quoted values
    pub fn build_set_sql(name: &str, value: &str) -> String {
        // Special case: search_path uses TO syntax without quotes
        if name.eq_ignore_ascii_case("search_path") {
            return format!("SET search_path TO {}", value);
        }

        // Escape single quotes by doubling them
        let escaped_value = value.replace('\'', "''");
        format!("SET {} = '{}'", name, escaped_value)
    }

    /// Replay state to a connection
    /// Uses simple query protocol to execute:
    /// - "PREPARE stmt_name AS query" for each prepared statement
    /// - "SET variable = 'value'" for each session variable
    pub async fn replay(
        &self,
        state: &ReplayableState,
        stream: &mut TcpStream,
    ) -> Result<ReplayResult, anyhow::Error> {
        let mut result = ReplayResult::default();

        // Replay session variables first (they might affect prepared statements)
        for (name, value) in &state.session_variables {
            let sql = Self::build_set_sql(name, value);
            let msg = Self::build_query_message(&sql);

            stream.write_all(&msg).await?;
            let success = Self::read_response(stream).await?;

            if success {
                result.session_variables_replayed += 1;
            } else {
                result.errors.push(ReplayError::SessionVariable {
                    name: name.clone(),
                    error: format!("Failed to replay SET {} = '{}'", name, value),
                });
            }
        }

        // Replay prepared statements
        for stmt in &state.prepared_statements {
            let sql = Self::build_prepare_sql(&stmt.name, &stmt.query);
            let msg = Self::build_query_message(&sql);

            stream.write_all(&msg).await?;
            let success = Self::read_response(stream).await?;

            if success {
                result.prepared_statements_replayed += 1;
            } else {
                result.errors.push(ReplayError::PreparedStatement {
                    name: stmt.name.clone(),
                    error: format!("Failed to replay PREPARE {} AS {}", stmt.name, stmt.query),
                });
            }
        }

        Ok(result)
    }
}

impl Default for StateReplayer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_query_message_simple() {
        // PostgreSQL Simple Query protocol:
        // 'Q' (1 byte) + length (4 bytes, big-endian, includes itself + query + null) + query + '\0'
        let query = "SELECT 1";
        let message = StateReplayer::build_query_message(query);

        // Expected:
        // 'Q' = 0x51
        // length = 4 (for length) + 8 (query) + 1 (null) = 13 = 0x0000000D
        // "SELECT 1" + '\0'
        assert_eq!(message[0], b'Q');

        // Length is big-endian i32
        let length = i32::from_be_bytes([message[1], message[2], message[3], message[4]]);
        assert_eq!(length, 13); // 4 + 8 + 1

        // Query string
        assert_eq!(&message[5..13], b"SELECT 1");

        // Null terminator
        assert_eq!(message[13], 0);

        // Total length
        assert_eq!(message.len(), 14); // 1 (Q) + 4 (len) + 8 (query) + 1 (null)
    }

    #[test]
    fn test_build_query_message_with_special_chars() {
        let query = "SET timezone = 'UTC'";
        let message = StateReplayer::build_query_message(query);

        assert_eq!(message[0], b'Q');

        // length = 4 + 20 + 1 = 25
        let length = i32::from_be_bytes([message[1], message[2], message[3], message[4]]);
        assert_eq!(length, 25);

        // Total length = 1 + 4 + 20 + 1 = 26
        assert_eq!(message.len(), 26);
    }

    // Helper to build a PostgreSQL message with tag and payload
    fn build_pg_message(tag: u8, payload: &[u8]) -> Vec<u8> {
        let mut msg = Vec::new();
        msg.push(tag);
        let length = (4 + payload.len()) as i32;
        msg.extend_from_slice(&length.to_be_bytes());
        msg.extend_from_slice(payload);
        msg
    }

    // Build CommandComplete message: 'C' + length + "SET\0"
    fn build_command_complete(command: &str) -> Vec<u8> {
        let mut payload = command.as_bytes().to_vec();
        payload.push(0); // null terminator
        build_pg_message(b'C', &payload)
    }

    // Build ReadyForQuery message: 'Z' + length(5) + status
    fn build_ready_for_query(status: u8) -> Vec<u8> {
        build_pg_message(b'Z', &[status])
    }

    // Build ErrorResponse message: 'E' + length + fields (S, M, etc.)
    fn build_error_response(severity: &str, message: &str) -> Vec<u8> {
        let mut payload = Vec::new();
        // Severity field
        payload.push(b'S');
        payload.extend_from_slice(severity.as_bytes());
        payload.push(0);
        // Message field
        payload.push(b'M');
        payload.extend_from_slice(message.as_bytes());
        payload.push(0);
        // Terminator
        payload.push(0);
        build_pg_message(b'E', &payload)
    }

    #[test]
    fn test_parse_response_success() {
        // CommandComplete + ReadyForQuery = success
        let mut response = build_command_complete("SET");
        response.extend(build_ready_for_query(b'I')); // Idle status

        let result = StateReplayer::parse_response(&response);
        assert!(result.is_ok());
        assert!(result.unwrap()); // true = success
    }

    #[test]
    fn test_parse_response_error() {
        // ErrorResponse + ReadyForQuery = failure
        let mut response = build_error_response("ERROR", "syntax error");
        response.extend(build_ready_for_query(b'I'));

        let result = StateReplayer::parse_response(&response);
        assert!(result.is_ok());
        assert!(!result.unwrap()); // false = error
    }

    #[test]
    fn test_parse_response_incomplete() {
        // Just 'Z' with no length or data
        let response = vec![b'Z'];

        let result = StateReplayer::parse_response(&response);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_response_only_command_complete() {
        // CommandComplete only, no ReadyForQuery - incomplete
        let response = build_command_complete("SET");

        let result = StateReplayer::parse_response(&response);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_prepare_sql_simple() {
        let sql = StateReplayer::build_prepare_sql("my_stmt", "SELECT $1");
        assert_eq!(sql, "PREPARE my_stmt AS SELECT $1");
    }

    #[test]
    fn test_build_prepare_sql_complex() {
        let sql = StateReplayer::build_prepare_sql(
            "insert_user",
            "INSERT INTO users (name, email) VALUES ($1, $2)",
        );
        assert_eq!(
            sql,
            "PREPARE insert_user AS INSERT INTO users (name, email) VALUES ($1, $2)"
        );
    }

    #[test]
    fn test_build_set_sql_simple_value() {
        let sql = StateReplayer::build_set_sql("timezone", "UTC");
        assert_eq!(sql, "SET timezone = 'UTC'");
    }

    #[test]
    fn test_build_set_sql_search_path() {
        // search_path uses TO syntax without quotes for schema names
        let sql = StateReplayer::build_set_sql("search_path", "public, myschema");
        assert_eq!(sql, "SET search_path TO public, myschema");
    }

    #[test]
    fn test_build_set_sql_with_quotes_in_value() {
        // Values with quotes should be escaped
        let sql = StateReplayer::build_set_sql("my_setting", "it's a test");
        assert_eq!(sql, "SET my_setting = 'it''s a test'");
    }

    #[test]
    fn test_build_set_sql_numeric_value() {
        let sql = StateReplayer::build_set_sql("statement_timeout", "5000");
        assert_eq!(sql, "SET statement_timeout = '5000'");
    }

    #[tokio::test]
    async fn test_replay_empty_state() {
        use std::collections::HashMap;
        use tokio::net::TcpListener;

        // Start a mock server
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Empty state - no replay needed
        let state = ReplayableState {
            prepared_statements: vec![],
            session_variables: HashMap::new(),
        };

        // Server task that accepts connection but doesn't need to respond
        let server_handle = tokio::spawn(async move {
            let (mut _socket, _) = listener.accept().await.unwrap();
            // Just accept and close - no messages expected for empty state
        });

        // Client connects and replays
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let replayer = StateReplayer::new();
        let result = replayer.replay(&state, &mut stream).await.unwrap();

        assert_eq!(result.prepared_statements_replayed, 0);
        assert_eq!(result.session_variables_replayed, 0);
        assert!(result.errors.is_empty());

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_replay_session_variable() {
        use std::collections::HashMap;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        // Start a mock server
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // State with one session variable
        let mut session_vars = HashMap::new();
        session_vars.insert("timezone".to_string(), "UTC".to_string());

        let state = ReplayableState {
            prepared_statements: vec![],
            session_variables: session_vars,
        };

        // Server task that responds to SET command
        let server_handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();

            // Read the query message
            let mut buf = vec![0u8; 1024];
            let n = socket.read(&mut buf).await.unwrap();
            assert!(n > 0);
            assert_eq!(buf[0], b'Q'); // Query message

            // Send CommandComplete + ReadyForQuery
            let mut response = build_command_complete("SET");
            response.extend(build_ready_for_query(b'I'));
            socket.write_all(&response).await.unwrap();
        });

        // Client connects and replays
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let replayer = StateReplayer::new();
        let result = replayer.replay(&state, &mut stream).await.unwrap();

        assert_eq!(result.session_variables_replayed, 1);
        assert_eq!(result.prepared_statements_replayed, 0);
        assert!(result.errors.is_empty());

        server_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_replay_prepared_statement() {
        use crate::proxy::PreparedStatementInfo;
        use std::collections::HashMap;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        // Start a mock server
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // State with one prepared statement
        let state = ReplayableState {
            prepared_statements: vec![PreparedStatementInfo {
                name: "my_stmt".to_string(),
                query: "SELECT $1".to_string(),
                param_oids: vec![23], // int4
            }],
            session_variables: HashMap::new(),
        };

        // Server task that responds to PREPARE command
        let server_handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();

            // Read the query message
            let mut buf = vec![0u8; 1024];
            let n = socket.read(&mut buf).await.unwrap();
            assert!(n > 0);
            assert_eq!(buf[0], b'Q'); // Query message

            // Send CommandComplete + ReadyForQuery
            let mut response = build_command_complete("PREPARE");
            response.extend(build_ready_for_query(b'I'));
            socket.write_all(&response).await.unwrap();
        });

        // Client connects and replays
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let replayer = StateReplayer::new();
        let result = replayer.replay(&state, &mut stream).await.unwrap();

        assert_eq!(result.prepared_statements_replayed, 1);
        assert_eq!(result.session_variables_replayed, 0);
        assert!(result.errors.is_empty());

        server_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_replay_with_error() {
        use crate::proxy::PreparedStatementInfo;
        use std::collections::HashMap;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        // Start a mock server
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // State with one prepared statement
        let state = ReplayableState {
            prepared_statements: vec![PreparedStatementInfo {
                name: "bad_stmt".to_string(),
                query: "SELECT * FROM nonexistent".to_string(),
                param_oids: vec![],
            }],
            session_variables: HashMap::new(),
        };

        // Server task that responds with error
        let server_handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();

            // Read the query message
            let mut buf = vec![0u8; 1024];
            let n = socket.read(&mut buf).await.unwrap();
            assert!(n > 0);

            // Send ErrorResponse + ReadyForQuery
            let mut response = build_error_response("ERROR", "relation does not exist");
            response.extend(build_ready_for_query(b'I'));
            socket.write_all(&response).await.unwrap();
        });

        // Client connects and replays
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let replayer = StateReplayer::new();
        let result = replayer.replay(&state, &mut stream).await.unwrap();

        assert_eq!(result.prepared_statements_replayed, 0);
        assert_eq!(result.errors.len(), 1);
        match &result.errors[0] {
            ReplayError::PreparedStatement { name, .. } => {
                assert_eq!(name, "bad_stmt");
            }
            _ => panic!("Expected PreparedStatement error"),
        }

        server_handle.await.unwrap();
    }

    #[test]
    fn test_replay_result_is_success() {
        let result = ReplayResult::default();
        assert!(result.is_success());

        let result_with_error = ReplayResult {
            errors: vec![ReplayError::SessionVariable {
                name: "tz".to_string(),
                error: "failed".to_string(),
            }],
            ..Default::default()
        };
        assert!(!result_with_error.is_success());
    }

    #[test]
    fn test_replay_result_total_replayed() {
        let result = ReplayResult {
            prepared_statements_replayed: 3,
            session_variables_replayed: 2,
            errors: vec![],
        };
        assert_eq!(result.total_replayed(), 5);
    }
}
