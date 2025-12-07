/// PostgreSQL wire protocol implementation
///
/// Implements the Protocol trait for PostgreSQL, providing:
/// - State reset via DISCARD ALL
/// - Health checking
/// - Query/error extraction (delegating to existing MessageExtractor)

use super::traits::Protocol;
use super::MessageExtractor;
use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, warn};

/// PostgreSQL protocol handler
pub struct PostgresProtocol {
    extractor: MessageExtractor,
}

impl PostgresProtocol {
    pub fn new() -> Self {
        Self {
            extractor: MessageExtractor::new(),
        }
    }

    /// Check if response contains CommandComplete message
    fn contains_command_complete(data: &[u8]) -> bool {
        for &byte in data {
            if byte == b'C' {
                return true;
            }
        }
        false
    }

    /// Check if response contains ErrorResponse message
    fn contains_error_response(data: &[u8]) -> bool {
        for &byte in data {
            if byte == b'E' {
                return true;
            }
        }
        false
    }
}

impl Default for PostgresProtocol {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Protocol for PostgresProtocol {
    fn name(&self) -> &'static str {
        "postgres"
    }

    fn default_port(&self) -> u16 {
        5432
    }

    async fn reset_connection(&self, stream: &mut TcpStream) -> Result<bool> {
        // Send DISCARD ALL to reset connection state
        // DISCARD ALL resets:
        // - Temporary tables
        // - Prepared statements
        // - Cursors
        // - Session variables (SET commands)
        // - Transaction state
        // - Listen/notify registrations
        // - Sequence state

        debug!("Sending DISCARD ALL to reset connection state");

        // Construct Query message for "DISCARD ALL"
        // Format: 'Q' (1 byte) + length (4 bytes) + query string (null-terminated)
        let query = b"DISCARD ALL\0";
        let message_length = (4 + query.len()) as i32; // length includes itself but not type byte

        let mut message = Vec::with_capacity(1 + 4 + query.len());
        message.push(b'Q'); // Query message type
        message.extend_from_slice(&message_length.to_be_bytes());
        message.extend_from_slice(query);

        // Send the message
        stream
            .write_all(&message)
            .await
            .context("Failed to send DISCARD ALL command")?;

        // Read response - expect CommandComplete ('C') and ReadyForQuery ('Z')
        let mut response_buffer = vec![0u8; 1024];
        let n = stream
            .read(&mut response_buffer)
            .await
            .context("Failed to read DISCARD ALL response")?;

        if n == 0 {
            warn!("Connection closed while reading DISCARD ALL response");
            return Ok(false);
        }

        let response = &response_buffer[..n];

        // Check for CommandComplete ('C') or ReadyForQuery ('Z')
        // We're looking for successful completion
        if Self::contains_command_complete(response) {
            debug!("DISCARD ALL completed successfully");
            Ok(true)
        } else if Self::contains_error_response(response) {
            warn!("DISCARD ALL returned error, connection will be recycled");
            Ok(false)
        } else {
            warn!("Unexpected response to DISCARD ALL, connection will be recycled");
            Ok(false)
        }
    }

    async fn health_check(&self, stream: &mut TcpStream) -> Result<bool> {
        // Simple health check: verify socket is still connected
        // We could implement a proper Postgres ping, but for now
        // we rely on TCP-level checks and connection recycling

        // Check if the socket is readable (would indicate data or closure)
        match stream.try_read(&mut [0u8; 1]) {
            Ok(0) => Ok(false), // EOF - connection closed
            Ok(_) => Ok(true),  // Data available - connection alive
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                Ok(true) // No data ready, but socket is open
            }
            Err(_) => Ok(false), // Other error - connection dead
        }
    }

    fn extract_query(&self, data: &[u8]) -> Option<String> {
        self.extractor.extract_query(data)
    }

    fn is_query_complete(&self, data: &[u8]) -> bool {
        self.extractor.is_query_complete(data)
    }

    fn extract_error(&self, data: &[u8]) -> Option<String> {
        self.extractor.extract_error(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_metadata() {
        let proto = PostgresProtocol::new();
        assert_eq!(proto.name(), "postgres");
        assert_eq!(proto.default_port(), 5432);
    }

    #[test]
    fn test_extract_query_delegates_to_extractor() {
        let proto = PostgresProtocol::new();

        // Simple query message: 'Q' + length + "SELECT 1" + null
        let mut msg = vec![b'Q'];
        let query = "SELECT 1\0";
        let len = (query.len() + 4) as u32;
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(query.as_bytes());

        let result = proto.extract_query(&msg);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), "SELECT 1");
    }
}
