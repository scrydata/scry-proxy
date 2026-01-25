/// PostgreSQL wire protocol implementation
///
/// Implements the Protocol trait for PostgreSQL, providing:
/// - State reset via DISCARD ALL
/// - Health checking
/// - Query/error extraction (delegating to existing MessageExtractor)
use super::traits::{AsyncStream, Protocol};
use super::MessageExtractor;
use anyhow::{Context, Result};
use async_trait::async_trait;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;
use tracing::{debug, warn};

/// PostgreSQL protocol handler
pub struct PostgresProtocol {
    extractor: MessageExtractor,
    reset_timeout_ms: u64,
}

impl PostgresProtocol {
    pub fn new() -> Self {
        Self {
            extractor: MessageExtractor::new(),
            reset_timeout_ms: 5000, // Default 5 seconds
        }
    }

    /// Configure the timeout for DISCARD ALL response
    pub fn with_reset_timeout(mut self, timeout_ms: u64) -> Self {
        self.reset_timeout_ms = timeout_ms;
        self
    }

    /// Read from stream until ReadyForQuery message is received
    ///
    /// Returns Ok(true) if ReadyForQuery with 'I' (idle) status received
    /// Returns Ok(false) if error response or unexpected state
    /// Returns Err if I/O error
    async fn read_until_ready_for_query(&self, stream: &mut dyn AsyncStream) -> Result<bool> {
        let mut buffer = Vec::with_capacity(4096);
        let mut temp = [0u8; 1024];

        loop {
            let n = stream.read(&mut temp).await.context("Failed to read response")?;

            if n == 0 {
                warn!("Connection closed while waiting for ReadyForQuery");
                return Ok(false);
            }

            buffer.extend_from_slice(&temp[..n]);

            // Check if we have a complete ReadyForQuery message
            if let Some(status) = self.extractor.extract_ready_for_query(&buffer) {
                match status {
                    b'I' => return Ok(true), // Idle - success
                    b'E' => {
                        warn!("Connection in error state after DISCARD ALL");
                        return Ok(false);
                    }
                    b'T' => {
                        // Still in transaction? This shouldn't happen after DISCARD ALL
                        warn!("Unexpected transaction state after DISCARD ALL");
                        return Ok(false);
                    }
                    _ => {
                        warn!(status = status, "Unknown ReadyForQuery status");
                        return Ok(false);
                    }
                }
            }

            // Check for error response before ReadyForQuery
            if let Some(error) = self.extractor.extract_error(&buffer) {
                warn!(error = %error, "Error response during DISCARD ALL");
                // Continue reading - error will be followed by ReadyForQuery
            }

            // Safety: prevent unbounded buffer growth
            if buffer.len() > 64 * 1024 {
                warn!("Response buffer exceeded 64KB without ReadyForQuery");
                return Ok(false);
            }
        }
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

    async fn reset_connection(&self, stream: &mut dyn AsyncStream) -> Result<bool> {
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
        stream.write_all(&message).await.context("Failed to send DISCARD ALL command")?;
        stream.flush().await.context("Failed to flush DISCARD ALL command")?;

        // Read response with timeout, loop until ReadyForQuery received
        let reset_timeout = Duration::from_millis(self.reset_timeout_ms);

        match timeout(reset_timeout, self.read_until_ready_for_query(stream)).await {
            Ok(Ok(true)) => {
                debug!("DISCARD ALL completed successfully");
                Ok(true)
            }
            Ok(Ok(false)) => {
                warn!("DISCARD ALL failed or returned error");
                Ok(false)
            }
            Ok(Err(e)) => {
                warn!(error = %e, "Error reading DISCARD ALL response");
                Ok(false)
            }
            Err(_) => {
                warn!("Timeout waiting for DISCARD ALL response");
                Ok(false)
            }
        }
    }

    async fn health_check(&self, stream: &mut dyn AsyncStream) -> Result<bool> {
        // For generic streams, we need an active health check since we can't
        // use try_read (which is TcpStream-specific).
        // Send an empty query (;) which is fast and returns ReadyForQuery.

        debug!("Performing active health check with empty query");

        // Construct Query message for ";" (empty query)
        let query = b";\0";
        let message_length = (4 + query.len()) as i32;

        let mut message = Vec::with_capacity(1 + 4 + query.len());
        message.push(b'Q');
        message.extend_from_slice(&message_length.to_be_bytes());
        message.extend_from_slice(query);

        // Send with short timeout
        let health_timeout = Duration::from_millis(1000);

        match timeout(health_timeout, async {
            stream.write_all(&message).await?;
            stream.flush().await?;
            self.read_until_ready_for_query(stream).await
        })
        .await
        {
            Ok(Ok(true)) => {
                debug!("Health check passed");
                Ok(true)
            }
            Ok(Ok(false)) => {
                warn!("Health check failed - connection in bad state");
                Ok(false)
            }
            Ok(Err(e)) => {
                warn!(error = %e, "Health check I/O error");
                Ok(false)
            }
            Err(_) => {
                warn!("Health check timed out");
                Ok(false)
            }
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
    fn test_default_reset_timeout() {
        let proto = PostgresProtocol::new();
        assert_eq!(proto.reset_timeout_ms, 5000); // Default 5 seconds
    }

    #[test]
    fn test_with_reset_timeout() {
        let proto = PostgresProtocol::new().with_reset_timeout(10000);
        assert_eq!(proto.reset_timeout_ms, 10000);
    }

    #[test]
    fn test_with_reset_timeout_zero() {
        // Edge case: zero timeout should be allowed
        let proto = PostgresProtocol::new().with_reset_timeout(0);
        assert_eq!(proto.reset_timeout_ms, 0);
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

    #[tokio::test]
    async fn test_reset_connection_with_duplex_stream() {
        use tokio::io::duplex;

        let proto = PostgresProtocol::new().with_reset_timeout(100);
        let (client, mut server) = duplex(1024);
        let mut client = Box::new(client) as Box<dyn AsyncStream>;

        // Spawn server that responds to DISCARD ALL
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            // Read the DISCARD ALL command
            let mut buf = [0u8; 100];
            let n = server.read(&mut buf).await.unwrap();
            assert!(n > 0, "Should receive DISCARD ALL command");

            // Verify it's a Query message with DISCARD ALL
            assert_eq!(buf[0], b'Q', "Should be Query message");

            // Send CommandComplete + ReadyForQuery
            // CommandComplete: 'C' + len(16) + "DISCARD ALL\0"
            let cc: [u8; 17] = [
                b'C', 0, 0, 0, 16, b'D', b'I', b'S', b'C', b'A', b'R', b'D', b' ', b'A', b'L', b'L',
                0,
            ];
            server.write_all(&cc).await.unwrap();

            // ReadyForQuery: 'Z' + len(5) + 'I'
            let rfq: [u8; 6] = [b'Z', 0, 0, 0, 5, b'I'];
            server.write_all(&rfq).await.unwrap();
        });

        let result = proto.reset_connection(client.as_mut()).await.unwrap();
        assert!(result, "reset_connection should succeed with mock server");
    }

    #[tokio::test]
    async fn test_health_check_with_duplex_stream() {
        use tokio::io::duplex;

        let proto = PostgresProtocol::new();
        let (client, mut server) = duplex(1024);
        let mut client = Box::new(client) as Box<dyn AsyncStream>;

        // Spawn server that responds to empty query health check
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            // Read the empty query command
            let mut buf = [0u8; 100];
            let n = server.read(&mut buf).await.unwrap();
            assert!(n > 0, "Should receive health check query");

            // Verify it's a Query message with ";"
            assert_eq!(buf[0], b'Q', "Should be Query message");

            // Send EmptyQueryResponse + ReadyForQuery
            // EmptyQueryResponse: 'I' + len(4)
            let eq: [u8; 5] = [b'I', 0, 0, 0, 4];
            server.write_all(&eq).await.unwrap();

            // ReadyForQuery: 'Z' + len(5) + 'I'
            let rfq: [u8; 6] = [b'Z', 0, 0, 0, 5, b'I'];
            server.write_all(&rfq).await.unwrap();
        });

        let result = proto.health_check(client.as_mut()).await.unwrap();
        assert!(result, "health_check should succeed with mock server");
    }
}
