//! PostgreSQL startup message reading utilities
//!
//! Provides helpers for reading complete startup messages from streams,
//! handling TCP fragmentation and large messages correctly.

use anyhow::{Context, Result};
use tokio::io::AsyncReadExt;
use tracing::debug;

/// Maximum allowed startup message size (64KB)
///
/// PostgreSQL doesn't specify a hard limit, but practical limits suggest
/// 64KB is more than sufficient for any reasonable connection parameters.
/// This prevents memory exhaustion from malformed length fields.
pub const MAX_STARTUP_MESSAGE_SIZE: usize = 65536;

/// Minimum valid startup message size
///
/// A startup message must have at least:
/// - 4 bytes: length field
/// - 4 bytes: protocol version
pub const MIN_STARTUP_MESSAGE_SIZE: usize = 8;

/// Read a complete PostgreSQL startup message from a stream
///
/// This function properly handles:
/// - TCP fragmentation (multiple reads to get complete message)
/// - Large startup messages (>8192 bytes)
/// - Malformed messages (invalid length, too small, too large)
///
/// # Protocol Format
///
/// ```text
/// | Length (4 bytes) | Protocol Version (4 bytes) | Parameters... | \0 |
/// ```
///
/// The length field is a big-endian i32 that includes itself (so minimum is 8).
///
/// # Errors
///
/// Returns an error if:
/// - Stream is closed before complete message is read
/// - Length field is < 8 (minimum valid startup message)
/// - Length field exceeds MAX_STARTUP_MESSAGE_SIZE
/// - Read times out (if timeout is applied by caller)
pub async fn read_startup_message<S>(stream: &mut S) -> Result<Vec<u8>>
where
    S: AsyncReadExt + Unpin,
{
    // Step 1: Read the 4-byte length prefix
    let mut length_buf = [0u8; 4];
    stream
        .read_exact(&mut length_buf)
        .await
        .context("Failed to read startup message length")?;

    let length = i32::from_be_bytes(length_buf) as usize;

    // Step 2: Validate the length
    if length < MIN_STARTUP_MESSAGE_SIZE {
        anyhow::bail!(
            "Invalid startup message length: {} (minimum is {})",
            length,
            MIN_STARTUP_MESSAGE_SIZE
        );
    }

    if length > MAX_STARTUP_MESSAGE_SIZE {
        anyhow::bail!(
            "Startup message too large: {} bytes (maximum is {})",
            length,
            MAX_STARTUP_MESSAGE_SIZE
        );
    }

    debug!(length = length, "Reading startup message");

    // Step 3: Allocate buffer for the complete message
    let mut buffer = vec![0u8; length];

    // Copy the length bytes we already read
    buffer[0..4].copy_from_slice(&length_buf);

    // Step 4: Read the remaining bytes
    let remaining = length - 4;
    stream
        .read_exact(&mut buffer[4..])
        .await
        .context("Failed to read startup message body")?;

    debug!(
        length = length,
        remaining_read = remaining,
        "Startup message read complete"
    );

    Ok(buffer)
}

/// Check if a message is an SSL request
///
/// SSL request is exactly 8 bytes:
/// - Length: 8 (i32 big-endian)
/// - Request code: 80877103 (i32 big-endian)
pub fn is_ssl_request(data: &[u8]) -> bool {
    if data.len() < 8 {
        return false;
    }

    let length = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    if length != 8 {
        return false;
    }

    let code = i32::from_be_bytes([data[4], data[5], data[6], data[7]]);
    code == 80877103 // SSL request code
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[tokio::test]
    async fn test_read_startup_message_basic() {
        // Build a minimal startup message
        let mut msg = Vec::new();
        let length: i32 = 8; // Just length + protocol version
        msg.extend_from_slice(&length.to_be_bytes());
        msg.extend_from_slice(&196608i32.to_be_bytes()); // Protocol 3.0

        let mut cursor = Cursor::new(msg.clone());
        let result = read_startup_message(&mut cursor).await.unwrap();
        assert_eq!(result, msg);
    }

    #[tokio::test]
    async fn test_read_startup_message_with_params() {
        // Build a startup message with parameters
        let params = b"user\0testuser\0database\0testdb\0\0";
        let length: i32 = 8 + params.len() as i32;

        let mut msg = Vec::new();
        msg.extend_from_slice(&length.to_be_bytes());
        msg.extend_from_slice(&196608i32.to_be_bytes()); // Protocol 3.0
        msg.extend_from_slice(params);

        let mut cursor = Cursor::new(msg.clone());
        let result = read_startup_message(&mut cursor).await.unwrap();
        assert_eq!(result, msg);
    }

    #[tokio::test]
    async fn test_read_startup_message_too_small() {
        // Length field says 4 bytes (less than minimum 8)
        let msg = vec![0, 0, 0, 4];
        let mut cursor = Cursor::new(msg);
        let result = read_startup_message(&mut cursor).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("minimum is 8"));
    }

    #[tokio::test]
    async fn test_read_startup_message_too_large() {
        // Length field exceeds maximum
        let length: i32 = (MAX_STARTUP_MESSAGE_SIZE + 1) as i32;
        let msg = length.to_be_bytes().to_vec();
        let mut cursor = Cursor::new(msg);
        let result = read_startup_message(&mut cursor).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too large"));
    }

    #[tokio::test]
    async fn test_read_startup_message_empty_stream() {
        let msg: Vec<u8> = vec![];
        let mut cursor = Cursor::new(msg);
        let result = read_startup_message(&mut cursor).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_read_startup_message_truncated() {
        // Length says 100 but only provide 50 bytes
        let length: i32 = 100;
        let mut msg = Vec::new();
        msg.extend_from_slice(&length.to_be_bytes());
        msg.extend_from_slice(&[0u8; 46]); // Only 46 more bytes, need 96

        let mut cursor = Cursor::new(msg);
        let result = read_startup_message(&mut cursor).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_is_ssl_request_valid() {
        let ssl_req: [u8; 8] = [0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f];
        assert!(is_ssl_request(&ssl_req));
    }

    #[test]
    fn test_is_ssl_request_normal_startup() {
        // Protocol version 3.0 startup (not SSL)
        let startup: [u8; 8] = [0, 0, 0, 8, 0, 3, 0, 0];
        assert!(!is_ssl_request(&startup));
    }

    #[test]
    fn test_is_ssl_request_too_short() {
        let short: [u8; 4] = [0, 0, 0, 8];
        assert!(!is_ssl_request(&short));
    }
}
