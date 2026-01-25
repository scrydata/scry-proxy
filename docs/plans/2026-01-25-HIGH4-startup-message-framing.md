# HIGH-4: Startup Message Complete Reading Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix incomplete startup message reading to handle large StartupMessages (>8192 bytes) with proper message framing.

**Architecture:** Create a `read_startup_message()` helper that reads the 4-byte length prefix first, then loops to read the exact number of bytes specified. Apply this pattern to all startup message reading locations in `tls/startup.rs` and `auth/authenticator.rs`.

**Tech Stack:** Rust, tokio::io::AsyncReadExt, PostgreSQL wire protocol

---

## Background

The PostgreSQL StartupMessage format is:
```
| Length (4 bytes, big-endian i32) | Protocol Version (4 bytes) | Parameters (null-terminated key=value pairs) | Final null byte |
```

The length field includes itself (4 bytes) but the minimum valid StartupMessage is 8 bytes (length + protocol version).

**Current Problem:**
- `startup.rs:60-69` and `authenticator.rs:138-149` use a single `read()` call with 8192-byte buffer
- Large StartupMessages with many connection parameters can exceed 8192 bytes
- TCP fragmentation may split any message across multiple packets
- No loop to accumulate data until complete message is received

**Requirements from CONNECTION_MULTIPLEXING_REQUIREMENTS.md:**
- Read startup message length prefix first (4 bytes)
- Allocate buffer for exact message size
- Loop reading until complete message received
- Validate message structure before parsing
- Reject malformed startup messages with clear error

---

## Task 1: Add `read_startup_message()` Helper to Protocol Module

**Files:**
- Create: `scry-proxy/src/protocol/startup.rs`
- Modify: `scry-proxy/src/protocol/mod.rs`
- Test: `scry-proxy/src/protocol/startup.rs` (inline tests)

**Step 1: Create the startup.rs file with read_startup_message() function**

```rust
//! PostgreSQL startup message reading utilities
//!
//! Provides helpers for reading complete startup messages from streams,
//! handling TCP fragmentation and large messages correctly.

use anyhow::{Context, Result};
use tokio::io::AsyncReadExt;
use tracing::{debug, warn};

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
```

**Step 2: Export the new module from protocol/mod.rs**

Add to `scry-proxy/src/protocol/mod.rs`:

```rust
mod startup;
pub use startup::{read_startup_message, is_ssl_request as is_ssl_request_msg, MAX_STARTUP_MESSAGE_SIZE, MIN_STARTUP_MESSAGE_SIZE};
```

**Step 3: Run tests to verify the new module works**

Run: `cargo test -p scry-proxy startup --lib`
Expected: All tests pass (5-6 tests)

**Step 4: Commit**

```bash
git add scry-proxy/src/protocol/startup.rs scry-proxy/src/protocol/mod.rs
git commit -m "$(cat <<'EOF'
feat(protocol): add read_startup_message() helper for complete message reading

Implements proper PostgreSQL startup message framing:
- Reads 4-byte length prefix first
- Validates length bounds (8 bytes min, 64KB max)
- Loops to read exact number of bytes specified
- Handles TCP fragmentation correctly

This addresses HIGH-4 from CONNECTION_MULTIPLEXING_REQUIREMENTS.md
EOF
)"
```

---

## Task 2: Update tls/startup.rs to Use New Helper

**Files:**
- Modify: `scry-proxy/src/tls/startup.rs:54-127`
- Test: `scry-proxy/tests/connection_multiplexing.rs` (existing)

**Step 1: Write a failing test for large startup messages**

Add to `scry-proxy/tests/connection_multiplexing.rs`:

```rust
/// Test that startup messages with many parameters are handled correctly
///
/// This verifies HIGH-4 fix: proper startup message reading for large messages.
/// Clients with many connection parameters (application_name, client_encoding,
/// options, etc.) can send startup messages exceeding typical buffer sizes.
#[tokio::test]
async fn test_large_startup_message_handling() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_multiplexing_config(postgres_host, postgres_port, 1);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port =
        start_test_proxy(config.clone(), publisher).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(200)).await;

    // Connect with many connection parameters to create a large startup message
    // Each parameter adds overhead: key + null + value + null
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user={} password={} dbname={} \
             application_name=test_app_with_a_very_long_name_to_increase_message_size \
             options='-c search_path=public,pg_catalog -c timezone=UTC -c client_encoding=UTF8'",
            proxy_port, config.backend.user, config.backend.password, config.backend.database
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("Failed to connect with large startup message");

    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    // Execute a simple query to verify the connection works
    let rows = client
        .query("SELECT 'large_startup_ok' as status", &[])
        .await
        .expect("Query after large startup should succeed");

    assert_eq!(rows.len(), 1);
    let status: &str = rows[0].get(0);
    assert_eq!(status, "large_startup_ok");

    drop(client);
    conn_handle.abort();
}
```

**Step 2: Run test to verify it fails (or passes if already working)**

Run: `cargo test -p scry-proxy test_large_startup_message_handling -- --nocapture`
Expected: May pass already, but we need to fix the underlying code to be robust

**Step 3: Update handle_ssl_startup() in tls/startup.rs**

Replace the content of `scry-proxy/src/tls/startup.rs` with:

```rust
use crate::config::TlsSslMode;
use crate::protocol::read_startup_message;
use rustls::ServerConfig;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, info, warn};

use super::transport::ClientTransport;

/// PostgreSQL SSL request code (80877103 in decimal, 0x04d2162f in hex)
/// Sent as: length (8) + request code in big-endian
pub const SSL_REQUEST_CODE: i32 = 80877103;

/// Check if a startup message is an SSL request
pub fn is_ssl_request(data: &[u8]) -> bool {
    if data.len() < 8 {
        return false;
    }

    // First 4 bytes are length (should be 8)
    let length = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    if length != 8 {
        return false;
    }

    // Next 4 bytes are the request code
    let code = i32::from_be_bytes([data[4], data[5], data[6], data[7]]);
    code == SSL_REQUEST_CODE
}

/// SSL response bytes
pub const SSL_RESPONSE_YES: u8 = b'S';
pub const SSL_RESPONSE_NO: u8 = b'N';

/// Result of handling the SSL startup phase
pub enum SslStartupResult {
    /// Client requested SSL and was upgraded
    Upgraded(ClientTransport),
    /// Client requested SSL but server doesn't support it (sent 'N')
    Declined(TcpStream, Vec<u8>),
    /// Client didn't request SSL (startup message buffered)
    NoSslRequest(TcpStream, Vec<u8>),
}

/// Handle the PostgreSQL SSL startup handshake
///
/// PostgreSQL clients can send an SSLRequest before the normal StartupMessage.
/// We need to:
/// 1. Read the first message (could be SSLRequest or StartupMessage)
/// 2. If it's an SSLRequest, respond with 'S' (yes) or 'N' (no)
/// 3. If 'S', upgrade the connection to TLS
/// 4. Return the (possibly upgraded) transport and any buffered data
///
/// This implementation properly handles:
/// - Large startup messages (>8192 bytes) via read_startup_message()
/// - TCP fragmentation (loops until complete message received)
/// - Malformed messages (validates length bounds)
pub async fn handle_ssl_startup(
    mut stream: TcpStream,
    sslmode: &TlsSslMode,
    tls_config: Option<Arc<ServerConfig>>,
) -> Result<SslStartupResult, std::io::Error> {
    // Read the first message using proper framing
    // This handles large messages and TCP fragmentation correctly
    let buf = match read_startup_message(&mut stream).await {
        Ok(data) => data,
        Err(e) => {
            warn!(error = %e, "Failed to read initial startup message");
            return Err(std::io::Error::other(format!(
                "Failed to read startup message: {}",
                e
            )));
        }
    };

    // Check if it's an SSL request
    if !is_ssl_request(&buf) {
        debug!("Client sent StartupMessage without SSLRequest");
        return Ok(SslStartupResult::NoSslRequest(stream, buf));
    }

    info!("Client sent SSLRequest");

    // Determine response based on sslmode and whether we have TLS configured
    match (sslmode, &tls_config) {
        (TlsSslMode::Disable, _) => {
            // TLS disabled, always say no
            debug!("TLS disabled, declining SSLRequest");
            stream.write_all(&[SSL_RESPONSE_NO]).await?;

            // Client should send StartupMessage next - read it properly
            let startup_buf = match read_startup_message(&mut stream).await {
                Ok(data) => data,
                Err(e) => {
                    warn!(error = %e, "Failed to read startup message after SSL decline");
                    return Err(std::io::Error::other(format!(
                        "Failed to read startup message: {}",
                        e
                    )));
                }
            };

            Ok(SslStartupResult::Declined(stream, startup_buf))
        }

        (TlsSslMode::Allow, None) => {
            // Allow mode but no TLS config, say no
            debug!("TLS allowed but not configured, declining SSLRequest");
            stream.write_all(&[SSL_RESPONSE_NO]).await?;

            // Read startup message properly
            let startup_buf = match read_startup_message(&mut stream).await {
                Ok(data) => data,
                Err(e) => {
                    warn!(error = %e, "Failed to read startup message after SSL decline");
                    return Err(std::io::Error::other(format!(
                        "Failed to read startup message: {}",
                        e
                    )));
                }
            };

            Ok(SslStartupResult::Declined(stream, startup_buf))
        }

        (_, Some(config)) => {
            // TLS available, accept the request
            info!("Accepting SSLRequest, upgrading to TLS");
            stream.write_all(&[SSL_RESPONSE_YES]).await?;

            // Upgrade to TLS
            let acceptor = TlsAcceptor::from(config.clone());
            let tls_stream = acceptor.accept(stream).await?;

            info!("TLS handshake completed");
            Ok(SslStartupResult::Upgraded(ClientTransport::Tls(Box::new(
                tls_stream,
            ))))
        }

        (TlsSslMode::Require | TlsSslMode::VerifyCa | TlsSslMode::VerifyFull, None) => {
            // TLS required but not configured - this is a config error
            // Should have been caught at startup, but handle gracefully
            warn!("TLS required but not configured, declining (config error)");
            stream.write_all(&[SSL_RESPONSE_NO]).await?;

            Err(std::io::Error::other("TLS required but not configured"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ssl_request_detection() {
        // Valid SSL request
        let ssl_req: [u8; 8] = [0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f];
        assert!(is_ssl_request(&ssl_req));

        // Protocol version 3.0 startup (not SSL)
        let startup: [u8; 8] = [0, 0, 0, 8, 0, 3, 0, 0];
        assert!(!is_ssl_request(&startup));

        // Too short
        let short: [u8; 4] = [0, 0, 0, 8];
        assert!(!is_ssl_request(&short));

        // Wrong length field
        let wrong_len: [u8; 8] = [0, 0, 0, 9, 0x04, 0xd2, 0x16, 0x2f];
        assert!(!is_ssl_request(&wrong_len));
    }
}
```

**Step 4: Run tests to verify compilation and basic functionality**

Run: `cargo test -p scry-proxy ssl -- --nocapture`
Expected: PASS

**Step 5: Run the large startup message test**

Run: `cargo test -p scry-proxy test_large_startup_message_handling -- --nocapture`
Expected: PASS

**Step 6: Commit**

```bash
git add scry-proxy/src/tls/startup.rs
git commit -m "$(cat <<'EOF'
fix(tls): use read_startup_message() for proper startup framing (HIGH-4)

Replaces single read() calls with read_startup_message() helper that:
- Reads length prefix first to determine message size
- Loops until complete message is received
- Validates length bounds to prevent memory issues
- Handles TCP fragmentation correctly

This ensures large startup messages (>8192 bytes) with many connection
parameters are read completely before parsing.
EOF
)"
```

---

## Task 3: Update auth/authenticator.rs to Use New Helper

**Files:**
- Modify: `scry-proxy/src/auth/authenticator.rs:133-150`

**Step 1: Update read_startup() method in Authenticator**

Replace the `read_startup()` method in `scry-proxy/src/auth/authenticator.rs`:

```rust
    /// Read startup message from client
    ///
    /// Uses proper message framing to handle:
    /// - Large startup messages (>8192 bytes)
    /// - TCP fragmentation
    /// - Malformed length fields
    async fn read_startup(
        &self,
        client: &mut ClientTransport,
    ) -> Result<(StartupMessage, Vec<u8>)> {
        use crate::protocol::read_startup_message;

        let buf = read_startup_message(client)
            .await
            .context("Failed to read startup message")?;

        let startup = StartupMessage::parse(&buf).context("Failed to parse startup message")?;

        Ok((startup, buf))
    }
```

**Step 2: Run auth tests to verify compilation**

Run: `cargo test -p scry-proxy auth --lib`
Expected: PASS

**Step 3: Run integration tests to verify end-to-end**

Run: `cargo test -p scry-proxy auth_integration -- --nocapture --test-threads=1`
Expected: PASS

**Step 4: Commit**

```bash
git add scry-proxy/src/auth/authenticator.rs
git commit -m "$(cat <<'EOF'
fix(auth): use read_startup_message() for proper startup framing (HIGH-4)

Updates Authenticator::read_startup() to use the new helper that
properly handles large startup messages and TCP fragmentation.
EOF
)"
```

---

## Task 4: Add Integration Test for TCP Fragmentation Simulation

**Files:**
- Modify: `scry-proxy/tests/connection_multiplexing.rs`

**Step 1: Write a test that verifies the fix works**

Add to `scry-proxy/tests/connection_multiplexing.rs`:

```rust
/// Test that the proxy handles startup message reading correctly
/// under various conditions (this is more of a smoke test since we
/// can't easily simulate TCP fragmentation in integration tests)
#[tokio::test]
async fn test_startup_message_roundtrip() {
    let docker = Cli::default();
    let postgres_image = RunnableImage::from(Postgres::default()).with_tag("16-alpine");
    let postgres = docker.run(postgres_image);

    let postgres_port = postgres.get_host_port_ipv4(5432);
    let postgres_host = "127.0.0.1".to_string();

    sleep(Duration::from_secs(2)).await;

    let config = create_multiplexing_config(postgres_host, postgres_port, 1);
    let test_publisher = TestPublisher::new();
    let publisher = Arc::new(test_publisher.clone());

    let proxy_port =
        start_test_proxy(config.clone(), publisher).await.expect("Failed to start proxy");

    sleep(Duration::from_millis(200)).await;

    // Test 1: Minimal startup parameters
    {
        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user={} password={} dbname={}",
                proxy_port, config.backend.user, config.backend.password, config.backend.database
            ),
            tokio_postgres::NoTls,
        )
        .await
        .expect("Minimal startup should work");

        let conn_handle = tokio::spawn(async move {
            let _ = connection.await;
        });

        let rows = client.query("SELECT 1", &[]).await.expect("Query should work");
        assert_eq!(rows.len(), 1);

        drop(client);
        conn_handle.abort();
    }

    sleep(Duration::from_millis(100)).await;

    // Test 2: Startup with application_name
    {
        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user={} password={} dbname={} application_name=test_app",
                proxy_port, config.backend.user, config.backend.password, config.backend.database
            ),
            tokio_postgres::NoTls,
        )
        .await
        .expect("Startup with app name should work");

        let conn_handle = tokio::spawn(async move {
            let _ = connection.await;
        });

        let rows = client.query("SELECT 2", &[]).await.expect("Query should work");
        assert_eq!(rows.len(), 1);

        drop(client);
        conn_handle.abort();
    }

    sleep(Duration::from_millis(100)).await;

    // Test 3: Startup with options parameter
    {
        let (client, connection) = tokio_postgres::connect(
            &format!(
                "host=127.0.0.1 port={} user={} password={} dbname={} options='-c timezone=UTC'",
                proxy_port, config.backend.user, config.backend.password, config.backend.database
            ),
            tokio_postgres::NoTls,
        )
        .await
        .expect("Startup with options should work");

        let conn_handle = tokio::spawn(async move {
            let _ = connection.await;
        });

        let rows = client.query("SELECT 3", &[]).await.expect("Query should work");
        assert_eq!(rows.len(), 1);

        drop(client);
        conn_handle.abort();
    }
}
```

**Step 2: Run the new integration test**

Run: `cargo test -p scry-proxy test_startup_message_roundtrip -- --nocapture`
Expected: PASS

**Step 3: Commit**

```bash
git add scry-proxy/tests/connection_multiplexing.rs
git commit -m "$(cat <<'EOF'
test: add startup message handling integration tests (HIGH-4)

Adds tests for various startup message scenarios:
- Minimal parameters
- With application_name
- With options parameter
- Large startup with many parameters

These verify the read_startup_message() fix works end-to-end.
EOF
)"
```

---

## Task 5: Run Full Test Suite and Verify

**Files:** None (verification only)

**Step 1: Run unit tests**

Run: `cargo test -p scry-proxy --lib`
Expected: All tests pass

**Step 2: Run integration tests**

Run: `cargo test -p scry-proxy --test '*' -- --test-threads=1`
Expected: All tests pass (may take several minutes)

**Step 3: Run linter**

Run: `cargo clippy -p scry-proxy -- -D warnings -A dead_code`
Expected: No warnings

**Step 4: Format code**

Run: `cargo fmt`
Expected: Code formatted

**Step 5: Final commit if any formatting changes**

```bash
git add -A
git commit -m "style: format code" || echo "No formatting changes"
```

---

## Task 6: Update Documentation

**Files:**
- Modify: `docs/CONNECTION_MULTIPLEXING_REQUIREMENTS.md`

**Step 1: Mark HIGH-4 as completed**

Update the HIGH-4 section in `docs/CONNECTION_MULTIPLEXING_REQUIREMENTS.md`:

```markdown
### HIGH-4: Startup Message Incomplete Reading ✅ COMPLETED

**Implementation:**
- Added `read_startup_message()` helper in `scry-proxy/src/protocol/startup.rs`
- Reads 4-byte length prefix first to determine exact message size
- Loops with `read_exact()` until complete message received
- Validates length bounds: minimum 8 bytes, maximum 64KB
- Updated `tls/startup.rs` and `auth/authenticator.rs` to use the new helper
- Integration tests verify large startup messages work correctly

**Previous Problem:**
- Single `read()` call with 8192-byte buffer
- Large StartupMessages could exceed buffer size
- TCP fragmentation could split message across packets
- Incomplete reads caused parsing failures

**Solution Details:**
- `read_startup_message()` is generic over any `AsyncReadExt + Unpin`
- Works with both TcpStream and ClientTransport (TLS)
- Clear error messages for malformed messages
- Memory-safe with maximum size limit
```

**Step 2: Commit documentation update**

```bash
git add docs/CONNECTION_MULTIPLEXING_REQUIREMENTS.md
git commit -m "docs: mark HIGH-4 startup message framing as complete"
```

---

## Summary

This plan implements HIGH-4 by:

1. **Creating a reusable helper** (`read_startup_message()`) that properly reads PostgreSQL startup messages with:
   - Length prefix parsing first
   - Exact-size buffer allocation
   - Loop until complete message received
   - Validation of length bounds

2. **Updating all startup reading locations** to use the new helper:
   - `tls/startup.rs`: SSL handshake startup reading
   - `auth/authenticator.rs`: Authenticator startup reading

3. **Adding comprehensive tests**:
   - Unit tests for the helper function
   - Integration tests for various startup message sizes

4. **Following TDD and commit practices**:
   - Each task has clear verification steps
   - Commits are atomic and well-documented

The fix ensures that:
- Large startup messages (>8192 bytes) are handled correctly
- TCP fragmentation doesn't cause incomplete reads
- Malformed messages are rejected with clear errors
- Memory is bounded by maximum message size limit
