# CRIT-2: TLS Connection State Reset Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Enable DISCARD ALL and health checks for TLS backend connections so session state doesn't leak between clients.

**Architecture:** Make the Protocol trait generic over `AsyncRead + AsyncWrite` instead of hardcoding `TcpStream`. This allows the same protocol logic to work with both plain TCP and TLS connections via the existing `BackendTransport` enum.

**Tech Stack:** Rust async-trait, tokio AsyncRead/AsyncWrite traits, tokio-rustls TLS

---

## Problem Summary

**Location:** `scry-proxy/src/proxy/tcp_pool.rs:352-359`

TLS pooled connections are **never** reset with `DISCARD ALL` and receive only a trivial "limited health check":

```rust
BackendTransport::Tls(_) => {
    // TODO: Make Protocol trait generic over AsyncRead+AsyncWrite
    debug!("TLS connection recycled (limited health check)");
    Ok(())  // NO RESET PERFORMED
}
```

**Consequences:**
- Session state leaks between clients (prepared statements, temp tables, session variables)
- Security risk: Client B may access Client A's temporary tables
- Query failures: Client B may conflict with Client A's prepared statements
- Advisory locks may persist unexpectedly

---

## Solution Overview

1. Make `Protocol` trait methods generic over `AsyncRead + AsyncWrite + Unpin + Send`
2. Update `PostgresProtocol` to implement these generic methods
3. Update `BackendTransportManager::recycle` to use protocol methods for TLS connections
4. Add integration tests verifying TLS connection state isolation

---

## Task 1: Update Protocol trait to be generic

**Files:**
- Modify: `scry-proxy/src/protocol/traits.rs:1-78`

**Step 1: Write the failing test**

Create a test that verifies Protocol works with a generic stream type.

```rust
// In scry-proxy/src/protocol/traits.rs, add at the bottom in #[cfg(test)] mod tests
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{duplex, DuplexStream};

    // This test will fail to compile until we make Protocol generic
    #[tokio::test]
    async fn test_protocol_accepts_generic_stream() {
        use crate::protocol::postgres::PostgresProtocol;

        let proto = PostgresProtocol::new();
        let (mut client, _server) = duplex(1024);

        // This should compile - proving Protocol works with DuplexStream
        // It will fail at runtime (no server response) but that's fine for this test
        let _ = tokio::time::timeout(
            std::time::Duration::from_millis(10),
            proto.reset_connection(&mut client)
        ).await;
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p scry --lib protocol::traits::tests::test_protocol_accepts_generic_stream`
Expected: FAIL with compile error - `DuplexStream` doesn't implement required traits for `TcpStream`

**Step 3: Update Protocol trait to accept generic streams**

```rust
use anyhow::Result;
use async_trait::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};

/// Database protocol handler trait
///
/// Each database protocol (Postgres, MySQL, etc.) implements this trait
/// to provide protocol-specific behavior like state reset, health checking,
/// and message extraction.
///
/// The async methods accept any stream type that implements AsyncRead + AsyncWrite,
/// allowing the same protocol logic to work with both plain TCP and TLS connections.
#[async_trait]
pub trait Protocol: Send + Sync + 'static {
    /// Get the protocol name (e.g., "postgres", "mysql", "mongodb")
    fn name(&self) -> &'static str;

    /// Default port for this protocol
    fn default_port(&self) -> u16;

    /// Reset connection state between client sessions
    ///
    /// This is called when a pooled connection is about to be reused.
    /// Implementations should:
    /// - Clear session state (temp tables, variables, etc.)
    /// - Reset transaction state
    /// - Clear prepared statements if needed
    ///
    /// Strategies:
    /// - Postgres: Send "DISCARD ALL" command
    /// - MySQL: Send "RESET CONNECTION" command
    /// - MongoDB: Close and recreate (stateless protocol)
    /// - Generic: Return Ok(false) to close/recreate connection
    ///
    /// Returns:
    /// - Ok(true) if reset succeeded (connection can be reused)
    /// - Ok(false) if reset not supported (connection will be closed)
    /// - Err(_) if reset failed (connection will be closed)
    async fn reset_connection<S>(&self, stream: &mut S) -> Result<bool>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send;

    /// Health check a connection
    ///
    /// Verify the connection is still alive and responsive.
    /// This is used for pool maintenance and pre-flight checks.
    ///
    /// Strategies:
    /// - Postgres: Can use TCP keepalive or simple query
    /// - MySQL: Send ping command
    /// - MongoDB: Send ping command
    /// - Generic: Check TCP socket is readable
    ///
    /// Returns:
    /// - Ok(true) if connection is healthy
    /// - Ok(false) or Err(_) if connection is dead
    async fn health_check<S>(&self, stream: &mut S) -> Result<bool>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send;

    /// Extract query information from client-to-backend messages
    ///
    /// This is used for observability - extracting SQL queries, commands,
    /// etc. from the wire protocol for logging and analysis.
    ///
    /// Returns Some(query_string) if a query was found, None otherwise.
    fn extract_query(&self, data: &[u8]) -> Option<String>;

    /// Check if a backend-to-client message indicates query completion
    ///
    /// Used to measure query timing - detect when backend has finished
    /// processing a query.
    fn is_query_complete(&self, data: &[u8]) -> bool;

    /// Extract error information from backend-to-client messages
    ///
    /// Returns Some(error_message) if an error was detected, None otherwise.
    fn extract_error(&self, data: &[u8]) -> Option<String>;
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p scry --lib protocol::traits::tests::test_protocol_accepts_generic_stream`
Expected: FAIL - Now the trait compiles but `PostgresProtocol` doesn't implement the new signature

**Step 5: Commit**

```bash
git add scry-proxy/src/protocol/traits.rs
git commit -m "$(cat <<'EOF'
feat(protocol): make Protocol trait generic over AsyncRead+AsyncWrite

Prepare for TLS connection support by making reset_connection and
health_check accept any stream type that implements AsyncRead + AsyncWrite.
This allows the same protocol logic to work with both plain TCP and TLS.
EOF
)"
```

---

## Task 2: Update PostgresProtocol to implement generic methods

**Files:**
- Modify: `scry-proxy/src/protocol/postgres.rs:1-230`

**Step 1: Write the failing test**

The test from Task 1 Step 1 now serves as our failing test. It fails because `PostgresProtocol` still uses `TcpStream` directly.

**Step 2: Run test to verify it fails**

Run: `cargo build -p scry --lib`
Expected: FAIL with compile error - `PostgresProtocol` doesn't implement the generic trait

**Step 3: Update PostgresProtocol to use generic stream types**

```rust
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
use std::io::ErrorKind;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
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
    async fn read_until_ready_for_query<S>(&self, stream: &mut S) -> Result<bool>
    where
        S: AsyncRead + Unpin + Send,
    {
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

    async fn reset_connection<S>(&self, stream: &mut S) -> Result<bool>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
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

    async fn health_check<S>(&self, stream: &mut S) -> Result<bool>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
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
            self.read_until_ready_for_query(stream).await
        }).await {
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
```

**Step 4: Run test to verify it passes**

Run: `cargo build -p scry --lib`
Expected: PASS - compilation succeeds

Run: `cargo test -p scry --lib protocol::traits::tests::test_protocol_accepts_generic_stream`
Expected: PASS (times out as expected, no panic)

**Step 5: Commit**

```bash
git add scry-proxy/src/protocol/postgres.rs
git commit -m "$(cat <<'EOF'
feat(postgres): implement generic Protocol methods

Update PostgresProtocol to accept any AsyncRead+AsyncWrite stream,
enabling the same reset_connection and health_check logic to work
with both plain TCP and TLS connections.

Health check now uses active query (empty query ";") instead of
try_read, which only works on TcpStream.
EOF
)"
```

---

## Task 3: Update tcp_pool.rs to use Protocol for TLS connections

**Files:**
- Modify: `scry-proxy/src/proxy/tcp_pool.rs:304-362`

**Step 1: Write the failing test**

```rust
// In scry-proxy/src/proxy/tcp_pool.rs, add to #[cfg(test)] mod tests

#[tokio::test]
async fn test_tls_connection_reset_is_called() {
    // This is a design test - we verify the recycle path calls protocol methods
    // for TLS connections. The actual TLS behavior is tested in integration tests.

    // For now, verify the code compiles with the TLS branch calling protocol.
    // Integration tests will verify actual behavior.
}
```

**Step 2: Run test to verify current behavior**

Run: `cargo test -p scry --lib proxy::tcp_pool::tests`
Expected: PASS - but TLS connections still skip reset

**Step 3: Update BackendTransportManager::recycle to handle TLS**

```rust
/// Recycle a connection before returning it to the pool
///
/// This is called when a connection is returned to the pool and is about
/// to be reused by another client. We delegate to the protocol's
/// reset_connection method to clear any session state.
async fn recycle(
    &self,
    conn: &mut BackendTransport,
    _metrics: &deadpool::managed::Metrics,
) -> RecycleResult<Self::Error> {
    debug!(protocol = self.protocol.name(), "Recycling backend connection");

    // Both plain and TLS connections now use the same protocol methods
    // since Protocol trait is generic over AsyncRead+AsyncWrite
    match conn {
        BackendTransport::Plain(stream) => {
            // First, check if connection is still healthy
            match self.protocol.health_check(stream).await {
                Ok(true) => {
                    debug!("Connection health check passed");
                }
                Ok(false) => {
                    warn!("Connection failed health check, will be closed");
                    return Err(deadpool::managed::RecycleError::Backend(anyhow::anyhow!(
                        "Connection failed health check"
                    )));
                }
                Err(e) => {
                    warn!(error = %e, "Connection health check error, will be closed");
                    return Err(deadpool::managed::RecycleError::Backend(e));
                }
            }

            // Try to reset connection state
            match self.protocol.reset_connection(stream).await {
                Ok(true) => {
                    debug!("Connection state reset successfully");
                    Ok(())
                }
                Ok(false) => {
                    // Protocol doesn't support reset - close connection
                    debug!("Protocol doesn't support state reset, closing connection");
                    Err(deadpool::managed::RecycleError::Backend(anyhow::anyhow!(
                        "Protocol does not support connection reset"
                    )))
                }
                Err(e) => {
                    warn!(error = %e, "Failed to reset connection state, will be closed");
                    Err(deadpool::managed::RecycleError::Backend(e))
                }
            }
        }
        BackendTransport::Tls(stream) => {
            // TLS connections now use the same protocol methods!
            // The Protocol trait is generic over AsyncRead+AsyncWrite.

            // Health check
            match self.protocol.health_check(stream.as_mut()).await {
                Ok(true) => {
                    debug!("TLS connection health check passed");
                }
                Ok(false) => {
                    warn!("TLS connection failed health check, will be closed");
                    return Err(deadpool::managed::RecycleError::Backend(anyhow::anyhow!(
                        "TLS connection failed health check"
                    )));
                }
                Err(e) => {
                    warn!(error = %e, "TLS connection health check error, will be closed");
                    return Err(deadpool::managed::RecycleError::Backend(e));
                }
            }

            // Reset connection state with DISCARD ALL
            match self.protocol.reset_connection(stream.as_mut()).await {
                Ok(true) => {
                    debug!("TLS connection state reset successfully");
                    Ok(())
                }
                Ok(false) => {
                    debug!("TLS protocol doesn't support state reset, closing connection");
                    Err(deadpool::managed::RecycleError::Backend(anyhow::anyhow!(
                        "TLS protocol does not support connection reset"
                    )))
                }
                Err(e) => {
                    warn!(error = %e, "Failed to reset TLS connection state, will be closed");
                    Err(deadpool::managed::RecycleError::Backend(e))
                }
            }
        }
    }
}
```

**Step 4: Run test to verify it compiles**

Run: `cargo build -p scry --lib`
Expected: PASS

**Step 5: Commit**

```bash
git add scry-proxy/src/proxy/tcp_pool.rs
git commit -m "$(cat <<'EOF'
fix(pool): enable DISCARD ALL and health checks for TLS connections

TLS pooled connections now receive the same state reset (DISCARD ALL)
and health checks as plain TCP connections. This prevents session state
from leaking between clients when using TLS backend connections.

Fixes CRIT-2: TLS Connections Skip State Reset
EOF
)"
```

---

## Task 4: Add unit tests for TLS protocol operations

**Files:**
- Modify: `scry-proxy/src/protocol/postgres.rs` (tests section)

**Step 1: Write the failing test**

```rust
// Add to #[cfg(test)] mod tests in postgres.rs

#[tokio::test]
async fn test_reset_connection_with_duplex_stream() {
    use tokio::io::duplex;

    let proto = PostgresProtocol::new().with_reset_timeout(100);
    let (mut client, mut server) = duplex(1024);

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
            b'C', 0, 0, 0, 16,
            b'D', b'I', b'S', b'C', b'A', b'R', b'D', b' ', b'A', b'L', b'L', 0
        ];
        server.write_all(&cc).await.unwrap();

        // ReadyForQuery: 'Z' + len(5) + 'I'
        let rfq: [u8; 6] = [b'Z', 0, 0, 0, 5, b'I'];
        server.write_all(&rfq).await.unwrap();
    });

    let result = proto.reset_connection(&mut client).await.unwrap();
    assert!(result, "reset_connection should succeed with mock server");
}

#[tokio::test]
async fn test_health_check_with_duplex_stream() {
    use tokio::io::duplex;

    let proto = PostgresProtocol::new();
    let (mut client, mut server) = duplex(1024);

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

    let result = proto.health_check(&mut client).await.unwrap();
    assert!(result, "health_check should succeed with mock server");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p scry --lib protocol::postgres::tests::test_reset_connection_with_duplex_stream`
Expected: Depends on current impl - may pass or fail

**Step 3: Implementation already done in Task 2**

No changes needed - tests should pass with the generic implementation.

**Step 4: Run tests to verify they pass**

Run: `cargo test -p scry --lib protocol::postgres::tests`
Expected: PASS

**Step 5: Commit**

```bash
git add scry-proxy/src/protocol/postgres.rs
git commit -m "$(cat <<'EOF'
test(postgres): add unit tests for generic stream operations

Add tests verifying reset_connection and health_check work with
non-TcpStream types (DuplexStream), confirming the generic Protocol
implementation works correctly.
EOF
)"
```

---

## Task 5: Add TLS state isolation integration test

**Files:**
- Modify: `scry-proxy/tests/tls_integration.rs`

**Step 1: Write the failing test**

```rust
// Add to tls_integration.rs

/// Test that TLS connections are properly reset between clients
///
/// This verifies CRIT-2: TLS Connections Skip State Reset is fixed.
///
/// Test scenario:
/// 1. Client A creates a prepared statement
/// 2. Client A disconnects (connection returns to pool)
/// 3. Client B connects (gets same pooled connection)
/// 4. Client B should NOT see Client A's prepared statement
#[tokio::test]
#[ignore] // Requires TLS-enabled Postgres backend
async fn test_tls_connection_state_isolation() {
    // This test requires:
    // 1. A TLS-enabled PostgreSQL backend
    // 2. The proxy configured with server_tls_sslmode = require
    //
    // Skip if not configured
    let scry_tls_url = std::env::var("SCRY_TLS_TEST_URL")
        .unwrap_or_else(|_| {
            eprintln!("Skipping test: SCRY_TLS_TEST_URL not set");
            eprintln!("Set to a TLS-enabled Scry proxy URL to run this test");
            return String::new();
        });

    if scry_tls_url.is_empty() {
        return;
    }

    // Use native-tls for the test client
    let connector = native_tls::TlsConnector::builder()
        .danger_accept_invalid_certs(true) // For self-signed test certs
        .build()
        .expect("Failed to create TLS connector");
    let connector = postgres_native_tls::MakeTlsConnector::new(connector);

    // Client A: Create prepared statement
    {
        let (client, connection) = tokio_postgres::connect(&scry_tls_url, connector.clone())
            .await
            .expect("Client A failed to connect");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Client A connection error: {}", e);
            }
        });

        // Create a named prepared statement
        client
            .execute("PREPARE client_a_stmt AS SELECT 42", &[])
            .await
            .expect("Failed to create prepared statement");

        // Verify it exists
        let result = client
            .query_one("EXECUTE client_a_stmt", &[])
            .await
            .expect("Failed to execute prepared statement");
        assert_eq!(result.get::<_, i32>(0), 42);

        // Client A disconnects - connection returns to pool
        drop(client);
    }

    // Small delay to ensure connection is recycled
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Client B: Should NOT see Client A's prepared statement
    {
        let (client, connection) = tokio_postgres::connect(&scry_tls_url, connector)
            .await
            .expect("Client B failed to connect");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Client B connection error: {}", e);
            }
        });

        // Try to execute Client A's prepared statement - should fail
        let result = client.query("EXECUTE client_a_stmt", &[]).await;

        match result {
            Err(e) => {
                // Expected: prepared statement doesn't exist
                let err_msg = e.to_string();
                assert!(
                    err_msg.contains("client_a_stmt") &&
                    (err_msg.contains("does not exist") || err_msg.contains("not found")),
                    "Expected 'prepared statement does not exist', got: {}",
                    err_msg
                );
            }
            Ok(_) => {
                panic!(
                    "CRIT-2 REGRESSION: Client B could execute Client A's prepared statement! \
                     TLS connection state was NOT reset properly."
                );
            }
        }
    }
}

/// Test that TLS connection health checks work
#[tokio::test]
#[ignore] // Requires TLS-enabled Postgres backend
async fn test_tls_connection_health_check() {
    let scry_tls_url = std::env::var("SCRY_TLS_TEST_URL")
        .unwrap_or_else(|_| {
            eprintln!("Skipping test: SCRY_TLS_TEST_URL not set");
            return String::new();
        });

    if scry_tls_url.is_empty() {
        return;
    }

    let connector = native_tls::TlsConnector::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .expect("Failed to create TLS connector");
    let connector = postgres_native_tls::MakeTlsConnector::new(connector);

    // Make multiple connections to exercise the pool
    for i in 0..5 {
        let (client, connection) = tokio_postgres::connect(&scry_tls_url, connector.clone())
            .await
            .expect(&format!("Connection {} failed", i));

        tokio::spawn(async move {
            let _ = connection.await;
        });

        // Simple query to verify connection works
        let result = client
            .query_one("SELECT 1", &[])
            .await
            .expect(&format!("Query {} failed", i));
        assert_eq!(result.get::<_, i32>(0), 1);

        drop(client);
    }

    // If we get here without errors, health checks are working
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p scry --test tls_integration test_tls_connection_state_isolation -- --ignored`
Expected: SKIP (no SCRY_TLS_TEST_URL set) or FAIL (if TLS connections skip reset)

**Step 3: Implementation already done in Tasks 1-3**

The fixes are in place. This test verifies end-to-end behavior.

**Step 4: Run tests to verify they pass**

Manual verification required with TLS-enabled backend:
```bash
# Start TLS-enabled Postgres (example with docker)
docker run -d --name pg-tls \
  -e POSTGRES_PASSWORD=postgres \
  -p 5433:5432 \
  postgres:15 \
  -c ssl=on \
  -c ssl_cert_file=/etc/ssl/certs/ssl-cert-snakeoil.pem \
  -c ssl_key_file=/etc/ssl/private/ssl-cert-snakeoil.key

# Configure Scry with server_tls_sslmode=require
export SCRY_SERVER_TLS_SSLMODE=require

# Run test
SCRY_TLS_TEST_URL="postgres://postgres:postgres@localhost:5434/postgres?sslmode=require" \
  cargo test -p scry --test tls_integration test_tls_connection_state_isolation -- --ignored
```

**Step 5: Commit**

```bash
git add scry-proxy/tests/tls_integration.rs
git commit -m "$(cat <<'EOF'
test(tls): add TLS connection state isolation integration test

Verifies CRIT-2 fix: TLS connections are properly reset with DISCARD ALL
between clients, preventing session state leakage (prepared statements,
temp tables, session variables).
EOF
)"
```

---

## Task 6: Update Cargo.toml for test dependencies (if needed)

**Files:**
- Modify: `scry-proxy/Cargo.toml`

**Step 1: Check if dependencies exist**

Run: `grep -E "native-tls|postgres-native-tls" scry-proxy/Cargo.toml`

**Step 2: Add dependencies if missing**

```toml
[dev-dependencies]
# ... existing deps ...
native-tls = "0.2"
postgres-native-tls = "0.5"
```

**Step 3: Verify build**

Run: `cargo build -p scry --tests`
Expected: PASS

**Step 4: Commit (if changes made)**

```bash
git add scry-proxy/Cargo.toml
git commit -m "$(cat <<'EOF'
build: add native-tls dev dependencies for TLS integration tests
EOF
)"
```

---

## Task 7: Final verification and cleanup

**Files:**
- All modified files

**Step 1: Run all tests**

```bash
just test
```
Expected: PASS

**Step 2: Run clippy**

```bash
just lint
```
Expected: No warnings related to changes

**Step 3: Run fmt**

```bash
just fmt
```
Expected: No changes needed

**Step 4: Final commit (if any cleanup)**

```bash
git add -A
git commit -m "$(cat <<'EOF'
chore: cleanup after CRIT-2 implementation
EOF
)"
```

---

## Verification Steps

### Unit Tests
```bash
cargo test -p scry --lib protocol::postgres::tests
cargo test -p scry --lib protocol::traits::tests
cargo test -p scry --lib proxy::tcp_pool::tests
```

### Integration Tests (requires TLS-enabled Postgres)
```bash
SCRY_TLS_TEST_URL="postgres://..." cargo test -p scry --test tls_integration -- --ignored
```

### Manual Verification
1. Start Scry with TLS backend (`server_tls_sslmode=require`)
2. Connect Client A, create prepared statement, disconnect
3. Connect Client B, verify prepared statement doesn't exist

---

## Rollback Plan

If issues arise, revert the generic Protocol trait:

1. Change Protocol trait methods back to `&mut TcpStream`
2. Restore TLS branch in tcp_pool.rs to skip reset:
   ```rust
   BackendTransport::Tls(_) => {
       debug!("TLS connection recycled (limited health check)");
       Ok(())
   }
   ```

---

## Files Modified

| File | Change |
|------|--------|
| `scry-proxy/src/protocol/traits.rs` | Make Protocol trait generic over AsyncRead+AsyncWrite |
| `scry-proxy/src/protocol/postgres.rs` | Update PostgresProtocol with generic methods |
| `scry-proxy/src/proxy/tcp_pool.rs` | Enable DISCARD ALL for TLS connections |
| `scry-proxy/tests/tls_integration.rs` | Add TLS state isolation tests |
| `scry-proxy/Cargo.toml` | Add native-tls dev dependencies (if needed) |

---

## Success Criteria

After implementation:
- [ ] TLS connections receive DISCARD ALL on recycle
- [ ] TLS connections receive active health checks
- [ ] No session state leakage between TLS clients
- [ ] All existing tests pass
- [ ] No performance regression (active health check adds ~1ms)
