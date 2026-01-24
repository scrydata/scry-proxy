# CRIT-1: Fix DISCARD ALL Response Handling

## Problem Summary

When recycling pooled connections, Scry sends `DISCARD ALL` but only performs a single `read()` call to consume the response. Under high load, PostgreSQL's response (CommandComplete + ReadyForQuery) may arrive in separate TCP packets. If only CommandComplete is read, ReadyForQuery ('Z' = 0x5A) remains in the socket buffer and corrupts the next client's protocol stream.

**Symptom:** "invalid frontend message type 0" errors from Postgres under 300+ concurrent connections.

## Current Implementation

**File:** `scry-proxy/src/protocol/postgres.rs:62-114`

```rust
async fn reset_connection(&self, stream: &mut TcpStream) -> Result<bool> {
    // ... send DISCARD ALL ...

    // PROBLEM: Single read, may not get all response data
    let mut response_buffer = vec![0u8; 1024];
    let n = stream.read(&mut response_buffer).await?;

    // PROBLEM: Byte search, not message parsing
    if Self::contains_command_complete(response) {
        Ok(true)  // Returns without ensuring ReadyForQuery consumed
    }
}
```

**Issues:**
1. Single `read()` may not capture complete response
2. `contains_command_complete()` searches for byte 'C' anywhere, not proper message parsing
3. Does not wait for ReadyForQuery ('Z') message
4. No timeout protection

## Solution

Leverage existing `MessageExtractor::extract_ready_for_query()` which properly parses PostgreSQL message frames. Loop reading until ReadyForQuery is found or timeout occurs.

## Implementation Tasks

### Task 1: Add timeout configuration

**File:** `scry-proxy/src/config/mod.rs`

Add a new configuration field for reset timeout:

```rust
// In PerformanceConfig struct (around line 258)
pub struct PerformanceConfig {
    pub target_latency_ms: u64,
    pub connection_pooling: PoolingStrategy,
    pub pool_size: usize,
    pub pool_min_idle: usize,
    pub pool_timeout_secs: u64,
    pub pool_queue_depth: usize,
    pub pool_idle_unpin_secs: u64,
    pub pool_lifo: bool,
    pub pool_recycle_secs: u64,
    pub pool_reset_timeout_ms: u64,  // NEW: Timeout for DISCARD ALL response (default 5000)
}
```

Update `Default` impl to include:
```rust
pool_reset_timeout_ms: 5000,  // 5 second timeout for reset
```

### Task 2: Refactor reset_connection to use proper message parsing

**File:** `scry-proxy/src/protocol/postgres.rs`

Replace the current `reset_connection` implementation:

```rust
use tokio::time::{timeout, Duration};
use crate::protocol::extractor::MessageExtractor;
use crate::protocol::MSG_READY_FOR_QUERY;

pub struct PostgresProtocol {
    extractor: MessageExtractor,
    reset_timeout_ms: u64,  // NEW field
}

impl PostgresProtocol {
    pub fn new() -> Self {
        Self {
            extractor: MessageExtractor::new(),
            reset_timeout_ms: 5000,  // Default 5 seconds
        }
    }

    pub fn with_reset_timeout(mut self, timeout_ms: u64) -> Self {
        self.reset_timeout_ms = timeout_ms;
        self
    }
}

#[async_trait]
impl Protocol for PostgresProtocol {
    async fn reset_connection(&self, stream: &mut TcpStream) -> Result<bool> {
        debug!("Sending DISCARD ALL to reset connection state");

        // Construct and send Query message for "DISCARD ALL"
        let query = b"DISCARD ALL\0";
        let message_length = (4 + query.len()) as i32;

        let mut message = Vec::with_capacity(1 + 4 + query.len());
        message.push(b'Q');
        message.extend_from_slice(&message_length.to_be_bytes());
        message.extend_from_slice(query);

        stream.write_all(&message).await.context("Failed to send DISCARD ALL")?;

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

    // ... other methods unchanged ...
}
```

### Task 3: Implement read_until_ready_for_query helper

**File:** `scry-proxy/src/protocol/postgres.rs`

Add this helper method to `PostgresProtocol`:

```rust
impl PostgresProtocol {
    /// Read from stream until ReadyForQuery message is received
    ///
    /// Returns Ok(true) if ReadyForQuery with 'I' (idle) status received
    /// Returns Ok(false) if error response or unexpected state
    /// Returns Err if I/O error
    async fn read_until_ready_for_query(&self, stream: &mut TcpStream) -> Result<bool> {
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
                    b'I' => return Ok(true),   // Idle - success
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
```

### Task 4: Remove obsolete helper methods

**File:** `scry-proxy/src/protocol/postgres.rs`

Remove these methods as they are no longer needed:

```rust
// DELETE these methods:
fn contains_command_complete(data: &[u8]) -> bool { ... }
fn contains_error_response(data: &[u8]) -> bool { ... }
```

### Task 5: Update tcp_pool.rs to pass config

**File:** `scry-proxy/src/proxy/tcp_pool.rs`

Update `BackendTransportManager` to pass reset timeout to protocol:

```rust
// In BackendTransportManager::new or where PostgresProtocol is created
let protocol = Arc::new(
    PostgresProtocol::new()
        .with_reset_timeout(config.performance.pool_reset_timeout_ms)
);
```

### Task 6: Add unit tests

**File:** `scry-proxy/src/protocol/postgres.rs`

Add tests for the new behavior:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{duplex, AsyncWriteExt};

    #[tokio::test]
    async fn test_reset_handles_split_response() {
        let proto = PostgresProtocol::new();
        let (mut client, mut server) = duplex(1024);

        // Spawn task to simulate slow server response
        tokio::spawn(async move {
            // Read the DISCARD ALL command
            let mut buf = [0u8; 100];
            let _ = server.read(&mut buf).await;

            // Send CommandComplete in first packet
            // 'C' + length + "DISCARD ALL\0"
            let cc = [
                b'C', 0, 0, 0, 16,
                b'D', b'I', b'S', b'C', b'A', b'R', b'D', b' ', b'A', b'L', b'L', 0
            ];
            server.write_all(&cc).await.unwrap();

            // Small delay to simulate packet split
            tokio::time::sleep(Duration::from_millis(10)).await;

            // Send ReadyForQuery in second packet
            let rfq = [b'Z', 0, 0, 0, 5, b'I'];
            server.write_all(&rfq).await.unwrap();
        });

        // This should succeed even with split response
        let result = proto.reset_connection(&mut client).await.unwrap();
        assert!(result);
    }

    #[tokio::test]
    async fn test_reset_handles_error_response() {
        let proto = PostgresProtocol::new();
        let (mut client, mut server) = duplex(1024);

        tokio::spawn(async move {
            let mut buf = [0u8; 100];
            let _ = server.read(&mut buf).await;

            // Send ErrorResponse followed by ReadyForQuery
            // Simplified error: 'E' + length + 'M' + "error\0" + terminator
            let err = [
                b'E', 0, 0, 0, 12,
                b'M', b'e', b'r', b'r', b'o', b'r', 0, 0
            ];
            server.write_all(&err).await.unwrap();

            // ReadyForQuery with error state
            let rfq = [b'Z', 0, 0, 0, 5, b'E'];
            server.write_all(&rfq).await.unwrap();
        });

        let result = proto.reset_connection(&mut client).await.unwrap();
        assert!(!result);  // Should return false for error state
    }

    #[tokio::test]
    async fn test_reset_timeout() {
        let proto = PostgresProtocol::new().with_reset_timeout(100); // 100ms timeout
        let (mut client, mut server) = duplex(1024);

        tokio::spawn(async move {
            let mut buf = [0u8; 100];
            let _ = server.read(&mut buf).await;
            // Don't send response - simulate hung connection
            tokio::time::sleep(Duration::from_secs(10)).await;
        });

        let result = proto.reset_connection(&mut client).await.unwrap();
        assert!(!result);  // Should return false after timeout
    }
}
```

### Task 7: Add integration test

**File:** `scry-proxy/tests/connection_multiplexing.rs` (new file)

```rust
//! Integration tests for connection multiplexing under load

use std::time::Duration;
use tokio::time::timeout;

/// Test that 300 concurrent connections work without protocol errors
#[tokio::test]
#[ignore] // Run with: cargo test --test connection_multiplexing -- --ignored
async fn test_300_concurrent_connections() {
    // This test requires Docker containers running
    // Start with: cd benchmarks && docker compose up -d postgres scry

    let scry_url = "postgres://postgres:postgres@localhost:5434/postgres";
    let num_clients = 300;
    let queries_per_client = 100;

    let mut handles = vec![];

    for i in 0..num_clients {
        let url = scry_url.to_string();
        handles.push(tokio::spawn(async move {
            let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
                .await
                .expect("Failed to connect");

            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("Connection {} error: {}", i, e);
                }
            });

            for _ in 0..queries_per_client {
                client.simple_query("SELECT 1").await.expect("Query failed");
            }

            Ok::<_, tokio_postgres::Error>(())
        }));
    }

    let results = futures::future::join_all(handles).await;

    let errors: Vec<_> = results.iter()
        .filter(|r| r.is_err() || r.as_ref().unwrap().is_err())
        .collect();

    assert!(errors.is_empty(), "Had {} errors out of {} clients", errors.len(), num_clients);
}
```

## Verification Steps

1. **Unit tests pass:**
   ```bash
   just test-unit
   ```

2. **Integration test passes:**
   ```bash
   cd benchmarks && docker compose up -d postgres scry
   cargo test --test connection_multiplexing -- --ignored
   ```

3. **Benchmark passes at 300 connections:**
   ```bash
   cd benchmarks
   docker compose up -d postgres
   docker compose up -d scry
   cargo run --release -- \
     --database-url "postgres://postgres:postgres@localhost:5434/postgres" \
     --label "scry-300conn" \
     --connections 300 \
     --queries 100000
   ```

   **Success criteria:** 0 failed queries, no protocol errors in logs.

## Rollback Plan

If issues arise, revert to previous implementation by:
1. Restoring single-read behavior in `reset_connection`
2. Removing timeout configuration
3. Re-adding `contains_command_complete` helper

## Files Modified

| File | Change |
|------|--------|
| `scry-proxy/src/config/mod.rs` | Add `pool_reset_timeout_ms` config |
| `scry-proxy/src/protocol/postgres.rs` | Rewrite `reset_connection`, add `read_until_ready_for_query` |
| `scry-proxy/src/proxy/tcp_pool.rs` | Pass timeout config to protocol |
| `scry-proxy/tests/connection_multiplexing.rs` | New integration test |

## Estimated Effort

- Implementation: 2-3 hours
- Testing: 1-2 hours
- Total: 3-5 hours
