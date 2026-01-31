# HIGH-3 Message Framing Issues Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace raw byte searches with proper PostgreSQL message framing to prevent false positives from binary data.

**Architecture:** PostgreSQL wire protocol uses a fixed format: `Type (1 byte) | Length (4 bytes, includes self) | Payload (Length - 4 bytes)`. Currently, code searches for byte `0x5A` ('Z') anywhere in buffers to detect ReadyForQuery. This fails when binary query results contain that byte. Fix by parsing message boundaries using the length field before checking message types.

**Tech Stack:** Rust, PostgreSQL wire protocol, tokio async I/O

---

## Problem Statement

From `docs/CONNECTION_MULTIPLEXING_REQUIREMENTS.md`:

> **Location:** `scry-proxy/src/proxy/connection.rs:243-286`
>
> **Current Behavior:**
> ```rust
> if data.contains(&b'Z') { break; }  // Byte search, not message parsing
> ```
>
> **Problem:**
> - Checks for byte 'Z' anywhere in data, not ReadyForQuery message
> - 'Z' could appear in error message text, query results, etc.
> - May break out of loop prematurely
> - May forward incomplete messages to client

## Affected Locations

### Location 1: `connection.rs` lines 423 and 449

```rust
// Line 423
if pending.contains(&b'Z') {
    debug!(connection_id, "Backend startup complete (ReadyForQuery received)");
    break;
}

// Line 449
if data.contains(&b'Z') {
    debug!(connection_id, "Backend startup complete (ReadyForQuery received)");
    break;
}
```

### Location 2: `extractor.rs` lines 42-56

```rust
pub fn is_query_complete(&self, data: &[u8]) -> bool {
    // Look for CommandComplete (C) or ReadyForQuery (Z) message
    for &msg_type in data {
        if msg_type == MSG_COMMAND_COMPLETE || msg_type == MSG_READY_FOR_QUERY {
            return true;
        }
    }
    false
}
```

## Solution

The codebase already has a proper implementation in `extract_ready_for_query()` (lines 454-488 of `extractor.rs`) that correctly parses message frames. We need to:

1. Add a new `contains_ready_for_query()` method for boolean checks
2. Fix `is_query_complete()` to use proper message framing
3. Update `connection.rs` to use the new method

---

## Task 1: Add `contains_ready_for_query()` Method

**Files:**
- Modify: `scry-proxy/src/protocol/extractor.rs`
- Test: `scry-proxy/src/protocol/extractor.rs` (inline tests)

**Step 1: Write the failing test**

Add to the `#[cfg(test)]` module at the end of `extractor.rs`:

```rust
#[test]
fn test_contains_ready_for_query_true() {
    let extractor = MessageExtractor::new();
    // Valid ReadyForQuery message: 'Z' + length(5) + status('I')
    let msg = vec![MSG_READY_FOR_QUERY, 0, 0, 0, 5, b'I'];
    assert!(extractor.contains_ready_for_query(&msg));
}

#[test]
fn test_contains_ready_for_query_false_for_z_in_data() {
    let extractor = MessageExtractor::new();
    // DataRow containing 'Z' byte in payload - should NOT match
    // DataRow: 'D' + length + column_count + column_data
    let mut msg = vec![MSG_DATA_ROW, 0, 0, 0, 11]; // length = 11
    msg.extend_from_slice(&[0, 1]); // 1 column
    msg.extend_from_slice(&[0, 0, 0, 1]); // column length = 1
    msg.push(b'Z'); // 'Z' as data value, not message type
    assert!(!extractor.contains_ready_for_query(&msg));
}

#[test]
fn test_contains_ready_for_query_in_stream() {
    let extractor = MessageExtractor::new();
    // DataRow + CommandComplete + ReadyForQuery
    let mut msg = vec![];
    // DataRow with 'Z' in data
    msg.extend_from_slice(&[MSG_DATA_ROW, 0, 0, 0, 11]);
    msg.extend_from_slice(&[0, 1, 0, 0, 0, 1, b'Z']);
    // CommandComplete
    msg.extend_from_slice(&[MSG_COMMAND_COMPLETE, 0, 0, 0, 13]);
    msg.extend_from_slice(b"SELECT 1\0");
    // ReadyForQuery
    msg.extend_from_slice(&[MSG_READY_FOR_QUERY, 0, 0, 0, 5, b'I']);

    assert!(extractor.contains_ready_for_query(&msg));
}

#[test]
fn test_contains_ready_for_query_incomplete_message() {
    let extractor = MessageExtractor::new();
    // Incomplete ReadyForQuery (missing status byte)
    let msg = vec![MSG_READY_FOR_QUERY, 0, 0, 0, 5];
    assert!(!extractor.contains_ready_for_query(&msg));
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p scry --lib contains_ready_for_query`
Expected: FAIL with "cannot find function `contains_ready_for_query`"

**Step 3: Write minimal implementation**

Add after `extract_ready_for_query()` method (around line 489):

```rust
/// Check if the data contains a properly-framed ReadyForQuery message
///
/// Unlike raw byte search (e.g., `data.contains(&b'Z')`), this method
/// correctly parses PostgreSQL message frames and only returns true
/// when a valid ReadyForQuery message is found at a message boundary.
///
/// This prevents false positives from:
/// - Binary data in query results containing byte 0x5A
/// - Error messages containing the letter 'Z'
/// - Parameter data with the 'Z' byte
pub fn contains_ready_for_query(&self, data: &[u8]) -> bool {
    self.extract_ready_for_query(data).is_some()
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p scry --lib contains_ready_for_query`
Expected: PASS (4 tests)

**Step 5: Commit**

```bash
git add scry-proxy/src/protocol/extractor.rs
git commit -m "$(cat <<'EOF'
feat(protocol): add contains_ready_for_query() for proper message framing

Adds a boolean method that checks for ReadyForQuery using proper
PostgreSQL message frame parsing instead of raw byte search.
This prevents false positives when binary data contains byte 0x5A.

Part of HIGH-3 message framing fix.
EOF
)"
```

---

## Task 2: Fix `is_query_complete()` Method

**Files:**
- Modify: `scry-proxy/src/protocol/extractor.rs:42-56`
- Test: `scry-proxy/src/protocol/extractor.rs` (inline tests)

**Step 1: Write the failing test**

Add to the `#[cfg(test)]` module:

```rust
#[test]
fn test_is_query_complete_false_for_c_in_data() {
    let extractor = MessageExtractor::new();
    // DataRow containing 'C' byte in payload - should NOT match
    let mut msg = vec![MSG_DATA_ROW, 0, 0, 0, 11];
    msg.extend_from_slice(&[0, 1]); // 1 column
    msg.extend_from_slice(&[0, 0, 0, 1]); // column length = 1
    msg.push(b'C'); // 'C' as data value, not CommandComplete
    assert!(!extractor.is_query_complete(&msg));
}

#[test]
fn test_is_query_complete_true_for_command_complete() {
    let extractor = MessageExtractor::new();
    // Valid CommandComplete message
    let mut msg = vec![MSG_COMMAND_COMPLETE, 0, 0, 0, 13];
    msg.extend_from_slice(b"SELECT 1\0");
    assert!(extractor.is_query_complete(&msg));
}

#[test]
fn test_is_query_complete_true_for_ready_for_query() {
    let extractor = MessageExtractor::new();
    let msg = vec![MSG_READY_FOR_QUERY, 0, 0, 0, 5, b'I'];
    assert!(extractor.is_query_complete(&msg));
}

#[test]
fn test_is_query_complete_after_data_rows() {
    let extractor = MessageExtractor::new();
    // DataRow with 'C' in data + actual CommandComplete
    let mut msg = vec![];
    // DataRow containing 'C'
    msg.extend_from_slice(&[MSG_DATA_ROW, 0, 0, 0, 11]);
    msg.extend_from_slice(&[0, 1, 0, 0, 0, 1, b'C']);
    // CommandComplete
    msg.extend_from_slice(&[MSG_COMMAND_COMPLETE, 0, 0, 0, 13]);
    msg.extend_from_slice(b"SELECT 1\0");

    assert!(extractor.is_query_complete(&msg));
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p scry --lib is_query_complete_false_for_c_in_data`
Expected: FAIL - the current implementation will incorrectly return `true` because it scans all bytes

**Step 3: Write minimal implementation**

Replace the `is_query_complete` method (lines 42-56):

```rust
/// Check if the data indicates a query is complete
///
/// Looks for CommandComplete ('C') or ReadyForQuery ('Z') messages
/// using proper PostgreSQL message framing. Only checks message type
/// bytes at actual message boundaries, not raw bytes in the stream.
///
/// This prevents false positives from binary data containing 0x43 ('C')
/// or 0x5A ('Z') bytes in query results or error messages.
pub fn is_query_complete(&self, data: &[u8]) -> bool {
    if data.is_empty() {
        return false;
    }

    let mut offset = 0;

    while offset + 5 <= data.len() {
        let msg_type = data[offset];

        // Check if this is a completion message
        if msg_type == MSG_COMMAND_COMPLETE || msg_type == MSG_READY_FOR_QUERY {
            trace!(msg_type = msg_type, "Found query completion marker");
            return true;
        }

        // Read the length field to skip to next message
        let length = i32::from_be_bytes([
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
            data[offset + 4],
        ]) as usize;

        // Validate length field
        if length < 4 || offset + 1 + length > data.len() {
            // Invalid or incomplete message, stop scanning
            break;
        }

        // Advance to next message
        offset += 1 + length;
    }

    false
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p scry --lib is_query_complete`
Expected: PASS (all is_query_complete tests including new ones)

**Step 5: Commit**

```bash
git add scry-proxy/src/protocol/extractor.rs
git commit -m "$(cat <<'EOF'
fix(protocol): use proper message framing in is_query_complete()

Replaces raw byte search with proper PostgreSQL message frame parsing.
Now only checks message type bytes at actual message boundaries,
preventing false positives from binary data in query results.

Fixes HIGH-3 from CONNECTION_MULTIPLEXING_REQUIREMENTS.md
EOF
)"
```

---

## Task 3: Fix Startup Handshake in `connection.rs`

**Files:**
- Modify: `scry-proxy/src/proxy/connection.rs:423` and `:449`
- Test: `scry-proxy/tests/connection_multiplexing.rs` (new test)

**Step 1: Write the failing test**

Create a new test in `scry-proxy/tests/connection_multiplexing.rs`:

```rust
/// Test that binary data containing 'Z' byte doesn't break startup
///
/// This verifies the HIGH-3 fix: proper message framing during startup.
/// The startup handshake should not be confused by binary data that
/// happens to contain the byte 0x5A ('Z').
#[tokio::test]
async fn test_startup_handles_z_byte_in_parameter_data() {
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

    // Connect and execute query that returns binary data containing 'Z' (0x5A)
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=127.0.0.1 port={} user={} password={} dbname={}",
            proxy_port, config.backend.user, config.backend.password, config.backend.database
        ),
        tokio_postgres::NoTls,
    )
    .await
    .expect("Failed to connect through proxy");

    let conn_handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    // Query that returns data with 'Z' byte (0x5A = 90 decimal)
    // This exercises the message parsing to ensure 'Z' in data doesn't
    // trigger false ReadyForQuery detection
    let rows = client
        .query("SELECT E'\\x5A5A5A'::bytea as binary_with_z", &[])
        .await
        .expect("Query with binary Z data should succeed");

    assert_eq!(rows.len(), 1);

    // Also test string containing 'Z'
    let rows = client
        .query("SELECT 'ZZZZZZ' as string_with_z", &[])
        .await
        .expect("Query with Z string should succeed");

    assert_eq!(rows.len(), 1);
    let value: &str = rows[0].get(0);
    assert_eq!(value, "ZZZZZZ");

    drop(client);
    conn_handle.abort();
}
```

**Step 2: Run test to verify it fails (or passes if lucky)**

Run: `cargo test -p scry --test connection_multiplexing test_startup_handles_z_byte`

Note: This test may pass even with the bug because the timing might not trigger the issue. The real fix is preventative.

**Step 3: Write minimal implementation**

Modify `scry-proxy/src/proxy/connection.rs`. First, add the extractor field to ConnectionHandler:

At line 24, inside `ConnectionHandler` struct, verify `extractor` is available or add it to the handshake method scope.

Replace lines 422-426:

```rust
// Check for ReadyForQuery using proper message framing
// (not raw byte search which could false-positive on binary data)
let extractor = MessageExtractor::new();
if extractor.contains_ready_for_query(&pending) {
    debug!(connection_id, "Backend startup complete (ReadyForQuery received)");
    break;
}
```

Replace lines 448-452:

```rust
// Check for ReadyForQuery using proper message framing
if extractor.contains_ready_for_query(data) {
    debug!(connection_id, "Backend startup complete (ReadyForQuery received)");
    break;
}
```

Note: Move the `let extractor = MessageExtractor::new();` line to the beginning of the loop (around line 413) so it's created once and reused:

```rust
// Forward any remaining data and continue reading until ReadyForQuery
let mut pending = remaining_data;
let mut backend_buffer = vec![0u8; 8192];
let extractor = MessageExtractor::new();

loop {
    // Check pending data first
    if !pending.is_empty() {
        // Forward to client
        self.client_stream
            .write_all(&pending)
            .await
            .context("Failed to forward startup data to client")?;

        // Check for ReadyForQuery using proper message framing
        if extractor.contains_ready_for_query(&pending) {
            debug!(connection_id, "Backend startup complete (ReadyForQuery received)");
            break;
        }
        pending.clear();
    }

    // Read more from backend
    let n = backend_stream
        .read(&mut backend_buffer)
        .await
        .context("Failed to read backend startup data")?;

    if n == 0 {
        anyhow::bail!("Backend closed connection during startup");
    }

    let data = &backend_buffer[..n];

    // Forward to client
    self.client_stream
        .write_all(data)
        .await
        .context("Failed to forward startup data to client")?;

    // Check for ReadyForQuery using proper message framing
    if extractor.contains_ready_for_query(data) {
        debug!(connection_id, "Backend startup complete (ReadyForQuery received)");
        break;
    }
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test -p scry --test connection_multiplexing test_startup_handles_z_byte`
Expected: PASS

**Step 5: Commit**

```bash
git add scry-proxy/src/proxy/connection.rs scry-proxy/tests/connection_multiplexing.rs
git commit -m "$(cat <<'EOF'
fix(proxy): use proper message framing in startup handshake

Replaces raw byte search `data.contains(&b'Z')` with proper PostgreSQL
message frame parsing using `contains_ready_for_query()`. This prevents
the startup handshake from being confused by binary data that happens
to contain the byte 0x5A ('Z').

Fixes HIGH-3 from CONNECTION_MULTIPLEXING_REQUIREMENTS.md
EOF
)"
```

---

## Task 4: Run Full Test Suite

**Files:**
- None (verification only)

**Step 1: Run unit tests**

Run: `cargo test -p scry --lib`
Expected: All tests PASS

**Step 2: Run integration tests**

Run: `cargo test -p scry --test connection_multiplexing`
Expected: All tests PASS

**Step 3: Run full test suite**

Run: `just test`
Expected: All tests PASS

**Step 4: Run linter**

Run: `just lint`
Expected: No warnings or errors

**Step 5: Commit (if any formatting fixes needed)**

```bash
just fmt
git add -A
git commit -m "style: format code"
```

---

## Task 5: Update Requirements Document

**Files:**
- Modify: `docs/CONNECTION_MULTIPLEXING_REQUIREMENTS.md`

**Step 1: Mark HIGH-3 as complete**

Change line 170 from:
```markdown
### HIGH-3: Message Framing Issues
```

To:
```markdown
### HIGH-3: Message Framing Issues ✅ COMPLETED
```

Add implementation notes after the requirements section:

```markdown
**Implementation:**
- Added `contains_ready_for_query()` method to `MessageExtractor` for proper message frame parsing
- Fixed `is_query_complete()` to iterate through message boundaries instead of scanning all bytes
- Updated startup handshake in `connection.rs` to use `contains_ready_for_query()` instead of `data.contains(&b'Z')`
- All message type checks now verify the byte is at a valid message boundary
- Integration test added to verify binary data containing 'Z' byte doesn't break connections
```

**Step 2: Commit**

```bash
git add docs/CONNECTION_MULTIPLEXING_REQUIREMENTS.md
git commit -m "docs: mark HIGH-3 message framing as complete"
```

---

## Verification Checklist

After implementation:

- [ ] `just test-unit` passes
- [ ] `just lint` passes
- [ ] `just test-integration` passes
- [ ] `contains_ready_for_query()` correctly parses message frames
- [ ] `is_query_complete()` doesn't false-positive on binary data
- [ ] Startup handshake uses proper message framing
- [ ] Binary data containing 'Z' (0x5A) doesn't break connections
- [ ] String data containing 'Z' doesn't break connections
- [ ] CONNECTION_MULTIPLEXING_REQUIREMENTS.md updated

---

## PostgreSQL Message Format Reference

```
| Type (1 byte) | Length (4 bytes, big-endian, includes self) | Payload (Length - 4 bytes) |
```

**ReadyForQuery Message:**
- Type: `Z` (0x5A)
- Length: 5 (always)
- Payload: 1 byte status (`I` = idle, `T` = in transaction, `E` = error)
- Total: 6 bytes

**CommandComplete Message:**
- Type: `C` (0x43)
- Length: varies
- Payload: command tag string (null-terminated)

**DataRow Message:**
- Type: `D` (0x44)
- Length: varies
- Payload: column count (2 bytes) + column data (may contain any bytes including 0x5A!)

---

## Rollback Plan

If issues arise:
1. Revert the three commits in reverse order
2. All changes are isolated to message parsing logic
3. No configuration changes required

---

## Success Criteria

| Metric | Target |
|--------|--------|
| False positive on 'Z' in data | Eliminated |
| False positive on 'C' in data | Eliminated |
| Message boundary validation | All message type checks |
| Test coverage | 4+ new unit tests |
| Breaking changes | None |
