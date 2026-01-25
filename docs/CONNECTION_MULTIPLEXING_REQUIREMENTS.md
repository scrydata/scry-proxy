# Connection Multiplexing Requirements

This document captures the findings from investigating Scry's connection handling under high load (300+ simultaneous client connections). These requirements must be addressed to enable proper connection multiplexing comparable to PgBouncer and PgCat.

## Problem Statement

Scry fails when 300+ clients connect simultaneously, producing:
- "Startup handshake failed" errors in Scry logs
- "invalid frontend message type 0" errors in Postgres logs
- "invalid length of startup packet" errors in Postgres logs

Competing proxies (PgBouncer, PgCat) handle 500+ client connections multiplexed to ~250 backend connections without issue.

## Current Benchmark Results (Transaction Mode)

| Proxy | 100 Clients | 300 Clients | 500 Clients |
|-------|-------------|-------------|-------------|
| PgBouncer | 13,540 qps | 12,667 qps | 11,587 qps |
| PgCat | 21,390 qps | 20,739 qps | 20,431 qps |
| Scry | 22,821 qps | **FAILS** | **FAILS** |

---

## Critical Issues

### CRIT-1: Incomplete DISCARD ALL Response Handling

**Location:** `scry-proxy/src/protocol/postgres.rs:88-113`

**Current Behavior:**
```rust
let n = stream.read(&mut response_buffer).await?;  // Single read
if Self::contains_command_complete(response) {
    Ok(true)
} else {
    Ok(false)
}
```

**Problem:**
- PostgreSQL sends two messages after DISCARD ALL: CommandComplete ('C') and ReadyForQuery ('Z')
- These may arrive in separate TCP packets under load
- Single `read()` may only capture CommandComplete
- ReadyForQuery ('Z' = 0x5A) remains in socket buffer
- Next client reads stale 'Z' byte as message type
- Results in "invalid frontend message type 90" (0x5A = 90 decimal)

**Requirements:**
- [ ] Loop reading until ReadyForQuery ('Z') message is fully received
- [ ] Parse complete PostgreSQL message frames, not raw bytes
- [ ] Validate message boundaries before returning connection to pool
- [ ] Add timeout for response reading to prevent hangs

**Test Case:**
- 300 concurrent connections in transaction mode
- All connections performing simple SELECT queries
- No protocol errors after 100,000 queries

---

### CRIT-2: TLS Connections Skip State Reset

**Location:** `scry-proxy/src/proxy/tcp_pool.rs:352-359`

**Current Behavior:**
```rust
BackendTransport::Tls(_) => {
    // TODO: Make Protocol trait generic over AsyncRead+AsyncWrite
    debug!("TLS connection recycled (limited health check)");
    Ok(())  // NO RESET PERFORMED
}
```

**Problem:**
- TLS pooled connections are NEVER reset with DISCARD ALL
- Session state leaks between clients:
  - Prepared statements
  - Temporary tables
  - Session variables (application_name, search_path, etc.)
  - Cursors
  - Advisory locks
- Data exposure risk: Client B may access Client A's temp tables
- Query failures: Client B may conflict with Client A's prepared statements

**Requirements:**
- [ ] Implement protocol operations for TLS connections
- [ ] Either make Protocol trait generic over AsyncRead+AsyncWrite
- [ ] Or create separate TlsProtocol implementation
- [ ] DISCARD ALL must execute on TLS connection recycle
- [ ] Health check must work for TLS connections

**Test Case:**
- TLS-enabled backend connection
- Client A creates prepared statement, releases connection
- Client B acquires same connection
- Verify prepared statement does NOT exist for Client B

---

### CRIT-3: No max_connections Enforcement ✅ COMPLETED

**Implementation:**
- Added AtomicUsize counter to ProxyServer for tracking active connections
- Counter incremented before spawning connection handler, decremented via RAII guard on task completion
- Connection limit checked BEFORE processing accepted connection
- Rejected clients receive PostgreSQL ErrorResponse with SQLSTATE 53300 (too_many_connections)
- Prometheus metrics exposed: `scry_active_connections`, `scry_max_connections`, `scry_connections_rejected_total`
- Stress tests verify behavior under load (150 concurrent connections against 100 limit)

---

## High Priority Issues

### HIGH-1: Pool Size vs Client Count Mismatch

**Location:** `scry-proxy/src/config/mod.rs` (defaults)

**Current Behavior:**
- Default pool_size: 10-100 (varies by config path)
- Default max_connections: 100
- No relationship enforced between these values

**Problem:**
- 300 clients competing for 10 backend connections
- Massive queuing and contention
- Most clients timeout waiting for connections

**Requirements:**
- [ ] Document recommended pool_size relative to expected client count
- [ ] Add validation warning if max_connections >> pool_size * 10
- [ ] Consider auto-scaling pool size based on demand
- [ ] Improve defaults for production use cases

**Recommended Defaults:**
```
pool_size = 50-100 (for production)
max_connections = pool_size * 20 (for transaction mode)
```

---

### HIGH-2: Wait Queue Saturation

**Location:** `scry-proxy/src/proxy/wait_queue.rs:49`

**Current Behavior:**
```rust
if waiters.len() >= self.max_depth {
    return Err(QueueFullError);  // Hard rejection
}
```

**Problem:**
- Default queue depth: 50-100
- Clients beyond pool_size + queue_depth are rejected immediately
- No backoff or retry suggestion
- Error message not helpful to client

**Requirements:**
- [ ] Increase default queue depth for production (500-1000)
- [ ] Add configurable backpressure behavior:
  - Reject immediately (current)
  - Wait with timeout
  - Return "server busy" with retry-after hint
- [ ] Expose queue depth in metrics
- [ ] Alert when queue consistently > 80% full

---

### HIGH-3: Message Framing Issues ✅ COMPLETED

**Implementation:**
- Added `contains_ready_for_query()` method to `MessageExtractor` for proper message frame parsing
- Fixed `is_query_complete()` to iterate through message boundaries instead of scanning all bytes
- Updated startup handshake in `connection.rs` to use `contains_ready_for_query()` instead of `data.contains(&b'Z')`
- All message type checks now verify the byte is at a valid message boundary
- Integration test added to verify binary data containing 'Z' byte doesn't break connections

**Previous Problem:**
- Checked for byte 'Z' anywhere in data, not ReadyForQuery message
- 'Z' could appear in error message text, query results, etc.
- Could break out of loop prematurely
- Could forward incomplete messages to client

**Message Format Reference:**
```
| Type (1 byte) | Length (4 bytes, includes self) | Payload (Length - 4 bytes) |
```

---

### HIGH-4: Startup Message Incomplete Reading

**Location:** `scry-proxy/src/tls/startup.rs:60-91`

**Current Behavior:**
```rust
let mut buf = vec![0u8; 8192];
let n = stream.read(&mut buf).await?;  // May be partial
buf.truncate(n);
// No validation that complete message was read
```

**Problem:**
- StartupMessage could exceed 8192 bytes (many connection parameters)
- No loop to read complete message
- Truncated startup causes incorrect database routing
- May cause authentication failures

**Requirements:**
- [ ] Read startup message length prefix first (4 bytes)
- [ ] Allocate buffer for exact message size
- [ ] Loop reading until complete message received
- [ ] Validate message structure before parsing
- [ ] Reject malformed startup messages with clear error

---

## Medium Priority Issues

### MED-1: Passive Health Checks

**Location:** `scry-proxy/src/protocol/postgres.rs` (health_check)

**Current Behavior:**
```rust
match stream.try_read(&mut [0u8; 1]) {
    Err(ref e) if e.kind() == WouldBlock => Ok(true),  // Assume alive
    ...
}
```

**Problem:**
- Only detects connections closed by database
- Stale connections (network issues) pass health check
- Half-open TCP connections not detected

**Requirements:**
- [ ] Implement active health check (send query, expect response)
- [ ] Use simple query: `SELECT 1` or `;` (empty query)
- [ ] Add configurable health check interval
- [ ] Remove connections that fail health check from pool

---

### MED-2: Sticky Connection Cleanup

**Location:** `scry-proxy/src/proxy/pool_manager.rs:300-336`

**Current Behavior:**
- Idle timeout: 300 seconds (5 minutes)
- Cleanup only runs periodically
- No force-eviction under pressure

**Problem:**
- Pinned connections hold backend connections for 5 minutes
- Under load, pool fills with sticky connections
- New clients cannot get connections

**Requirements:**
- [ ] Reduce default idle_unpin_secs to 60-120 seconds
- [ ] Add force-eviction when pool is exhausted
- [ ] Implement LRU eviction for sticky connections under pressure
- [ ] Expose sticky connection count in metrics

---

### MED-3: Race Condition in Pool Recycle

**Location:** `scry-proxy/src/proxy/pool_manager.rs:182-230`

**Current Behavior:**
1. Connection released to pool
2. Recycle starts DISCARD ALL
3. New client may acquire same connection
4. Race between recycle completion and new client use

**Problem:**
- No synchronization between recycle and acquire
- New client may read leftover recycle response

**Requirements:**
- [ ] Ensure recycle completes before connection available
- [ ] Use connection state flag during recycle
- [ ] Add synchronization primitive if needed
- [ ] Test with high concurrency acquire/release cycles

---

## Implementation Order

### Phase 1: Critical Protocol Fixes (Required for 300+ connections)
1. CRIT-1: Fix DISCARD ALL response handling
2. CRIT-3: Enforce max_connections
3. HIGH-3: Implement proper message framing

### Phase 2: TLS Support
1. CRIT-2: TLS connection state reset

### Phase 3: Scalability
1. HIGH-1: Pool size defaults and validation
2. HIGH-2: Wait queue improvements
3. HIGH-4: Startup message handling

### Phase 4: Reliability
1. MED-1: Active health checks
2. MED-2: Sticky connection management
3. MED-3: Recycle race condition

---

## Success Criteria

After all fixes implemented:

| Metric | Target |
|--------|--------|
| 500 concurrent clients | No errors |
| Protocol errors | 0 |
| Connection timeouts | < 1% |
| Throughput at 500 clients | > 15,000 qps |
| Memory growth | Bounded, no leaks |
| Latency p99 at 500 clients | < 50ms |

---

## References

- PostgreSQL Frontend/Backend Protocol: https://www.postgresql.org/docs/current/protocol.html
- PgBouncer Architecture: https://www.pgbouncer.org/features.html
- Deadpool crate: https://docs.rs/deadpool/latest/deadpool/
