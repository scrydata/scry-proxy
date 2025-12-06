# Connection Pooling Implementation Plan

## Current Architecture

**Model:** 1:1 connection mapping
- Each client connection → dedicated backend Postgres connection
- Connection created in `ConnectionHandler::handle()` (connection.rs:64-67)
- Connection lives for entire client session
- **Benefit:** All stateful Postgres features work perfectly (proven by 20 passing tests)
- **Drawback:** Number of clients limited by Postgres max_connections

## Why Add Connection Pooling?

1. **Scale beyond Postgres connection limit** - Handle 1000s of clients with 100s of backend connections
2. **Reduce connection overhead** - Connection setup/teardown is expensive
3. **Better resource utilization** - Idle client connections don't hold Postgres resources
4. **Faster connection acquisition** - Reuse warm connections from pool

## Pooling Strategy Options

### Option 1: Transaction Pooling (PgBouncer transaction mode)

**How it works:**
- Return connection to pool after each transaction (COMMIT/ROLLBACK)
- Client gets different backend connection for each transaction
- Lowest connection count, highest throughput

**What breaks:**
```sql
-- All of these FAIL with transaction pooling:
SET application_name = 'myapp';        -- Lost between transactions
CREATE TEMP TABLE foo (...);           -- Lost after COMMIT
DECLARE cursor FOR SELECT ...;         -- Cursors only work in transactions
PREPARE stmt AS SELECT $1;             -- Prepared statement lost
SELECT pg_advisory_lock(123);          -- Lock released
LISTEN notifications;                  -- Subscription lost
```

**What works:**
```sql
-- Simple transaction patterns work fine:
BEGIN;
INSERT INTO users VALUES (...);
COMMIT;

-- Autocommit queries:
SELECT * FROM users WHERE id = 1;
```

**Verdict:** ❌ Breaks 90% of our stateful tests. Too restrictive for general proxy.

---

### Option 2: Session Pooling (PgBouncer session mode)

**How it works:**
- Connection assigned to client for entire session
- Returned to pool only when client disconnects
- **This is basically what we have now!**

**What works:** ✅ Everything (all 20 stateful tests pass)

**Benefit over current:**
- Reuse connections from pool (faster than creating new connection)
- Can have pool warmup, health checks, connection limits

**Drawback:**
- Doesn't reduce connection count (still 1:1 while client active)
- Only helps with connection creation overhead

**Verdict:** ⚠️ Safe but limited benefit. Good first step.

---

### Option 3: Hybrid / Smart Pooling (Recommended)

**How it works:**
- Track client session state dynamically
- When client is "stateless" → return connection to pool
- When client enters stateful mode → pin connection until state clears
- Automatically detect state transitions

**State Detection:**

```rust
enum ClientState {
    Stateless,           // Can use any connection from pool
    InTransaction,       // Pinned until COMMIT/ROLLBACK
    HasTempTables,       // Pinned until disconnect or DISCARD
    HasCursors,          // Pinned until cursors closed
    HasPreparedStmts,    // Pinned until DEALLOCATE
    HasSessionVars,      // Pinned until RESET or DISCARD
    HasAdvisoryLocks,    // Pinned until unlocked
    HasNotifications,    // Pinned until UNLISTEN
}
```

**State Transitions:**

```
Client connects
    ↓
[STATELESS] ← can borrow any connection from pool
    ↓
BEGIN / SET / CREATE TEMP / DECLARE / PREPARE / LISTEN
    ↓
[STATEFUL] ← connection pinned to this client
    ↓
COMMIT / ROLLBACK / CLOSE / DEALLOCATE / UNLISTEN / DISCARD
    ↓
[STATELESS] ← return connection to pool
```

**Example Flow:**

```sql
-- Client A connects, gets connection #1 from pool
SELECT * FROM users;                    -- [STATELESS] - quick query
-- Connection #1 returned to pool

-- Client B connects, gets connection #1 (reused!)
SELECT * FROM orders;                   -- [STATELESS]
-- Connection #1 returned to pool

-- Client A makes new query
BEGIN;                                  -- [STATEFUL] - gets connection #2, PINNED
INSERT INTO users VALUES (...);
COMMIT;                                 -- [STATELESS] - return connection #2
-- Connection #2 back in pool

-- Client A continues
CREATE TEMP TABLE session_data (...);   -- [STATEFUL] - gets connection #3, PINNED
-- Connection stays pinned until client disconnects
```

**Benefits:**
- ✅ Reduces connection count for stateless workloads
- ✅ All stateful features work when needed
- ✅ Automatic - no client changes required
- ✅ Progressive - can handle mixed workloads

**Complexity:**
- Must parse all queries to detect state changes
- Complex state tracking logic
- Edge cases (DISCARD ALL, connection reset)

**Verdict:** ⭐ Best balance of compatibility and efficiency. Most complex.

---

## Implementation Plan: Hybrid Pooling

### Phase 1: Add Session Pooling (Safe First Step)

**Goal:** Improve connection reuse without breaking anything

1. **Add `deadpool-postgres` pool** (already in dependencies)
2. **Pool lifecycle:**
   - Create pool at proxy startup with configured size
   - Acquire connection when client connects
   - Return when client disconnects
3. **No state tracking yet** - keep 1:1 mapping while client active
4. **Benefits:**
   - Faster connection acquisition (reuse warm connections)
   - Connection health checks
   - Configurable pool size and timeouts

**Changes Required:**
```rust
// src/proxy/server.rs
pub struct ProxyServer {
    pool: Pool<PostgresConnectionManager>,  // Add pool
    // ...
}

// src/proxy/connection.rs
impl ConnectionHandler {
    pub async fn handle(mut self) -> Result<()> {
        // Get connection from pool instead of creating new
        let backend_conn = self.pool.get().await?;

        // Use connection for entire client session (existing logic)
        // ...

        // Connection automatically returned to pool when dropped
    }
}
```

**Tests:** All 20 stateful tests should still pass (behavior unchanged)

---

### Phase 2: Add State Tracking

**Goal:** Detect when client is stateless vs stateful

1. **Create `ConnectionState` tracker:**

```rust
// src/proxy/state_tracker.rs
pub struct ConnectionState {
    in_transaction: bool,
    isolation_level: Option<IsolationLevel>,
    savepoints: Vec<String>,
    open_cursors: HashSet<String>,
    prepared_statements: HashSet<String>,
    session_variables: HashMap<String, String>,
    temp_tables: HashSet<String>,
    advisory_locks: HashSet<i64>,
    listen_channels: HashSet<String>,
}

impl ConnectionState {
    pub fn is_stateless(&self) -> bool {
        !self.in_transaction
            && self.open_cursors.is_empty()
            && self.prepared_statements.is_empty()
            && self.session_variables.is_empty()
            && self.temp_tables.is_empty()
            && self.advisory_locks.is_empty()
            && self.listen_channels.is_empty()
    }

    pub fn update_from_query(&mut self, query: &str) {
        // Parse query and update state
        // BEGIN -> in_transaction = true
        // COMMIT -> in_transaction = false, clear savepoints
        // SET -> update session_variables
        // CREATE TEMP TABLE -> add to temp_tables
        // etc.
    }
}
```

2. **Query pattern matching:**

```rust
// Regex patterns or parser for detecting state changes
const STATE_PATTERNS: &[(&str, StateChange)] = &[
    (r"(?i)^\s*BEGIN", StateChange::EnterTransaction),
    (r"(?i)^\s*COMMIT", StateChange::ExitTransaction),
    (r"(?i)^\s*ROLLBACK", StateChange::ExitTransaction),
    (r"(?i)^\s*SET\s+(\w+)", StateChange::SetVariable),
    (r"(?i)^\s*CREATE\s+TEMP", StateChange::CreateTempTable),
    (r"(?i)^\s*DECLARE\s+(\w+)", StateChange::DeclareCursor),
    (r"(?i)^\s*CLOSE\s+(\w+)", StateChange::CloseCursor),
    // ... more patterns
];
```

3. **Track state through message extraction:**
   - Extend `MessageExtractor` to detect state-changing queries
   - Update `ConnectionState` for each query
   - Log state transitions for debugging

**Tests:** Create new `state_tracker_test.rs` with state transition scenarios

---

### Phase 3: Implement Dynamic Connection Pinning

**Goal:** Return connections to pool when stateless

1. **Connection pinning logic:**

```rust
// src/proxy/connection.rs
pub struct ConnectionHandler {
    pool: Pool,
    pinned_connection: Option<PooledConnection>,
    state: ConnectionState,
}

impl ConnectionHandler {
    async fn get_connection(&mut self) -> Result<&mut PooledConnection> {
        // If we have a pinned connection, use it
        if let Some(conn) = &mut self.pinned_connection {
            return Ok(conn);
        }

        // Otherwise get from pool and pin it
        let conn = self.pool.get().await?;
        self.pinned_connection = Some(conn);
        Ok(self.pinned_connection.as_mut().unwrap())
    }

    async fn maybe_unpin_connection(&mut self) {
        // After each query completion, check state
        if self.state.is_stateless() {
            // Return connection to pool
            self.pinned_connection = None;
            debug!("Connection returned to pool (stateless)");
        }
    }
}
```

2. **Integration into bidirectional forwarding:**
   - After each query completes, check if state is clean
   - If stateless, drop `pinned_connection` (returns to pool)
   - Next query will acquire new connection from pool

3. **Edge cases to handle:**
   - Client disconnects mid-transaction → connection needs cleanup before returning to pool
   - Connection errors → don't return corrupted connection
   - `DISCARD ALL` command → clears all state
   - Implicit transactions (some queries auto-commit)

**Tests:**
- New `pooling_test.rs` to verify connections are reused
- Verify stateful tests still pass with pooling enabled

---

## State Tracking Details

### Transaction State

**Detect:**
- `BEGIN` / `START TRANSACTION` → enter transaction
- `COMMIT` / `END` → exit transaction
- `ROLLBACK` → exit transaction
- `SAVEPOINT name` → add savepoint
- `ROLLBACK TO name` → rollback to savepoint
- `RELEASE name` → remove savepoint

**Track:**
- `in_transaction: bool`
- `isolation_level: Option<IsolationLevel>`
- `savepoints: Vec<String>`

### Cursors

**Detect:**
- `DECLARE cursor_name CURSOR FOR ...` → open cursor
- `CLOSE cursor_name` → close cursor
- `COMMIT` / `ROLLBACK` → closes all cursors (unless WITH HOLD)

**Track:**
- `open_cursors: HashMap<String, CursorInfo>`
- `CursorInfo { name, with_hold, position }`

### Session Variables

**Detect:**
- `SET var = value` → set variable
- `SET LOCAL var = value` → transaction-scoped variable
- `RESET var` → clear variable
- `DISCARD ALL` → clear all variables

**Track:**
- `session_variables: HashMap<String, (String, Scope)>`
- `Scope::Session` or `Scope::Transaction`

### Temporary Tables

**Detect:**
- `CREATE TEMP TABLE name` → create temp table
- `DROP TABLE name` → might drop temp table
- `ON COMMIT DROP` → auto-drop after commit
- `ON COMMIT DELETE ROWS` → keep table, delete data

**Track:**
- `temp_tables: HashMap<String, TempTableInfo>`
- `TempTableInfo { name, on_commit_action }`

### Prepared Statements

**Detect:**
- `PREPARE name AS ...` → create prepared statement
- `DEALLOCATE name` → remove prepared statement
- `DEALLOCATE ALL` → remove all

**Track:**
- `prepared_statements: HashSet<String>`

### Advisory Locks

**Detect:**
- `SELECT pg_advisory_lock(id)` → acquire lock
- `SELECT pg_advisory_unlock(id)` → release lock
- `SELECT pg_advisory_xact_lock(id)` → transaction-scoped lock
- `COMMIT` / `ROLLBACK` → release xact locks

**Track:**
- `advisory_locks: HashMap<i64, LockType>`
- `LockType::Session` or `LockType::Transaction`

### LISTEN/NOTIFY

**Detect:**
- `LISTEN channel` → subscribe
- `UNLISTEN channel` → unsubscribe
- `UNLISTEN *` → unsubscribe all

**Track:**
- `listen_channels: HashSet<String>`

---

## Configuration

```toml
[performance]
# Connection pooling strategy:
# - "disabled": No pooling, 1:1 client-to-backend (current behavior)
# - "session": Pool connections, assign for entire client session
# - "hybrid": Smart pooling with automatic state tracking and dynamic pinning
connection_pooling = "session"     # "disabled" | "session" | "hybrid"

# Pool configuration (only applies when connection_pooling != "disabled")
pool_size = 100                    # Max backend connections
pool_min_idle = 10                 # Keep 10 connections warm
pool_timeout_secs = 30             # Wait 30s to get connection from pool
pool_recycle_secs = 3600           # Recycle connections after 1h

# Hybrid pooling options (only applies when connection_pooling = "hybrid")
# When true, unpin connections even with session variables set
# (assumes app will re-set variables on next query if needed)
pool_aggressive_unpinning = false
```

---

## Testing Strategy

### Pooling Tests

1. **Connection Reuse Test:**
   - Create 10 clients executing simple queries
   - Verify they use < 10 backend connections (reuse happening)

2. **State Pinning Test:**
   - Client starts transaction → verify connection stays pinned
   - Client commits → verify connection returned to pool
   - Next query → verify gets different connection (from pool)

3. **Concurrent Transactions Test:**
   - 100 clients, each running transaction
   - Verify pool doesn't deadlock
   - Verify transactions complete correctly

4. **Pool Exhaustion Test:**
   - Create `pool_size + 1` clients in long transactions
   - Verify last client waits for timeout
   - Verify error handling when pool exhausted

5. **Mixed Workload Test:**
   - Some clients doing simple queries (stateless)
   - Some clients in long transactions (stateful)
   - Verify stateless queries share connections
   - Verify stateful transactions stay pinned

### Regression Tests

- **All 20 stateful tests must still pass** with pooling enabled
- Verify test with `connection_pooling = false` (current behavior)
- Verify tests with `connection_pooling = true` (new behavior)

---

## Metrics to Add

```rust
pub struct PoolingMetrics {
    pub connections_created: u64,
    pub connections_reused: u64,
    pub connections_in_use: u64,
    pub connections_idle: u64,
    pub pool_wait_time_ms: Histogram,
    pub state_transitions: Counter,
    pub pinned_connections: Gauge,
}
```

Expose via `/metrics` endpoint for monitoring.

---

## Migration Path

### Phase 1: Session Pooling (Low Risk)
- Timeline: 1-2 days
- Add pool, use for entire client session
- No behavior change, just reuse
- All tests pass

### Phase 2: State Tracking (Medium Risk)
- Timeline: 3-5 days
- Implement state detection
- Log state transitions (don't act on them yet)
- Validate state tracking accuracy

### Phase 3: Dynamic Pinning (High Risk)
- Timeline: 5-7 days
- Unpin connections when stateless
- Extensive testing
- Gradual rollout with feature flag

### Total: 2-3 weeks for full hybrid pooling

---

## Alternative: PgBouncer Integration

Instead of implementing pooling in the proxy, could deploy PgBouncer separately:

```
Clients → Scry Proxy → PgBouncer → Postgres
```

**Pros:**
- PgBouncer is battle-tested for pooling
- Offload complexity
- Scry focuses on observability

**Cons:**
- Additional infrastructure
- Two proxies in path (latency?)
- PgBouncer can't pool stateful connections well either
- Doesn't solve our core problem

**Verdict:** Implement pooling in Scry for better control and observability integration.

---

## Risks & Mitigations

### Risk: State tracking bugs
- **Impact:** Connections returned to pool with dirty state
- **Mitigation:**
  - Conservative defaults (don't unpin unless certain)
  - Connection validation before returning to pool
  - Comprehensive test coverage
  - Feature flag to disable

### Risk: Performance regression
- **Impact:** State tracking overhead > pooling benefit
- **Mitigation:**
  - Benchmark with/without pooling
  - Optimize state tracking (lazy evaluation)
  - Make configurable

### Risk: Edge cases break stateful features
- **Impact:** Our 20 tests start failing
- **Mitigation:**
  - Tests run continuously in CI
  - Gradual rollout with monitoring
  - Easy rollback via config

---

## Recommendation

**Start with Phase 1 (Session Pooling):**
- Low risk, immediate benefit (connection reuse)
- Builds foundation for hybrid approach
- Validates pool integration with existing code
- Can ship to production quickly

**Then add Phase 2/3 if needed:**
- Measure actual connection pressure in production
- If mostly stateless workload → big win from hybrid
- If mostly stateful → session pooling is sufficient

**Configuration:**
```toml
[performance]
connection_pooling = "session"  # Start conservative
pool_size = 100
```

Later upgrade to:
```toml
connection_pooling = "hybrid"   # Automatic state tracking enabled
pool_size = 100
```

This gives us a clear, incremental path forward! 🚀
