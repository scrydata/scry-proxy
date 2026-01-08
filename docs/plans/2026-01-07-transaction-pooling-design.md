# Transaction Pooling Design

**Date:** 2026-01-07
**Status:** Draft
**Author:** Brainstorming session

## Overview & Goals

This design adds two connection pooling modes to Scry, enabling it to fully replace PgBouncer:

**Transaction Mode** (strict, PgBouncer-compatible)
- Connection returns to pool after each transaction (COMMIT/ROLLBACK)
- Hard errors on session state operations (SET, temp tables, cursors, advisory locks)
- Prepared statements handled via transparent re-preparation
- For users who want predictable, strict pooling behavior

**Hybrid Mode** (smart, default)
- Connection returns to pool after transaction *unless* pinned by session state
- Automatically detects and tracks: prepared statements, SET variables, temp tables, cursors, advisory locks
- Pins connection when state exists, unpins when state is cleared
- Works with unmodified clients, maximum efficiency

### Design Principles

- Zero-copy streaming where possible (no response buffering)
- Reuse state tracking for both pinning decisions and transparent reconnection
- Bounded queue depth to prevent memory runaway under load
- Comprehensive metrics for observability

### Non-Goals

- Statement-level pooling (rarely used, high complexity)
- Authentication passthrough (separate feature)
- Admin console (separate feature)

---

## Architecture & Components

### New Components

```
┌─────────────────────────────────────────────────────────────────┐
│                        ConnectionPool                            │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │
│  │ Backend 1   │  │ Backend 2   │  │ Backend 3   │  ...         │
│  │ (idle)      │  │ (pinned)    │  │ (in-use)    │              │
│  └─────────────┘  └─────────────┘  └─────────────┘              │
│                                                                  │
│  ┌──────────────────────┐  ┌──────────────────────┐             │
│  │ ConnectionSelector   │  │ WaitQueue            │             │
│  │ - LIFO for txn mode  │  │ - bounded depth (50) │             │
│  │ - sticky for hybrid  │  │ - timeout per waiter │             │
│  └──────────────────────┘  └──────────────────────┘             │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                     ConnectionState (per backend)                │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
│  │ PreparedStmtMap │  │ SessionVarMap   │  │ PinTracker      │  │
│  │ (existing cache)│  │ name → value    │  │ - temp_tables   │  │
│  └─────────────────┘  └─────────────────┘  │ - cursors       │  │
│                                            │ - advisory_locks│  │
│                                            └─────────────────┘  │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                  TransactionTracker (per client)                 │
│  - state: Idle | InTransaction | InError                        │
│  - updated via ReadyForQuery ('I', 'T', 'E')                    │
└─────────────────────────────────────────────────────────────────┘
```

### Modified Components

- `ConnectionHandler` - integrates with pool based on mode, routes through state tracking
- `PreparedStatementCache` - already exists, becomes part of `ConnectionState`

---

## Data Flow

### Client Request Flow (Query Execution)

```
Client                    Scry                         Backend Pool
  │                        │                               │
  │──── Query/Parse ──────>│                               │
  │                        │                               │
  │                        │  ┌─────────────────────────┐  │
  │                        │  │ 1. Check TransactionTracker│
  │                        │  │    - If Idle: need conn    │
  │                        │  │    - If InTxn: use current │
  │                        │  └─────────────────────────┘  │
  │                        │                               │
  │                        │  ┌─────────────────────────┐  │
  │                        │  │ 2. Acquire Connection     │
  │                        │  │    - Hybrid: sticky pref  │
  │                        │  │    - Txn mode: LIFO       │
  │                        │  │    - Queue if exhausted   │
  │                        │  └─────────────────────────┘  │
  │                        │                               │
  │                        │──── Forward Query ──────────>│
  │                        │                               │
  │                        │<─── Response stream ─────────│
  │                        │                               │
  │                        │  ┌─────────────────────────┐  │
  │                        │  │ 3. Scan for ReadyForQuery │
  │                        │  │    (streaming, no buffer) │
  │                        │  │    Update txn state       │
  │                        │  └─────────────────────────┘  │
  │                        │                               │
  │<─── Response stream ───│                               │
  │                        │                               │
  │                        │  ┌─────────────────────────┐  │
  │                        │  │ 4. If txn complete:       │
  │                        │  │    - Txn mode: release    │
  │                        │  │    - Hybrid: check pins   │
  │                        │  └─────────────────────────┘  │
```

### State Detection Flow (Hybrid Mode)

```
Client sends command ──> Parse SQL for state-changing operations
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
   SET var=val         CREATE TEMP TABLE      PREPARE stmt
        │                     │                     │
        ▼                     ▼                     ▼
   Store in             Add to PinTracker      Add to PreparedStmtCache
   SessionVarMap        (unsafe_state=true)    (replayable=true)
        │                     │                     │
        └─────────────────────┼─────────────────────┘
                              ▼
                     Connection now PINNED
```

### Unpin Flow (Hybrid Mode)

```
                    Transaction Ends (ReadyForQuery 'I')
                              │
                              ▼
                    Check PinTracker
                              │
          ┌───────────────────┼───────────────────┐
          ▼                   ▼                   ▼
    No pins remaining   Only safe pins      Unsafe pins exist
          │             (prepared, SET)           │
          ▼                   │                   ▼
    Release to pool           │            Stay pinned
                              ▼                   │
                    Start idle timer              │
                              │                   │
                              ▼                   │
                    Timeout expires ──────────────┘
                              │
                              ▼
                    Release to pool (DISCARD ALL)
```

### Transaction Boundary Detection

Transaction state is tracked via the `ReadyForQuery` message from Postgres:
- Message type `Z` (1 byte) + length (4 bytes, always 5) + status (1 byte)
- Status values: `I` = idle, `T` = in transaction, `E` = error

This is detected via **streaming scan** with zero buffering:
1. Forward response bytes to client as they arrive
2. Track message boundaries (type byte + length prefix)
3. When message type `Z` is seen, peek at status byte
4. Update transaction state, continue forwarding

**Memory overhead: ~0 bytes**

---

## Error Handling

### Backend Connection Failure

```
Connection dies
      │
      ▼
Check client state
      │
      ├─── InTransaction ───────────> Propagate error to client
      │                               (transaction is lost)
      │
      ├─── Idle + unsafe pins ──────> Propagate error to client
      │    (temp tables, cursors,     (state cannot be replayed)
      │     advisory locks)
      │
      └─── Idle + safe pins only ───> Transparent reconnect
           (prepared stmts, SET vars)       │
                                            ▼
                                     Acquire new backend
                                            │
                                            ▼
                                     Replay safe state:
                                     - Re-send Parse messages
                                     - Re-send SET commands
                                            │
                                            ▼
                                     Continue as normal
```

### State Tracking for Reconnection

| State Type | Tracked For Pinning | Safe to Replay | How |
|------------|--------------------|-----------------|----|
| Prepared statements | Yes (PreparedStatementCache has SQL) | Yes | Re-send Parse messages |
| SET variables | Yes (capture SET commands) | Yes | Re-send SET commands |
| Temp tables | Yes (detect CREATE TEMP) | No | Data is lost |
| Cursors | Yes (detect DECLARE) | No | Position is lost |
| Advisory locks | Yes (detect pg_advisory_lock) | No | Semantics too risky |

### Transaction Mode Violations

When a client sends a state-changing command in transaction mode:

| Command | Response |
|---------|----------|
| `SET variable` (outside txn) | Error: `session variables not supported in transaction pooling mode` |
| `CREATE TEMP TABLE` | Error: `temporary tables not supported in transaction pooling mode` |
| `DECLARE CURSOR` (WITH HOLD) | Error: `cursors not supported in transaction pooling mode` |
| `pg_advisory_lock()` | Error: `advisory locks not supported in transaction pooling mode` |
| `PREPARE` | Allowed (transparent re-preparation handles it) |
| `SET` inside transaction | Allowed (scoped to transaction, lost on commit anyway) |

### Pool Exhaustion

```
Client requests connection
         │
         ▼
    Pool has idle connection? ──yes──> Return connection
         │ no
         ▼
    Queue depth < max (50)? ──yes──> Add to wait queue
         │ no                              │
         ▼                                 ▼
    Reject immediately              Wait up to pool_timeout_secs
    Error: "connection pool              │
           exhausted"                    ▼
                                   Timeout? ──yes──> Error: "timeout
                                         │            waiting for connection"
                                         │ no
                                         ▼
                                   Return connection
```

---

## Configuration

### Configuration Loading Priority

```
1. Scry environment variables (SCRY_*)      ──┐
2. Scry config file (scry.toml)               ├── Highest priority
3. PgBouncer environment variables            │
4. PgBouncer INI file (pgbouncer.ini)       ──┘ Lowest priority
```

### PgBouncer INI File Support

```bash
# Point Scry at existing pgbouncer.ini
SCRY_PGBOUNCER_CONFIG=/etc/pgbouncer/pgbouncer.ini

# Or auto-detect in standard locations:
# - /etc/pgbouncer/pgbouncer.ini
# - ./pgbouncer.ini
```

### PgBouncer INI Mapping

```ini
; pgbouncer.ini
[pgbouncer]
pool_mode = transaction          ; → connection_pooling = "transaction"
default_pool_size = 20           ; → pool_size = 20
min_pool_size = 5                ; → pool_min_idle = 5
reserve_pool_size = 5            ; → pool_queue_depth = 5
reserve_pool_timeout = 3         ; → (added to pool_timeout_secs logic)
max_client_conn = 1000           ; → proxy.max_connections = 1000
query_timeout = 30               ; → (new: query_timeout_secs)
server_lifetime = 3600           ; → pool_recycle_secs = 3600
server_idle_timeout = 600        ; → pool_idle_unpin_secs = 600
server_connect_timeout = 5       ; → backend.connection_timeout_ms = 5000

[databases]
mydb = host=localhost port=5432 dbname=mydb
; → backend.host, backend.port, backend.database
```

### PgBouncer Environment Variable Aliases

| PgBouncer Env Var | Scry Equivalent |
|-------------------|-----------------|
| `PGBOUNCER_POOL_MODE` | `connection_pooling` |
| `PGBOUNCER_DEFAULT_POOL_SIZE` | `pool_size` |
| `PGBOUNCER_MIN_POOL_SIZE` | `pool_min_idle` |
| `PGBOUNCER_MAX_CLIENT_CONN` | `proxy.max_connections` |
| `PGBOUNCER_SERVER_LIFETIME` | `pool_recycle_secs` |
| `PGBOUNCER_QUERY_TIMEOUT` | `query_timeout_secs` |

### Scry Native Config (Full)

```toml
[performance]
connection_pooling = "hybrid"     # disabled | session | transaction | hybrid
pool_size = 100
pool_min_idle = 10
pool_queue_depth = 50
pool_timeout_secs = 30
pool_idle_unpin_secs = 60
pool_recycle_secs = 3600
pool_lifo = true

[proxy]
max_connections = 1000
query_timeout_secs = 0            # 0 = disabled

[backend]
host = "localhost"
port = 5432
connection_timeout_ms = 5000
```

### Unsupported PgBouncer Settings

These PgBouncer settings have no Scry equivalent (documented for migration):

| Setting | Reason |
|---------|--------|
| `auth_type`, `auth_file` | Auth passthrough not yet implemented |
| `admin_users`, `stats_users` | Admin console not yet implemented |
| `server_reset_query` | Scry uses DISCARD ALL unconditionally |
| `ignore_startup_parameters` | Scry handles all startup params |
| `application_name_add_host` | Use Scry's observability instead |

---

## Metrics

### Connection Metrics

| Metric | Description |
|--------|-------------|
| `scry_pool_connections_pinned` | Currently pinned connections (hybrid mode) |
| `scry_pool_pin_reason{reason="..."}` | Why connections are pinned (prepared_stmt, set_var, temp_table, cursor, advisory_lock) |
| `scry_pool_queue_depth` | Clients waiting for a connection |
| `scry_pool_queue_rejected_total` | Clients rejected due to full queue |

### Efficiency Metrics

| Metric | Description |
|--------|-------------|
| `scry_pool_reuse_total` | Connections reused (sticky hits) |
| `scry_pool_reassign_total` | Client got different backend than before |
| `scry_pool_unpin_total{reason="..."}` | How connections unpinned (explicit_reset, state_cleared, idle_timeout) |

### Latency Metrics

| Metric | Description |
|--------|-------------|
| `scry_pool_wait_seconds` | Time spent waiting for connection (histogram) |
| `scry_pool_replay_seconds` | Time spent replaying state on reconnect (histogram) |

### Error Metrics

| Metric | Description |
|--------|-------------|
| `scry_pool_reconnect_total{result="..."}` | Transparent reconnect attempts (success, failed) |
| `scry_pool_transaction_mode_rejected_total{reason="..."}` | Commands rejected in strict transaction mode |

---

## Testing

### 6.1 Pooling Mode Selection

| Test | Description |
|------|-------------|
| `test_disabled_mode_no_pooling` | Each client gets dedicated backend, no reuse |
| `test_session_mode_sticky` | Client keeps same backend for entire session |
| `test_transaction_mode_release_on_commit` | Connection returns to pool after COMMIT |
| `test_transaction_mode_release_on_rollback` | Connection returns to pool after ROLLBACK |
| `test_hybrid_mode_default` | Verify hybrid is default when not specified |
| `test_hybrid_mode_unpin_after_transaction` | Connection released when no pins |

### 6.2 Transaction Boundary Detection

| Test | Description |
|------|-------------|
| `test_ready_for_query_idle` | Detect 'I' status, transaction complete |
| `test_ready_for_query_in_transaction` | Detect 'T' status, hold connection |
| `test_ready_for_query_error` | Detect 'E' status, handle failed transaction |
| `test_implicit_transaction_autocommit` | Single statement without BEGIN releases |
| `test_explicit_transaction_begin_commit` | BEGIN...COMMIT cycle |
| `test_nested_savepoints` | SAVEPOINT doesn't affect outer transaction state |
| `test_streaming_large_result` | ReadyForQuery detected without buffering 1GB result |

### 6.3 Hybrid Mode Pinning

| Test | Description |
|------|-------------|
| `test_pin_on_prepared_statement` | PREPARE pins connection |
| `test_pin_on_set_variable` | SET outside transaction pins |
| `test_pin_on_temp_table` | CREATE TEMP TABLE pins |
| `test_pin_on_cursor_with_hold` | DECLARE ... WITH HOLD pins |
| `test_pin_on_advisory_lock` | pg_advisory_lock() pins |
| `test_no_pin_set_in_transaction` | SET inside transaction doesn't pin (scoped) |
| `test_multiple_pin_reasons` | Connection with multiple pins tracked correctly |

### 6.4 Hybrid Mode Unpinning

| Test | Description |
|------|-------------|
| `test_unpin_on_deallocate` | DEALLOCATE removes prepared stmt pin |
| `test_unpin_on_deallocate_all` | DEALLOCATE ALL clears all prepared stmt pins |
| `test_unpin_on_reset_variable` | RESET var removes that SET pin |
| `test_unpin_on_reset_all` | RESET ALL clears all SET pins |
| `test_unpin_on_discard_all` | DISCARD ALL clears all safe pins |
| `test_unpin_on_drop_temp_table` | DROP clears temp table pin |
| `test_unpin_on_close_cursor` | CLOSE cursor removes cursor pin |
| `test_unpin_on_advisory_unlock` | pg_advisory_unlock() removes lock pin |
| `test_unpin_idle_timeout` | Safe pins cleared after idle period |
| `test_no_unpin_unsafe_state` | Temp tables don't unpin on idle timeout |
| `test_partial_unpin` | One pin removed, others remain, still pinned |

### 6.5 Transaction Mode Strict Errors

| Test | Description |
|------|-------------|
| `test_txn_mode_error_set_outside_transaction` | SET rejected with error |
| `test_txn_mode_error_temp_table` | CREATE TEMP TABLE rejected |
| `test_txn_mode_error_cursor_with_hold` | WITH HOLD cursor rejected |
| `test_txn_mode_error_advisory_lock` | Advisory lock rejected |
| `test_txn_mode_allow_set_in_transaction` | SET inside BEGIN...COMMIT allowed |
| `test_txn_mode_allow_prepare` | PREPARE allowed (transparent re-prep) |
| `test_txn_mode_error_matches_pgbouncer` | Error messages match PgBouncer format |

### 6.6 Prepared Statement Re-preparation

| Test | Description |
|------|-------------|
| `test_reprep_after_connection_switch` | Statement works on new backend |
| `test_reprep_preserves_parameter_types` | Re-prepared with correct types |
| `test_reprep_multiple_statements` | Multiple statements all re-prepared |
| `test_reprep_lru_eviction` | Evicted statements still re-prepare |
| `test_reprep_latency_overhead` | Re-preparation adds <1ms |

### 6.7 Connection Selection

| Test | Description |
|------|-------------|
| `test_lifo_selection` | Most recently returned connection selected |
| `test_sticky_preference_hybrid` | Same backend returned if available |
| `test_sticky_fallback_lifo` | Different backend if sticky not available |
| `test_no_sticky_transaction_mode` | Transaction mode uses pure LIFO |

### 6.8 Pool Exhaustion & Queueing

| Test | Description |
|------|-------------|
| `test_queue_when_pool_exhausted` | Clients wait when no connections |
| `test_queue_depth_limit_reject` | Client 51 rejected when queue full |
| `test_queue_timeout` | Waiting client times out with error |
| `test_queue_fifo_fairness` | First waiter gets first available |
| `test_queue_metrics_depth` | Queue depth metric accurate |

### 6.9 Error Handling & Recovery

| Test | Description |
|------|-------------|
| `test_backend_death_in_transaction` | Error propagated to client |
| `test_backend_death_idle_no_state` | Transparent reconnect |
| `test_backend_death_idle_safe_state` | Reconnect + replay prepared stmts |
| `test_backend_death_idle_safe_state_set` | Reconnect + replay SET vars |
| `test_backend_death_idle_unsafe_state` | Error propagated (temp table) |
| `test_replay_failure_propagates` | If replay fails, error to client |
| `test_circuit_breaker_integration` | Pool respects circuit breaker state |

### 6.10 PgBouncer Configuration Compatibility

| Test | Description |
|------|-------------|
| `test_pgbouncer_ini_parsing` | Loads pgbouncer.ini correctly |
| `test_pgbouncer_ini_pool_mode_transaction` | pool_mode mapped correctly |
| `test_pgbouncer_ini_database_section` | [databases] section parsed |
| `test_pgbouncer_env_vars` | PGBOUNCER_* env vars work |
| `test_scry_config_overrides_pgbouncer` | Scry settings take priority |
| `test_unsupported_settings_warned` | Unknown settings log warning |

### 6.11 Metrics

| Test | Description |
|------|-------------|
| `test_metric_connections_pinned` | Pinned count accurate |
| `test_metric_pin_reason_breakdown` | Per-reason counters correct |
| `test_metric_queue_depth` | Queue depth tracked |
| `test_metric_queue_rejected` | Rejection counter increments |
| `test_metric_reuse_vs_reassign` | Sticky hits vs misses tracked |
| `test_metric_unpin_reasons` | Unpin reason counters correct |
| `test_metric_wait_histogram` | Queue wait time recorded |
| `test_metric_replay_histogram` | Replay time recorded |

---

## Future Work

### PgBouncer Feature Parity

Features not in this design but needed for complete PgBouncer replacement:

| Feature | Priority | Description |
|---------|----------|-------------|
| **TLS Support** | High | Client-side and backend TLS connections |
| **Authentication** | High | `auth_type`, `auth_file`, user/password mapping |
| **Admin Console** | Medium | Virtual database for SHOW/PAUSE/RESUME commands |
| **Multi-database** | Medium | `[databases]` section with multiple backends |
| **User Mapping** | Medium | Different pool settings per user |
| **Online Reload** | Low | RELOAD command, SIGHUP config reload |
| **UNIX Sockets** | Low | Listen on unix socket path |
| **DNS Caching** | Low | `dns_max_ttl`, `dns_nxdomain_ttl` |

### Potential Enhancements

| Feature | Description |
|---------|-------------|
| **Read Replica Routing** | Route read-only queries to replicas (framework supports this) |
| **Statement Pooling** | Most aggressive pooling mode (low priority) |
| **Query Caching** | Cache results for identical queries |

---

## Appendix: Decision Log

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Default pooling mode | Hybrid | "Just works" for most apps, Scry's differentiator |
| Prepared statement handling | Transparent re-preparation | Better than PgBouncer, leverages existing cache |
| Transaction mode violations | Hard errors | Strict means strict, matches PgBouncer |
| Transaction boundary detection | ReadyForQuery streaming | Source of truth, zero memory overhead |
| Connection selection | LIFO + sticky (hybrid) | Warm connections, state reuse |
| Pool exhaustion | Bounded queue (50) | Prevents memory runaway |
| Backend failure recovery | Transparent reconnect for safe state | Leverages existing state tracking |
| PgBouncer config support | Both env vars and INI file | True drop-in replacement |
