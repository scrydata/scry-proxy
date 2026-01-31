# Implementation Plan: Connection Pool Warmup

## Problem Statement

The scry-proxy experiences high latency (~2 seconds) on the first queries after startup because backend connections are created lazily. The `pool_min_idle` configuration exists (default: 10) but is not actually implemented - it only logs the value without pre-warming connections.

**Evidence from benchmarks:**
- Max latency: 25ms in recent runs (was ~2s before some optimizations)
- First query to each backend requires TCP handshake + Postgres authentication
- Cold pool causes latency spikes that violate <1ms overhead target

## Current State

### Configuration
File: `scry-proxy/src/config/mod.rs:262`
```rust
pub pool_min_idle: usize,  // Default: 10
```

### Pool Creation (NOT implementing warmup)
File: `scry-proxy/src/proxy/tcp_pool.rs:90-95`
```rust
if let Some(min) = min_idle {
    builder = builder.runtime(deadpool::Runtime::Tokio1);
    // Note: deadpool doesn't have a direct min_idle, but we can
    // pre-warm the pool by creating connections on startup
    debug!(min_idle = min, "Pool min_idle configured");
}
```

The comment acknowledges warmup is needed but it's not implemented.

### Server Startup
File: `scry-proxy/src/proxy/server.rs:430-500`

The `run()` method starts idle cleanup tasks and accept loops but has no warmup step.

## Implementation Plan

### Task 1: Add `warmup()` method to `TcpConnectionPool`

**File:** `scry-proxy/src/proxy/tcp_pool.rs`

Add a new async method that pre-creates connections:

```rust
/// Pre-warm the pool by creating connections up to min_idle count
///
/// This should be called after pool creation but before accepting client
/// connections. Connections are created in parallel for faster warmup.
///
/// # Arguments
/// * `count` - Number of connections to pre-create (typically pool_min_idle)
///
/// # Returns
/// The number of connections successfully created
pub async fn warmup(&self, count: usize) -> usize {
    if count == 0 {
        return 0;
    }

    info!(target_count = count, "Warming up connection pool");

    // Create connections in parallel using join_all
    let futures: Vec<_> = (0..count)
        .map(|_| async {
            match self.pool.get().await {
                Ok(conn) => {
                    // Connection is created and will be returned to pool when dropped
                    drop(conn);
                    true
                }
                Err(e) => {
                    warn!(error = %e, "Failed to create warmup connection");
                    false
                }
            }
        })
        .collect();

    let results = futures::future::join_all(futures).await;
    let created = results.iter().filter(|&&success| success).count();

    if created < count {
        warn!(
            created = created,
            target = count,
            "Pool warmup incomplete - some connections failed"
        );
    } else {
        info!(created = created, "Pool warmup complete");
    }

    created
}
```

**Location:** Add after line 175 (after `protocol()` method), before the closing `}` of `impl TcpConnectionPool`.

**Dependencies to add to `Cargo.toml`:** None (futures is already available via tokio).

### Task 2: Add `warmup()` method to `PoolManager`

**File:** `scry-proxy/src/proxy/pool_manager.rs`

Add a method that delegates to the underlying pool:

```rust
/// Pre-warm the underlying connection pool
///
/// # Arguments
/// * `count` - Number of connections to pre-create
///
/// # Returns
/// The number of connections successfully created
pub async fn warmup(&self, count: usize) -> usize {
    self.pool.warmup(count).await
}
```

**Location:** Add after `pool()` method around line 340.

### Task 3: Add `warmup_pools()` method to `ProxyServer`

**File:** `scry-proxy/src/proxy/server.rs`

Add a public method to warm up all pools:

```rust
/// Warm up all connection pools before accepting client connections
///
/// This pre-creates backend connections to avoid cold-start latency.
/// Should be called between `new()` and `run()`.
///
/// # Arguments
/// * `min_idle` - Number of connections to pre-create per pool
///
/// # Returns
/// Total number of connections created across all pools
pub async fn warmup_pools(&self, min_idle: usize) -> usize {
    if self.pool_managers.is_empty() || min_idle == 0 {
        return 0;
    }

    info!(
        pool_count = self.pool_managers.len(),
        min_idle = min_idle,
        "Warming up connection pools"
    );

    let mut total_created = 0;
    for (db_name, pool_manager) in &self.pool_managers {
        let created = pool_manager.warmup(min_idle).await;
        info!(database = %db_name, created = created, "Pool warmup complete");
        total_created += created;
    }

    info!(total_created = total_created, "All pools warmed up");
    total_created
}
```

**Location:** Add after `reload_sender()` method around line 383.

### Task 4: Call warmup in `main.rs`

**File:** `scry-proxy/src/main.rs`

Find where `ProxyServer::new()` and `server.run()` are called, and add warmup between them:

```rust
// Create server
let server = ProxyServer::new(config.clone()).await?;

// Warm up connection pools before accepting connections
let min_idle = config.performance.pool_min_idle;
if min_idle > 0 {
    server.warmup_pools(min_idle).await;
}

// Run server
server.run().await?;
```

**Location:** In the `main()` function, after server creation but before `run()`.

### Task 5: Add unit tests

**File:** `scry-proxy/src/proxy/tcp_pool.rs` (add to existing `#[cfg(test)]` module)

```rust
#[tokio::test]
async fn test_warmup_zero_count() {
    let protocol = Arc::new(PostgresProtocol::new()) as Arc<dyn Protocol>;
    let config = ProtocolConfig {
        host: "localhost".to_string(),
        port: 5432,
        database: Some("test".to_string()),
        user: Some("postgres".to_string()),
        password: Some("password".to_string()),
    };
    let tls_config = TlsConfig::default();

    let pool = TcpConnectionPool::new(
        protocol,
        config,
        &tls_config,
        10,
        Some(0),
        None,
        None,
        true,
    )
    .unwrap();

    // Warmup with 0 should return immediately
    let created = pool.warmup(0).await;
    assert_eq!(created, 0);
}
```

**Note:** Full integration tests require a running Postgres instance.

### Task 6: Add integration test

**File:** `scry-proxy/tests/integration_tests.rs` (or new file)

Add a test that verifies warmup creates connections:

```rust
#[tokio::test]
async fn test_pool_warmup_creates_connections() {
    // Start Postgres testcontainer
    // Create pool with pool_min_idle=5
    // Call warmup()
    // Verify pool.status().size >= 5
}
```

## Verification Steps

1. **Build:** `just build` - Ensure code compiles
2. **Unit tests:** `just test-unit` - Run unit tests
3. **Integration tests:** `just test-integration` - Run with testcontainers
4. **Benchmark comparison:**
   - Run benchmark without warmup (comment out warmup call)
   - Run benchmark with warmup
   - Compare max latency - should see significant reduction

## Expected Results

- First query latency should drop from ~25ms to <5ms
- Max latency in benchmarks should be closer to p99 latency
- No impact on steady-state throughput or latency

## Rollback Plan

If issues arise, the warmup call in `main.rs` can be removed or made conditional on a new config flag:
```rust
if config.performance.pool_warmup_enabled && min_idle > 0 {
    server.warmup_pools(min_idle).await;
}
```

## Files Modified

| File | Change |
|------|--------|
| `scry-proxy/src/proxy/tcp_pool.rs` | Add `warmup()` method |
| `scry-proxy/src/proxy/pool_manager.rs` | Add `warmup()` delegation |
| `scry-proxy/src/proxy/server.rs` | Add `warmup_pools()` method |
| `scry-proxy/src/main.rs` | Call warmup before run() |
| `scry-proxy/src/proxy/tcp_pool.rs` | Add unit test |
| `scry-proxy/tests/integration_tests.rs` | Add integration test |
