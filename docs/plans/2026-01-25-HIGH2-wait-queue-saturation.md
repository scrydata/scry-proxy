# Implementation Plan: HIGH-2 Wait Queue Saturation

**Date:** 2026-01-25
**Issue:** HIGH-2 from CONNECTION_MULTIPLEXING_REQUIREMENTS.md
**Status:** COMPLETE

## Problem Statement

From the requirements document:

> **Location:** `scry-proxy/src/proxy/wait_queue.rs:49`
>
> **Current Behavior:**
> ```rust
> if waiters.len() >= self.max_depth {
>     return Err(QueueFullError);  // Hard rejection
> }
> ```
>
> **Problem:**
> - Default queue depth: 50-100
> - Clients beyond pool_size + queue_depth are rejected immediately
> - No backoff or retry suggestion
> - Error message not helpful to client
>
> **Requirements:**
> - [ ] Increase default queue depth for production (500-1000)
> - [ ] Add configurable backpressure behavior:
>   - Reject immediately (current)
>   - Wait with timeout
>   - Return "server busy" with retry-after hint
> - [ ] Expose queue depth in metrics
> - [ ] Alert when queue consistently > 80% full

## Current State Analysis

### What Already Works (HIGH-1 completed)

1. **Improved Defaults** (`config/mod.rs`):
   - `pool_queue_depth = 500` (was 50-100)
   - `pool_size = 50` (rationalized)
   - Validation warnings for misconfigured ratios

2. **Metrics Already Exposed** (`observability/prometheus.rs`):
   - `scry_pool_queue_depth` - Current queue depth
   - `scry_pool_queue_max_depth` - Max queue depth from config
   - `scry_pool_queue_saturation_ratio` - Queue saturation (0.0-1.0)
   - `scry_pool_queue_rejected_total` - Rejection counter

### What Still Needs Work

1. **Client Error Experience**: When `QueueFullError` occurs:
   - Client receives generic connection error
   - No PostgreSQL-level error message with helpful SQLSTATE
   - No retry hint

2. **Backpressure Modes**: Only "reject immediately" exists:
   - No "return busy with retry hint" option
   - No server-side rate limiting

3. **Alerting**: No built-in alerting when queue is filling up:
   - Metrics exist but no log warnings at thresholds
   - Operators must set up external alerting

---

## Requirements Checklist

- [x] Increase default queue depth for production (500-1000) ← **DONE in HIGH-1**
- [x] Add PostgreSQL ErrorResponse when queue is full
- [x] Include retry-after hint in error message
- [x] Add configurable backpressure behavior enum
- [x] Expose queue depth in metrics ← **DONE in HIGH-1**
- [x] Add log warning when queue > 80% full

---

## Implementation Tasks

### Task 1: Send PostgreSQL ErrorResponse on Queue Full

**Files:** `scry-proxy/src/proxy/connection.rs`

**Problem:** When `PoolManager::acquire()` returns `AcquireError::QueueFull`, the client currently gets a TCP connection reset or generic error. PostgreSQL clients expect a proper ErrorResponse message.

**Changes:**

1. **Find where AcquireError is handled** in connection handler
2. **Send PostgreSQL ErrorResponse** with:
   - SQLSTATE `53300` (too_many_connections) - same as max_connections rejection
   - Message: "connection pool queue is full, please retry"
   - Hint field with suggested retry delay

**Implementation:**

```rust
// In connection.rs, where pool_manager.acquire() is called
match pool_manager.acquire(client_id, needs_sticky).await {
    Ok(conn) => { /* use connection */ }
    Err(AcquireError::QueueFull(_)) => {
        // Record metric
        metrics.pool_metrics().record_queue_rejected();

        // Send PostgreSQL error to client
        let error_msg = build_queue_full_error();
        client_stream.write_all(&error_msg).await?;
        return Ok(()); // Close connection gracefully
    }
    Err(AcquireError::WaitTimeout) => {
        // Similar handling for timeout
    }
    Err(e) => return Err(e.into()),
}
```

**Helper function:**

```rust
/// Build PostgreSQL ErrorResponse for queue full condition
fn build_queue_full_error() -> Vec<u8> {
    let mut response = Vec::new();
    response.push(b'E'); // ErrorResponse

    let mut fields = Vec::new();

    // Severity
    fields.push(b'S');
    fields.extend_from_slice(b"ERROR\0");

    // SQLSTATE 53300 (too_many_connections)
    fields.push(b'C');
    fields.extend_from_slice(b"53300\0");

    // Message
    fields.push(b'M');
    fields.extend_from_slice(b"connection pool queue is full\0");

    // Hint with retry suggestion
    fields.push(b'H');
    fields.extend_from_slice(b"Server is under load. Please retry in 100-500ms.\0");

    // Terminator
    fields.push(0);

    // Length (including self, 4 bytes)
    let len = (fields.len() + 4) as u32;
    response.extend_from_slice(&len.to_be_bytes());
    response.extend(fields);

    response
}
```

**Verification:**
```bash
# Test with pgbench at high concurrency to trigger queue full
pgbench -c 600 -j 10 -T 10 -h localhost -p 5433 postgres
# Should see "connection pool queue is full" in psql errors
```

---

### Task 2: Add Backpressure Mode Configuration

**Files:** `scry-proxy/src/config/mod.rs`

**Changes:**

Add new enum and config field:

```rust
/// Backpressure behavior when connection pool queue is full
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum BackpressureMode {
    /// Reject immediately with error (default, current behavior)
    #[default]
    RejectImmediate,

    /// Return "server busy" error with retry hint
    /// Clients receive SQLSTATE 53300 with retry delay suggestion
    RetryHint,

    /// Log and reject (for debugging high load scenarios)
    /// Same as RejectImmediate but logs each rejection at WARN level
    LogAndReject,
}
```

Add to `PerformanceConfig`:

```rust
pub struct PerformanceConfig {
    // ... existing fields ...

    /// Backpressure behavior when pool queue is full
    #[serde(default)]
    pub pool_backpressure_mode: BackpressureMode,

    /// Suggested retry delay in milliseconds (for RetryHint mode)
    #[serde(default = "default_retry_hint_ms")]
    pub pool_retry_hint_ms: u64,
}

fn default_retry_hint_ms() -> u64 {
    200 // 200ms default retry suggestion
}
```

**Verification:**
```bash
just test-unit -- config
```

---

### Task 3: Implement Backpressure Modes in Connection Handler

**Files:** `scry-proxy/src/proxy/connection.rs`

**Changes:**

Update queue full handling to respect backpressure mode:

```rust
Err(AcquireError::QueueFull(_)) => {
    metrics.pool_metrics().record_queue_rejected();

    match config.performance.pool_backpressure_mode {
        BackpressureMode::RejectImmediate => {
            // Current behavior: just close connection
            debug!(client_id, "Queue full, rejecting connection");
        }
        BackpressureMode::RetryHint => {
            // Send helpful error with retry hint
            let retry_ms = config.performance.pool_retry_hint_ms;
            let error_msg = build_queue_full_error_with_retry(retry_ms);
            let _ = client_stream.write_all(&error_msg).await;
        }
        BackpressureMode::LogAndReject => {
            // Log at WARN for visibility, then reject
            warn!(
                client_id,
                queue_depth = pool_manager.wait_queue_depth(),
                "Connection rejected: pool queue full"
            );
        }
    }
    return Ok(());
}
```

**Verification:**
```bash
# Set mode and verify behavior
SCRY_PERFORMANCE__POOL_BACKPRESSURE_MODE=retry_hint just run
# Connect with psql during high load, should see retry hint
```

---

### Task 4: Add Queue Saturation Warning Logs

**Files:** `scry-proxy/src/observability/health.rs` or new `queue_monitor.rs`

**Problem:** Operators should be warned before the queue fills completely, not just when rejections start.

**Changes:**

Option A: Add to existing HealthMonitor:

```rust
impl HealthMonitor {
    /// Check queue saturation and log warning if above threshold
    pub fn check_queue_saturation(&self, pool_metrics: &PoolMetrics) {
        let saturation = pool_metrics.get_queue_saturation();

        if saturation >= 0.8 {
            warn!(
                saturation_pct = saturation * 100.0,
                queue_depth = pool_metrics.get_queue_depth(),
                max_depth = pool_metrics.get_max_queue_depth(),
                "Pool queue saturation above 80% - consider increasing pool_size or queue_depth"
            );
        }
    }
}
```

Option B: Add to background metrics update task in server.rs:

```rust
// In the metrics update loop
let saturation = metrics.pool_metrics().get_queue_saturation();
if saturation >= 0.8 && saturation < last_logged_saturation.unwrap_or(0.0) + 0.1 {
    warn!(
        saturation_pct = format!("{:.1}%", saturation * 100.0),
        "Pool wait queue is filling up - clients may be rejected soon"
    );
    last_logged_saturation = Some(saturation);
}
```

**Config option:**

```rust
/// Saturation threshold for warning logs (0.0-1.0)
#[serde(default = "default_queue_saturation_warn_threshold")]
pub pool_queue_saturation_warn_threshold: f64,

fn default_queue_saturation_warn_threshold() -> f64 {
    0.8 // Warn at 80% full
}
```

**Verification:**
```bash
# Set small queue and high load to trigger warnings
SCRY_PERFORMANCE__POOL_QUEUE_DEPTH=10 SCRY_PERFORMANCE__POOL_SIZE=2 just run
# Run pgbench, should see saturation warnings in logs
```

---

### Task 5: Add Unit Tests

**Files:** `scry-proxy/src/config/mod.rs`, `scry-proxy/src/proxy/connection.rs`

**Test cases:**

```rust
#[cfg(test)]
mod backpressure_tests {
    use super::*;

    #[test]
    fn test_backpressure_mode_default() {
        let config = Config::default();
        assert_eq!(
            config.performance.pool_backpressure_mode,
            BackpressureMode::RejectImmediate
        );
    }

    #[test]
    fn test_backpressure_mode_from_env() {
        // Test serde deserialization
        let json = r#"{"pool_backpressure_mode": "retry_hint"}"#;
        let perf: PerformanceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(perf.pool_backpressure_mode, BackpressureMode::RetryHint);
    }

    #[test]
    fn test_retry_hint_default() {
        let config = Config::default();
        assert_eq!(config.performance.pool_retry_hint_ms, 200);
    }

    #[test]
    fn test_queue_saturation_warn_threshold_default() {
        let config = Config::default();
        assert_eq!(config.performance.pool_queue_saturation_warn_threshold, 0.8);
    }
}
```

**Error message test:**

```rust
#[test]
fn test_build_queue_full_error_format() {
    let error = build_queue_full_error_with_retry(200);

    // Should start with 'E' (ErrorResponse)
    assert_eq!(error[0], b'E');

    // Should contain SQLSTATE 53300
    let error_str = String::from_utf8_lossy(&error);
    assert!(error_str.contains("53300"));

    // Should contain retry hint
    assert!(error_str.contains("retry"));
}
```

**Verification:**
```bash
just test-unit -- backpressure
```

---

### Task 6: Update Documentation

**Files:** `docs/configuration.md`, `docs/connection-pooling.md`

**Add to configuration.md:**

```markdown
### Backpressure Configuration

When the connection pool queue fills up, Scry can respond in different ways:

| Mode | Description | Use Case |
|------|-------------|----------|
| `reject_immediate` | Silently close connection (default) | High-throughput, clients handle retries |
| `retry_hint` | Send PostgreSQL error with retry suggestion | User-facing, helpful error messages |
| `log_and_reject` | Log warning and reject | Debugging, monitoring queue pressure |

```toml
[performance]
pool_backpressure_mode = "retry_hint"  # Send helpful error to clients
pool_retry_hint_ms = 200               # Suggest 200ms retry delay
pool_queue_saturation_warn_threshold = 0.8  # Warn at 80% full
```

**Prometheus Alerts (example):**

```yaml
- alert: ScryQueueSaturationHigh
  expr: scry_pool_queue_saturation_ratio > 0.8
  for: 1m
  labels:
    severity: warning
  annotations:
    summary: "Scry connection pool queue is filling up"
    description: "Queue is {{ $value | humanizePercentage }} full. Consider increasing pool_size or pool_queue_depth."

- alert: ScryQueueRejections
  expr: rate(scry_pool_queue_rejected_total[5m]) > 0
  for: 1m
  labels:
    severity: warning
  annotations:
    summary: "Scry is rejecting connections due to queue pressure"
```
```

**Verification:**
- Manual review of documentation

---

## Implementation Order

1. **Task 2** - Add BackpressureMode enum to config (foundation)
2. **Task 5** - Write tests first (TDD)
3. **Task 1** - Implement PostgreSQL ErrorResponse for queue full
4. **Task 3** - Wire up backpressure modes in connection handler
5. **Task 4** - Add queue saturation warning logs
6. **Task 6** - Update documentation

---

## Verification Checklist

After implementation:

- [ ] `just test-unit` passes
- [ ] `just lint` passes
- [ ] `just test-integration` passes
- [ ] Proxy starts with default config without warnings
- [ ] When queue fills:
  - [ ] `reject_immediate` mode: connection closes silently
  - [ ] `retry_hint` mode: psql shows "connection pool queue is full" with hint
  - [ ] `log_and_reject` mode: WARN log appears with queue depth
- [ ] When queue > 80% full: WARN log appears (before any rejections)
- [ ] `/metrics` shows updated `scry_pool_queue_rejected_total` on rejections
- [ ] Documentation updated with backpressure configuration

---

## Environment Variables Reference

New environment variables:

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SCRY_PERFORMANCE__POOL_BACKPRESSURE_MODE` | enum | `reject_immediate` | How to handle queue-full condition |
| `SCRY_PERFORMANCE__POOL_RETRY_HINT_MS` | u64 | 200 | Suggested retry delay for clients |
| `SCRY_PERFORMANCE__POOL_QUEUE_SATURATION_WARN_THRESHOLD` | f64 | 0.8 | Saturation level to trigger warning logs |

---

## Rollback Plan

If issues arise:
1. Set `pool_backpressure_mode = "reject_immediate"` to restore current behavior
2. All new features are additive and don't change default behavior
3. Queue saturation warnings can be disabled by setting threshold to 1.0

---

## Success Criteria

After implementation:

| Metric | Target |
|--------|--------|
| Queue full errors | Include SQLSTATE 53300 |
| Retry hint visible | In psql error output |
| Queue saturation warning | Logged before rejections start |
| Configuration | 3 backpressure modes available |
| Breaking changes | None (defaults unchanged) |
