# Implementation Plan: HIGH-1 Pool Size vs Client Count Mismatch

**Date:** 2026-01-25
**Issue:** HIGH-1 from CONNECTION_MULTIPLEXING_REQUIREMENTS.md
**Status:** ✅ COMPLETED

## Problem Statement

From the requirements document:

> - Default pool_size: 10-100 (varies by config path)
> - Default max_connections: 100
> - No relationship enforced between these values
> - 300 clients competing for 10 backend connections
> - Massive queuing and contention
> - Most clients timeout waiting for connections

## Requirements Checklist

- [x] Document recommended pool_size relative to expected client count
- [x] Add validation warning if max_connections >> pool_size * 10
- [x] Consider auto-scaling pool size based on demand (deferred: better defaults + warnings instead)
- [x] Improve defaults for production use cases

## Current State Analysis

### Current Defaults (from `scry-proxy/src/config/mod.rs`)

| Config Location | Parameter | Current Value |
|-----------------|-----------|---------------|
| `backend.pool_size` | Backend pool size | 10 |
| `performance.pool_size` | Performance pool size | 100 |
| `proxy.max_connections` | Max client connections | 100 |
| `performance.pool_queue_depth` | Wait queue depth | 50 |
| `performance.pool_idle_unpin_secs` | Idle unpin timeout | 60 |

**Problem:** Two different `pool_size` values exist:
- `backend.pool_size = 10` (default in BackendConfig)
- `performance.pool_size = 100` (default in PerformanceConfig)

The actual pool creation in `server.rs:200-201` uses `performance.pool_size`:
```rust
let pool_size = db_config.pool_size.unwrap_or(perf_config.pool_size);
```

But the `DatabaseConfig` pool_size override comes from routing config which may default to `backend.pool_size`.

### Metrics Already Exposed

From `observability/prometheus.rs`:
- `scry_pool_queue_depth` - Current wait queue depth
- Pool size metrics exist but may not be correctly exported

---

## Implementation Tasks

### Task 1: Rationalize Pool Size Configuration

**Files to modify:**
- `scry-proxy/src/config/mod.rs`

**Changes:**

1. **Deprecate `backend.pool_size`** - This creates confusion. The pool size should only be in `performance.pool_size`.

2. **Update defaults for production:**
   ```rust
   // In PerformanceConfig default
   pool_size: 50,           // was 100, more sensible default
   pool_min_idle: 5,        // was 10, keep low for dev/test
   pool_queue_depth: 500,   // was 50, production needs larger queue
   ```

3. **Add new fields to PerformanceConfig:**
   ```rust
   /// Maximum wait queue depth (0 = unlimited, not recommended)
   pub pool_queue_depth: usize,

   /// Maximum ratio of max_connections to pool_size before warning
   /// Default: 20 (transaction pooling can handle 20:1 multiplexing)
   #[serde(default = "default_pool_ratio_warning")]
   pub pool_ratio_warning_threshold: usize,
   ```

**Verification:**
```bash
# Unit test
just test-unit -- --test config
```

---

### Task 2: Add Configuration Validation with Warnings

**Files to modify:**
- `scry-proxy/src/config/mod.rs` (add validate method)
- `scry-proxy/src/main.rs` (call validation at startup)

**Changes:**

1. **Add validation method to Config:**
   ```rust
   impl Config {
       /// Validate configuration and return warnings
       /// Returns Ok(warnings) on success, Err on fatal errors
       pub fn validate(&self) -> Result<Vec<String>, anyhow::Error> {
           let mut warnings = Vec::new();

           // Check pool ratio
           let ratio = self.proxy.max_connections as f64 / self.performance.pool_size as f64;
           if ratio > self.performance.pool_ratio_warning_threshold as f64 {
               warnings.push(format!(
                   "WARN: max_connections ({}) is {}x pool_size ({}). \
                    Clients may experience long wait times. \
                    Consider increasing pool_size or decreasing max_connections.",
                   self.proxy.max_connections,
                   ratio as usize,
                   self.performance.pool_size
               ));
           }

           // Check queue depth relative to multiplexing ratio
           let expected_waiters = self.proxy.max_connections.saturating_sub(self.performance.pool_size);
           if self.performance.pool_queue_depth < expected_waiters / 2 {
               warnings.push(format!(
                   "WARN: pool_queue_depth ({}) may be too small. \
                    With {} max_connections and {} pool_size, \
                    up to {} clients may need to queue. \
                    Consider setting pool_queue_depth >= {}.",
                   self.performance.pool_queue_depth,
                   self.proxy.max_connections,
                   self.performance.pool_size,
                   expected_waiters,
                   expected_waiters
               ));
           }

           // Warn if pool_size > max_connections (wasteful)
           if self.performance.pool_size > self.proxy.max_connections {
               warnings.push(format!(
                   "WARN: pool_size ({}) exceeds max_connections ({}). \
                    Extra pool connections will never be used.",
                   self.performance.pool_size,
                   self.proxy.max_connections
               ));
           }

           Ok(warnings)
       }
   }
   ```

2. **Call validation in main.rs:**
   ```rust
   // After Config::load()
   match config.validate() {
       Ok(warnings) => {
           for warning in warnings {
               warn!("{}", warning);
           }
       }
       Err(e) => {
           error!("Configuration validation failed: {}", e);
           return Err(e);
       }
   }
   ```

**Verification:**
```bash
# Unit test with various configurations
just test-unit -- --test config
```

---

### Task 3: Add Pool Size Guidance to Documentation

**Files to modify:**
- `docs/configuration.md`
- `docs/connection-pooling.md`

**Changes to `docs/configuration.md`:**

Add new section after PerformanceConfig:

```markdown
### Pool Sizing Guidelines

The relationship between `max_connections` and `pool_size` is critical for performance:

| Pooling Mode | Recommended Ratio | Example |
|--------------|-------------------|---------|
| Session | 1:1 | max_connections=100, pool_size=100 |
| Transaction | 10:1 to 20:1 | max_connections=500, pool_size=50 |
| Hybrid | 5:1 to 10:1 | max_connections=300, pool_size=50 |

**Formula for Transaction Mode:**
```
pool_size = expected_concurrent_queries + (max_connections / 20)
pool_queue_depth = max_connections - pool_size
```

**Example Production Configuration:**
```toml
[proxy]
max_connections = 500

[performance]
connection_pooling = "transaction"
pool_size = 50
pool_min_idle = 10
pool_queue_depth = 500
pool_timeout_secs = 30
```

**Warning Thresholds:**
- Scry warns if `max_connections > pool_size * 20`
- Scry warns if `pool_queue_depth < (max_connections - pool_size) / 2`
```

**Changes to `docs/connection-pooling.md`:**

Add sizing section if not present, explaining:
- Why transaction mode allows higher multiplexing
- How to monitor queue depth via Prometheus
- When to increase pool_size vs queue_depth

**Verification:**
```bash
# Manual review of documentation
```

---

### Task 4: Expose Queue Saturation Metrics

**Files to modify:**
- `scry-proxy/src/observability/prometheus.rs`
- `scry-proxy/src/observability/metrics.rs`

**Changes:**

1. **Add queue saturation percentage to PoolMetrics:**
   ```rust
   // In PoolMetrics struct
   max_queue_depth: AtomicUsize,

   // In PoolMetrics impl
   pub fn set_max_queue_depth(&self, max: usize) {
       self.max_queue_depth.store(max, Ordering::Relaxed);
   }

   pub fn get_queue_saturation(&self) -> f64 {
       let max = self.max_queue_depth.load(Ordering::Relaxed);
       if max == 0 { return 0.0; }
       let current = self.queue_depth.load(Ordering::Relaxed);
       current as f64 / max as f64
   }
   ```

2. **Add new Prometheus metrics:**
   ```rust
   // In build_prometheus_output
   writeln!(output, "# HELP scry_pool_queue_max_depth Maximum wait queue depth").unwrap();
   writeln!(output, "# TYPE scry_pool_queue_max_depth gauge").unwrap();
   writeln!(output, "scry_pool_queue_max_depth {}", pool_metrics.get_max_queue_depth()).unwrap();

   writeln!(output, "# HELP scry_pool_queue_saturation_ratio Queue saturation (0.0-1.0)").unwrap();
   writeln!(output, "# TYPE scry_pool_queue_saturation_ratio gauge").unwrap();
   writeln!(output, "scry_pool_queue_saturation_ratio {:.4}", pool_metrics.get_queue_saturation()).unwrap();
   ```

3. **Set max_queue_depth at startup** (in server.rs or connection handler):
   ```rust
   metrics.pool_metrics().set_max_queue_depth(config.performance.pool_queue_depth);
   ```

**Verification:**
```bash
# Start proxy and check metrics endpoint
curl http://localhost:9090/metrics | grep queue
```

---

### Task 5: Add Unit Tests for Validation

**Files to modify:**
- `scry-proxy/src/config/mod.rs` (add tests)

**Test cases:**

```rust
#[cfg(test)]
mod validation_tests {
    use super::*;

    #[test]
    fn test_validate_warns_on_high_ratio() {
        let mut config = Config::default();
        config.proxy.max_connections = 1000;
        config.performance.pool_size = 10;  // 100:1 ratio

        let warnings = config.validate().unwrap();
        assert!(!warnings.is_empty());
        assert!(warnings[0].contains("100x pool_size"));
    }

    #[test]
    fn test_validate_warns_on_small_queue() {
        let mut config = Config::default();
        config.proxy.max_connections = 500;
        config.performance.pool_size = 50;
        config.performance.pool_queue_depth = 50;  // Too small

        let warnings = config.validate().unwrap();
        assert!(warnings.iter().any(|w| w.contains("pool_queue_depth")));
    }

    #[test]
    fn test_validate_warns_on_wasteful_pool() {
        let mut config = Config::default();
        config.proxy.max_connections = 50;
        config.performance.pool_size = 100;  // Wasteful

        let warnings = config.validate().unwrap();
        assert!(warnings.iter().any(|w| w.contains("exceeds max_connections")));
    }

    #[test]
    fn test_validate_no_warnings_for_good_config() {
        let mut config = Config::default();
        config.proxy.max_connections = 500;
        config.performance.pool_size = 50;       // 10:1 ratio
        config.performance.pool_queue_depth = 500;

        let warnings = config.validate().unwrap();
        assert!(warnings.is_empty());
    }
}
```

**Verification:**
```bash
just test-unit -- config::validation_tests
```

---

## Implementation Order

1. **Task 1** - Rationalize defaults (5 min)
2. **Task 5** - Write tests first (TDD) (10 min)
3. **Task 2** - Implement validation (10 min)
4. **Task 4** - Add metrics (10 min)
5. **Task 3** - Update documentation (10 min)

## Verification Checklist

After implementation:

- [ ] `just test-unit` passes
- [ ] `just lint` passes
- [ ] Proxy starts with default config without warnings
- [ ] Proxy warns when started with `SCRY_PROXY__MAX_CONNECTIONS=1000 SCRY_PERFORMANCE__POOL_SIZE=10`
- [ ] `/metrics` endpoint shows `scry_pool_queue_saturation_ratio`
- [ ] Documentation updated with pool sizing guidelines

## Rollback Plan

If issues arise:
1. Revert defaults to original values
2. Remove validation (make it optional via config flag)
3. Keep metrics additions (non-breaking)

---

## Environment Variables Reference

New/changed environment variables:

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SCRY_PERFORMANCE__POOL_SIZE` | usize | 50 | Backend connection pool size |
| `SCRY_PERFORMANCE__POOL_QUEUE_DEPTH` | usize | 500 | Max clients waiting for connections |
| `SCRY_PERFORMANCE__POOL_RATIO_WARNING_THRESHOLD` | usize | 20 | Warn if max_connections/pool_size exceeds this |
