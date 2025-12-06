# Benchmarking Scry Proxy Performance

This document explains how to run and interpret latency benchmarks for the Scry proxy.

## Running Benchmarks

```bash
# Run all benchmarks (requires Docker for testcontainers)
just bench

# Or using cargo directly
cargo bench
```

Benchmarks take approximately 30-60 seconds to run as Criterion collects statistical samples.

## What Gets Measured

### 1. Query Latency Comparison (`query_latency` group)
Compares end-to-end latency for the same query:
- **direct_postgres**: Direct connection to Postgres (baseline)
- **through_proxy**: Connection through Scry proxy

**Goal**: Measure proxy overhead. Target is <1ms additional latency.

### 2. Query Types (`query_types` group)
Measures proxy performance across different query patterns:
- Simple SELECT
- Arithmetic operations
- String concatenation
- Time functions (NOW())

**Goal**: Ensure consistent overhead across different query types.

### 3. Event Publishing (`event_publishing` group)
Compares publisher implementations:
- **noop_publisher**: No event processing (minimal overhead baseline)
- **counting_publisher**: Tracks events in memory

**Goal**: Measure event publishing overhead.

## Interpreting Results

Criterion outputs:
```
query_latency/direct_postgres
                        time:   [2.1234 ms 2.1567 ms 2.1876 ms]

query_latency/through_proxy
                        time:   [2.8123 ms 2.8456 ms 2.8789 ms]
```

### Key Metrics

1. **Mean Time**: Middle value in brackets (e.g., `2.1567 ms`)
2. **Confidence Interval**: [lower, mean, upper] at 95% confidence
3. **Proxy Overhead**: Difference between proxy and direct times
   - In example above: ~0.7ms overhead (within acceptable range)

### What to Look For

✅ **Good**:
- Proxy overhead <1ms on average
- Low variance (tight confidence intervals)
- Consistent overhead across query types

⚠️ **Investigate**:
- Overhead >1ms consistently
- High variance (wide confidence intervals)
- Overhead increasing with different query types

## Detailed Results

After running, find detailed HTML reports in:
```
target/criterion/
```

Open `target/criterion/report/index.html` in a browser for:
- Latency distributions
- Violin plots showing variance
- Percentile charts (p50, p95, p99)
- Regression analysis
- Comparison with previous runs

## Continuous Performance Tracking

Criterion automatically compares against previous benchmark runs:
```
query_latency/through_proxy
                        time:   [2.8456 ms 2.8567 ms 2.8678 ms]
                        change: [-2.4% -1.8% -1.2%] (improvement)
```

This helps detect performance regressions over time.

## Tips for Accurate Benchmarks

1. **Close other applications** to reduce system noise
2. **Run on consistent hardware** - benchmarks are machine-specific
3. **Use release builds** - benchmarks automatically use `--release`
4. **Allow warm-up** - Criterion handles this automatically
5. **Run multiple times** if results seem inconsistent
6. **Connection reuse** - Benchmarks reuse connections to avoid port exhaustion and measure realistic latency

## Implementation Details

### Connection Reuse
The benchmarks create persistent connections **outside** the measurement loop to:
- Avoid TCP connection overhead (which would skew latency measurements)
- Prevent port exhaustion from creating thousands of connections
- Measure actual query execution latency, not connection setup time

This reflects real-world usage where applications maintain connection pools.

## Example Workflow

```bash
# Initial benchmark
just bench

# Make performance optimization
# ... code changes ...

# Re-run to see improvement
just bench

# Check detailed HTML report
start target/criterion/report/index.html  # Windows
# or
open target/criterion/report/index.html   # macOS
# or
xdg-open target/criterion/report/index.html  # Linux
```

## Performance Goals

From CLAUDE.md:
- Target: <1ms additional latency per query
- Best-effort event publishing (never block the proxy)
- Focus on maximum observability within latency budget

The benchmarks help verify we're meeting these goals.

---

# Query Anonymization Performance

## Overview

The query anonymization feature adds privacy protection and hot data detection capabilities to Scry with minimal performance impact.

## Implementation Summary

The query anonymization feature provides:

1. **Query Normalization**: Replaces literal values with placeholders
   - Example: `SELECT * FROM users WHERE id = 123` → `SELECT * FROM users WHERE id = ?`
2. **Value Fingerprinting**: Creates consistent hashes for each literal value
   - Enables hot data detection without exposing actual values
3. **Privacy Protection**: Uses Blake3 hashing with configurable salt
   - Prevents rainbow table attacks

### Key Features

- **Hot Data Detection**: Track frequently accessed IDs, keys, or values without exposing actual data
- **Query Pattern Analysis**: Group queries by shape while preserving value distribution insights
- **Configurable**: Enable/disable via `config.publisher.anonymize` flag
- **Zero PII Exposure**: All literal values are hashed using cryptographic hash functions

## Benchmark Results

### Anonymization Overhead (End-to-End)

Benchmarks run with Postgres 16 in Docker, measuring complete query latency through the proxy:

| Scenario | Mean Latency | Overhead vs Baseline |
|----------|--------------|----------------------|
| Anonymization Disabled | 1.4192 ms | baseline |
| Anonymization Enabled | 1.4300 ms | +0.0108 ms (~11 µs) |
| Complex Query (Multiple Values) | 939.67 µs | N/A |

### Analysis

- **Overhead**: Anonymization adds approximately **11 microseconds** per query
- **Target Budget**: <1ms additional latency
- **Result**: ✅ **Well within budget** - anonymization overhead is ~1% of the target
- **Impact**: Negligible for production workloads (~0.76% overhead)

### Test Queries

The benchmarks tested various query types:

1. **Standard Query**: `SELECT * FROM pg_catalog.pg_tables WHERE tablename = 'pg_class'`
2. **Complex Query**: `SELECT 1, 2, 3, 'hello', 'world', 42 WHERE 1 = 1 AND 2 = 2`
   - Multiple literal values to stress-test the anonymization logic

## Hot Data Detection Example

With anonymization enabled, you can detect hot keys across different queries:

```sql
-- Query 1: SELECT * FROM orders WHERE user_id = 999
-- Normalized: SELECT * FROM orders WHERE user_id = ?
-- Fingerprint: [abc123...]

-- Query 2: SELECT * FROM purchases WHERE buyer_id = 999
-- Normalized: SELECT * FROM purchases WHERE buyer_id = ?
-- Fingerprint: [abc123...]  (same as Query 1!)

-- Query 3: SELECT * FROM orders WHERE user_id = 777
-- Normalized: SELECT * FROM orders WHERE user_id = ?
-- Fingerprint: [def456...]  (different user)
```

The analytics service can identify that `user_id=999` appears in 50% of queries without knowing the actual user ID.

## QueryEvent Structure

Events published with anonymization enabled include:

```json
{
  "event_id": "uuid",
  "timestamp": "...",
  "query": "SELECT * FROM users WHERE user_id = ?",
  "normalized_query": "SELECT * FROM users WHERE user_id = ?",
  "value_fingerprints": ["blake3_hash_of_123"],
  "duration": "...",
  "success": true,
  "database": "postgres",
  "connection_id": "..."
}
```

When disabled, only the `query` field is populated (with raw query text).

## Configuration

To enable anonymization:

```toml
[publisher]
enabled = true
batch_size = 100
flush_interval_ms = 1000
anonymize = true  # Enable query anonymization
```

Or via environment variables:
```bash
SCRY_PUBLISHER_ANONYMIZE=true
```

## Implementation Details

### Dependencies

- `sqlparser = { version = "0.52", features = ["visitor"] }` - SQL parsing with AST visitor
- `blake3 = "1.5"` - Fast cryptographic hashing

### Key Components

- `src/protocol/anonymize.rs` - Core anonymization logic (QueryAnonymizer)
- `src/publisher/event.rs` - Updated QueryEvent structure with new fields
- `src/proxy/connection.rs` - Integration point in connection handler

### Performance Characteristics

- **Parsing**: Uses `sqlparser-rs` with PostgreSQL dialect
- **Hashing**: Blake3 (one of the fastest cryptographic hash functions)
- **Memory**: Minimal overhead - processes AST in-place where possible
- **Async-safe**: Anonymization happens synchronously but doesn't block I/O

## Testing

All tests pass:

- ✅ 19 unit tests (including 9 anonymization-specific tests)
- ✅ 6 integration tests (proxy functionality with real Postgres)
- ✅ Benchmark suite including anonymization overhead measurements

### Unit Test Coverage

- Simple SELECT queries
- Complex WHERE clauses with multiple values
- INSERT statements
- UPDATE statements
- Hot data detection (consistent fingerprints for same values)
- Salt variation (different salts produce different fingerprints)
- Invalid SQL handling (graceful degradation)

## Running Anonymization Benchmarks

```bash
# Run just the anonymization benchmarks
cargo bench --bench proxy_throughput -- anonymization

# View detailed results
open target/criterion/anonymization/report/index.html
```

## Conclusion

The query anonymization implementation successfully:

1. ✅ Meets the <1ms latency budget (adds only ~11µs overhead, or ~0.76%)
2. ✅ Enables hot data detection while protecting PII
3. ✅ Provides flexible query pattern analysis for observability
4. ✅ Maintains backward compatibility (optional feature flag)
5. ✅ Passes all unit and integration tests

The implementation is production-ready and adds negligible overhead to proxy operations.
