# Benchmark Results

Performance comparison of scry-proxy against other PostgreSQL proxies.

**Date:** 2026-01-18
**Branch:** `proxy-benchmarking`
**Commit:** `357a525` (perf: eliminate string cloning in hot path)

## Test Environment

- Platform: Windows 11
- PostgreSQL: 16 (Docker)
- Workload: OLTP e-commerce queries (SELECT with JOINs, pagination, search)
- Benchmark tool: `bench-runner` (custom Rust benchmark)

## Results After String Allocation Optimization

### 10 Concurrent Connections, 10K Queries

| Proxy | Throughput | p50 | p95 | p99 | Max |
|-------|------------|-----|-----|-----|-----|
| **Direct Postgres** | 6,959 qps | 1.38 ms | 1.84 ms | 2.29 ms | 18.0 ms |
| **Scry Proxy** | 2,788 qps | 1.72 ms | 2.21 ms | 2.62 ms | 2,067 ms* |

*Max latency includes connection pool cold-start overhead on first query.

### 5 Concurrent Connections, 10K Queries

| Proxy | Throughput | p50 | p95 | p99 | Max |
|-------|------------|-----|-----|-----|-----|
| **Direct Postgres** | 4,306 qps | 1.11 ms | 1.46 ms | 1.72 ms | 26.2 ms |
| **Scry Proxy** | 2,239 qps | 1.39 ms | 1.76 ms | 2.03 ms | 2,052 ms* |

### Latency Overhead Analysis

| Metric | Overhead vs Direct | Target |
|--------|-------------------|--------|
| p50 | +280-340 μs (+24-25%) | < 1 ms |
| p95 | +300-370 μs (+20-21%) | < 1 ms |
| p99 | +310-330 μs (+14-18%) | < 1 ms |

**Result: Latency overhead is well within the <1ms target.**

## Historical Comparison (Pre-Optimization)

Previous benchmark run with 20 connections showed configuration issues:

| Proxy | Throughput | p50 | p95 | p99 |
|-------|------------|-----|-----|-----|
| Direct Postgres | 9,067 qps | 1.78 ms | 4.71 ms | 9.34 ms |
| PgBouncer | 5,295 qps | 3.60 ms | 4.45 ms | 5.74 ms |
| PgCat | 446 qps | 44.1 ms | 48.5 ms | 53.6 ms |
| Scry (before opt) | 581 qps | 43.9 ms | 48.4 ms | 50.0 ms |

Note: The PgCat and Scry results from this run show anomalous latency (~44ms p50),
suggesting a configuration issue (likely connection pool exhaustion or Docker networking).

## Optimization Details

Commit `357a525` eliminated ~4 string allocations per query:

1. **Switch to `parking_lot::Mutex`** - Faster sync locking without async overhead
2. **Pre-format `connection_id`** - Convert `u64` to `Arc<str>` once per connection
3. **Use `Arc<str>` for database** - Avoid repeated `String::clone()` calls
4. **Move query ownership** - Pass query by value instead of cloning

## Known Issues

1. **High max latency** - First queries hit connection pool cold-start (~2s)
2. **Throughput gap** - 40-52% of direct Postgres throughput (investigation needed)
3. **Unknown parameter warnings** - Extended protocol cache misses cause benign warnings

## Running Benchmarks

```bash
# Start Postgres
cd benchmarks && docker compose up -d postgres

# Run direct Postgres benchmark
./target/release/bench-runner \
  --database-url "postgres://postgres:postgres@localhost:5432/postgres" \
  --connections 10 --queries 10000 \
  --label "direct" --proxy "postgres"

# Start scry-proxy
SCRY_BACKEND_HOST=localhost SCRY_BACKEND_PORT=5432 \
SCRY_BACKEND_DATABASE=postgres SCRY_BACKEND_USER=postgres \
SCRY_BACKEND_PASSWORD=postgres SCRY_LISTEN_PORT=5433 \
./target/release/scry &

# Run scry benchmark
./target/release/bench-runner \
  --database-url "postgres://postgres:postgres@localhost:5433/postgres" \
  --connections 10 --queries 10000 \
  --label "scry" --proxy "scry"

# Cleanup
docker compose down -v
```

## Next Steps

- [ ] Investigate throughput gap vs direct Postgres
- [ ] Fix connection pool warmup causing high max latency
- [ ] Re-run full comparison with PgBouncer/PgCat under identical conditions
- [ ] Profile CPU usage to identify remaining bottlenecks
