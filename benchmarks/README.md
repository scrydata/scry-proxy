# Proxy Comparison Benchmarks

Performance comparison framework for scry-proxy against PgBouncer and PgCat.

## Quick Start

```bash
# Build the benchmark runner
just bench-build

# Run a quick sanity check
just bench-quick

# Run full comparison suite
just bench-full

# Generate charts from results
just bench-charts benchmarks/results/YYYYMMDD-HHMMSS
```

## Test Matrix

| Proxy | Configuration |
|-------|---------------|
| Direct Postgres | Baseline (no proxy) |
| PgBouncer | Transaction pooling mode |
| PgCat | Sharding disabled |
| Scry | anon=off, events=off |
| Scry | anon=off, events=on |
| Scry | anon=on, events=off |
| Scry | anon=on, events=on |

## Metrics Captured

- **Latency**: p50, p75, p90, p95, p99, p99.9, min, max, mean, stddev (microseconds)
- **Throughput**: Queries per second
- **Resource Usage**: CPU %, Memory MB (sampled every 100ms)

## Workload

OLTP e-commerce workload with weighted query distribution:
- 42% Browse products (pagination)
- 26% View product detail (point lookup)
- 16% Search products (ILIKE)
- 11% Order history (user-specific)
- 5% Order details (JOIN)

Data volume: ~1000 users, ~1000 products, ~10000 orders

## Running Individual Benchmarks

```bash
# Direct to Postgres
./target/release/bench-runner \
  --database-url "postgres://postgres:postgres@localhost:5432/postgres" \
  --connections 50 \
  --queries 100000 \
  --label "direct" \
  --proxy "postgres"

# Through PgBouncer
./target/release/bench-runner \
  --database-url "postgres://postgres:postgres@localhost:6432/postgres" \
  --connections 50 \
  --queries 100000 \
  --label "pgbouncer" \
  --proxy "pgbouncer"

# Through scry-proxy
./target/release/bench-runner \
  --database-url "postgres://postgres:postgres@localhost:5433/postgres" \
  --connections 50 \
  --queries 100000 \
  --label "scry-anon-on-events-on" \
  --proxy "scry" \
  --anonymize true \
  --events true
```

## Docker Compose Services

```bash
cd benchmarks

# Start just Postgres
docker compose up -d postgres

# Start PgBouncer (requires Postgres)
docker compose up -d pgbouncer

# Start PgCat (requires Postgres)
docker compose up -d pgcat

# Tear down everything
docker compose down -v
```

## Output Format

Results are saved as JSON:

```json
{
  "label": "pgbouncer",
  "proxy": "pgbouncer",
  "config": {
    "connections": 50,
    "target_queries": 100000
  },
  "duration_secs": 45.2,
  "throughput_qps": 2212.4,
  "latency_us": {
    "p50": 412,
    "p95": 1230,
    "p99": 2100,
    "max": 15400
  }
}
```

## Generating Charts

```bash
# Install Python dependencies
pip install -r benchmarks/requirements.txt

# Generate charts
python3 benchmarks/generate_charts.py benchmarks/results/YYYYMMDD-HHMMSS
```

Generated charts:
- `latency_comparison.png` - Bar chart of p50/p95/p99 by proxy
- `latency_vs_connections.png` - Line chart of p99 across connection counts
- `throughput_comparison.png` - Bar chart of QPS by proxy
- `throughput_vs_connections.png` - Line chart of QPS across connection counts
- `SUMMARY.md` - Markdown table of all results
