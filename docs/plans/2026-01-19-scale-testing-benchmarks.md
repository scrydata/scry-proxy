# Scale Testing Benchmarks Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Run comprehensive scale testing benchmarks comparing Scry, PgBouncer, PgCat, and direct Postgres at 50, 100, and 200 concurrent connections.

**Architecture:** Use existing `bench-runner` tool with docker-compose infrastructure. Increase pool sizes to support 200 connections. Run sequential benchmarks for each proxy at each connection level, then generate comparison charts.

**Tech Stack:** Rust (bench-runner), Docker Compose, Python (charts), PostgreSQL 16

---

### Task 1: Update Pool Sizes for Scale Testing

**Files:**
- Modify: `benchmarks/docker-compose.yml`
- Modify: `benchmarks/pgbouncer/pgbouncer.ini`
- Modify: `benchmarks/pgcat/pgcat.toml`

**Step 1: Update docker-compose.yml pool sizes**

Change all pool sizes from 50 to 250 to support 200 concurrent connections with headroom.

In `benchmarks/docker-compose.yml`, update:

```yaml
# pgbouncer section - change:
PGBOUNCER_DEFAULT_POOL_SIZE: 250
PGBOUNCER_MAX_CLIENT_CONN: 2000

# All scry-base, scry-events, scry-full sections - change:
SCRY_BACKEND__POOL_SIZE: 250
SCRY_PERFORMANCE__POOL_SIZE: 250
SCRY_PERFORMANCE__POOL_MIN_IDLE: 50
```

**Step 2: Update pgbouncer.ini**

In `benchmarks/pgbouncer/pgbouncer.ini`, update:

```ini
max_client_conn = 2000
default_pool_size = 250
min_pool_size = 50
```

**Step 3: Update pgcat.toml**

In `benchmarks/pgcat/pgcat.toml`, update:

```toml
[pools.postgres.users.0]
pool_size = 250
min_pool_size = 50
```

**Step 4: Verify syntax**

Run: `cd benchmarks && docker compose config > /dev/null && echo "Config valid"`
Expected: "Config valid"

**Step 5: Commit**

```bash
git add benchmarks/docker-compose.yml benchmarks/pgbouncer/pgbouncer.ini benchmarks/pgcat/pgcat.toml
git commit -m "chore(benchmarks): increase pool sizes to support 200 connection scale tests"
```

---

### Task 2: Create Scale Test Runner Script

**Files:**
- Create: `benchmarks/run-scale-test.sh`

**Step 1: Write the scale test script**

Create `benchmarks/run-scale-test.sh`:

```bash
#!/bin/bash
set -e

# Scale test runner for proxy comparison benchmarks
# Tests: direct postgres, pgbouncer, pgcat, scry at 50, 100, 200 connections

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Configuration
QUERIES=${QUERIES:-50000}
CONNECTION_COUNTS="50 100 200"
RESULTS_DIR="$SCRIPT_DIR/results/scale-$(date +%Y%m%d-%H%M%S)"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log() { echo -e "${GREEN}[INFO]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }

mkdir -p "$RESULTS_DIR"
log "Results will be saved to: $RESULTS_DIR"

# Build benchmark runner
log "Building benchmark runner..."
(cd "$REPO_ROOT" && cargo build --release -p scry-benchmarks) || error "Build failed"
BENCH_BIN="$REPO_ROOT/target/release/bench-runner"

# Build scry proxy image
log "Building scry proxy image..."
(cd "$SCRIPT_DIR" && docker compose build scry-base) || error "Docker build failed"

cd "$SCRIPT_DIR"

# Start postgres
log "Starting postgres..."
docker compose down -v 2>/dev/null || true
docker compose up -d postgres
sleep 5

# Wait for postgres
for i in {1..30}; do
    if docker compose exec -T postgres pg_isready -U postgres >/dev/null 2>&1; then
        log "Postgres ready"
        break
    fi
    sleep 1
done

run_benchmark() {
    local label=$1
    local proxy=$2
    local url=$3
    local conns=$4
    local output="$RESULTS_DIR/${label}-${conns}conn.json"

    log "Running: $label @ $conns connections"

    $BENCH_BIN \
        --database-url "$url" \
        --connections "$conns" \
        --queries "$QUERIES" \
        --label "$label-${conns}conn" \
        --proxy "$proxy" \
        --output "$output" 2>&1 | tail -20

    sleep 2  # Cool-down between runs
}

# Run benchmarks
for conns in $CONNECTION_COUNTS; do
    log "=== Testing with $conns connections ==="

    # Direct Postgres
    run_benchmark "direct" "postgres" "postgres://postgres:postgres@localhost:5432/postgres" "$conns"

    # PgBouncer
    log "Starting pgbouncer..."
    docker compose up -d pgbouncer
    sleep 3
    run_benchmark "pgbouncer" "pgbouncer" "postgres://postgres:postgres@localhost:6432/postgres" "$conns"
    docker compose stop pgbouncer

    # PgCat
    log "Starting pgcat..."
    docker compose up -d pgcat
    sleep 3
    run_benchmark "pgcat" "pgcat" "postgres://postgres:postgres@localhost:6433/postgres" "$conns"
    docker compose stop pgcat

    # Scry
    log "Starting scry..."
    docker compose up -d scry-base
    sleep 5
    run_benchmark "scry" "scry" "postgres://postgres:postgres@localhost:5434/postgres" "$conns"
    docker compose stop scry-base
done

# Cleanup
docker compose down

log "=== Scale Test Complete ==="
log "Results: $RESULTS_DIR"
log ""
log "Generate charts: python3 generate_charts.py $RESULTS_DIR"
```

**Step 2: Make script executable**

Run: `chmod +x benchmarks/run-scale-test.sh`
Expected: No output (success)

**Step 3: Verify script syntax**

Run: `bash -n benchmarks/run-scale-test.sh && echo "Syntax OK"`
Expected: "Syntax OK"

**Step 4: Commit**

```bash
git add benchmarks/run-scale-test.sh
git commit -m "feat(benchmarks): add scale test runner script for 50/100/200 connections"
```

---

### Task 3: Update Chart Generator for Scale Comparison

**Files:**
- Modify: `benchmarks/generate_charts.py`

**Step 1: Read current chart generator**

Check existing implementation to understand what modifications are needed.

**Step 2: Add scale comparison chart function**

Add function to generate multi-connection comparison charts showing throughput and latency scaling.

```python
def generate_scale_charts(results_dir):
    """Generate charts comparing proxies across connection counts."""
    import json
    import glob

    # Load all results
    results = {}
    for f in glob.glob(f"{results_dir}/*.json"):
        with open(f) as fp:
            data = json.load(fp)
            proxy = data['proxy']
            conns = data['config']['connections']
            if proxy not in results:
                results[proxy] = {}
            results[proxy][conns] = data

    # Generate throughput scaling chart
    proxies = sorted(results.keys())
    conn_counts = sorted(set(c for p in results.values() for c in p.keys()))

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    # Throughput chart
    ax1 = axes[0]
    for proxy in proxies:
        conns = sorted(results[proxy].keys())
        throughputs = [results[proxy][c]['throughput_qps'] for c in conns]
        ax1.plot(conns, throughputs, marker='o', label=proxy)
    ax1.set_xlabel('Concurrent Connections')
    ax1.set_ylabel('Throughput (qps)')
    ax1.set_title('Throughput Scaling')
    ax1.legend()
    ax1.grid(True)

    # p99 Latency chart
    ax2 = axes[1]
    for proxy in proxies:
        conns = sorted(results[proxy].keys())
        p99s = [results[proxy][c]['latency_us']['p99'] / 1000 for c in conns]
        ax2.plot(conns, p99s, marker='o', label=proxy)
    ax2.set_xlabel('Concurrent Connections')
    ax2.set_ylabel('p99 Latency (ms)')
    ax2.set_title('p99 Latency Scaling')
    ax2.legend()
    ax2.grid(True)

    plt.tight_layout()
    plt.savefig(f"{results_dir}/scale-comparison.png", dpi=150)
    print(f"Saved: {results_dir}/scale-comparison.png")
```

**Step 3: Commit**

```bash
git add benchmarks/generate_charts.py
git commit -m "feat(benchmarks): add scale comparison chart generator"
```

---

### Task 4: Run Scale Tests

**Step 1: Ensure docker is running**

Run: `docker info > /dev/null && echo "Docker OK"`
Expected: "Docker OK"

**Step 2: Run scale tests**

Run: `cd benchmarks && ./run-scale-test.sh`
Expected: Benchmark output for each proxy at each connection count (takes ~15-20 minutes)

**Step 3: Verify results exist**

Run: `ls benchmarks/results/scale-*/`
Expected: JSON files for each proxy/connection combination

---

### Task 5: Generate Charts and Document Results

**Step 1: Generate comparison charts**

Run: `cd benchmarks && python3 generate_charts.py results/scale-*`
Expected: PNG chart files generated

**Step 2: Create results summary**

Add new section to `BENCHMARK_RESULTS.md` with the scale test findings:

```markdown
## Scale Testing Results (50, 100, 200 Connections)

**Date:** 2026-01-19

### Throughput Scaling

| Connections | Direct PG | PgBouncer | PgCat | Scry |
|-------------|-----------|-----------|-------|------|
| 50          | X qps     | X qps     | X qps | X qps |
| 100         | X qps     | X qps     | X qps | X qps |
| 200         | X qps     | X qps     | X qps | X qps |

### p99 Latency Scaling

| Connections | Direct PG | PgBouncer | PgCat | Scry |
|-------------|-----------|-----------|-------|------|
| 50          | X ms      | X ms      | X ms  | X ms |
| 100         | X ms      | X ms      | X ms  | X ms |
| 200         | X ms      | X ms      | X ms  | X ms |

### Analysis

[Fill in observations about how each proxy scales]
```

**Step 3: Commit results**

```bash
git add BENCHMARK_RESULTS.md benchmarks/results/
git commit -m "docs: add scale testing benchmark results (50/100/200 connections)"
```

---

## Verification Checklist

- [ ] Pool sizes updated to 250 in all configs
- [ ] Scale test script runs without errors
- [ ] Benchmarks complete for all 4 proxies at all 3 connection counts (12 total runs)
- [ ] Charts generated showing throughput and latency scaling
- [ ] Results documented in BENCHMARK_RESULTS.md
