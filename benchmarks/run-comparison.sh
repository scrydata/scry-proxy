#!/bin/bash
set -e

# Calculate script and repo directories
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Configuration
QUERIES=${QUERIES:-100000}
RESULTS_DIR="$SCRIPT_DIR/results/$(date +%Y%m%d-%H%M%S)"
CONNECTION_COUNTS="1 10 50 100 200"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[INFO]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Create results directory
mkdir -p "$RESULTS_DIR"
log "Results will be saved to: $RESULTS_DIR"

# Build the benchmark runner from repo root
log "Building benchmark runner..."
(cd "$REPO_ROOT" && cargo build --release -p scry-benchmarks)

BENCH_BIN="$REPO_ROOT/target/release/bench-runner"

# Function to run a single benchmark
run_benchmark() {
    local label=$1
    local proxy=$2
    local url=$3
    local connections=$4
    local anonymize=$5
    local events=$6

    local output_file="$RESULTS_DIR/${label}-${connections}conn.json"

    log "Running: $label with $connections connections"

    local extra_args=""
    [ -n "$anonymize" ] && extra_args="$extra_args --anonymize=$anonymize"
    [ -n "$events" ] && extra_args="$extra_args --events=$events"

    $BENCH_BIN \
        --database-url "$url" \
        --connections "$connections" \
        --queries "$QUERIES" \
        --label "$label" \
        --proxy "$proxy" \
        --output "$output_file" \
        $extra_args
}

# Function to start fresh postgres and wait for it
start_fresh_postgres() {
    log "Starting fresh Postgres container..."
    docker compose down -v 2>/dev/null || true
    docker compose up -d postgres

    # Wait for postgres to be ready
    log "Waiting for Postgres to be ready..."
    for i in {1..30}; do
        if docker compose exec -T postgres pg_isready -U postgres >/dev/null 2>&1; then
            log "Postgres is ready"
            return 0
        fi
        sleep 1
    done
    error "Postgres failed to start"
    exit 1
}

# Function to start a proxy
start_proxy() {
    local proxy=$1
    log "Starting $proxy..."
    docker compose up -d "$proxy"
    sleep 3  # Give proxy time to connect to postgres
}

# Test matrix
run_all_benchmarks() {
    for conns in $CONNECTION_COUNTS; do
        # Direct Postgres (baseline)
        start_fresh_postgres
        run_benchmark "direct" "postgres" "postgres://postgres:postgres@localhost:5432/postgres" "$conns"

        # PgBouncer (transaction mode)
        start_fresh_postgres
        start_proxy pgbouncer
        run_benchmark "pgbouncer" "pgbouncer" "postgres://postgres:postgres@localhost:6432/postgres" "$conns"
        docker compose stop pgbouncer

        # PgCat (no sharding)
        start_fresh_postgres
        start_proxy pgcat
        run_benchmark "pgcat" "pgcat" "postgres://postgres:postgres@localhost:6433/postgres" "$conns"
        docker compose stop pgcat

        # Scry: anon=off, events=off
        start_fresh_postgres
        # TODO: Start scry-proxy with appropriate config
        # For now, skip scry tests - they require scry-proxy to be running
        warn "Scry benchmarks require scry-proxy to be running externally"

        # Cleanup
        docker compose down -v
    done
}

# Main
cd "$SCRIPT_DIR"

log "=== Proxy Comparison Benchmark ==="
log "Queries per run: $QUERIES"
log "Connection counts: $CONNECTION_COUNTS"
log ""

run_all_benchmarks

log ""
log "=== Benchmark Complete ==="
log "Results saved to: $RESULTS_DIR"
log ""
log "Generate charts with: python3 generate_charts.py $RESULTS_DIR"
