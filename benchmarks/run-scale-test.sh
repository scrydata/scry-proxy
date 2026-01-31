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
