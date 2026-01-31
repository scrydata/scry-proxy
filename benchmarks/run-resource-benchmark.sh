#!/bin/bash
# Run resource benchmarks for all proxies at 20 and 50 connections
# All proxies run in Docker containers, measured via Docker stats API
set -e

cd "$(dirname "$0")"

RESULTS_DIR="results/resources-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RESULTS_DIR"

POSTGRES_CONTAINER="benchmarks-postgres-1"

# Port mapping
declare -A PORTS=(
    ["postgres"]=5432
    ["pgbouncer"]=6432
    ["pgcat"]=6433
    ["scry"]=5434
)

# Function to wait for postgres to be ready
wait_for_postgres() {
    echo "Waiting for postgres to be ready..."
    for i in {1..30}; do
        if docker compose exec -T postgres pg_isready -U postgres > /dev/null 2>&1; then
            echo "Postgres is ready"
            return 0
        fi
        sleep 1
    done
    echo "Postgres failed to start"
    return 1
}

# Function to wait for a proxy to be ready
wait_for_proxy() {
    local PORT=$1
    local NAME=$2
    echo "Waiting for $NAME to be ready on port $PORT..."
    for i in {1..30}; do
        if PGPASSWORD=postgres psql -h localhost -p $PORT -U postgres -c "SELECT 1" > /dev/null 2>&1; then
            echo "$NAME is ready"
            return 0
        fi
        sleep 1
    done
    echo "$NAME failed to start"
    return 1
}

# Function to run benchmark for a proxy
run_benchmark() {
    local PROXY=$1
    local CONN=$2
    local LABEL=$3
    local EXTRA_ENV=${4:-""}

    echo ""
    echo "=============================================="
    echo "Testing $LABEL at $CONN connections"
    echo "=============================================="

    local PORT=${PORTS[$PROXY]}
    local PROXY_CONTAINER="benchmarks-${PROXY}-1"

    # For direct postgres test, no proxy container
    if [ "$PROXY" = "postgres" ]; then
        ../target/release/bench-runner \
            --database-url "postgres://postgres:postgres@localhost:$PORT/postgres" \
            --connections $CONN \
            --queries 100000 \
            --label "$LABEL" \
            --proxy "$PROXY" \
            --postgres-container "$POSTGRES_CONTAINER" \
            --output "$RESULTS_DIR/${LABEL}.json"
        return
    fi

    # For scry with custom env vars, we use docker compose run
    if [ "$PROXY" = "scry" ] && [ -n "$EXTRA_ENV" ]; then
        # Stop any existing scry container
        docker compose stop scry 2>/dev/null || true
        docker rm -f "$PROXY_CONTAINER" 2>/dev/null || true

        # Start with custom environment using run
        echo "Starting scry with: $EXTRA_ENV"
        eval "docker compose run -d --name $PROXY_CONTAINER --service-ports $EXTRA_ENV scry"
    else
        docker compose up -d $PROXY
    fi

    wait_for_proxy $PORT $PROXY

    ../target/release/bench-runner \
        --database-url "postgres://postgres:postgres@localhost:$PORT/postgres" \
        --connections $CONN \
        --queries 100000 \
        --label "$LABEL" \
        --proxy "$PROXY" \
        --proxy-container "$PROXY_CONTAINER" \
        --postgres-container "$POSTGRES_CONTAINER" \
        --output "$RESULTS_DIR/${LABEL}.json"

    # Stop the proxy
    if [ "$PROXY" = "scry" ] && [ -n "$EXTRA_ENV" ]; then
        docker stop "$PROXY_CONTAINER" 2>/dev/null || true
        docker rm -f "$PROXY_CONTAINER" 2>/dev/null || true
    else
        docker compose stop $PROXY 2>/dev/null || true
    fi
}

# Build bench-runner
echo "Building bench-runner..."
cargo build --release --manifest-path ../Cargo.toml -p scry-benchmarks

# Start postgres
echo ""
echo "Starting postgres..."
docker compose down -v 2>/dev/null || true
docker compose up -d postgres
wait_for_postgres

# Test Direct Postgres (baseline)
for CONN in 20 50; do
    run_benchmark postgres $CONN "postgres-direct-${CONN}conn"
done

# Test PgBouncer
for CONN in 20 50; do
    run_benchmark pgbouncer $CONN "pgbouncer-${CONN}conn"
done

# Test PgCat
for CONN in 20 50; do
    run_benchmark pgcat $CONN "pgcat-${CONN}conn"
done

# Test Scry (base - no events)
for CONN in 20 50; do
    run_benchmark scry $CONN "scry-base-${CONN}conn" \
        "-e SCRY_PUBLISHER__ENABLED=false"
done

# Test Scry (events enabled)
for CONN in 20 50; do
    run_benchmark scry $CONN "scry-events-${CONN}conn" \
        "-e SCRY_PUBLISHER__ENABLED=true -e SCRY_PUBLISHER__ANONYMIZE=false"
done

# Test Scry (full - events + anonymization)
for CONN in 20 50; do
    run_benchmark scry $CONN "scry-full-${CONN}conn" \
        "-e SCRY_PUBLISHER__ENABLED=true -e SCRY_PUBLISHER__ANONYMIZE=true"
done

docker compose down -v

echo ""
echo "=============================================="
echo "Results saved to $RESULTS_DIR"
echo "=============================================="
echo ""
echo "Summary of result files:"
ls -la "$RESULTS_DIR"
