# Scry - Transparent SQL Proxy
# Development commands using just (https://github.com/casey/just)

# OS-specific shell configuration
set windows-shell := ["powershell.exe", "-NoLogo", "-Command"]

# Container runtime: set CONTAINER_CMD env var to override (e.g., "docker")
container_cmd := env("CONTAINER_CMD", "podman")

# List available commands
default:
    @just --list

# Build the project
build:
    cargo build

# Build with optimizations
build-release:
    cargo build --release

# Run all tests
test:
    cargo test

# Run unit tests only
test-unit:
    cargo test --lib

# Run integration tests (requires Docker)
test-integration:
    cargo test --test '*' -- --test-threads=1

# Run a specific test by name
test-specific TEST:
    cargo test {{TEST}}

# Run benchmarks
bench:
    cargo bench

# Run the proxy (development mode)
run *ARGS:
    cargo run -- {{ARGS}}

# Run the proxy with release optimizations
run-release *ARGS:
    cargo run --release -- {{ARGS}}

# Check code without building
check:
    cargo check

# Run clippy linter
lint:
    cargo clippy -- -D warnings -A dead_code

# Fix clippy warnings automatically where possible
lint-fix:
    cargo clippy --fix --allow-dirty --allow-staged

# Format code
fmt:
    cargo fmt

# Check formatting without modifying files
fmt-check:
    cargo fmt -- --check

# Run all quality checks (fmt, clippy, test)
ci: fmt-check lint test

# Clean build artifacts
clean:
    cargo clean

# Update dependencies
update:
    cargo update

# Generate and open documentation
doc:
    cargo doc --open --no-deps

# Watch for changes and run tests
watch-test:
    cargo watch -x test

# Watch for changes and run the proxy
watch-run:
    cargo watch -x run

# Start a local Postgres instance for testing (requires container runtime)
postgres-up:
    {{container_cmd}} run --name scry-postgres -e POSTGRES_PASSWORD=password -e POSTGRES_DB=testdb -p 5432:5432 -d postgres:16-alpine

# Stop the local Postgres instance
postgres-down:
    {{container_cmd}} stop scry-postgres && {{container_cmd}} rm scry-postgres

# View Postgres logs
postgres-logs:
    {{container_cmd}} logs -f scry-postgres

# Generate FlatBuffers schema bindings
generate-schemas:
    @echo "Generating FlatBuffers schemas..."
    @echo "Run: flatc --rust -o src/generated schemas/*.fbs"

# ============================================
# Container Commands
# ============================================

# Build container image (run from parent directory to include scry-protocol)
container-build:
    cd .. && {{container_cmd}} build -t scry-proxy -f scry-proxy/Dockerfile .

# Build container image with no cache
container-build-no-cache:
    cd .. && {{container_cmd}} build --no-cache -t scry-proxy -f scry-proxy/Dockerfile .

# Run container
container-run:
    {{container_cmd}} run --rm -it -p 5433:5433 -p 9090:9090 scry-proxy

# Tag and push to registry (requires REGISTRY env var)
container-push TAG="latest":
    {{container_cmd}} tag scry-proxy $REGISTRY/scry-proxy:{{TAG}} && {{container_cmd}} push $REGISTRY/scry-proxy:{{TAG}}

# ============================================
# Benchmark Commands
# ============================================

# Compose command: set COMPOSE_CMD env var to override (e.g., "docker compose")
compose_cmd := env("COMPOSE_CMD", "podman-compose")

# Benchmark configuration
bench_queries := env("BENCH_QUERIES", "100000")
bench_dir := "benchmarks"

# Build the benchmark runner
bench-build:
    cargo build --release -p scry-benchmarks

# Clean up benchmark containers
bench-clean:
    cd {{bench_dir}} && {{compose_cmd}} down -v 2>/dev/null || true

# Start postgres for benchmarks and wait for it to be ready
bench-postgres-up: bench-clean
    #!/usr/bin/env bash
    set -e
    cd {{bench_dir}}
    {{compose_cmd}} up -d postgres
    for i in {1..30}; do
        if {{compose_cmd}} exec -T postgres pg_isready -U postgres >/dev/null 2>&1; then
            echo "Postgres is ready"
            exit 0
        fi
        echo "Waiting for postgres... $i"
        sleep 1
    done
    echo "Postgres failed to start"
    exit 1

# Start a proxy service and wait for it
[private]
bench-start-proxy proxy:
    cd {{bench_dir}} && {{compose_cmd}} up -d {{proxy}}
    sleep 3

# Stop a proxy service
[private]
bench-stop-proxy proxy:
    cd {{bench_dir}} && {{compose_cmd}} stop {{proxy}} 2>/dev/null || true

# Run benchmark against a target
[private]
bench-run label proxy url connections output_dir:
    #!/usr/bin/env bash
    set -e
    echo -e "\033[0;32m[INFO]\033[0m Running: {{label}} with {{connections}} connections"
    ./target/release/bench-runner \
        --database-url "{{url}}" \
        --connections {{connections}} \
        --queries {{bench_queries}} \
        --label "{{label}}" \
        --proxy "{{proxy}}" \
        --output "{{output_dir}}/{{label}}-{{connections}}conn.json"

# Run a quick benchmark against direct Postgres (10 connections, 1000 queries)
bench-quick: bench-build bench-postgres-up
    #!/usr/bin/env bash
    set -e
    mkdir -p {{bench_dir}}/results
    ./target/release/bench-runner \
        --database-url "postgres://postgres:postgres@localhost:5432/postgres" \
        --connections 10 \
        --queries 1000 \
        --label "quick-test" \
        --proxy "postgres" \
        --output "{{bench_dir}}/results/quick-test.json"
    just bench-clean

# Run benchmark for direct postgres at specified connections
bench-direct connections="10": bench-build bench-postgres-up
    #!/usr/bin/env bash
    set -e
    RESULTS_DIR="{{bench_dir}}/results/direct-$(date +%Y%m%d-%H%M%S)"
    mkdir -p "$RESULTS_DIR"
    just bench-run "direct" "postgres" "postgres://postgres:postgres@localhost:5432/postgres" {{connections}} "$RESULTS_DIR"
    echo "Results saved to: $RESULTS_DIR"
    just bench-clean

# Run benchmark for pgbouncer at specified connections
bench-pgbouncer connections="10": bench-build bench-postgres-up
    #!/usr/bin/env bash
    set -e
    RESULTS_DIR="{{bench_dir}}/results/pgbouncer-$(date +%Y%m%d-%H%M%S)"
    mkdir -p "$RESULTS_DIR"
    just bench-start-proxy pgbouncer
    just bench-run "pgbouncer" "pgbouncer" "postgres://postgres:postgres@localhost:6432/postgres" {{connections}} "$RESULTS_DIR"
    echo "Results saved to: $RESULTS_DIR"
    just bench-clean

# Run benchmark for pgcat at specified connections
bench-pgcat connections="10": bench-build bench-postgres-up
    #!/usr/bin/env bash
    set -e
    RESULTS_DIR="{{bench_dir}}/results/pgcat-$(date +%Y%m%d-%H%M%S)"
    mkdir -p "$RESULTS_DIR"
    just bench-start-proxy pgcat
    just bench-run "pgcat" "pgcat" "postgres://postgres:postgres@localhost:6433/postgres" {{connections}} "$RESULTS_DIR"
    echo "Results saved to: $RESULTS_DIR"
    just bench-clean

# Run benchmark for scry proxy at specified connections
bench-scry connections="10": bench-build bench-postgres-up
    #!/usr/bin/env bash
    set -e
    RESULTS_DIR="{{bench_dir}}/results/scry-$(date +%Y%m%d-%H%M%S)"
    mkdir -p "$RESULTS_DIR"
    just bench-start-proxy scry
    # Wait a bit longer for scry to initialize
    sleep 2
    just bench-run "scry" "scry" "postgres://postgres:postgres@localhost:5434/postgres" {{connections}} "$RESULTS_DIR"
    echo "Results saved to: $RESULTS_DIR"
    just bench-clean

# Run full comparison benchmark suite across all proxies
bench-full connections="1 10 50 100": bench-build
    #!/usr/bin/env bash
    set -euo pipefail
    RESULTS_DIR="{{bench_dir}}/results/comparison-$(date +%Y%m%d-%H%M%S)"
    mkdir -p "$RESULTS_DIR"

    echo -e "\033[0;32m[INFO]\033[0m === Proxy Comparison Benchmark ==="
    echo -e "\033[0;32m[INFO]\033[0m Queries per run: {{bench_queries}}"
    echo -e "\033[0;32m[INFO]\033[0m Connection counts: {{connections}}"
    echo -e "\033[0;32m[INFO]\033[0m Results directory: $RESULTS_DIR"
    echo ""

    for conns in {{connections}}; do
        echo -e "\033[0;32m[INFO]\033[0m === Testing with $conns connections ==="

        # Direct Postgres (baseline)
        just bench-postgres-up
        just bench-run "direct" "postgres" "postgres://postgres:postgres@localhost:5432/postgres" "$conns" "$RESULTS_DIR"

        # PgBouncer
        just bench-start-proxy pgbouncer
        just bench-run "pgbouncer" "pgbouncer" "postgres://postgres:postgres@localhost:6432/postgres" "$conns" "$RESULTS_DIR"
        just bench-stop-proxy pgbouncer

        # PgCat
        just bench-start-proxy pgcat
        just bench-run "pgcat" "pgcat" "postgres://postgres:postgres@localhost:6433/postgres" "$conns" "$RESULTS_DIR"
        just bench-stop-proxy pgcat

        # Scry
        just bench-start-proxy scry
        sleep 2
        just bench-run "scry" "scry" "postgres://postgres:postgres@localhost:5434/postgres" "$conns" "$RESULTS_DIR"
        just bench-stop-proxy scry

        just bench-clean
    done

    echo ""
    echo -e "\033[0;32m[INFO]\033[0m === Benchmark Complete ==="
    echo -e "\033[0;32m[INFO]\033[0m Results saved to: $RESULTS_DIR"
    echo -e "\033[0;32m[INFO]\033[0m Generate charts with: just bench-charts $RESULTS_DIR"

# Run scale test (high connection counts)
bench-scale: bench-build
    #!/usr/bin/env bash
    set -euo pipefail
    RESULTS_DIR="{{bench_dir}}/results/scale-$(date +%Y%m%d-%H%M%S)"
    mkdir -p "$RESULTS_DIR"

    echo -e "\033[0;32m[INFO]\033[0m === Scale Test ==="
    echo -e "\033[0;32m[INFO]\033[0m Testing at 50, 100, 200 connections"

    for conns in 50 100 200; do
        echo -e "\033[0;32m[INFO]\033[0m === Testing with $conns connections ==="

        just bench-postgres-up
        just bench-run "direct" "postgres" "postgres://postgres:postgres@localhost:5432/postgres" "$conns" "$RESULTS_DIR"

        just bench-start-proxy pgbouncer
        just bench-run "pgbouncer" "pgbouncer" "postgres://postgres:postgres@localhost:6432/postgres" "$conns" "$RESULTS_DIR"
        just bench-stop-proxy pgbouncer

        just bench-start-proxy pgcat
        just bench-run "pgcat" "pgcat" "postgres://postgres:postgres@localhost:6433/postgres" "$conns" "$RESULTS_DIR"
        just bench-stop-proxy pgcat

        just bench-start-proxy scry
        sleep 2
        just bench-run "scry" "scry" "postgres://postgres:postgres@localhost:5434/postgres" "$conns" "$RESULTS_DIR"
        just bench-stop-proxy scry

        just bench-clean
    done

    echo -e "\033[0;32m[INFO]\033[0m Results saved to: $RESULTS_DIR"

# Generate charts from benchmark results
bench-charts results_dir:
    python3 {{bench_dir}}/generate_charts.py {{results_dir}}

# ============================================
# Profiling Commands
# ============================================

# Profiling compose command (uses overlay)
profile_compose := compose_cmd + " -f docker-compose.yml -f docker-compose.profile.yml"

# Start scry with profiling overlay (perf + inferno tools available)
bench-profile-up:
    cd {{bench_dir}} && {{profile_compose}} up -d

# Stop profiling environment
bench-profile-down:
    cd {{bench_dir}} && {{profile_compose}} down -v 2>/dev/null || true

# Run CPU profile and generate flamegraph (duration in seconds)
bench-profile duration="30":
    #!/usr/bin/env bash
    set -e
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    PROFILE_NAME="scry_profile_${TIMESTAMP}"

    echo "=== Scry Proxy CPU Profiler ==="
    echo "Duration: {{duration}}s"
    echo "Output: {{bench_dir}}/profiles/${PROFILE_NAME}.svg"
    echo ""

    # Check if scry container is running
    cd {{bench_dir}}
    if ! {{profile_compose}} ps scry | grep -q "running"; then
        echo "Error: scry container not running"
        echo "Start with: just bench-profile-up"
        exit 1
    fi

    echo "Starting perf recording..."
    echo "(Run your benchmark in another terminal now)"
    echo ""

    # Record perf data inside the container
    {{profile_compose}} exec scry bash -c "
        cd /profiles
        # Find the scry-proxy PID
        PID=\$(pgrep scry-proxy)
        if [ -z \"\$PID\" ]; then
            echo 'Error: scry-proxy process not found'
            exit 1
        fi
        echo \"Profiling PID \$PID for {{duration}} seconds...\"

        # Record with call graphs (-g), targeting specific PID
        perf record -F 99 -g -p \$PID -o perf.data -- sleep {{duration}}

        echo 'Generating flamegraph...'
        perf script -i perf.data | inferno-collapse-perf | inferno-flamegraph > ${PROFILE_NAME}.svg

        echo 'Done!'
        ls -la ${PROFILE_NAME}.svg
    "

    echo ""
    echo "=== Profile Complete ==="
    echo "Flamegraph saved to: {{bench_dir}}/profiles/${PROFILE_NAME}.svg"
    echo "Open in a browser to explore the results"

# ============================================
# Resource Benchmark Commands
# ============================================

# Port mappings for resource benchmarks
postgres_port := "5432"
pgbouncer_port := "6432"
pgcat_port := "6433"
scry_port := "5434"

# Resource benchmark settings
resource_queries := "100000"

# Container names (based on compose project name "benchmarks")
postgres_container := "benchmarks-postgres-1"

# Wait for postgres to be ready
[private]
bench-wait-postgres:
    #!/usr/bin/env bash
    set -e
    cd {{bench_dir}}
    echo "Waiting for postgres to be ready..."
    for i in {1..30}; do
        if {{compose_cmd}} exec -T postgres pg_isready -U postgres > /dev/null 2>&1; then
            echo "Postgres is ready"
            exit 0
        fi
        sleep 1
    done
    echo "Postgres failed to start"
    exit 1

# Wait for a proxy to be ready on specified port
[private]
bench-wait-port port name:
    #!/usr/bin/env bash
    set -e
    echo "Waiting for {{name}} to be ready on port {{port}}..."
    for i in {1..30}; do
        if PGPASSWORD=postgres psql -h localhost -p {{port}} -U postgres -c "SELECT 1" > /dev/null 2>&1; then
            echo "{{name}} is ready"
            exit 0
        fi
        sleep 1
    done
    echo "{{name}} failed to start"
    exit 1

# Run bench-runner with resource monitoring flags
[private]
bench-resource-run label proxy port connections proxy_container="":
    #!/usr/bin/env bash
    set -e
    echo ""
    echo "=============================================="
    echo "Testing {{label}} at {{connections}} connections"
    echo "=============================================="

    RESULTS_DIR="{{bench_dir}}/results/resources-$(date +%Y%m%d-%H%M%S)"
    mkdir -p "$RESULTS_DIR"

    PROXY_ARGS=""
    if [ -n "{{proxy_container}}" ]; then
        PROXY_ARGS="--proxy-container {{proxy_container}}"
    fi

    ./target/release/bench-runner \
        --database-url "postgres://postgres:postgres@localhost:{{port}}/postgres" \
        --connections {{connections}} \
        --queries {{resource_queries}} \
        --label "{{label}}" \
        --proxy "{{proxy}}" \
        --postgres-container "{{postgres_container}}" \
        $PROXY_ARGS \
        --output "$RESULTS_DIR/{{label}}.json"

    echo "Results saved to: $RESULTS_DIR/{{label}}.json"

# Run resource benchmark against direct Postgres (baseline)
bench-resource-direct connections="20": bench-build
    #!/usr/bin/env bash
    set -e
    cd {{bench_dir}} && {{compose_cmd}} down -v 2>/dev/null || true
    cd {{bench_dir}} && {{compose_cmd}} up -d postgres
    just bench-wait-postgres
    just bench-resource-run "postgres-direct-{{connections}}conn" "postgres" "{{postgres_port}}" "{{connections}}"
    cd {{bench_dir}} && {{compose_cmd}} down -v

# Run resource benchmark against PgBouncer
bench-resource-pgbouncer connections="20": bench-build
    #!/usr/bin/env bash
    set -e
    cd {{bench_dir}} && {{compose_cmd}} down -v 2>/dev/null || true
    cd {{bench_dir}} && {{compose_cmd}} up -d postgres
    just bench-wait-postgres
    cd {{bench_dir}} && {{compose_cmd}} up -d pgbouncer
    just bench-wait-port "{{pgbouncer_port}}" "pgbouncer"
    just bench-resource-run "pgbouncer-{{connections}}conn" "pgbouncer" "{{pgbouncer_port}}" "{{connections}}" "benchmarks-pgbouncer-1"
    cd {{bench_dir}} && {{compose_cmd}} down -v

# Run resource benchmark against PgCat
bench-resource-pgcat connections="20": bench-build
    #!/usr/bin/env bash
    set -e
    cd {{bench_dir}} && {{compose_cmd}} down -v 2>/dev/null || true
    cd {{bench_dir}} && {{compose_cmd}} up -d postgres
    just bench-wait-postgres
    cd {{bench_dir}} && {{compose_cmd}} up -d pgcat
    just bench-wait-port "{{pgcat_port}}" "pgcat"
    just bench-resource-run "pgcat-{{connections}}conn" "pgcat" "{{pgcat_port}}" "{{connections}}" "benchmarks-pgcat-1"
    cd {{bench_dir}} && {{compose_cmd}} down -v

# Run resource benchmark against Scry (publisher disabled - base overhead)
bench-resource-scry-base connections="20": bench-build
    #!/usr/bin/env bash
    set -e
    PROXY_CONTAINER="benchmarks-scry-base-1"
    cd {{bench_dir}} && {{compose_cmd}} down -v 2>/dev/null || true
    cd {{bench_dir}} && {{compose_cmd}} up -d postgres
    just bench-wait-postgres

    # Start scry with publisher disabled
    cd {{bench_dir}} && {{compose_cmd}} run -d --name "$PROXY_CONTAINER" --service-ports \
        -e SCRY_PUBLISHER__ENABLED=false scry
    just bench-wait-port "{{scry_port}}" "scry-base"

    just bench-resource-run "scry-base-{{connections}}conn" "scry" "{{scry_port}}" "{{connections}}" "$PROXY_CONTAINER"

    {{container_cmd}} stop "$PROXY_CONTAINER" 2>/dev/null || true
    {{container_cmd}} rm -f "$PROXY_CONTAINER" 2>/dev/null || true
    cd {{bench_dir}} && {{compose_cmd}} down -v

# Run resource benchmark against Scry (events enabled, no anonymization)
bench-resource-scry-events connections="20": bench-build
    #!/usr/bin/env bash
    set -e
    PROXY_CONTAINER="benchmarks-scry-events-1"
    cd {{bench_dir}} && {{compose_cmd}} down -v 2>/dev/null || true
    cd {{bench_dir}} && {{compose_cmd}} up -d postgres
    just bench-wait-postgres

    # Start scry with publisher enabled, anonymization disabled
    cd {{bench_dir}} && {{compose_cmd}} run -d --name "$PROXY_CONTAINER" --service-ports \
        -e SCRY_PUBLISHER__ENABLED=true -e SCRY_PUBLISHER__ANONYMIZE=false scry
    just bench-wait-port "{{scry_port}}" "scry-events"

    just bench-resource-run "scry-events-{{connections}}conn" "scry" "{{scry_port}}" "{{connections}}" "$PROXY_CONTAINER"

    {{container_cmd}} stop "$PROXY_CONTAINER" 2>/dev/null || true
    {{container_cmd}} rm -f "$PROXY_CONTAINER" 2>/dev/null || true
    cd {{bench_dir}} && {{compose_cmd}} down -v

# Run resource benchmark against Scry (full - events + anonymization)
bench-resource-scry-full connections="20": bench-build
    #!/usr/bin/env bash
    set -e
    PROXY_CONTAINER="benchmarks-scry-full-1"
    cd {{bench_dir}} && {{compose_cmd}} down -v 2>/dev/null || true
    cd {{bench_dir}} && {{compose_cmd}} up -d postgres
    just bench-wait-postgres

    # Start scry with publisher and anonymization enabled
    cd {{bench_dir}} && {{compose_cmd}} run -d --name "$PROXY_CONTAINER" --service-ports \
        -e SCRY_PUBLISHER__ENABLED=true -e SCRY_PUBLISHER__ANONYMIZE=true scry
    just bench-wait-port "{{scry_port}}" "scry-full"

    just bench-resource-run "scry-full-{{connections}}conn" "scry" "{{scry_port}}" "{{connections}}" "$PROXY_CONTAINER"

    {{container_cmd}} stop "$PROXY_CONTAINER" 2>/dev/null || true
    {{container_cmd}} rm -f "$PROXY_CONTAINER" 2>/dev/null || true
    cd {{bench_dir}} && {{compose_cmd}} down -v

# Run complete resource benchmark suite (all proxies at 20 and 50 connections)
bench-resource-full: bench-build
    #!/usr/bin/env bash
    set -euo pipefail
    RESULTS_DIR="{{bench_dir}}/results/resources-$(date +%Y%m%d-%H%M%S)"
    mkdir -p "$RESULTS_DIR"

    echo "=============================================="
    echo "Resource Benchmark Full Suite"
    echo "=============================================="
    echo "Queries per run: {{resource_queries}}"
    echo "Connection counts: 20, 50"
    echo "Results directory: $RESULTS_DIR"
    echo ""

    # Build bench-runner
    cargo build --release -p scry-benchmarks

    # Start postgres
    cd {{bench_dir}} && {{compose_cmd}} down -v 2>/dev/null || true
    cd {{bench_dir}} && {{compose_cmd}} up -d postgres
    just bench-wait-postgres

    # Test Direct Postgres (baseline)
    for CONN in 20 50; do
        echo ""
        echo "=== Direct Postgres - $CONN connections ==="
        ./target/release/bench-runner \
            --database-url "postgres://postgres:postgres@localhost:{{postgres_port}}/postgres" \
            --connections $CONN \
            --queries {{resource_queries}} \
            --label "postgres-direct-${CONN}conn" \
            --proxy "postgres" \
            --postgres-container "{{postgres_container}}" \
            --output "$RESULTS_DIR/postgres-direct-${CONN}conn.json"
    done

    # Test PgBouncer
    cd {{bench_dir}} && {{compose_cmd}} up -d pgbouncer
    just bench-wait-port "{{pgbouncer_port}}" "pgbouncer"
    for CONN in 20 50; do
        echo ""
        echo "=== PgBouncer - $CONN connections ==="
        ./target/release/bench-runner \
            --database-url "postgres://postgres:postgres@localhost:{{pgbouncer_port}}/postgres" \
            --connections $CONN \
            --queries {{resource_queries}} \
            --label "pgbouncer-${CONN}conn" \
            --proxy "pgbouncer" \
            --proxy-container "benchmarks-pgbouncer-1" \
            --postgres-container "{{postgres_container}}" \
            --output "$RESULTS_DIR/pgbouncer-${CONN}conn.json"
    done
    cd {{bench_dir}} && {{compose_cmd}} stop pgbouncer

    # Test PgCat
    cd {{bench_dir}} && {{compose_cmd}} up -d pgcat
    just bench-wait-port "{{pgcat_port}}" "pgcat"
    for CONN in 20 50; do
        echo ""
        echo "=== PgCat - $CONN connections ==="
        ./target/release/bench-runner \
            --database-url "postgres://postgres:postgres@localhost:{{pgcat_port}}/postgres" \
            --connections $CONN \
            --queries {{resource_queries}} \
            --label "pgcat-${CONN}conn" \
            --proxy "pgcat" \
            --proxy-container "benchmarks-pgcat-1" \
            --postgres-container "{{postgres_container}}" \
            --output "$RESULTS_DIR/pgcat-${CONN}conn.json"
    done
    cd {{bench_dir}} && {{compose_cmd}} stop pgcat

    # Test Scry (base - no events)
    for CONN in 20 50; do
        echo ""
        echo "=== Scry Base (no events) - $CONN connections ==="
        PROXY_CONTAINER="benchmarks-scry-base-1"
        cd {{bench_dir}} && {{compose_cmd}} run -d --name "$PROXY_CONTAINER" --service-ports \
            -e SCRY_PUBLISHER__ENABLED=false scry
        just bench-wait-port "{{scry_port}}" "scry-base"
        ./target/release/bench-runner \
            --database-url "postgres://postgres:postgres@localhost:{{scry_port}}/postgres" \
            --connections $CONN \
            --queries {{resource_queries}} \
            --label "scry-base-${CONN}conn" \
            --proxy "scry" \
            --proxy-container "$PROXY_CONTAINER" \
            --postgres-container "{{postgres_container}}" \
            --output "$RESULTS_DIR/scry-base-${CONN}conn.json"
        {{container_cmd}} stop "$PROXY_CONTAINER" 2>/dev/null || true
        {{container_cmd}} rm -f "$PROXY_CONTAINER" 2>/dev/null || true
    done

    # Test Scry (events enabled)
    for CONN in 20 50; do
        echo ""
        echo "=== Scry Events (no anonymization) - $CONN connections ==="
        PROXY_CONTAINER="benchmarks-scry-events-1"
        cd {{bench_dir}} && {{compose_cmd}} run -d --name "$PROXY_CONTAINER" --service-ports \
            -e SCRY_PUBLISHER__ENABLED=true -e SCRY_PUBLISHER__ANONYMIZE=false scry
        just bench-wait-port "{{scry_port}}" "scry-events"
        ./target/release/bench-runner \
            --database-url "postgres://postgres:postgres@localhost:{{scry_port}}/postgres" \
            --connections $CONN \
            --queries {{resource_queries}} \
            --label "scry-events-${CONN}conn" \
            --proxy "scry" \
            --proxy-container "$PROXY_CONTAINER" \
            --postgres-container "{{postgres_container}}" \
            --output "$RESULTS_DIR/scry-events-${CONN}conn.json"
        {{container_cmd}} stop "$PROXY_CONTAINER" 2>/dev/null || true
        {{container_cmd}} rm -f "$PROXY_CONTAINER" 2>/dev/null || true
    done

    # Test Scry (full - events + anonymization)
    for CONN in 20 50; do
        echo ""
        echo "=== Scry Full (events + anonymization) - $CONN connections ==="
        PROXY_CONTAINER="benchmarks-scry-full-1"
        cd {{bench_dir}} && {{compose_cmd}} run -d --name "$PROXY_CONTAINER" --service-ports \
            -e SCRY_PUBLISHER__ENABLED=true -e SCRY_PUBLISHER__ANONYMIZE=true scry
        just bench-wait-port "{{scry_port}}" "scry-full"
        ./target/release/bench-runner \
            --database-url "postgres://postgres:postgres@localhost:{{scry_port}}/postgres" \
            --connections $CONN \
            --queries {{resource_queries}} \
            --label "scry-full-${CONN}conn" \
            --proxy "scry" \
            --proxy-container "$PROXY_CONTAINER" \
            --postgres-container "{{postgres_container}}" \
            --output "$RESULTS_DIR/scry-full-${CONN}conn.json"
        {{container_cmd}} stop "$PROXY_CONTAINER" 2>/dev/null || true
        {{container_cmd}} rm -f "$PROXY_CONTAINER" 2>/dev/null || true
    done

    # Cleanup
    cd {{bench_dir}} && {{compose_cmd}} down -v

    echo ""
    echo "=============================================="
    echo "Resource Benchmark Complete"
    echo "=============================================="
    echo "Results saved to: $RESULTS_DIR"
    echo ""
    echo "Summary of result files:"
    ls -la "$RESULTS_DIR"
