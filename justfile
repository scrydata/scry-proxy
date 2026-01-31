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
