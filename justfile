# Scry - Transparent SQL Proxy
# Development commands using just (https://github.com/casey/just)

# Use PowerShell on Windows, bash on Unix
set shell := ["powershell.exe", "-c"]

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

# Start a local Postgres instance for testing (requires Docker)
postgres-up:
    docker run --name scry-postgres -e POSTGRES_PASSWORD=password -e POSTGRES_DB=testdb -p 5432:5432 -d postgres:16-alpine

# Stop the local Postgres instance
postgres-down:
    docker stop scry-postgres && docker rm scry-postgres

# View Postgres logs
postgres-logs:
    docker logs -f scry-postgres

# Generate FlatBuffers schema bindings
generate-schemas:
    @echo "Generating FlatBuffers schemas..."
    @echo "Run: flatc --rust -o src/generated schemas/*.fbs"

# ============================================
# Docker Commands
# ============================================

# Build Docker image (run from parent directory to include scry-protocol)
docker-build:
    Push-Location ..; docker build -t scry-proxy -f scry-proxy/Dockerfile .; Pop-Location

# Build Docker image with no cache
docker-build-no-cache:
    Push-Location ..; docker build --no-cache -t scry-proxy -f scry-proxy/Dockerfile .; Pop-Location

# Run Docker container
docker-run:
    docker run --rm -it -p 5433:5433 -p 9090:9090 scry-proxy

# Tag and push to registry (requires REGISTRY env var)
docker-push TAG="latest":
    docker tag scry-proxy ${env:REGISTRY}/scry-proxy:{{TAG}}; docker push ${env:REGISTRY}/scry-proxy:{{TAG}}

# ============================================
# Benchmark Commands
# ============================================

# Build the benchmark runner
bench-build:
    cargo build --release -p scry-benchmarks

# Run a quick benchmark against direct Postgres
bench-quick:
    Push-Location benchmarks; docker compose up -d postgres; Pop-Location
    Start-Sleep -Seconds 5
    ./target/release/bench-runner.exe --database-url "postgres://postgres:postgres@localhost:5432/postgres" --connections 10 --queries 1000 --label "quick-test" --proxy "postgres" --output "benchmarks/quick-test.json"
    Push-Location benchmarks; docker compose down -v; Pop-Location

# Run full comparison benchmark suite
bench-full:
    Push-Location benchmarks; bash run-comparison.sh; Pop-Location

# Generate charts from benchmark results
bench-charts RESULTS_DIR:
    python benchmarks/generate_charts.py {{RESULTS_DIR}}

# Clean up benchmark containers
bench-clean:
    Push-Location benchmarks; docker compose down -v; Pop-Location
