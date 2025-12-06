# Development Guide

This guide covers setting up your development environment, running tests, contributing to Scry, and extending its functionality.

## Table of Contents

- [Development Setup](#development-setup)
- [Project Structure](#project-structure)
- [Building](#building)
- [Testing](#testing)
- [Code Quality](#code-quality)
- [Adding Features](#adding-features)
- [Contributing](#contributing)

## Development Setup

### Prerequisites

Install required tools:

```bash
# Rust (latest stable)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# just (command runner)
cargo install just

# Docker (for integration tests)
# Install from https://docker.com

# Optional: Rust analyzer for IDE support
rustup component add rust-analyzer
```

### Clone Repository

```bash
git clone https://github.com/your-org/scry.git
cd scry
```

### IDE Setup

#### VS Code

Install extensions:
- `rust-analyzer` - Rust language server
- `CodeLLDB` - Debugger
- `Better TOML` - TOML syntax highlighting

Settings (`.vscode/settings.json`):
```json
{
  "rust-analyzer.cargo.features": "all",
  "rust-analyzer.checkOnSave.command": "clippy",
  "editor.formatOnSave": true,
  "[rust]": {
    "editor.defaultFormatter": "rust-lang.rust-analyzer"
  }
}
```

Launch config (`.vscode/launch.json`):
```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "type": "lldb",
      "request": "launch",
      "name": "Debug Scry",
      "cargo": {
        "args": ["build", "--bin=scry"],
        "filter": {
          "name": "scry",
          "kind": "bin"
        }
      },
      "args": [],
      "cwd": "${workspaceFolder}",
      "env": {
        "RUST_LOG": "debug,scry=trace",
        "SCRY_BACKEND__HOST": "localhost",
        "SCRY_BACKEND__PORT": "5432"
      }
    }
  ]
}
```

## Project Structure

```
scry/
├── src/
│   ├── main.rs                    # Entry point
│   ├── config/                    # Configuration
│   │   └── mod.rs
│   ├── proxy/                     # Core proxy
│   │   ├── server.rs             # TCP server
│   │   ├── tcp_pool.rs           # Connection pooling
│   │   ├── event_batcher.rs      # Event batching
│   │   └── protocol/             # Protocol abstraction
│   │       └── postgres.rs
│   ├── protocol/                  # Protocol parsing
│   │   ├── message.rs            # Message extraction
│   │   └── anonymize.rs          # Query anonymization
│   ├── publisher/                 # Event publishing
│   │   ├── trait.rs
│   │   ├── event.rs
│   │   ├── debug_logger.rs
│   │   ├── http_publisher.rs
│   │   └── flatbuffers_serializer.rs
│   ├── resilience/                # Resilience features
│   │   ├── circuit_breaker.rs
│   │   ├── retry.rs
│   │   └── healthcheck.rs
│   └── observability/             # Observability
│       ├── metrics.rs
│       ├── prometheus.rs
│       ├── health.rs
│       ├── hot_data.rs
│       ├── timeline.rs
│       └── metrics_server.rs
├── tests/                         # Integration tests
│   └── integration_tests.rs
├── benches/                       # Benchmarks
│   └── proxy_throughput.rs
├── docs/                          # Documentation
├── Cargo.toml                     # Dependencies
├── justfile                       # Command runner config
├── CLAUDE.md                      # Claude Code guidance
└── README.md
```

## Building

### Development Build

```bash
# Build in debug mode (fast compilation, slow runtime)
just build

# Or directly with cargo
cargo build
```

Binary location: `target/debug/scry`

### Release Build

```bash
# Build in release mode (slow compilation, fast runtime)
cargo build --release
```

Binary location: `target/release/scry`

**Release optimizations**:
- Inlining
- Dead code elimination
- Link-time optimization (LTO)

### Build Options

**With specific features** (future):
```bash
cargo build --features mysql
cargo build --features mongodb
```

**For a specific target**:
```bash
cargo build --target x86_64-unknown-linux-musl
```

## Testing

### Unit Tests

Run unit tests (fast, no external dependencies):

```bash
just test-unit

# Or directly
cargo test --lib
```

**Write unit tests** in same file as code:

```rust
// src/protocol/anonymize.rs

pub fn anonymize_query(query: &str) -> Result<AnonymizedQuery> {
    // ... implementation
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_anonymize_simple_query() {
        let query = "SELECT * FROM users WHERE id = 123";
        let result = anonymize_query(query).unwrap();

        assert_eq!(result.normalized_query, "SELECT * FROM users WHERE id = ?");
        assert_eq!(result.value_fingerprints.len(), 1);
    }
}
```

### Integration Tests

Run integration tests (require Docker for Postgres):

```bash
# Start Postgres
just postgres-up

# Run integration tests
just test-integration

# Or directly
cargo test --test integration_tests

# Stop Postgres
just postgres-down
```

**Write integration tests** in `tests/`:

```rust
// tests/integration_tests.rs

use testcontainers::clients::Cli;
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::NoTls;

#[tokio::test]
async fn test_proxy_basic_query() {
    // Start Postgres container
    let docker = Cli::default();
    let postgres = docker.run(Postgres::default());
    let port = postgres.get_host_port_ipv4(5432);

    // Start Scry proxy
    let proxy = start_proxy(port).await;

    // Connect through proxy
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port=5433 user=postgres"),
        NoTls,
    )
    .await
    .unwrap();

    tokio::spawn(connection);

    // Run query
    let result = client.query_one("SELECT 1 as num", &[]).await.unwrap();
    let num: i32 = result.get(0);

    assert_eq!(num, 1);
}
```

### All Tests

Run all tests (unit + integration):

```bash
just test
```

### Test Coverage

Generate coverage report:

```bash
# Install tarpaulin
cargo install cargo-tarpaulin

# Generate coverage
cargo tarpaulin --out Html --output-dir coverage/
```

View: `coverage/index.html`

## Code Quality

### Linting

Run Clippy (Rust linter):

```bash
just lint

# Or directly
cargo clippy -- -D warnings
```

**Fix automatically**:
```bash
cargo clippy --fix
```

### Formatting

Format code with rustfmt:

```bash
just fmt

# Or directly
cargo fmt
```

**Check formatting** (without modifying):
```bash
cargo fmt -- --check
```

### CI Checks

Run all CI checks locally:

```bash
just ci
```

This runs:
1. `cargo fmt -- --check` (formatting)
2. `cargo clippy -- -D warnings` (linting)
3. `cargo test` (all tests)

**Always run before committing!**

### Pre-Commit Hook

Create `.git/hooks/pre-commit`:

```bash
#!/bin/bash
set -e

echo "Running pre-commit checks..."

# Format check
cargo fmt -- --check

# Linting
cargo clippy -- -D warnings

# Unit tests
cargo test --lib

echo "✓ Pre-commit checks passed"
```

Make executable:
```bash
chmod +x .git/hooks/pre-commit
```

## Adding Features

### Adding a New Protocol

To add MySQL or MongoDB support:

1. **Implement Protocol trait**:

```rust
// src/proxy/protocol/mysql.rs

use async_trait::async_trait;
use tokio::net::TcpStream;

pub struct MySQLProtocol {
    // MySQL-specific config
}

#[async_trait]
impl Protocol for MySQLProtocol {
    async fn connect(&self, addr: SocketAddr) -> Result<TcpStream> {
        let stream = TcpStream::connect(addr).await?;
        // MySQL handshake
        Ok(stream)
    }

    async fn health_check(&self, conn: &mut TcpStream) -> Result<()> {
        // Send MySQL ping
        Ok(())
    }

    async fn reset_connection(&self, conn: &mut TcpStream) -> Result<()> {
        // Send RESET CONNECTION
        Ok(())
    }
}
```

2. **Add to protocol module**:

```rust
// src/proxy/protocol/mod.rs

#[cfg(feature = "mysql")]
pub mod mysql;

pub enum DatabaseProtocol {
    Postgres,
    #[cfg(feature = "mysql")]
    MySQL,
}
```

3. **Add feature flag** (`Cargo.toml`):

```toml
[features]
mysql = ["mysql_async"]

[dependencies]
mysql_async = { version = "0.32", optional = true }
```

4. **Test**:

```rust
#[cfg(all(test, feature = "mysql"))]
mod tests {
    #[tokio::test]
    async fn test_mysql_protocol() {
        // Test MySQL protocol
    }
}
```

### Adding a New Publisher

To add Kafka publisher:

1. **Implement EventPublisher trait**:

```rust
// src/publisher/kafka_publisher.rs

use async_trait::async_trait;
use rdkafka::producer::{FutureProducer, FutureRecord};

pub struct KafkaPublisher {
    producer: FutureProducer,
    topic: String,
}

#[async_trait]
impl EventPublisher for KafkaPublisher {
    async fn publish_batch(&self, events: Vec<QueryEvent>) -> Result<()> {
        let payload = serialize_events(&events)?;

        self.producer
            .send(
                FutureRecord::to(&self.topic).payload(&payload),
                Duration::from_secs(0),
            )
            .await
            .map_err(|(e, _)| anyhow!("Kafka send failed: {}", e))?;

        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        self.producer.flush(Duration::from_secs(30)).await?;
        Ok(())
    }
}
```

2. **Add to publisher module**:

```rust
// src/publisher/mod.rs

#[cfg(feature = "kafka")]
pub mod kafka_publisher;
```

3. **Add configuration**:

```rust
// src/config/mod.rs

#[derive(Debug, Deserialize)]
pub struct PublisherConfig {
    pub publisher_type: String,  // "debug", "http", "kafka"

    #[cfg(feature = "kafka")]
    pub kafka_brokers: Option<String>,
    #[cfg(feature = "kafka")]
    pub kafka_topic: Option<String>,
}
```

### Adding Metrics

To add new metrics:

1. **Add to ProxyMetrics**:

```rust
// src/observability/metrics.rs

pub struct ProxyMetrics {
    // Existing metrics...

    // New metric
    pub custom_metric: AtomicU64,
}

impl ProxyMetrics {
    pub fn record_custom(&self, value: u64) {
        self.custom_metric.fetch_add(value, Ordering::Relaxed);
    }
}
```

2. **Export to Prometheus**:

```rust
// src/observability/prometheus.rs

pub fn format_metrics(metrics: &ProxyMetrics) -> String {
    let mut output = String::new();

    // Existing metrics...

    // New metric
    writeln!(
        &mut output,
        "# HELP scry_custom_metric Description here\n\
         # TYPE scry_custom_metric counter\n\
         scry_custom_metric {}",
        metrics.custom_metric.load(Ordering::Relaxed)
    );

    output
}
```

3. **Record metric**:

```rust
// src/proxy/server.rs

metrics.record_custom(42);
```

## Contributing

### Contribution Workflow

1. **Fork repository** on GitHub

2. **Clone your fork**:
   ```bash
   git clone https://github.com/YOUR_USERNAME/scry.git
   cd scry
   git remote add upstream https://github.com/original/scry.git
   ```

3. **Create feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

4. **Make changes**:
   - Write code
   - Add tests
   - Update documentation

5. **Run checks**:
   ```bash
   just ci
   ```

6. **Commit**:
   ```bash
   git add .
   git commit -m "Add feature: description"
   ```

7. **Push to your fork**:
   ```bash
   git push origin feature/your-feature-name
   ```

8. **Create Pull Request** on GitHub

### Commit Message Guidelines

Use conventional commits:

```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types**:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `test`: Adding tests
- `refactor`: Code refactoring
- `perf`: Performance improvements
- `chore`: Build/tooling changes

**Examples**:
```
feat(circuit-breaker): add health monitor integration

Integrate circuit breaker with health monitor for predictive opening.
Circuit opens when health status becomes unhealthy.

Closes #123
```

```
fix(pool): prevent connection leak on error

Fixed bug where connections were not returned to pool on health check failure.

Fixes #456
```

### Code Review Checklist

Before submitting PR:

- [ ] Tests pass (`just ci`)
- [ ] New features have tests
- [ ] Documentation updated
- [ ] No clippy warnings
- [ ] Code formatted (`just fmt`)
- [ ] Commit messages follow guidelines
- [ ] PR description explains changes
- [ ] No breaking changes (or clearly documented)

### Documentation

Update documentation when:
- Adding new feature
- Changing configuration options
- Modifying behavior
- Fixing bugs that affect usage

Docs to update:
- Code comments
- `CLAUDE.md` - Implementation status
- `docs/` - Relevant documentation files
- `README.md` - If feature is user-facing

## See Also

- [Architecture](architecture.md) - System architecture
- [Configuration](configuration.md) - Configuration reference
- [Deployment](deployment.md) - Production deployment
- [Getting Started](getting-started.md) - Quick start guide
