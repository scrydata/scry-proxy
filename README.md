# Scry

**A high-performance, transparent proxy for SQL databases with production-grade observability and resilience**

Built in Rust with Tokio, Scry sits between your application and database to provide deep visibility into query behavior, automatic failover, and intelligent connection management—all with <1ms overhead.

Scry is a modern alternative to traditional connection poolers like PgBouncer, adding comprehensive observability, circuit breaking, and health monitoring while maintaining the same pooling efficiency. It protects your database from connection explosions even when applications have their own connection pools.

## Project Structure

This repository is organized as a Cargo workspace:

- **`scry-proxy/`** - Main proxy server implementation
- **`benchmarks/`** - Performance benchmarking suite

The event protocol library ([`scry-protocol`](https://crates.io/crates/scry-protocol)) is published as a standalone crate on crates.io. It can be used independently by analytics services, monitoring dashboards, or any tool that needs to serialize/deserialize Scry query events.

## Features

Scry enhances your database infrastructure with enterprise-grade capabilities:

- **[Observability](docs/observability.md)** - Capture, anonymize, and publish per-query metrics with value fingerprinting for hot data detection
- **[Circuit Breaking](docs/circuit-breaker.md)** - Automatic failover with lock-free, three-state circuit breaker to protect your database
- **[Health Monitoring](docs/health-checks.md)** - Active and passive health checks with predictive anomaly detection using EMA baselines
- **[Connection Pooling](docs/connection-pooling.md)** - Protocol-agnostic connection pooling with automatic state reset and health validation
- **[TLS/SSL Support](#tls-configuration)** - Full TLS support for client and backend connections with configurable SSL modes
- **[Resilience](docs/resilience.md)** - Exponential backoff retries with jitter, integrated with circuit breaking and health monitoring
- **[Query Anonymization](docs/query-anonymization.md)** - Privacy-preserving query logging with Blake3 fingerprinting for compliance
- **[Metrics & Monitoring](docs/metrics.md)** - Prometheus metrics with percentile latencies, pool utilization, and circuit breaker state

## Quick Start

```bash
# Install prerequisites: Rust, just, Docker
just build

# Start local Postgres for testing
just postgres-up

# Run the proxy (listens on 127.0.0.1:5433, forwards to localhost:5432)
just run

# In another terminal, connect through the proxy
psql -h 127.0.0.1 -p 5433 -U postgres

# View metrics
curl http://localhost:9090/metrics
```

For a comprehensive getting started guide, see **[Getting Started](docs/getting-started.md)**.

## Run the published image

The recommended way to run Scry is the prebuilt container image, published to
the GitHub Container Registry. Images are multi-arch (`linux/amd64` +
`linux/arm64`) and tagged with each release version and `latest`:

```bash
docker pull ghcr.io/scrydata/scry-proxy:latest
```

Scry is configured with environment variables (12-factor). Point it at your
backend database, expose the client-facing port (`5433`) and the metrics port
(`9090`), then send your application's connections to the proxy instead of the
database:

```bash
docker run --rm \
  -p 5433:5433 -p 9090:9090 \
  -e SCRY_BACKEND__HOST=your-db.example.com \
  -e SCRY_BACKEND__PORT=5432 \
  ghcr.io/scrydata/scry-proxy:latest

# Application (or psql) connects to the proxy on 5433; it forwards to the backend
psql -h 127.0.0.1 -p 5433 -U postgres

# Prometheus metrics
curl http://localhost:9090/metrics
```

See the [Environment Variables Reference](#environment-variables-reference)
and **[Configuration](docs/configuration.md)** for the full set of options
(TLS, circuit breaker, event publishing). The `just`-based [Quick Start](#quick-start)
above builds from source and is intended for local development.

## Architecture

Scry is built on a high-performance, async architecture:

```
Client → Proxy (Circuit Breaker + Pool) → Backend Database
           ↓
    Event Publisher → Analytics Service
           ↓
    Metrics Server → Prometheus
```

The proxy intercepts Postgres wire protocol messages, extracts query metadata, and forwards requests to the backend with minimal overhead. Events are published asynchronously via batching for zero impact on query latency.

See **[Architecture](docs/architecture.md)** for detailed system design.

## Configuration

Scry follows the [12-factor app](https://12factor.net/) methodology. Configure via environment variables:

```bash
export SCRY_BACKEND__HOST=localhost
export SCRY_BACKEND__PORT=5432
export SCRY_RESILIENCE__CIRCUIT_BREAKER__ENABLED=true
export SCRY_PUBLISHER__ANONYMIZE=true
```

Or use a `scry.toml` configuration file. See **[Configuration](docs/configuration.md)** for complete reference.

## TLS Configuration

Scry supports TLS/SSL encryption for both client connections (clients → proxy) and backend connections (proxy → database), with SSL modes matching PgBouncer conventions.

### Client-Facing TLS (Clients → Proxy)

Accept encrypted connections from database clients:

```bash
# Enable TLS for client connections
export SCRY_TLS__CLIENT_TLS_SSLMODE=require
export SCRY_TLS__CLIENT_TLS_CERT_FILE=/path/to/server.crt
export SCRY_TLS__CLIENT_TLS_KEY_FILE=/path/to/server.key

# Optional: Require client certificates (mTLS)
export SCRY_TLS__CLIENT_TLS_SSLMODE=verify-ca
export SCRY_TLS__CLIENT_TLS_CA_FILE=/path/to/ca.crt
```

### Backend TLS (Proxy → Database)

Connect to TLS-enabled databases (RDS, Cloud SQL, etc.):

```bash
# Require TLS to backend (recommended for cloud databases)
export SCRY_TLS__SERVER_TLS_SSLMODE=require

# Verify backend certificate against CA
export SCRY_TLS__SERVER_TLS_SSLMODE=verify-ca
export SCRY_TLS__SERVER_TLS_CA_FILE=/path/to/backend-ca.crt

# Optional: Client certificate for backend authentication
export SCRY_TLS__SERVER_TLS_CERT_FILE=/path/to/client.crt
export SCRY_TLS__SERVER_TLS_KEY_FILE=/path/to/client.key
```

### SSL Modes

| Mode | Description |
|------|-------------|
| `disable` | No TLS (default) |
| `allow` | Use TLS if client/server requests it, otherwise plain TCP |
| `require` | Require TLS, but don't verify certificates |
| `verify-ca` | Require TLS with CA-verified certificate |
| `verify-full` | Require TLS with CA-verified certificate and hostname match |

### Environment Variables Reference

| Variable | Description | Default |
|----------|-------------|---------|
| `SCRY_TLS__CLIENT_TLS_SSLMODE` | Client connection TLS mode | `disable` |
| `SCRY_TLS__CLIENT_TLS_CERT_FILE` | Server certificate (PEM) | - |
| `SCRY_TLS__CLIENT_TLS_KEY_FILE` | Server private key (PEM) | - |
| `SCRY_TLS__CLIENT_TLS_CA_FILE` | CA for client cert verification | - |
| `SCRY_TLS__SERVER_TLS_SSLMODE` | Backend connection TLS mode | `disable` |
| `SCRY_TLS__SERVER_TLS_CA_FILE` | CA for backend cert verification | - |
| `SCRY_TLS__SERVER_TLS_CERT_FILE` | Client cert for backend auth | - |
| `SCRY_TLS__SERVER_TLS_KEY_FILE` | Client key for backend auth | - |

## Documentation

### Getting Started
- **[Getting Started Guide](docs/getting-started.md)** - Installation, setup, and first queries

### Core Concepts
- **[Architecture](docs/architecture.md)** - System design and component overview
- **[Configuration](docs/configuration.md)** - Complete configuration reference

### Features
- **[Observability](docs/observability.md)** - Event publishing, batching, and FlexBuffers serialization
- **[Connection Pooling](docs/connection-pooling.md)** - Pool management and lifecycle
- **[Circuit Breaker](docs/circuit-breaker.md)** - Automatic failover and state management
- **[Health Checks](docs/health-checks.md)** - Active/passive monitoring and predictive warnings
- **[Resilience](docs/resilience.md)** - Retry logic, backoff, and integrated resilience features
- **[Query Anonymization](docs/query-anonymization.md)** - Privacy-preserving query logging
- **[Metrics](docs/metrics.md)** - Prometheus metrics and monitoring

### Operations
- **[Deployment](docs/deployment.md)** - Production deployment patterns
- **[Development](docs/development.md)** - Developer guide and contributing

## Development

```bash
just build              # Build the project
just test               # Run all tests
just test-unit          # Run unit tests only
just test-integration   # Run integration tests (requires Docker)
just lint               # Run clippy linter
just fmt                # Format code
just ci                 # Run all CI checks (fmt, lint, test)
```

See **[Development Guide](docs/development.md)** for detailed development workflows.

## Performance

Scry is designed for production workloads with strict performance requirements:

- **Target Latency**: <1ms additional overhead per query
- **Lock-Free**: Circuit breaker and metrics use atomic operations
- **Async Throughout**: Tokio-based async runtime for maximum concurrency
- **Best-Effort Publishing**: Events published asynchronously, never block queries

## License

MIT OR Apache-2.0
