# Getting Started with Scry

This guide will walk you through installing Scry, running it for the first time, and verifying that all observability features are working correctly.

## Prerequisites

Before you begin, ensure you have the following installed:

- **Rust** (latest stable) - Install from [rustup.rs](https://rustup.rs/)
- **just** - Command runner - Install with `cargo install just`
- **Docker** - For running Postgres locally - Install from [docker.com](https://www.docker.com/)
- **psql** (PostgreSQL client) - For connecting to the proxy

Verify your installations:

```bash
rustc --version    # Should show 1.70+
just --version     # Should show 1.0+
docker --version   # Should show 20.0+
psql --version     # Should show 12.0+
```

## Installation

### 1. Clone and Build

```bash
# Clone the repository (or use your own fork)
git clone https://github.com/your-org/scry.git
cd scry

# Build the project in release mode for best performance
just build

# Or build in debug mode for development
cargo build
```

The build process will:
- Compile all Rust dependencies
- Run cargo checks
- Produce an optimized binary at `target/release/scry`

### 2. Verify the Build

```bash
# Run tests to ensure everything works
just test

# Run just unit tests (faster)
just test-unit
```

You should see output indicating all tests passed.

## First Run

### 1. Start a Postgres Database

Scry needs a backend Postgres database to proxy. Use the included just command to start one locally:

```bash
# Start Postgres in Docker (runs on port 5432)
just postgres-up
```

This starts a Postgres 15 container with:
- Host: `localhost`
- Port: `5432`
- Database: `postgres`
- User: `postgres`
- Password: `postgres`

### 2. Start Scry Proxy

In a new terminal window, start the Scry proxy:

```bash
# Run with default configuration
just run

# Or run directly with cargo
cargo run --release
```

You should see output like:

```
2025-12-06T10:00:00.123Z INFO scry: Starting Scry proxy
2025-12-06T10:00:00.124Z INFO scry: Proxy listening on 127.0.0.1:5433
2025-12-06T10:00:00.125Z INFO scry: Metrics server listening on 127.0.0.1:9090
2025-12-06T10:00:00.126Z INFO scry: Backend: localhost:5432
2025-12-06T10:00:00.127Z INFO scry: Circuit breaker: enabled
2025-12-06T10:00:00.128Z INFO scry: Event publisher: debug (anonymization: enabled)
```

The proxy is now:
- **Accepting connections** on `127.0.0.1:5433`
- **Forwarding queries** to `localhost:5432`
- **Serving metrics** on `http://localhost:9090`

### 3. Connect Through the Proxy

In a third terminal, connect using psql:

```bash
# Connect to Scry proxy (port 5433)
psql -h 127.0.0.1 -p 5433 -U postgres -d postgres
```

Enter password: `postgres`

You're now connected through Scry! Try running some queries:

```sql
-- Create a test table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100)
);

-- Insert some data
INSERT INTO users (name, email) VALUES
    ('Alice', 'alice@example.com'),
    ('Bob', 'bob@example.com'),
    ('Charlie', 'charlie@example.com');

-- Query the data
SELECT * FROM users WHERE id = 1;

-- Update a record
UPDATE users SET email = 'alice@newdomain.com' WHERE id = 1;

-- Delete a record
DELETE FROM users WHERE id = 3;
```

### 4. Verify Observability Features

#### Check Metrics

In a browser or with curl, view the Prometheus metrics:

```bash
curl http://localhost:9090/metrics
```

You should see metrics like:

```
# HELP scry_queries_total Total number of queries processed
# TYPE scry_queries_total counter
scry_queries_total 5

# HELP scry_query_latency_seconds Query latency in seconds
# TYPE scry_query_latency_seconds summary
scry_query_latency_seconds{quantile="0.5"} 0.000123
scry_query_latency_seconds{quantile="0.99"} 0.000456

# HELP scry_pool_connections_total Current pool size
# TYPE scry_pool_connections_total gauge
scry_pool_connections_total 1
```

#### Check Health Status

```bash
curl http://localhost:9090/health
```

You should see:

```json
{
  "status": "Healthy",
  "uptime_secs": 123,
  "queries_total": 5,
  "error_rate": 0.0,
  "pool_utilization": 0.1,
  "warnings": []
}
```

#### Check Pool Status

```bash
curl http://localhost:9090/debug/pool
```

Output:

```json
{
  "size": 1,
  "available": 1,
  "max_size": 100,
  "utilization": 0.0
}
```

#### Check Hot Data

```bash
curl http://localhost:9090/debug/hot_data
```

This shows the most frequently accessed value fingerprints:

```json
{
  "top_k": [
    {
      "fingerprint": "blake3:abc123...",
      "access_count": 15
    }
  ]
}
```

#### View Event Logs

In the Scry terminal, you should see DEBUG logs showing query events:

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2025-12-06T10:05:00.123Z",
  "query": "SELECT * FROM users WHERE id = ?",
  "normalized_query": "SELECT * FROM users WHERE id = ?",
  "value_fingerprints": ["blake3:abc123..."],
  "duration_ms": 0.45,
  "success": true,
  "database": "postgres"
}
```

Notice that the query value (`1`) has been replaced with `?` and a fingerprint was generated. This is **query anonymization** in action!

## Configuration

### Using Environment Variables

Stop the proxy (Ctrl+C) and restart with custom configuration:

```bash
# Configure via environment variables
export SCRY_PROXY__LISTEN_ADDRESS=127.0.0.1:6543
export SCRY_BACKEND__HOST=localhost
export SCRY_BACKEND__PORT=5432
export SCRY_PUBLISHER__ANONYMIZE=true
export SCRY_RESILIENCE__CIRCUIT_BREAKER__ENABLED=true

# Run with custom config
just run
```

### Using a Config File

Create a `scry.toml` file:

```toml
[proxy]
listen_address = "127.0.0.1:5433"
max_connections = 100
shutdown_timeout_secs = 30

[backend]
protocol = "Postgres"
host = "localhost"
port = 5432
database = "postgres"
user = "postgres"
password = "postgres"
pool_size = 10

[publisher]
enabled = true
batch_size = 100
flush_interval_ms = 1000
anonymize = true
publisher_type = "debug"

[resilience.circuit_breaker]
enabled = true
failure_threshold = 5
success_threshold = 2
open_timeout_secs = 60

[resilience.healthcheck]
active_enabled = true
interval_secs = 30
timeout_ms = 1000
```

Run with the config file:

```bash
SCRY_CONFIG_FILE=scry.toml cargo run --release
```

See the [Configuration Guide](configuration.md) for complete reference.

## Testing Resilience Features

### Circuit Breaker

Test the circuit breaker by stopping the backend database:

```bash
# Stop Postgres
just postgres-down

# Try to query through the proxy
psql -h 127.0.0.1 -p 5433 -U postgres -c "SELECT 1"
# First few queries will fail slowly as circuit breaker opens

# Check circuit breaker state
curl http://localhost:9090/metrics | grep circuit_breaker_state
# scry_circuit_breaker_state 1  (1 = Open)

# Restart Postgres
just postgres-up

# Wait 60 seconds (open_timeout_secs)
# Circuit breaker will transition to HalfOpen and test backend

# Try query again
psql -h 127.0.0.1 -p 5433 -U postgres -c "SELECT 1"
# Should succeed, circuit breaker closes

# Check state again
curl http://localhost:9090/metrics | grep circuit_breaker_state
# scry_circuit_breaker_state 0  (0 = Closed)
```

### Connection Retry

The proxy automatically retries failed connections with exponential backoff:

```bash
# Monitor logs while restarting backend
just postgres-down
just postgres-up

# In Scry logs you'll see:
# "Connection attempt 1 failed, retrying in 50ms..."
# "Connection attempt 2 failed, retrying in 100ms..."
# "Connection succeeded on attempt 3"
```

### Health Checks

View health status during normal operation:

```bash
curl http://localhost:9090/health
```

The health monitor tracks:
- Error rate baseline and spikes
- Latency baseline (P99) and spikes
- Pool saturation and starvation

See [Health Checks](health-checks.md) for details.

## Next Steps

Now that you have Scry running, explore these topics:

- **[Architecture](architecture.md)** - Understand how Scry works internally
- **[Configuration](configuration.md)** - Learn about all configuration options
- **[Observability](observability.md)** - Set up event publishing to a central service
- **[Circuit Breaker](circuit-breaker.md)** - Deep dive into circuit breaker patterns
- **[Metrics](metrics.md)** - Set up Prometheus and Grafana dashboards
- **[Deployment](deployment.md)** - Deploy Scry to production

## Troubleshooting

### Proxy won't start

**Error**: `Address already in use`

```bash
# Check if something is using port 5433
lsof -i :5433

# Kill the process or use a different port
export SCRY_PROXY__LISTEN_ADDRESS=127.0.0.1:6543
```

### Can't connect to backend

**Error**: `Connection refused to localhost:5432`

```bash
# Verify Postgres is running
docker ps | grep postgres

# Check Postgres logs
docker logs scry-postgres

# Restart Postgres
just postgres-down
just postgres-up
```

### No metrics appearing

**Error**: `curl http://localhost:9090/metrics` returns connection refused

- Verify `enable_metrics_server` is true (default)
- Check logs for metrics server startup message
- Verify port 9090 is not in use

### Queries are slow

- Check if circuit breaker is open: `curl http://localhost:9090/metrics | grep circuit_breaker_state`
- Check backend latency: `curl http://localhost:9090/metrics | grep backend_seconds`
- Verify pool has available connections: `curl http://localhost:9090/debug/pool`

## Cleanup

When done testing:

```bash
# Stop Scry proxy (Ctrl+C in proxy terminal)

# Stop Postgres
just postgres-down

# Optionally remove build artifacts
cargo clean
```

## See Also

- [Configuration Guide](configuration.md) - Complete configuration reference
- [Development Guide](development.md) - Setting up development environment
- [Deployment Guide](deployment.md) - Production deployment patterns
