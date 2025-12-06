# Configuration

Scry follows the [12-factor app](https://12factor.net/) methodology for configuration, allowing flexible deployment across different environments.

## Table of Contents

- [Configuration Priority](#configuration-priority)
- [Environment Variables](#environment-variables)
- [Configuration File](#configuration-file)
- [Configuration Reference](#configuration-reference)
- [Common Scenarios](#common-scenarios)

## Configuration Priority

Configuration is loaded in this priority order (highest to lowest):

```
Environment Variables (SCRY_*)
         ↓
Configuration File (scry.toml or SCRY_CONFIG_FILE)
         ↓
Default Values
```

This allows you to:
- Set defaults in `scry.toml`
- Override specific values with environment variables in production
- Use different config files per environment

## Environment Variables

All configuration options can be set via environment variables with the `SCRY_` prefix.

### Naming Convention

Nested configuration uses double underscores (`__`):

```bash
# proxy.listen_address → SCRY_PROXY__LISTEN_ADDRESS
export SCRY_PROXY__LISTEN_ADDRESS="127.0.0.1:5433"

# backend.host → SCRY_BACKEND__HOST
export SCRY_BACKEND__HOST="localhost"

# resilience.circuit_breaker.enabled → SCRY_RESILIENCE__CIRCUIT_BREAKER__ENABLED
export SCRY_RESILIENCE__CIRCUIT_BREAKER__ENABLED=true
```

### Example Environment Configuration

```bash
# Proxy settings
export SCRY_PROXY__LISTEN_ADDRESS="0.0.0.0:5433"
export SCRY_PROXY__MAX_CONNECTIONS=1000
export SCRY_PROXY__SHUTDOWN_TIMEOUT_SECS=60

# Backend database
export SCRY_BACKEND__HOST="postgres.production.internal"
export SCRY_BACKEND__PORT=5432
export SCRY_BACKEND__DATABASE="app_db"
export SCRY_BACKEND__USER="scry_proxy"
export SCRY_BACKEND__PASSWORD="$SECRET_PASSWORD"

# Event publishing
export SCRY_PUBLISHER__ENABLED=true
export SCRY_PUBLISHER__PUBLISHER_TYPE="http"
export SCRY_PUBLISHER__HTTP_ENDPOINT="https://analytics.company.com/events"
export SCRY_PUBLISHER__HTTP_API_KEY="$API_KEY"
export SCRY_PUBLISHER__ANONYMIZE=true

# Resilience
export SCRY_RESILIENCE__CIRCUIT_BREAKER__ENABLED=true
export SCRY_RESILIENCE__CONNECTION_RETRY__ENABLED=true
export SCRY_RESILIENCE__HEALTHCHECK__ACTIVE_ENABLED=true
```

## Configuration File

Create a `scry.toml` file for declarative configuration:

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
connection_timeout_ms = 5000

[observability]
enable_tracing = true
service_name = "scry-proxy"
metrics_server_address = "127.0.0.1:9090"
enable_metrics_server = true

[publisher]
enabled = true
batch_size = 100
flush_interval_ms = 1000
anonymize = true
publisher_type = "debug"
max_queue_size = 10000

[publisher.http]
endpoint = "https://analytics.example.com/events"
timeout_ms = 500
max_retries = 2
compression = true

[performance]
target_latency_ms = 1
buffer_size = 8192

[resilience.circuit_breaker]
enabled = true
failure_threshold = 5
success_threshold = 2
window_secs = 30
open_timeout_secs = 60
use_health_monitor = true

[resilience.connection_retry]
enabled = true
max_attempts = 3
initial_backoff_ms = 50
max_backoff_ms = 5000
backoff_multiplier = 2.0
jitter_factor = 0.1

[resilience.healthcheck]
active_enabled = true
interval_secs = 30
timeout_ms = 1000
failure_threshold = 3

[health]
error_rate_spike_factor = 3.0
latency_spike_factor = 2.0
pool_saturation_threshold = 0.95
ema_alpha = 0.1
```

### Loading Custom Config File

```bash
# Specify custom config file location
export SCRY_CONFIG_FILE=/etc/scry/production.toml
cargo run --release

# Or pass as argument (if implemented)
scry --config /etc/scry/production.toml
```

## Configuration Reference

### ProxyConfig

Main proxy server settings.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `listen_address` | String | `"127.0.0.1:5433"` | Address and port for proxy to listen on |
| `max_connections` | u32 | `100` | Maximum concurrent client connections |
| `shutdown_timeout_secs` | u64 | `30` | Seconds to wait for graceful shutdown |

**Environment Variables**:
```bash
SCRY_PROXY__LISTEN_ADDRESS="0.0.0.0:5433"
SCRY_PROXY__MAX_CONNECTIONS=1000
SCRY_PROXY__SHUTDOWN_TIMEOUT_SECS=60
```

### BackendConfig

Backend database connection settings.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `protocol` | String | `"Postgres"` | Database protocol (Postgres, MySQL, MongoDB) |
| `host` | String | `"localhost"` | Backend database hostname |
| `port` | u16 | `5432` | Backend database port |
| `database` | String | `""` | Database name to connect to |
| `user` | String | `""` | Database username |
| `password` | String | `""` | Database password |
| `pool_size` | u32 | `10` | Size of backend connection pool |
| `connection_timeout_ms` | u64 | `5000` | Connection timeout in milliseconds |

**Environment Variables**:
```bash
SCRY_BACKEND__PROTOCOL="Postgres"
SCRY_BACKEND__HOST="db.example.com"
SCRY_BACKEND__PORT=5432
SCRY_BACKEND__DATABASE="myapp"
SCRY_BACKEND__USER="scry"
SCRY_BACKEND__PASSWORD="secret"
SCRY_BACKEND__POOL_SIZE=20
SCRY_BACKEND__CONNECTION_TIMEOUT_MS=3000
```

### ObservabilityConfig

Tracing and metrics configuration.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enable_tracing` | bool | `true` | Enable OpenTelemetry tracing |
| `otlp_endpoint` | String | `None` | OpenTelemetry collector endpoint (optional) |
| `service_name` | String | `"scry-proxy"` | Service name for tracing |
| `metrics_server_address` | String | `"127.0.0.1:9090"` | Address for Prometheus metrics server |
| `enable_metrics_server` | bool | `true` | Enable metrics HTTP server |

**Environment Variables**:
```bash
SCRY_OBSERVABILITY__ENABLE_TRACING=true
SCRY_OBSERVABILITY__OTLP_ENDPOINT="http://jaeger:4317"
SCRY_OBSERVABILITY__SERVICE_NAME="scry-production"
SCRY_OBSERVABILITY__METRICS_SERVER_ADDRESS="0.0.0.0:9090"
SCRY_OBSERVABILITY__ENABLE_METRICS_SERVER=true
```

### PublisherConfig

Event publishing configuration.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enabled` | bool | `true` | Enable event publishing |
| `batch_size` | usize | `100` | Events per batch before flush |
| `flush_interval_ms` | u64 | `1000` | Milliseconds between flushes |
| `anonymize` | bool | `true` | Anonymize queries before publishing |
| `publisher_type` | String | `"debug"` | Publisher type: "debug" or "http" |
| `max_queue_size` | usize | `10000` | Max queued events before dropping |
| `http_endpoint` | String | `None` | HTTP endpoint for HTTP publisher |
| `http_timeout_ms` | u64 | `500` | HTTP request timeout |
| `http_max_retries` | u32 | `2` | Max HTTP retries on failure |
| `http_api_key` | String | `None` | API key for HTTP authentication |
| `http_compression` | bool | `true` | Enable gzip compression |

**Environment Variables**:
```bash
SCRY_PUBLISHER__ENABLED=true
SCRY_PUBLISHER__BATCH_SIZE=250
SCRY_PUBLISHER__FLUSH_INTERVAL_MS=500
SCRY_PUBLISHER__ANONYMIZE=true
SCRY_PUBLISHER__PUBLISHER_TYPE="http"
SCRY_PUBLISHER__MAX_QUEUE_SIZE=50000
SCRY_PUBLISHER__HTTP_ENDPOINT="https://api.example.com/events"
SCRY_PUBLISHER__HTTP_TIMEOUT_MS=1000
SCRY_PUBLISHER__HTTP_MAX_RETRIES=3
SCRY_PUBLISHER__HTTP_API_KEY="sk-..."
SCRY_PUBLISHER__HTTP_COMPRESSION=true
```

### PerformanceConfig

Performance tuning settings.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `target_latency_ms` | u64 | `1` | Target added latency in milliseconds |
| `buffer_size` | usize | `8192` | TCP buffer size in bytes |

**Environment Variables**:
```bash
SCRY_PERFORMANCE__TARGET_LATENCY_MS=1
SCRY_PERFORMANCE__BUFFER_SIZE=16384
```

### ResilienceConfig

Resilience features configuration.

#### Circuit Breaker

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enabled` | bool | `true` | Enable circuit breaker |
| `failure_threshold` | u32 | `5` | Consecutive failures before opening |
| `success_threshold` | u32 | `2` | Consecutive successes to close from half-open |
| `window_secs` | u64 | `30` | Time window for failure counting |
| `open_timeout_secs` | u64 | `60` | Seconds to wait before transitioning to half-open |
| `use_health_monitor` | bool | `true` | Use health monitor for predictive opening |

**Environment Variables**:
```bash
SCRY_RESILIENCE__CIRCUIT_BREAKER__ENABLED=true
SCRY_RESILIENCE__CIRCUIT_BREAKER__FAILURE_THRESHOLD=10
SCRY_RESILIENCE__CIRCUIT_BREAKER__SUCCESS_THRESHOLD=3
SCRY_RESILIENCE__CIRCUIT_BREAKER__WINDOW_SECS=60
SCRY_RESILIENCE__CIRCUIT_BREAKER__OPEN_TIMEOUT_SECS=120
SCRY_RESILIENCE__CIRCUIT_BREAKER__USE_HEALTH_MONITOR=true
```

#### Connection Retry

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `enabled` | bool | `true` | Enable connection retry with backoff |
| `max_attempts` | u32 | `3` | Maximum retry attempts |
| `initial_backoff_ms` | u64 | `50` | Initial backoff in milliseconds |
| `max_backoff_ms` | u64 | `5000` | Maximum backoff in milliseconds |
| `backoff_multiplier` | f64 | `2.0` | Backoff multiplier per attempt |
| `jitter_factor` | f64 | `0.1` | Jitter factor (0.0-1.0) |

**Environment Variables**:
```bash
SCRY_RESILIENCE__CONNECTION_RETRY__ENABLED=true
SCRY_RESILIENCE__CONNECTION_RETRY__MAX_ATTEMPTS=5
SCRY_RESILIENCE__CONNECTION_RETRY__INITIAL_BACKOFF_MS=100
SCRY_RESILIENCE__CONNECTION_RETRY__MAX_BACKOFF_MS=10000
SCRY_RESILIENCE__CONNECTION_RETRY__BACKOFF_MULTIPLIER=2.0
SCRY_RESILIENCE__CONNECTION_RETRY__JITTER_FACTOR=0.1
```

#### Active Healthcheck

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `active_enabled` | bool | `true` | Enable active health checks |
| `interval_secs` | u64 | `30` | Seconds between health checks |
| `timeout_ms` | u64 | `1000` | Health check timeout in milliseconds |
| `failure_threshold` | u32 | `3` | Consecutive failures before unhealthy |

**Environment Variables**:
```bash
SCRY_RESILIENCE__HEALTHCHECK__ACTIVE_ENABLED=true
SCRY_RESILIENCE__HEALTHCHECK__INTERVAL_SECS=15
SCRY_RESILIENCE__HEALTHCHECK__TIMEOUT_MS=2000
SCRY_RESILIENCE__HEALTHCHECK__FAILURE_THRESHOLD=5
```

### HealthConfig

Health monitoring and anomaly detection.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `error_rate_spike_factor` | f64 | `3.0` | Factor for error rate spike warning |
| `latency_spike_factor` | f64 | `2.0` | Factor for latency spike warning |
| `pool_saturation_threshold` | f64 | `0.95` | Pool utilization threshold (0.0-1.0) |
| `ema_alpha` | f64 | `0.1` | EMA smoothing factor (0.0-1.0) |

**Environment Variables**:
```bash
SCRY_HEALTH__ERROR_RATE_SPIKE_FACTOR=5.0
SCRY_HEALTH__LATENCY_SPIKE_FACTOR=3.0
SCRY_HEALTH__POOL_SATURATION_THRESHOLD=0.90
SCRY_HEALTH__EMA_ALPHA=0.2
```

## Common Scenarios

### Development Environment

```toml
[proxy]
listen_address = "127.0.0.1:5433"

[backend]
host = "localhost"
port = 5432
database = "dev_db"
user = "dev"
password = "dev"

[publisher]
publisher_type = "debug"  # Log to console
anonymize = false          # See actual query values

[resilience.circuit_breaker]
failure_threshold = 3      # More sensitive
open_timeout_secs = 10     # Faster recovery

[observability]
enable_tracing = true
enable_metrics_server = true
```

### Production Environment

```toml
[proxy]
listen_address = "0.0.0.0:5433"
max_connections = 1000
shutdown_timeout_secs = 60

[backend]
host = "postgres.production.internal"
port = 5432
pool_size = 50

[publisher]
publisher_type = "http"
http_endpoint = "https://analytics.company.com/events"
anonymize = true           # Privacy protection
batch_size = 250           # Larger batches
max_queue_size = 50000     # Larger buffer

[resilience.circuit_breaker]
failure_threshold = 10     # Less sensitive
open_timeout_secs = 120    # Longer recovery period

[observability]
enable_tracing = true
otlp_endpoint = "http://jaeger-collector:4317"
metrics_server_address = "0.0.0.0:9090"
```

### High-Availability Setup

```toml
[proxy]
max_connections = 5000
shutdown_timeout_secs = 120

[backend]
pool_size = 100
connection_timeout_ms = 3000

[resilience.circuit_breaker]
enabled = true
failure_threshold = 15
use_health_monitor = true

[resilience.connection_retry]
enabled = true
max_attempts = 5
max_backoff_ms = 10000

[resilience.healthcheck]
active_enabled = true
interval_secs = 15
failure_threshold = 5

[health]
pool_saturation_threshold = 0.90  # Alert earlier
```

### Privacy-Focused Configuration

```toml
[publisher]
anonymize = true
publisher_type = "http"

# All queries anonymized with value fingerprinting
# Example: "SELECT * FROM users WHERE id = 123"
#       → "SELECT * FROM users WHERE id = ?"
#         + fingerprint: "blake3:abc..."
```

### Debugging/Troubleshooting

```toml
[publisher]
publisher_type = "debug"
anonymize = false
flush_interval_ms = 100    # Frequent flushes

[observability]
enable_tracing = true
# Set RUST_LOG=debug for detailed logs
```

## Validation

Scry validates configuration at startup and will exit with errors if:
- Invalid address format for `listen_address`
- Invalid port numbers (<1 or >65535)
- Connection pool size is 0
- Batch size or flush interval is 0
- Backoff multiplier <1.0
- Thresholds are 0
- Factors are negative

Example error:

```
Error: Invalid configuration: proxy.listen_address
  → Expected format: "host:port"
  → Got: "invalid"
```

## See Also

- [Getting Started](getting-started.md) - Quick start with default configuration
- [Deployment](deployment.md) - Production deployment configurations
- [Architecture](architecture.md) - Understanding configuration impact on architecture
