# Metrics

Scry exposes comprehensive Prometheus metrics for monitoring query performance, connection pool health, circuit breaker state, and system resource usage.

## Table of Contents

- [Metrics Server](#metrics-server)
- [Prometheus Metrics](#prometheus-metrics)
- [Debug Endpoints](#debug-endpoints)
- [Grafana Dashboards](#grafana-dashboards)
- [Alerting](#alerting)
- [Performance](#performance)

## Metrics Server

Scry runs an HTTP server exposing metrics and debug endpoints.

### Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `metrics_server_address` | `"127.0.0.1:9090"` | Listen address for metrics server |
| `enable_metrics_server` | `true` | Enable metrics HTTP server |

```toml
[observability]
metrics_server_address = "127.0.0.1:9090"
enable_metrics_server = true
```

```bash
export SCRY_OBSERVABILITY__METRICS_SERVER_ADDRESS="0.0.0.0:9090"
export SCRY_OBSERVABILITY__ENABLE_METRICS_SERVER=true
```

### Endpoints

| Endpoint | Description |
|----------|-------------|
| `/metrics` | Prometheus text exposition format |
| `/health` | JSON health status with warnings |
| `/debug/pool` | JSON connection pool status |
| `/debug/hot_data` | JSON top-K hot data fingerprints |
| `/debug/timeline` | JSON query timeline phase breakdowns |

## Prometheus Metrics

### Query Metrics

#### scry_queries_total

**Type**: Counter
**Description**: Total number of queries processed

```
scry_queries_total 12345
```

**Usage**:
```promql
# Queries per second
rate(scry_queries_total[1m])

# Total queries in last hour
increase(scry_queries_total[1h])
```

#### scry_query_errors_total

**Type**: Counter
**Description**: Total number of failed queries

```
scry_query_errors_total 56
```

**Usage**:
```promql
# Error rate (errors per second)
rate(scry_query_errors_total[1m])

# Error percentage
(rate(scry_query_errors_total[1m]) / rate(scry_queries_total[1m])) * 100
```

#### scry_query_error_rate

**Type**: Gauge
**Description**: Current error rate (0.0-1.0)

```
scry_query_error_rate 0.025
```

**Interpretation**:
- `0.0` = No errors
- `0.025` = 2.5% error rate
- `1.0` = 100% error rate

### Latency Metrics

#### scry_query_latency_seconds

**Type**: Summary
**Description**: End-to-end query latency (queue + pool + backend)

```
scry_query_latency_seconds{quantile="0.5"} 0.001234
scry_query_latency_seconds{quantile="0.9"} 0.002345
scry_query_latency_seconds{quantile="0.95"} 0.003456
scry_query_latency_seconds{quantile="0.99"} 0.005678
scry_query_latency_seconds{quantile="0.999"} 0.010123
scry_query_latency_seconds_sum 123.456
scry_query_latency_seconds_count 12345
```

**Interpretation**:
- P50 (median): 1.23ms
- P90: 2.35ms
- P95: 3.46ms
- P99: 5.68ms
- P99.9: 10.12ms

**Usage**:
```promql
# P99 latency over time
scry_query_latency_seconds{quantile="0.99"}

# Average latency
rate(scry_query_latency_seconds_sum[5m]) / rate(scry_query_latency_seconds_count[5m])
```

### Timeline Phase Metrics

Query execution is broken into three phases:

#### scry_query_queue_time_seconds

**Type**: Summary
**Description**: Time waiting before pool acquisition

```
scry_query_queue_time_seconds{quantile="0.5"} 0.000012
scry_query_queue_time_seconds{quantile="0.99"} 0.000045
```

**Interpretation**: Low values are good (no queuing). High values indicate requests waiting before even attempting pool acquisition.

#### scry_query_pool_acquire_seconds

**Type**: Summary
**Description**: Time to acquire connection from pool

```
scry_query_pool_acquire_seconds{quantile="0.5"} 0.000234
scry_query_pool_acquire_seconds{quantile="0.99"} 0.001234
```

**Interpretation**:
- Low values: Connections readily available
- High values: Pool contention or slow connection creation

#### scry_query_backend_seconds

**Type**: Summary
**Description**: Time executing query on backend database

```
scry_query_backend_seconds{quantile="0.5"} 0.001000
scry_query_backend_seconds{quantile="0.99"} 0.005000
```

**Interpretation**: This is the database query execution time (the part Scry doesn't control).

### Connection Pool Metrics

#### scry_pool_connections_total

**Type**: Gauge
**Description**: Current pool size (active + idle connections)

```
scry_pool_connections_total 15
```

#### scry_pool_connections_available

**Type**: Gauge
**Description**: Idle connections available for use

```
scry_pool_connections_available 8
```

#### scry_pool_connections_max

**Type**: Gauge
**Description**: Maximum configured pool size

```
scry_pool_connections_max 50
```

#### scry_pool_utilization

**Type**: Gauge
**Description**: Pool utilization ratio (0.0-1.0)

```
scry_pool_utilization 0.46
```

**Formula**: `(total - available) / max`

**Interpretation**:
- `0.0` = No connections in use
- `0.46` = 46% of pool in use
- `1.0` = Pool saturated (all connections in use)

**Alert**:
```promql
scry_pool_utilization > 0.95  # Alert at 95% saturation
```

### Connection Metrics

#### scry_active_connections

**Type**: Gauge
**Description**: Current active client connections

```
scry_active_connections 42
```

**Interpretation**: Number of clients currently connected to Scry proxy.

### Circuit Breaker Metrics

#### scry_circuit_breaker_state

**Type**: Gauge
**Description**: Circuit breaker state

```
scry_circuit_breaker_state 0
```

**Values**:
- `0` = Closed (normal operation)
- `1` = Open (failing fast)
- `2` = HalfOpen (testing recovery)

**Alert**:
```promql
scry_circuit_breaker_state == 1  # Circuit opened
```

#### scry_circuit_breaker_consecutive_failures

**Type**: Gauge
**Description**: Consecutive failures in Closed state

```
scry_circuit_breaker_consecutive_failures 0
```

**Interpretation**: Tracks failures accumulating. When reaches threshold, circuit opens.

#### scry_circuit_breaker_consecutive_successes

**Type**: Gauge
**Description**: Consecutive successes in HalfOpen state

```
scry_circuit_breaker_consecutive_successes 0
```

**Interpretation**: Tracks successes in HalfOpen state. When reaches threshold, circuit closes.

#### scry_circuit_breaker_requests_allowed_total

**Type**: Counter
**Description**: Total requests allowed through circuit breaker

```
scry_circuit_breaker_requests_allowed_total 12000
```

#### scry_circuit_breaker_requests_rejected_total

**Type**: Counter
**Description**: Total requests rejected (circuit open)

```
scry_circuit_breaker_requests_rejected_total 45
```

**Usage**:
```promql
# Rejection rate
rate(scry_circuit_breaker_requests_rejected_total[5m])
```

### Uptime Metric

#### scry_uptime_seconds

**Type**: Counter
**Description**: Proxy uptime in seconds

```
scry_uptime_seconds 3600
```

**Interpretation**: 3600 seconds = 1 hour uptime

## Debug Endpoints

### /health

**Request**:
```bash
curl http://localhost:9090/health
```

**Response**:
```json
{
  "status": "Healthy",
  "uptime_secs": 3600,
  "queries_total": 12345,
  "error_rate": 0.001,
  "latency_p99_ms": 5.2,
  "pool_utilization": 0.45,
  "warnings": []
}
```

**Fields**:
- `status`: "Healthy" | "Degraded" | "Unhealthy"
- `uptime_secs`: Proxy uptime
- `queries_total`: Total queries processed
- `error_rate`: Current error rate (0.0-1.0)
- `latency_p99_ms`: P99 latency in milliseconds
- `pool_utilization`: Pool utilization (0.0-1.0)
- `warnings`: Array of health warnings

### /debug/pool

**Request**:
```bash
curl http://localhost:9090/debug/pool
```

**Response**:
```json
{
  "size": 15,
  "available": 8,
  "max_size": 50,
  "utilization": 0.46
}
```

**Fields**:
- `size`: Current pool size
- `available`: Available connections
- `max_size`: Maximum pool size
- `utilization`: Utilization ratio

### /debug/hot_data

**Request**:
```bash
curl http://localhost:9090/debug/hot_data
```

**Response**:
```json
{
  "top_k": [
    {
      "fingerprint": "blake3:a1b2c3d4e5f6...",
      "access_count": 15234
    },
    {
      "fingerprint": "blake3:f6e5d4c3b2a1...",
      "access_count": 8901
    }
  ]
}
```

**Fields**:
- `top_k`: Array of top-K hot data entries
  - `fingerprint`: Value fingerprint (Blake3 hash)
  - `access_count`: Number of accesses

### /debug/timeline

**Request**:
```bash
curl http://localhost:9090/debug/timeline
```

**Response**:
```json
{
  "queue_time_p50_ms": 0.01,
  "queue_time_p99_ms": 0.05,
  "pool_acquire_p50_ms": 0.23,
  "pool_acquire_p99_ms": 1.23,
  "backend_p50_ms": 1.00,
  "backend_p99_ms": 5.00
}
```

**Fields**: P50 and P99 latencies for each query phase (in milliseconds)

## Grafana Dashboards

### Recommended Panels

#### Query Performance

**Panel 1: Queries Per Second**
```promql
rate(scry_queries_total[1m])
```

**Panel 2: Error Rate**
```promql
(rate(scry_query_errors_total[1m]) / rate(scry_queries_total[1m])) * 100
```

**Panel 3: Latency Percentiles**
```promql
scry_query_latency_seconds{quantile="0.5"}
scry_query_latency_seconds{quantile="0.95"}
scry_query_latency_seconds{quantile="0.99"}
```

#### Connection Pool

**Panel 4: Pool Utilization**
```promql
scry_pool_utilization
```

**Panel 5: Pool Connections**
```promql
scry_pool_connections_total
scry_pool_connections_available
scry_pool_connections_max
```

#### Circuit Breaker

**Panel 6: Circuit Breaker State**
```promql
scry_circuit_breaker_state
```

**Panel 7: Rejection Rate**
```promql
rate(scry_circuit_breaker_requests_rejected_total[5m])
```

#### Query Timeline

**Panel 8: Phase Breakdown (Stacked)**
```promql
scry_query_queue_time_seconds{quantile="0.99"}
scry_query_pool_acquire_seconds{quantile="0.99"}
scry_query_backend_seconds{quantile="0.99"}
```

### Example Dashboard JSON

```json
{
  "dashboard": {
    "title": "Scry Proxy Metrics",
    "panels": [
      {
        "title": "Queries Per Second",
        "targets": [
          {
            "expr": "rate(scry_queries_total[1m])"
          }
        ],
        "type": "graph"
      },
      {
        "title": "Error Rate %",
        "targets": [
          {
            "expr": "(rate(scry_query_errors_total[1m]) / rate(scry_queries_total[1m])) * 100"
          }
        ],
        "type": "graph"
      },
      {
        "title": "P99 Latency",
        "targets": [
          {
            "expr": "scry_query_latency_seconds{quantile=\"0.99\"} * 1000",
            "legendFormat": "P99 Latency (ms)"
          }
        ],
        "type": "graph"
      }
    ]
  }
}
```

## Alerting

### Recommended Alerts

#### Critical Alerts

**Circuit Breaker Opened**
```promql
ALERT CircuitBreakerOpen
IF scry_circuit_breaker_state == 1
FOR 1m
LABELS { severity = "critical" }
ANNOTATIONS {
  summary = "Circuit breaker opened - database issues",
  description = "Circuit breaker has been open for 1 minute"
}
```

**Pool Starvation**
```promql
ALERT PoolStarvation
IF scry_pool_connections_available == 0 AND scry_active_connections > 0
FOR 30s
LABELS { severity = "critical" }
ANNOTATIONS {
  summary = "Connection pool exhausted",
  description = "No available connections in pool"
}
```

**High Error Rate**
```promql
ALERT HighErrorRate
IF (rate(scry_query_errors_total[5m]) / rate(scry_queries_total[5m])) > 0.05
FOR 2m
LABELS { severity = "critical" }
ANNOTATIONS {
  summary = "Error rate above 5%",
  description = "{{ $value }}% of queries failing"
}
```

#### Warning Alerts

**Pool Saturation**
```promql
ALERT PoolSaturation
IF scry_pool_utilization > 0.9
FOR 5m
LABELS { severity = "warning" }
ANNOTATIONS {
  summary = "Connection pool highly utilized",
  description = "Pool utilization at {{ $value | humanizePercentage }}"
}
```

**Latency Spike**
```promql
ALERT LatencySpike
IF scry_query_latency_seconds{quantile="0.99"} > 0.050
FOR 5m
LABELS { severity = "warning" }
ANNOTATIONS {
  summary = "P99 latency above 50ms",
  description = "P99 latency: {{ $value | humanizeDuration }}"
}
```

**Circuit Flapping**
```promql
ALERT CircuitFlapping
IF changes(scry_circuit_breaker_state[10m]) > 5
LABELS { severity = "warning" }
ANNOTATIONS {
  summary = "Circuit breaker state changing frequently",
  description = "Circuit state changed {{ $value }} times in 10 minutes"
}
```

### Alert Runbook

#### When Circuit Breaker Opens

1. **Check health**:
   ```bash
   curl http://localhost:9090/health
   ```

2. **Check database**:
   ```bash
   psql -h $DB_HOST -p $DB_PORT -U $DB_USER -c "SELECT 1"
   ```

3. **Review metrics**:
   - Error rate: High?
   - Pool utilization: Saturated?
   - Backend latency: Slow?

4. **Actions**:
   - Database down? Restart it
   - Database slow? Optimize queries or add resources
   - Network issue? Check connectivity
   - Circuit will auto-recover when health improves

#### When Pool Saturated

1. **Check pool status**:
   ```bash
   curl http://localhost:9090/debug/pool
   ```

2. **Check backend latency**:
   ```bash
   curl http://localhost:9090/metrics | grep backend_seconds
   ```

3. **Actions**:
   - Increase pool size: `export SCRY_BACKEND__POOL_SIZE=100`
   - Optimize slow queries
   - Scale horizontally (add more proxy instances)

## Performance

### Metrics Overhead

Scry's metrics system is highly optimized:

- **Per-query overhead**: <300ns
  - Atomic counter increments: ~10ns each
  - Histogram recording: ~150ns
  - Hot data tracking: ~50-100ns

- **Memory footprint**: ~150KB
  - HDR histograms: ~50KB
  - Count-Min Sketch: ~64KB
  - Counters and gauges: ~1KB
  - Top-K heap: ~10KB

- **Prometheus scrape**: <10ms
  - Formatting metrics: ~5ms
  - Network transfer: ~2-5ms

### Scalability

Metrics system scales linearly:

- **1,000 qps**: ~0.3ms total metrics overhead
- **10,000 qps**: ~0.3ms total metrics overhead (lock-free atomics)
- **100,000 qps**: ~0.3ms total metrics overhead

**No performance degradation** with increased query volume.

## See Also

- [Observability](observability.md) - Event publishing and batching
- [Health Checks](health-checks.md) - Health monitoring system
- [Circuit Breaker](circuit-breaker.md) - Circuit breaker metrics
- [Connection Pooling](connection-pooling.md) - Pool metrics
- [Configuration](configuration.md) - Metrics server configuration
