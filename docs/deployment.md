# Deployment

This guide covers deploying Scry to production environments using Docker, Kubernetes, and other deployment patterns.

## Table of Contents

- [Pre-Deployment Checklist](#pre-deployment-checklist)
- [Docker Deployment](#docker-deployment)
- [Kubernetes Deployment](#kubernetes-deployment)
- [Production Configuration](#production-configuration)
- [High Availability](#high-availability)
- [Monitoring Setup](#monitoring-setup)
- [Security Considerations](#security-considerations)
- [Performance Tuning](#performance-tuning)
- [Troubleshooting](#troubleshooting)

## Pre-Deployment Checklist

Before deploying to production:

- [ ] **Build tested**: Run `just ci` successfully
- [ ] **Integration tests passing**: All tests pass with real Postgres
- [ ] **Configuration reviewed**: Production config file created and validated
- [ ] **Secrets managed**: Database credentials stored securely (not in config files)
- [ ] **Monitoring configured**: Prometheus and Grafana dashboards ready
- [ ] **Alerts configured**: Critical alerts set up (circuit breaker, pool saturation, etc.)
- [ ] **Capacity planned**: Pool size, max connections calculated for expected load
- [ ] **Network configured**: Firewall rules, security groups set up
- [ ] **Documentation ready**: Runbooks for common issues
- [ ] **Rollback plan**: Procedure to roll back if deployment fails

## Docker Deployment

### Creating a Dockerfile

Create `Dockerfile` in project root:

```dockerfile
# Build stage
FROM rust:1.75 as builder

WORKDIR /app

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Copy source
COPY src ./src

# Build release binary
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 scry

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/target/release/scry /usr/local/bin/scry

# Change ownership
RUN chown -R scry:scry /app

# Switch to non-root user
USER scry

# Expose ports
EXPOSE 5433 9090

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:9090/health || exit 1

# Run scry
CMD ["scry"]
```

### Building the Image

```bash
# Build image
docker build -t scry:latest .

# Tag for registry
docker tag scry:latest your-registry.com/scry:v1.0.0

# Push to registry
docker push your-registry.com/scry:v1.0.0
```

### Running with Docker Compose

Create `docker-compose.yml`:

```yaml
version: '3.8'

services:
  scry:
    image: your-registry.com/scry:v1.0.0
    container_name: scry-proxy
    ports:
      - "5433:5433"  # Proxy port
      - "9090:9090"  # Metrics port
    environment:
      # Backend database
      SCRY_BACKEND__HOST: postgres.production.internal
      SCRY_BACKEND__PORT: 5432
      SCRY_BACKEND__DATABASE: production_db
      SCRY_BACKEND__USER: scry_proxy
      # Provide the backend password value directly. (Loading secrets from a
      # file path is planned but not yet implemented.)
      SCRY_BACKEND__PASSWORD: "${DB_PASSWORD}"
      SCRY_BACKEND__POOL_SIZE: 50

      # Proxy settings
      SCRY_PROXY__LISTEN_ADDRESS: "0.0.0.0:5433"
      SCRY_PROXY__MAX_CONNECTIONS: 1000

      # Resilience
      SCRY_RESILIENCE__CIRCUIT_BREAKER__ENABLED: "true"
      SCRY_RESILIENCE__CONNECTION_RETRY__ENABLED: "true"
      SCRY_RESILIENCE__HEALTHCHECK__ACTIVE_ENABLED: "true"

      # Observability
      SCRY_PUBLISHER__ENABLED: "true"
      SCRY_PUBLISHER__PUBLISHER_TYPE: "http"
      SCRY_PUBLISHER__HTTP_ENDPOINT: "https://analytics.company.com/events"
      # Provide the API key value directly. (Loading secrets from a file path
      # is planned but not yet implemented.)
      SCRY_PUBLISHER__HTTP_API_KEY: "${ANALYTICS_API_KEY}"
      SCRY_PUBLISHER__ANONYMIZE: "true"

      # Metrics
      SCRY_OBSERVABILITY__METRICS_SERVER_ADDRESS: "0.0.0.0:9090"
      SCRY_OBSERVABILITY__ENABLE_METRICS_SERVER: "true"

    secrets:
      - db_password
      - analytics_api_key

    networks:
      - app-network

    restart: unless-stopped

    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 1G
        reservations:
          cpus: '1'
          memory: 512M

secrets:
  db_password:
    external: true
  analytics_api_key:
    external: true

networks:
  app-network:
    external: true
```

**Run**:
```bash
docker-compose up -d
```

## Kubernetes Deployment

### Deployment Manifest

Create `k8s/deployment.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: scry-proxy
  namespace: production
  labels:
    app: scry-proxy
    version: v1.0.0
spec:
  replicas: 3
  selector:
    matchLabels:
      app: scry-proxy
  template:
    metadata:
      labels:
        app: scry-proxy
        version: v1.0.0
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
        prometheus.io/path: "/metrics"
    spec:
      serviceAccountName: scry-proxy
      containers:
      - name: scry
        image: your-registry.com/scry:v1.0.0
        imagePullPolicy: IfNotPresent

        ports:
        - name: proxy
          containerPort: 5433
          protocol: TCP
        - name: metrics
          containerPort: 9090
          protocol: TCP

        env:
        # Backend database
        - name: SCRY_BACKEND__HOST
          value: "postgres.production.svc.cluster.local"
        - name: SCRY_BACKEND__PORT
          value: "5432"
        - name: SCRY_BACKEND__DATABASE
          value: "production_db"
        - name: SCRY_BACKEND__USER
          valueFrom:
            secretKeyRef:
              name: scry-db-credentials
              key: username
        - name: SCRY_BACKEND__PASSWORD
          valueFrom:
            secretKeyRef:
              name: scry-db-credentials
              key: password
        - name: SCRY_BACKEND__POOL_SIZE
          value: "50"

        # Proxy settings
        - name: SCRY_PROXY__LISTEN_ADDRESS
          value: "0.0.0.0:5433"
        - name: SCRY_PROXY__MAX_CONNECTIONS
          value: "1000"

        # Resilience
        - name: SCRY_RESILIENCE__CIRCUIT_BREAKER__ENABLED
          value: "true"
        - name: SCRY_RESILIENCE__CONNECTION_RETRY__ENABLED
          value: "true"

        # Publisher
        - name: SCRY_PUBLISHER__PUBLISHER_TYPE
          value: "http"
        - name: SCRY_PUBLISHER__HTTP_ENDPOINT
          value: "https://analytics.company.com/events"
        - name: SCRY_PUBLISHER__HTTP_API_KEY
          valueFrom:
            secretKeyRef:
              name: scry-analytics
              key: api-key
        - name: SCRY_PUBLISHER__ANONYMIZE
          value: "true"

        # Metrics
        - name: SCRY_OBSERVABILITY__METRICS_SERVER_ADDRESS
          value: "0.0.0.0:9090"

        resources:
          requests:
            cpu: 500m
            memory: 512Mi
          limits:
            cpu: 2000m
            memory: 1Gi

        livenessProbe:
          httpGet:
            path: /health
            port: 9090
          initialDelaySeconds: 10
          periodSeconds: 30
          timeoutSeconds: 5
          failureThreshold: 3

        readinessProbe:
          httpGet:
            path: /health
            port: 9090
          initialDelaySeconds: 5
          periodSeconds: 10
          timeoutSeconds: 3
          failureThreshold: 2

        securityContext:
          runAsNonRoot: true
          runAsUser: 1000
          readOnlyRootFilesystem: true
          allowPrivilegeEscalation: false
          capabilities:
            drop:
            - ALL

      restartPolicy: Always
```

### Service Manifest

Create `k8s/service.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: scry-proxy
  namespace: production
  labels:
    app: scry-proxy
spec:
  type: ClusterIP
  selector:
    app: scry-proxy
  ports:
  - name: proxy
    port: 5433
    targetPort: 5433
    protocol: TCP
  - name: metrics
    port: 9090
    targetPort: 9090
    protocol: TCP
```

### ConfigMap (Optional)

Create `k8s/configmap.yaml`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: scry-config
  namespace: production
data:
  scry.toml: |
    [proxy]
    listen_address = "0.0.0.0:5433"
    max_connections = 1000

    [resilience.circuit_breaker]
    enabled = true
    failure_threshold = 5
    open_timeout_secs = 60

    [resilience.connection_retry]
    enabled = true
    max_attempts = 3

    [health]
    error_rate_spike_factor = 3.0
    pool_saturation_threshold = 0.95
```

### Secrets

Create `k8s/secrets.yaml`:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: scry-db-credentials
  namespace: production
type: Opaque
stringData:
  username: scry_proxy
  password: <redacted>
---
apiVersion: v1
kind: Secret
metadata:
  name: scry-analytics
  namespace: production
type: Opaque
stringData:
  api-key: <redacted>
```

### Deploy

```bash
kubectl apply -f k8s/secrets.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml

# Verify
kubectl get pods -n production -l app=scry-proxy
kubectl logs -n production -l app=scry-proxy
```

## Production Configuration

### Environment-Specific Settings

**Production** (`prod.env`):
```bash
# Backend
SCRY_BACKEND__HOST=postgres-primary.prod.internal
SCRY_BACKEND__POOL_SIZE=100

# Resilience
SCRY_RESILIENCE__CIRCUIT_BREAKER__FAILURE_THRESHOLD=10
SCRY_RESILIENCE__CIRCUIT_BREAKER__OPEN_TIMEOUT_SECS=120

# Publisher
SCRY_PUBLISHER__PUBLISHER_TYPE=http
SCRY_PUBLISHER__BATCH_SIZE=250
SCRY_PUBLISHER__ANONYMIZE=true
```

**Staging** (`staging.env`):
```bash
# Backend
SCRY_BACKEND__HOST=postgres-staging.internal
SCRY_BACKEND__POOL_SIZE=20

# Resilience (more sensitive for testing)
SCRY_RESILIENCE__CIRCUIT_BREAKER__FAILURE_THRESHOLD=3
SCRY_RESILIENCE__CIRCUIT_BREAKER__OPEN_TIMEOUT_SECS=30

# Publisher (debug for visibility)
SCRY_PUBLISHER__PUBLISHER_TYPE=debug
SCRY_PUBLISHER__ANONYMIZE=false
```

## High Availability

### Multi-Instance Deployment

Run multiple Scry instances behind a load balancer:

```
                ┌────────────────┐
                │ Load Balancer  │
                │   (HAProxy)    │
                └───────┬────────┘
                        │
        ┌───────────────┼───────────────┐
        │               │               │
  ┌─────▼─────┐  ┌─────▼─────┐  ┌─────▼─────┐
  │  Scry 1   │  │  Scry 2   │  │  Scry 3   │
  │  (Pool:50)│  │  (Pool:50)│  │  (Pool:50)│
  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘
        └───────────────┴───────────────┘
                        │
                  ┌─────▼─────┐
                  │ Postgres  │
                  │ (max:200) │
                  └───────────┘
```

**Database Connections**: `(instances × pool_size) + buffer`
- Example: `(3 × 50) + 50 = 200 max connections`

### Load Balancer Configuration (HAProxy)

```
frontend scry_proxy
    bind *:5433
    mode tcp
    default_backend scry_instances

backend scry_instances
    mode tcp
    balance leastconn
    option tcp-check
    tcp-check connect port 9090
    tcp-check send GET\ /health\ HTTP/1.1\r\nHost:\ localhost\r\n\r\n
    tcp-check expect string Healthy

    server scry1 scry-1:5433 check inter 10s fall 3 rise 2
    server scry2 scry-2:5433 check inter 10s fall 3 rise 2
    server scry3 scry-3:5433 check inter 10s fall 3 rise 2
```

### Database Failover

Use Postgres with read replicas:

```yaml
# Primary (write traffic)
SCRY_BACKEND__HOST: postgres-primary.prod.internal

# Read replica (read-only traffic, separate Scry instances)
SCRY_BACKEND__HOST: postgres-replica.prod.internal
```

**Note**: Scry fills the same role as traditional connection poolers (like PgBouncer), but provides significant observability advantages including per-query metrics, anomaly detection, and query anonymization. You should connect Scry directly to your database rather than introducing another pooling layer.

## Graceful Shutdown

Scry drains connections gracefully on **both `SIGINT` (Ctrl+C) and `SIGTERM`** —
the signal container orchestrators (Kubernetes, Docker, systemd) send to stop a
process. This makes rolling deploys safe: in-flight queries are allowed to
finish rather than being cut off.

**The drain contract:**

1. On receiving `SIGINT` or `SIGTERM`, Scry **stops accepting new connections**.
2. It waits for existing in-flight queries/connections to complete, up to
   `proxy.shutdown_timeout_secs` (env `SCRY_PROXY__SHUTDOWN_TIMEOUT_SECS`,
   default 30s).
3. Any connections still active when the timeout expires are terminated so the
   process can exit.
4. Buffered observability events are flushed to the publisher before exit.

Set the drain timeout to comfortably exceed your longest expected query, and
make it shorter than your orchestrator's kill grace period so Scry drains on its
own terms. For Kubernetes, set `terminationGracePeriodSeconds` a few seconds
higher than `SCRY_PROXY__SHUTDOWN_TIMEOUT_SECS`:

```yaml
spec:
  terminationGracePeriodSeconds: 35   # > shutdown_timeout_secs (30s)
  containers:
    - name: scry-proxy
      env:
        - name: SCRY_PROXY__SHUTDOWN_TIMEOUT_SECS
          value: "30"
```

## Monitoring Setup

### Prometheus Configuration

Add Scry to Prometheus scrape config:

```yaml
scrape_configs:
  - job_name: 'scry'
    static_configs:
      - targets: ['scry-1:9090', 'scry-2:9090', 'scry-3:9090']
    scrape_interval: 15s
    scrape_timeout: 10s
```

Or use Kubernetes service discovery:

```yaml
scrape_configs:
  - job_name: 'scry'
    kubernetes_sd_configs:
      - role: pod
        namespaces:
          names:
            - production
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
        action: keep
        regex: true
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_port]
        action: replace
        target_label: __address__
        regex: ([^:]+)(?::\d+)?;(\d+)
        replacement: $1:$2
```

### Grafana Dashboards

Import dashboard template (see [Metrics](metrics.md#grafana-dashboards)).

### Alerts

Configure critical alerts:

```yaml
groups:
  - name: scry
    rules:
      - alert: ScryCircuitBreakerOpen
        expr: scry_circuit_breaker_state == 1
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Scry circuit breaker opened"

      - alert: ScryPoolSaturation
        expr: scry_pool_utilization > 0.95
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Scry pool highly saturated"
```

## Security Considerations

### Secrets Management

**Never** store secrets in:
- Config files committed to git
- Environment variables in Dockerfiles
- Kubernetes ConfigMaps

**Use**:
- Kubernetes Secrets
- HashiCorp Vault
- AWS Secrets Manager
- Azure Key Vault

**Example** (Kubernetes with Vault):
```yaml
env:
- name: SCRY_BACKEND__PASSWORD
  valueFrom:
    secretKeyRef:
      name: vault-generated-secret
      key: db-password
```

### Network Security

**Firewall Rules**:
- Only allow necessary connections to Scry proxy port (5433)
- Restrict metrics port (9090) to monitoring systems only
- Use network policies in Kubernetes

**TLS**:
- Enable TLS for Postgres connections
- Use TLS for HTTP publisher endpoint

### Run as Non-Root

Always run Scry as non-root user:

```dockerfile
USER scry
```

```yaml
securityContext:
  runAsNonRoot: true
  runAsUser: 1000
```

## Performance Tuning

### CPU Sizing

Scry is CPU-efficient:
- **Light load** (< 1000 qps): 0.5-1 CPU core
- **Medium load** (1000-10000 qps): 1-2 CPU cores
- **High load** (> 10000 qps): 2-4 CPU cores

### Memory Sizing

Memory usage scales with:
- Connection pool size: ~50KB per connection
- Event queue size: ~100 bytes per event
- Base overhead: ~20MB

**Formula**: `memory = 20MB + (pool_size × 50KB) + (max_queue_size × 100 bytes)`

**Example**:
- Pool size: 100
- Queue size: 10,000
- Memory: 20MB + 5MB + 1MB = ~26MB minimum
- **Recommend**: 512MB (allows headroom)

### Benchmarking

Run benchmarks to validate performance:

```bash
# Build with optimizations
cargo build --release

# Run criterion benchmarks
cargo bench
```

## Troubleshooting

### Deployment Fails

**Check logs**:
```bash
# Docker
docker logs scry-proxy

# Kubernetes
kubectl logs -n production -l app=scry-proxy
```

**Common issues**:
- Invalid configuration
- Can't connect to database
- Secrets not mounted
- Port already in use

### High Memory Usage

**Check metrics**:
```bash
curl http://localhost:9090/debug/pool
curl http://localhost:9090/metrics | grep memory
```

**Solutions**:
- Reduce `max_queue_size`
- Reduce `pool_size`
- Check for memory leaks (shouldn't happen in Rust, but monitor)

### Connection Issues

**Test connectivity**:
```bash
# From Scry container/pod
telnet $SCRY_BACKEND__HOST $SCRY_BACKEND__PORT

# Test authentication
psql -h $SCRY_BACKEND__HOST -p $SCRY_BACKEND__PORT -U $SCRY_BACKEND__USER -d $SCRY_BACKEND__DATABASE -c "SELECT 1"
```

## See Also

- [Configuration](configuration.md) - Production configuration reference
- [Metrics](metrics.md) - Monitoring and alerting
- [Architecture](architecture.md) - System architecture
- [Development](development.md) - Building and testing
