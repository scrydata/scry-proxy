# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the `scry` project repository. This is a repository that is for a transparent proxy for SQL systems that allows our Rust-based system to gather per-query observations and send them out for analysis out of band.

Our proxy will provide observability out of the box about what's going on at the proxy layer, provide some circuit breaking, retries and healthchecks automatically. Basically all of the robust, resiliant properties that not all well behaved clients might have configured.

Eventually it might even handle connection pooling, but the focus for now is on Observability, ease of use, and very, very low overhead. This is why we're writing it it Rust and all observability (especially redirecting anonimized query information) is best effort, and we target adding no more than 1ms per query additional latency.

## Development Commands

All commands use `just` (https://github.com/casey/just):

```bash
just build              # Build the project
just test               # Run all tests
just test-unit          # Run unit tests only
just test-integration   # Run integration tests (requires Docker)
just run                # Run the proxy in dev mode
just lint               # Run clippy linter
just fmt                # Format code
just ci                 # Run all CI checks (fmt, lint, test)
```

For local Postgres testing:
```bash
just postgres-up        # Start Postgres container
just postgres-down      # Stop Postgres container
```

## Workspace Structure

This repository is organized as a Cargo workspace with two crates:

- **`scry-proxy/`** - Main SQL proxy server
- **`scry-protocol/`** - Event protocol library (standalone, reusable)

### scry-protocol

Standalone crate for the query event protocol. Can be used independently by:
- The proxy itself (for event creation and serialization)
- Analytics services (for event deserialization and processing)
- Monitoring dashboards
- Third-party tools

**Location**: `scry-protocol/`
**Public API**:
- `QueryEvent`, `QueryEventBuilder` - Event types
- `FlatBuffersSerializer` - FlexBuffers serialization
- `FlexBuffersDeserializer` - FlexBuffers deserialization
- `DeserializedBatch` - Deserialized batch result

**Schema**: `scry-protocol/schema/query_event.fbs` (canonical FlatBuffers schema)

See `scry-protocol/README.md` for usage examples.

### scry-proxy

Main proxy server implementation.

**Location**: `scry-proxy/src/`

## Architecture

### Core Modules (scry-proxy)

- **`scry-proxy/src/proxy/`** - Main proxy server, connection handling, request forwarding
- **`scry-proxy/src/protocol/`** - Postgres wire protocol parsing and message handling
- **`scry-proxy/src/publisher/`** - Event publishing abstraction and implementations
- **`scry-proxy/src/config/`** - Configuration loading (12-factor app style)
- **`scry-proxy/src/observability/`** - Tracing, metrics, OpenTelemetry setup

### Key Architectural Decisions

**Async Runtime**: Tokio (battle-tested for proxy workloads)

**Protocol Handling**: Using `tokio-postgres` and `postgres-protocol` crates for Postgres wire protocol

**Event Publishing**: Trait-based abstraction (`EventPublisher`) allows swapping implementations:
- Current: `DebugLoggerPublisher` - logs events as JSON with metrics (dev/testing)
- Future: HTTP/gRPC publisher for sending to central service

**Event Batching**: Background task with `tokio::mpsc::channel`, flushes on:
- Batch size threshold (configurable, default 100 events)
- Time threshold (configurable, default 1000ms)
- Whichever comes first

**Query Journaling Format**: FlatBuffers for maximum performance (<1ms overhead target)
- Events are published asynchronously, best-effort
- Designed to send batches over the internet to a central service (not yet implemented)

**Observability**:
- OpenTelemetry for distributed tracing
- `tracing` crate for structured logging
- Metrics for publisher throughput, batch sizes, latency

**Configuration**: 12-factor app style - environment variables + optional config files

**Connection Pooling**: `deadpool-postgres` for backend connection pooling

**Testing**:
- Unit tests with mocking
- Integration tests using `testcontainers` with real Postgres instances
- Benchmarks with `criterion` for latency and throughput testing:
  - Direct Postgres vs Through Proxy comparison
  - Different query types performance
  - Publisher overhead measurement
  - Statistical analysis with p50, p95, p99 latencies

### Performance Requirements

- Target: <1ms additional latency per query
- Best-effort event publishing (never block the proxy)
- Focus on maximum observability within latency budget

### Current Implementation Status

**Completed:**
- ✅ Basic module structure
- ✅ Event publisher trait and debug logger stub
- ✅ Configuration types (12-factor style)
- ✅ Observability initialization (tracing)
- ✅ Proxy server implementation
  - TCP listener accepting client connections
  - Connection handler with bidirectional forwarding
  - Event batcher with size and time-based flushing
- ✅ Protocol message extraction
  - Query message parsing (simple protocol)
  - Parse message parsing (extended protocol)
  - Query completion detection
- ✅ End-to-end query event flow
  - Extract queries from client messages
  - Track query timing
  - Publish events via batcher
- ✅ Error query event handling
  - Parse ErrorResponse messages from Postgres wire protocol
  - Extract severity and error message from error fields
  - Create QueryEvents with success=false and error details
  - Distinguish between successful and failed queries
- ✅ Unit tests (10 passing)
  - Protocol message extraction tests
  - Error message parsing tests
  - Event batching tests
- ✅ Integration tests with real Postgres
  - Testcontainers-based integration tests
  - Basic query proxying test (6 tests)
  - Prepared statements (extended protocol) test
  - Error handling tests (syntax errors, missing tables)
  - Mixed success/error queries test
- ✅ Stateful connection tests (20 tests)
  - Transaction management (BEGIN/COMMIT/ROLLBACK, savepoints, isolation levels)
  - Cursors (DECLARE/FETCH/CLOSE, scrollable cursors, WITH HOLD)
  - Session variables (SET/SHOW, transaction-scoped variables)
  - Temporary tables (lifecycle, ON COMMIT behaviors)
  - Advisory locks (basic locks, cross-connection conflicts, transaction-scoped)
  - LISTEN/NOTIFY (command validation)
- ✅ Graceful shutdown handling (2 tests)
  - Signal handling (Ctrl+C, SIGTERM)
  - Connection draining with configurable timeout
  - Event batcher flush on shutdown
  - Publisher shutdown
  - Tracked with `JoinSet` for proper cleanup
- ✅ Backend connection pooling (complete)
  - Protocol-agnostic TCP connection pool using deadpool
  - Passive healthchecks during pool recycle
  - Connection state reset between reuses (DISCARD ALL)
- ✅ Resilience features (Circuit Breaking, Retries, Healthchecks)
  - Lock-free circuit breaker with atomic state machine (13 tests passing)
  - Three-state circuit breaker: Closed → Open → HalfOpen
  - Integration with HealthMonitor for intelligent state transitions
  - Connection retry with exponential backoff and jitter
  - Active healthchecks with configurable intervals
  - All features independently configurable via 12-factor env vars
  - Prometheus metrics exposed via /metrics endpoint
  - <1ms latency overhead (lock-free atomic operations)
- ✅ TLS/SSL support for client and backend connections
  - Client-facing TLS (clients → proxy) with PostgreSQL SSL handshake
  - Backend TLS (proxy → database) for cloud databases (RDS, Cloud SQL)
  - SSL modes: disable, allow, require, verify-ca, verify-full
  - ClientTransport and BackendTransport abstractions for unified I/O
  - Certificate loading with rustls and tokio-rustls
  - 7 TLS integration tests

**TODO:**
- ✅ Backend connection pooling (complete)
- ✅ Production event publisher (HTTP with FlexBuffers)
- ✅ Query anonymization (with value fingerprinting for hot data detection)
- ✅ Circuit breaking, retries, and health checks (complete with 12-factor configuration)
- ✅ TLS/SSL support (client and backend connections with PgBouncer-compatible SSL modes)