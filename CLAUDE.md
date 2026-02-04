# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the `scry` project repository. A transparent proxy for SQL systems that gathers per-query observations and sends them out for analysis out of band.

The proxy provides observability out of the box, connection pooling, circuit breaking, retries, and healthchecks automatically. It's written in Rust for very low overhead — all observability (especially redirecting anonymized query information) is best effort, targeting no more than 1ms per query additional latency.

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

This repository is organized as a Cargo workspace:

- **`scry-proxy/`** - Main SQL proxy server
- **`benchmarks/`** - Performance benchmarking suite

### scry-protocol

The event protocol crate is published separately on [crates.io](https://crates.io/crates/scry-protocol). It can be used independently by analytics services, monitoring dashboards, and third-party tools for serializing/deserializing Scry query events.

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
- `DebugLoggerPublisher` - logs events as JSON with metrics (dev/testing)
- `HttpPublisher` - production HTTP publisher with FlexBuffers serialization

**Event Batching**: Background task with `tokio::mpsc::channel`, flushes on:
- Batch size threshold (configurable, default 100 events)
- Time threshold (configurable, default 1000ms)
- Whichever comes first

**Query Journaling Format**: FlatBuffers for maximum performance (<1ms overhead target)
- Events are published asynchronously, best-effort
- Batches sent via HTTP to a central analytics service

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

