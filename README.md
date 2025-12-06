# Scry

A transparent proxy for SQL systems with built-in observability.

## Overview

Scry is a high-performance, transparent proxy for Postgres that provides:

- **Observability**: Capture and publish per-query metrics and events
- **Resilience**: Circuit breaking, retries, and health checks
- **Low Overhead**: Target <1ms additional latency per query
- **Connection Pooling**: Efficient backend connection management

Built in Rust with Tokio for maximum performance and reliability.

## Quick Start

### Prerequisites

- Rust (latest stable)
- [just](https://github.com/casey/just) - command runner
- Docker (for integration tests)

### Build and Run

```bash
# Build the project
just build

# Run tests
just test

# Start local Postgres for testing
just postgres-up

# Run the proxy
just run

# Clean up
just postgres-down
```

## Development

See [CLAUDE.md](CLAUDE.md) for detailed architecture and development guidance.

### Common Commands

```bash
just build              # Build the project
just test               # Run all tests
just test-unit          # Run unit tests only
just test-integration   # Run integration tests
just lint               # Run clippy linter
just fmt                # Format code
just ci                 # Run all CI checks
```

## Configuration

Scry follows the 12-factor app methodology for configuration:

- Environment variables (recommended for production)
- Config files (`config.toml`)
- Command-line arguments

See `config.example.toml` for available options (to be added).

## Architecture

- **Async Runtime**: Tokio
- **Protocol**: Postgres wire protocol
- **Event Publishing**: Pluggable trait-based system
- **Observability**: OpenTelemetry + structured logging
- **Testing**: Unit tests + integration tests with Testcontainers

## Current Status

🚧 **Early Development** - Core scaffolding complete, implementing proxy logic.

See [CLAUDE.md](CLAUDE.md) for implementation status.

## License

MIT OR Apache-2.0
