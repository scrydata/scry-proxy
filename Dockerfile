# Scry Proxy Dockerfile
#
# Multi-stage build for the scry-proxy binary
#
# Build from parent directory to include scry-protocol:
#   docker build -t scry-proxy -f scry-proxy/Dockerfile .

FROM rust:1.85-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy scry-protocol first (dependency)
COPY scry-protocol /scry-protocol

# Copy workspace root files
COPY scry-proxy/Cargo.toml scry-proxy/Cargo.lock ./

# Copy the scry-proxy crate
COPY scry-proxy/scry-proxy ./scry-proxy

# Update the crate's Cargo.toml to use absolute path for scry-protocol
# The dependency is in scry-proxy/Cargo.toml (the crate, not workspace root)
RUN sed -i 's|path = "../../scry-protocol"|path = "/scry-protocol"|g' scry-proxy/Cargo.toml && \
    sed -i 's|members = \["scry-proxy", "benchmarks"\]|members = ["scry-proxy"]|g' Cargo.toml

# Build release binary
RUN cargo build --release --package scry

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -r -s /bin/false -u 1000 scry

# Copy binary from builder
COPY --from=builder /app/target/release/scry /usr/local/bin/scry-proxy

USER scry

# Expose proxy port and metrics port
EXPOSE 5433 9090

# Health check via metrics endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -sf http://localhost:9090/metrics || exit 1

ENTRYPOINT ["scry-proxy"]
