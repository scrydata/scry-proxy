# Scry Proxy Profiling Dockerfile
#
# Builds scry with debug symbols and includes perf + flamegraph tools
#
# Build from repo root:
#   docker build -t scry-proxy-profile -f benchmarks/Dockerfile.profile .

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
RUN sed -i 's|path = "../../scry-protocol"|path = "/scry-protocol"|g' scry-proxy/Cargo.toml && \
    sed -i 's|members = \["scry-proxy", "benchmarks"\]|members = ["scry-proxy"]|g' Cargo.toml

# Build release binary WITH debug symbols for profiling
# debuginfo=2 gives full debug info
ENV RUSTFLAGS="-C debuginfo=2"
RUN cargo build --release --package scry

# Install inferno flamegraph tools in builder stage
RUN cargo install inferno

# Runtime stage - use full debian for perf tools
FROM debian:bookworm

# Install runtime dependencies + profiling tools
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    curl \
    linux-perf \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Copy binary from builder (includes debug symbols)
COPY --from=builder /app/target/release/scry /usr/local/bin/scry-proxy

# Copy inferno tools from builder
COPY --from=builder /usr/local/cargo/bin/inferno-collapse-perf /usr/local/bin/
COPY --from=builder /usr/local/cargo/bin/inferno-flamegraph /usr/local/bin/

# Create directory for profile output
RUN mkdir -p /profiles
WORKDIR /profiles

# Expose proxy port and metrics port
EXPOSE 5433 9090

# Run as root for perf access (required for profiling)
# In production, use the regular Dockerfile with non-root user

ENTRYPOINT ["scry-proxy"]
