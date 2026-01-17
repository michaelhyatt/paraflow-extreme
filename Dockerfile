# Stage 1: Build
FROM rust:1.88-slim AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests first for dependency caching
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY cli/ cli/
COPY tests/ tests/

# Build release binaries
RUN cargo build --release -p pf-discoverer-cli -p pf-worker-cli

# Stage 2: Runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/pf-discoverer /usr/local/bin/
COPY --from=builder /app/target/release/pf-worker /usr/local/bin/

ENV RUST_BACKTRACE=1

# Default to worker (most common use case)
ENTRYPOINT ["pf-worker"]
