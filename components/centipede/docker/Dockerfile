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

# Copy all workspace members matching the actual directory structure
# Core crates
COPY core/ core/
# Adapter crates
COPY adapters/ adapters/
# Support crates
COPY support/ support/
# Component crates (centipede: discoverer, worker, cli, etc.)
COPY components/ components/

# Build profile: use "release" for faster local builds, "release-max" for production
ARG CARGO_PROFILE=release-max

# Build optimized release binaries with tokio_unstable for full runtime metrics
ENV RUSTFLAGS="--cfg tokio_unstable"
RUN cargo build --profile ${CARGO_PROFILE} -p pf-discoverer-cli -p pf-worker-cli

# Stage 2: Discoverer target
FROM debian:bookworm-slim AS discoverer

ARG CARGO_PROFILE=release-max

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/${CARGO_PROFILE}/pf-discoverer /usr/local/bin/

ENV RUST_BACKTRACE=1
EXPOSE 9090
ENTRYPOINT ["pf-discoverer"]

# Stage 3: Worker target
FROM debian:bookworm-slim AS worker

ARG CARGO_PROFILE=release-max

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/${CARGO_PROFILE}/pf-worker /usr/local/bin/

ENV RUST_BACKTRACE=1
EXPOSE 9090
ENTRYPOINT ["pf-worker"]

# Stage 4: Combined (default, backwards compatible)
FROM debian:bookworm-slim

ARG CARGO_PROFILE=release-max

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/${CARGO_PROFILE}/pf-discoverer /usr/local/bin/
COPY --from=builder /app/target/${CARGO_PROFILE}/pf-worker /usr/local/bin/

ENV RUST_BACKTRACE=1

# Default to worker (most common use case)
ENTRYPOINT ["pf-worker"]
