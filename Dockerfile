# =============================================================================
# Vireon — multi-stage Dockerfile
# =============================================================================
# Builder: rust:1.85-slim  (matches workspace rust-version = "1.85")
# Runtime: debian:bookworm-slim  (glibc + ca-certificates for TLS handshakes)
#
# Builds the `quic-server` production binary (io_uring transport, QUIC data
# plane on UDP/4433, optional Prometheus metrics on TCP/9090).
# =============================================================================

# ── Builder ─────────────────────────────────────────────────────────────────
FROM rust:1.85-slim AS builder

# Build deps: gcc + make for io_uring bindings; cmake + go + ninja + perl
# + python3 for quiche's BoringSSL build.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      pkg-config \
      libssl-dev \
      ca-certificates \
      make \
      g++ \
      cmake \
      golang-go \
      ninja-build \
      perl \
      python3 \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the whole workspace. The Cargo stub-build trick (copy Cargo.toml
# first, stub main.rs, pre-build deps) does not work for a multi-crate
# workspace with `path =` deps — only the root crate's deps get fetched,
# the rest fail to resolve. A full source copy trades layer caching for
# correctness.
COPY . .

# Release build of the production server binary.
RUN cargo build --release -p quic-server

# ── Runtime ─────────────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

# ca-certificates for TLS chain validation; tini for PID-1 signal forwarding
# (the server needs SIGTERM → graceful drain).
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates \
      tini \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Stripped release binary.
COPY --from=builder /app/target/release/quic-server /app/quic-server

# Non-root user.
RUN useradd --system --uid 1000 --home /app vireon \
 && mkdir -p /var/lib/vireon/wal \
 && chown -R vireon:vireon /app /var/lib/vireon
USER vireon:vireon

# QUIC data plane (UDP) + Prometheus /metrics (TCP). Override with --port /
# --metrics-port at runtime; publish both with `docker -p`.
EXPOSE 4433/udp
EXPOSE 9090/tcp

# Persistent state (WAL segments) lives here when --wal-root is set.
VOLUME ["/var/lib/vireon/wal"]

ENTRYPOINT ["/usr/bin/tini", "--", "/app/quic-server"]
CMD ["--help"]
