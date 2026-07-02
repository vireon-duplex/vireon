# =============================================================================
# Vireon — multi-stage Dockerfile
# =============================================================================
# Builder: rust:1.90-slim  (compiles the `vireon` binary in release mode)
# Runtime: gcr.io/distroless/cc-debian12  (no shell, minimal attack surface)
# =============================================================================

# ── Builder ─────────────────────────────────────────────────────────────────
FROM rust:1.90-slim AS builder

WORKDIR /app

# Copy workspace manifests first for better layer caching.
COPY Cargo.toml Cargo.lock ./

# Create stub sources so cargo can fetch and compile dependencies.
RUN mkdir src \
 && echo 'fn main() {}' > src/main.rs \
 && touch src/lib.rs

# Pre-build dependencies (cached unless Cargo.toml/Cargo.lock change).
RUN cargo build --release 2>/dev/null || true

# Copy the real source code and config.
COPY src ./src
COPY config ./config

# Build the actual binary (lib `vireon` + bin `vireon`).
RUN touch src/main.rs src/lib.rs && cargo build --release

# ── Runtime ─────────────────────────────────────────────────────────────────
FROM gcr.io/distroless/cc-debian12 AS runtime

WORKDIR /app

# Compiled binary.
COPY --from=builder /app/target/release/vireon /app/vireon

# Runtime config (config::load() reads config/default.toml).
COPY config/ /app/config/

# Non-root user (distroless provides "nonroot", uid 65532).
USER nonroot:nonroot

# Control / admin HTTP server. Broker data-plane port(s) are exposed once a
# transport is selected (see ARCHITECTURE.md §3).
EXPOSE 8080/tcp

ENTRYPOINT ["/app/vireon"]
