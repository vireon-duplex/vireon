# Engineering Standards — Vireon

Engineering standards, architecture principles, and operational requirements for
building a high-performance realtime runtime built on QUIC and io_uring for multi-stream, full-duplex communication at massive scale. Every contributor and agent
MUST follow these.

## 1. Core technology stack

| Category | Primary | Notes |
|---|---|---|
| Language | Rust stable (Edition 2024) | — |
| Async runtime | Tokio | — |
| Serialization | Serde (public API); zero-copy for hot paths (§13) | — |
| Logging | `tracing` + `tracing-subscriber` | — |
| Metrics | Prometheus | — |
| Tracing | OpenTelemetry (OTLP) | — |
| Config | `config-rs` + `dotenvy` | — |
| API docs | `utoipa` (control/admin plane) | — |
| **Storage** | **TBD** (pluggable port) | log / KV / in-memory — not locked |
| **Transport** | **TBD** (pluggable port) | TCP/TLS / QUIC / others — not locked |

## 2. Architecture

Clean Architecture — dependencies point inward only.

```text
api  →  application  →  domain  ←  infrastructure
```

- **Domain** — no infra, transport, storage, HTTP, or Tokio deps.
- **Application** — orchestrates use cases (publish, subscribe, ack).
- **Infrastructure** — implements domain port traits (transport + storage adapters).

See [ARCHITECTURE.md](./ARCHITECTURE.md).

## 3. Workspace layout (target)

```text
crates/
  domain/          # streams, messages, offsets, port traits
  application/     # publish/subscribe/ack use cases
  api/             # wire protocols + control/admin HTTP
  infrastructure/  # transport + storage adapters
src/               # current: single crate (lib + bin `vireon`)
```

## 4. Layer standards

| Layer | Contains | Must NOT contain |
|---|---|---|
| **Domain** | entities, value objects, events, aggregates, port traits | SQL, HTTP, transport, storage, Tokio |
| **Application** | use cases calling domain + port traits | concrete infrastructure |
| **Infrastructure** | transport/storage adapters, all I/O | routing, handlers |

## 5. Error handling

Never `unwrap()`/`expect()`/`panic!()` outside tests and bootstrap. Use `thiserror`
for typed domain/library errors; `anyhow` for app-level aggregation only.

```rust
#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    #[error("stream already exists")]
    AlreadyExists,
}
```

## 6. API standards (control / admin plane)

- Plural resources (`/streams`, `/consumers`); never verb paths.
- Versioned: `/api/v1`. Consistent success/error envelope.
- The data plane (produce/consume) uses the broker wire protocol, not REST.

## 7. Dependency injection

Constructor injection; no global state.

```rust
pub struct PublishUseCase { log: Arc<dyn MessageLog> }
```

## 8. Async standards

Never block Tokio. `tokio::time::sleep` (not `std::thread::sleep`);
`spawn_blocking` for CPU work.

## 9. Storage standards (port-based)

Storage is a domain port; backends are chosen later. Whatever the backend:

- Treat the message log as **append-only**.
- Reads seek by offset/time; writes are sequential.
- Transactions span a single stream/aggregate boundary.
- Never hand-edit production data; migrations are reviewed + tested on staging.

## 10. Authentication & authorization

| Item | Choice |
|---|---|
| Transport auth | TLS 1.3; mTLS for service-to-service |
| Credentials | Short-lived signed tokens / client certs |
| Authorization | Per-stream ACLs (publish / subscribe / admin) |
| Never | Plaintext creds, MD5/SHA1 |

## 11. Security standards (summary)

Input validation · rate limiting + quotas · TLS 1.3 · secrets in Vault/KMS ·
audit log · encryption at rest · least privilege. Full policy:
[SECURITY.md](./SECURITY.md).

## 12. Observability

Three pillars: structured JSON logs (`trace_id`/`span_id`), Prometheus metrics
(see [OPERATIONS.md](./OPERATIONS.md)), OTLP traces to Jaeger/Tempo.

## 13. Performance

| Metric | Target |
|---|---|
| Publish P95 | < 10ms |
| Delivery P95 | < 50ms |
| Availability | > 99.9% |
| Durability | no acknowledged message lost |

## 14. Testing

Min 80% / preferred 90%+. Unit (domain, heavily), integration (real
storage/transport via testcontainers), contract, load. Full strategy:
[TESTING.md](./TESTING.md).

## 15. Code quality (CI fails on violation)

```bash
cargo fmt && cargo clippy && cargo audit && cargo deny check
```

## 16. Documentation

Every crate has `README.md`; public APIs have `///` doc comments (describe
**what**, not how). Wrap CamelCase/product names in backticks (`doc_markdown`
denied).

## 17. CI/CD

```text
Lint → Test → Audit → Build → Docker → Deploy
```

## 18. Docker

Multi-stage only; distroless final image; Alpine/Debian-slim as fallback.

## 19. Kubernetes

Each deployment: `Deployment`, `Service`, `ConfigMap`, `Secret`, `HPA`, `PDB`,
`NetworkPolicy`. Pod runs non-root (uid 65532).

## 20. Git workflow

```text
main  feature/*  fix/*  hotfix/*
```

Commit convention: `<type>(<scope>): <summary>` with mandatory `why`. Full
template in [`.gitmessage`](./.gitmessage). Types: `feat fix refactor test docs chore`.

## 21. AI agent workflow

End-of-task flow + commit rules: see [AGENTS.md](./AGENTS.md).

## 22. Production readiness checklist

- [ ] tests, clippy, audit, deny pass
- [ ] rollback plan ready
- [ ] dashboards + alerts configured
- [ ] load test passed
- [ ] health endpoints live
- [ ] image signed

---

## Advanced topics (streaming / distributed systems at scale)

These are core to a high-throughput broker — read carefully.

### 23. Event-driven architecture

Services communicate asynchronously via events.

| Concept | Rule |
|---|---|
| Event schema | Versioned (`stream.created.v1`), backward-compatible |
| Payload | Minimal + immutable; reference by ID, never embed entities |
| Idempotency | Consumers dedupe by event ID (at-least-once default) |
| Ordering | Per-stream/partition; never assume global order |
| DLQ | Failed events go to a dead-letter after N retries |

```text
Producer → Vireon log → Consumer(s)
                    ├→ projection (read model)
                    ├→ notification
                    └→ analytics
```

### 24. CQRS

Separate write model (commands → aggregate state) from read models (optimized
projections). Apply when read/write ratio is high. Avoid for simple CRUD.

### 25. Event sourcing

Store state as an append-only event sequence; current state = replay.

- Events are immutable — emit compensating events, never delete.
- Snapshot periodically to bound replay cost.
- Build projections by replaying asynchronously; version schemas with upcasters.

### 26. Distributed systems

| Principle | Guideline |
|---|---|
| CAP | Choose CP for metadata/membership; AP for the data plane |
| Idempotency | Every external mutation idempotent (idempotency keys) |
| Retries | Exponential backoff + jitter; bounded; circuit breakers |
| Timeouts | Explicit on ALL I/O; never infinite |
| Clocks | Never rely on wall clocks for ordering — use HLC/logical clocks |
| Partitions | Design for partition tolerance; network is never reliable |

### 27. Network programming

| Area | Standard |
|---|---|
| I/O model | async `tokio::net`; never blocking sockets |
| Buffers | `bytes::Bytes`/`BytesMut` (cheap clones, zero-copy slicing) — never `Vec<u8>` on hot paths |
| Backpressure | Propagate `Poll::Pending` / bounded channels up the stack |
| Connections | Pool/reuse; never 1-conn-per-request |
| DNS | Cache resolutions; `hickory-resolver` for async |

```rust
let frame: Bytes = bytes.split_to(n); // zero-copy slice
```

### 28. Lock-free programming

Only when profiling proves a `Mutex` is the bottleneck. `std::sync::atomic`
(counters/seq#), `arc-swap` (read-heavy config), `crossbeam` (MPMC queues, epoch
reclamation). Default to `std::sync::Mutex` for simplicity. Use `Relaxed` for
independent counters, `Acquire`/`Release` to publish data. Never hand-roll — use
vetted crates; benchmark against the `Mutex` baseline; document ordering reasoning.

### 29. High-performance Rust

| Practice | Detail |
|---|---|
| Measure first | `cargo flamegraph`, `perf` before optimizing |
| Benchmark | `criterion` in CI; guard regressions |
| Allocations | Minimize heap allocs in hot loops; reuse buffers, `SmallVec` |
| Inlining | `#[inline]` on tiny hot helpers |
| LTO | Thin LTO + `codegen-units = 1` in release |
| Clones | Use refs/`Bytes` slices, not `.clone()`, on hot paths |

### 30. Zero-copy serialization

| Crate | Best for |
|---|---|
| `bytes` | raw network frames |
| `rkyv` | fast internal RPC, persistence |
| `capnp` / `flatbuffers` | inter-service, cross-language |
| `serde` + JSON | public/human-readable API (default) |

Public API → JSON; internal hot path → zero-copy (`rkyv`/`capnp`); versioned,
backward-compatible schemas.

### 31. Backpressure handling

A slow consumer MUST NOT crash the system.

| Mechanism | Use |
|---|---|
| Bounded channels | `tokio::sync::mpsc` with explicit capacity |
| Concurrency limit | `buffer_unordered(n)` / tower `concurrency_limit` |
| Storage pool | Bound pool size; queue when exhausted |
| Load shedding | Drop low-priority work under extreme load |

Never use unbounded channels in production. Choose drop policy deliberately
(oldest / newest / reject) and expose queue depth as a metric.

### 32. Consensus algorithms

For cluster membership, leader election, replicated metadata only — never the
primary data path.

| Algo | Crate | Use |
|---|---|---|
| Raft | `openraft` | leader election, replicated logs, metadata |

Rules: use a vetted crate (don't hand-roll); quorum writes; joint-consensus for
membership changes; keep consensus off the data plane.

### 33. Distributed storage

| Concern | Approach |
|---|---|
| Message log | Append-only segments on disk/in-memory; never in a relational blob column |
| Sharding | By stream/key; consistent hashing for rebalancing |
| Replication | RF ≥ 3 across failure domains |
| Hot vs cold | Active streams in hot tier; cold/retained → cheaper tier |
| Rebalance | Background, throttled — never starve foreground traffic |

### 34. Observability at scale

Tail-based sampling for errors/slow ops; Prometheus exemplars linking metrics →
traces; track P99/P99.9; avoid unbounded Prometheus labels (use logs/traces for
high-cardinality like `stream_id`); every log carries `trace_id` + `span_id`.

```text
metric spike → exemplar → trace → span → log → root cause
```

---

## 35. Golden rules

1. Simplicity over cleverness. 2. Correctness over optimization. 3. Observability
first. 4. Security by default. 5. Domain first. 6. Test everything important.
7. Never panic in production. 8. Measure before optimizing. 9. Automate
everything. 10. Backpressure everywhere.
