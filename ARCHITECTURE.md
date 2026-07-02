# Architecture — Vireon

Vireon is a **high-performance realtime runtime built on QUIC and io_uring for multi-stream, full-duplex communication at massive scale.** This document is the authoritative reference for its design. Every
contributor and agent MUST follow it.

> Transport and storage backends are **not yet locked in**. They are modeled as
> pluggable ports (§3) and decided per deployment. Do not hard-code either.

---

## 1. Layered architecture (Clean Architecture)

Dependencies point **inward only**. The domain knows nothing about I/O.

```text
┌─────────────── API ───────────────┐  wire protocols, handlers, DTOs
└────────────────┬──────────────────┘
                 ▼
┌──────────── Application ──────────┐  use cases: publish, subscribe, ack
└────────────────┬──────────────────┘
                 ▼
┌────────────── Domain ─────────────┐  streams, messages, offsets, semantics
│  (zero external dependencies)     │   repository / port traits
└────────────────▲──────────────────┘
                 │ implements ports
┌──────────── Infrastructure ───────┘  transport adapters, storage adapters
```

| Layer | MAY depend on | MAY NOT depend on |
|---|---|---|
| **api** | application, domain | infrastructure internals |
| **application** | domain | infrastructure, wire types |
| **domain** | std, thiserror, serde (derive) | tokio, transport, storage, http |
| **infrastructure** | domain ports, transport/storage crates | api handlers, routing |

---

## 2. Broker domain model

| Concept | Meaning |
|---|---|
| **Stream / Topic** | Append-ordered, named channel of messages. |
| **Message** | Payload + key + headers + timestamp. Immutable once appended. |
| **Offset** | Monotonic per-consumer position within a stream. |
| **Producer** | Client that appends messages to a stream. |
| **Consumer** | Client that reads a stream from an offset. |
| **Consumer Group** | Set of competing consumers sharing a subscription; each message delivered to one member. |
| **Partition** (future) | Ordered shard of a stream for parallelism. |

### Delivery semantics (configurable per stream/subscription)

- **at-most-once** — fire and forget; may drop or duplicate.
- **at-least-once** — redelivered until acked; may duplicate (consumers must be idempotent).
- **exactly-once** — deduplicated via transactional/idempotent producer + offset commit.

### Subscription modes

- **pub/sub** — fan-out to all subscribers.
- **queue** — competing consumers (work distribution).
- **log / replay** — durable, retained, seekable by offset/time.

---

## 3. Ports (pluggable, decided per deployment)

Transport and storage are trait-based ports defined in the domain/infrastructure
boundary. Concrete adapters are chosen later.

```rust
// Storage port — how messages are appended and read back.
#[async_trait]
pub trait MessageLog: Send + Sync {
    async fn append(&self, stream: &StreamId, msg: &Message) -> Result<Offset, StorageError>;
    async fn read(&self, stream: &StreamId, from: Offset) -> Result<Vec<Message>, StorageError>;
}

// Transport port — how clients talk to the broker (TCP/TLS, QUIC, etc. — TBD).
#[async_trait]
pub trait Transport: Send + Sync {
    async fn serve(&self, engine: Arc<dyn BrokerEngine>) -> Result<(), TransportError>;
}
```

---

## 4. Target workspace layout

The repository starts as a single crate; the intended split as it grows:

```text
vireon/
├── crates/
│   ├── domain/           # streams, messages, offsets, port traits
│   ├── application/      # publish/subscribe/ack use cases
│   ├── api/              # wire protocols + handlers
│   └── infrastructure/   # transport + storage adapters
└── src/                  # current: single crate (lib + bin `vireon`)
```

---

## 5. Cross-cutting concerns

| Concern | Approach |
|---|---|
| **Errors** | `thiserror` typed enums per layer; no `unwrap`/`expect`/`panic` outside tests/bootstrap. |
| **Async** | `tokio`; never block the runtime (`spawn_blocking` for CPU work). |
| **Observability** | `tracing` spans per use case; Prometheus metrics; OTLP tracing. |
| **Backpressure** | Bounded channels everywhere; slow consumers must not crash the broker. |
| **Idempotency** | Consumers handle duplicates (at-least-once default). |

---

## 6. Anti-patterns to avoid

- Domain importing transport/storage/HTTP crates.
- Business logic inside protocol handlers (handlers stay thin).
- Infrastructure types leaking into domain or application.
- Unbounded channels in production paths (hides backpressure until OOM).
- `unwrap()` / `expect()` / `panic!()` in non-test code.

---

## 7. Checklist before merge

- [ ] Domain crate has no transport/storage/HTTP dependencies.
- [ ] No `unwrap`/`expect`/`panic!` outside tests and bootstrap.
- [ ] Handlers are thin — delegate to application use cases.
- [ ] Storage/transport access goes through domain-defined port traits.
- [ ] Errors typed with `thiserror` at the right layer.
- [ ] `cargo fmt`, `cargo clippy -D warnings`, `cargo test` pass.
