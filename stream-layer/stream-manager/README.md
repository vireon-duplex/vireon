# stream-manager

> Owner of every QUIC stream on a connection: lifecycle, FSM, stream
> table, ID allocation, send/recv buffers, per-stream flow control.

**Crate**: `stream-layer/stream-manager`
**Phase**: 0 (architecture + skeleton)
**Full spec**: [DESIGN.md](DESIGN.md)

---

## Architecture

```text
┌─────────────────────────────────────────────────────────────┐
│  Application / Session                                      │
│    ▲ StreamEvent (RecvData, FinReceived, ResetReceived)     │
│    ▼ stream ops (write, read, close, priority)              │
├─────────────────────────────────────────────────────────────┤
│  ★ Stream Manager ★                                         │
│    ├── StreamTable      (slab + FxHashMap + generation)     │
│    ├── StreamIdAllocator (RFC 9000 §3.2)                    │
│    ├── SendBuffer       (offset + FIN, Phase 2)             │
│    ├── RecvBuffer       (gap + reassembly, Phase 2)         │
│    ├── StreamFlowControl (RFC 9000 §4.1, Phase 2)           │
│    ├── Stream FSM       (RFC 9000 §3.1/§3.3)                │
│    ├── StreamPriority   (RFC 9218)                          │
│    ├── StreamStats      (8 AtomicU64)                       │
│    └── StreamLifecycle  (create → drain → destroy, Ph 3)    │
│    ▲ StreamEvent                                            │
│    ▼ SchedulingDecision                                     │
├─────────────────────────────────────────────────────────────┤
│  Stream Scheduler (sibling)  │  quic-engine (wire)          │
└─────────────────────────────────────────────────────────────┘
```

The Stream Manager is **protocol-agnostic** — wire encoding (STREAM,
RESET_STREAM, MAX_STREAM_DATA, STOP_SENDING) stays in the transport
layer (`quic-engine` wrapping `quiche`). This crate emits scheduling
decisions + high-level stream events.

---

## Quick Start (Phase 0)

```rust
use stream_manager::{Preset, StreamManagerBuilder};

// Build from a preset
let builder = StreamManagerBuilder::from_preset(Preset::Hyperscale);
let cfg = builder.config();
cfg.validate().expect("preset config valid");

// Build custom
let builder = StreamManagerBuilder::new()
    .max_streams_bidi(10_000)
    .max_streams_uni(10_000)
    .slab_chunk_streams(4096);
```

Phase 1 will deliver `StreamManager::new(cfg)` with functional
`create_stream` / `recv_frame` / `poll_event` methods.

---

## Module Map

| # | Module | Phase | Responsibility |
|---|--------|-------|----------------|
| 1 | [`error`](src/error.rs) | 0 ✅ | `StreamManagerError` (9 variants, fatal/retryable) |
| 2 | [`config`](src/config.rs) | 0 ✅ | `StreamManagerConfig` + `Preset` (5 presets) |
| 3 | [`id`](src/id.rs) | 0 ✅ | `StreamId` + `StreamType` (RFC 9000 §3.2) |
| 4 | [`priority`](src/priority.rs) | 0 ✅ | `StreamPriority` + `PriorityUpdate` (RFC 9218) |
| 5 | [`state`](src/state.rs) | 0 ✅ | `SendState`, `RecvState`, `StreamState` (RFC 9000 §3.1) |
| 6 | [`slab`](src/slab.rs) | 0 ✅ | `StreamSlab` (slot + generation, ABA-safe) |
| 7 | [`stats`](src/stats.rs) | 0 ✅ | `StreamStats` (8 atomics) + `StatsSnapshot` |
| 8 | [`health`](src/health.rs) | 0 ✅ | `HealthReport` (Healthy/Degraded/Critical) |
| 9 | [`stream`](src/stream.rs) | 1 | `Stream` struct (metadata + FSM) |
| 10 | [`table`](src/table.rs) | 1 | `StreamTable` (slab + index) |
| 11 | [`allocator`](src/allocator.rs) | 1 | `StreamIdAllocator` |
| 12 | [`send`](src/send.rs) | 2 | `SendBuffer` |
| 13 | [`recv`](src/recv.rs) | 2 | `RecvBuffer` (reassembly) |
| 14 | [`flow_control`](src/flow_control.rs) | 2 | `StreamFlowControl` |
| 15 | [`lifecycle`](src/lifecycle.rs) | 3 | `StreamLifecycle` |
| 16 | [`event`](src/event.rs) | 1 | `StreamEvent` enum |
| 17 | [`builder`](src/builder.rs) | 1 | `StreamManagerBuilder` |
| 18 | [`manager`](src/manager.rs) | 1 | `StreamManager` facade |

---

## Layer Dependencies

```text
stream-manager
├── thiserror        (error derive)
├── tracing          (Phase 1+ logging)
└── (Phase 1+)
    ├── bytes        (buffer ownership)
    ├── buffer-pool  (runtime-layer)
    └── frame        (message-layer, app-stream id)
```

The sibling `stream-scheduler` crate depends on `stream-manager`
for the `StreamPriority` / `StreamId` types.

---

## Presets

| Preset | max_bidi | slab_chunk | window | Use case |
|--------|---------|------------|--------|----------|
| `WebRpc` | 1 000 | 1 024 | 256 KiB | HTTP/3 RPC |
| `MediaStream` | 100 | 256 | 1 MiB (uni) | Video on demand |
| `PubSub` | 10 000 | 4 096 | 8 KiB | Fan-out |
| `Hyperscale` | 1 000 000 | 16 384 | 128 KiB | 1 M+ streams |
| `LowLatency` | 1 000 | 512 | 16 KiB | Trading / gaming |

---

## Benchmarks

| Bench | Phase | Measures |
|-------|-------|----------|
| `table` | 1 | Insert / lookup / remove |
| `flow_control` | 2 | Consume / refresh / blocked-check |
| `slab` | 1 | Reserve / release / generation advance |
| `fsm` | 1 | State-transition micro-benchmarks |

Run: `cargo bench -p stream-manager`

---

## Key Design Decisions

1. **Protocol-agnostic** — wire encoding stays in `quic-engine`.
2. **Thread-per-core** — single-threaded by design; no locks.
3. **Generation-tracked slab** — ABA-safe slot recycling.
4. **RFC 9218 priority** — urgency (u7) + incremental (bool).
5. **Cache-line aware** — hot/cold field split in `Stream`.

See [DESIGN.md](DESIGN.md) for the full architecture specification.
