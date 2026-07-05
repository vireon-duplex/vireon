# connection-shard

Production-grade **Connection Ownership Layer** for the Vireon
runtime. Each Reactor core owns exactly one [`ConnectionShard`] and
all connections on that core. There is no shared mutable connection
state, no `Arc<Mutex>`, no cross-core locks — thread-per-core from
end to end.

The shard owns:

- **Lifecycle** — `create → activate → (backpressure) → close → recycle`
- **Placement** — which slot a connection occupies, generation-tagged
  handles that survive slot reuse (ABA-safe)
- **Routing** — `stream_id → ConnectionId` lookup at packet arrival
- **Migration** — explicit `prepare / accept / complete` protocol for
  handing a connection to another shard without shared state
- **Backpressure** — None / Soft / Hard, derived from fill ratio
- **Stats & health** — atomic counters readable from monitoring threads

The shard does **not** parse protocols, touch io_uring, or make
syscalls. It is pure user-space state management.

## Architecture

```text
+-------------------------------------------------------------------+
|                   CONNECTION SHARD (Reactor Core N)                |
|                                                                   |
|  ── Fast path (per-packet, zero alloc, zero lock) ─────────────   |
|  stream_id ──► stream_index (HashMap) ──► slot ──► ConnectionEntry|
|                                              │                    |
|  touch_recv / touch_send ────────────────────┘                    |
|                                                                   |
|  ── Slab ──────────────────────────────────────────────────────   |
|  slab: Vec<ConnectionEntry>     (cache-aligned, 64 B each)        |
|  generations: Vec<u16>          (bumped on free)                  |
|  free_list: Vec<u32>            (LIFO — hot slots stay hot)       |
|                                                                   |
|  ── Lifecycle ─────────────────────────────────────────────────  |
|  Init ─► Active ─► Backpressured ─► Migrating ─► Closing ─► Closed│
|                                                                   |
|  ── Migration protocol (no shared state) ──────────────────────   |
|  prepare_migration (source) ──► MigrationRequest                  |
|  accept_migration  (target)  ──► new ConnectionId                 |
|  complete_migration_out (source) ──► slot retired                 |
|                                                                   |
|  ── Stats / Health (atomic, monitor-readable) ─────────────────   |
|  StatsCollector (AtomicU64 × 12) ──► StatsSnapshot (Copy)         |
|  BackpressureLevel ──► HealthReport ──► HealthState              |
+-------------------------------------------------------------------+
```

## Quick start

```rust
use connection_shard::{ConnectionShardBuilder, ConnectionSpec};

let mut shard = ConnectionShardBuilder::new()
    .reactor_id(0)
    .capacity(4096)
    .build()
    .expect("build shard");

let spec = ConnectionSpec::new(42, "10.0.0.1:443".parse().unwrap());
let id = shard.create(spec).expect("create");
shard.activate(id).expect("activate");

// Hot path — packet arrives with stream_id 42.
let looked_up = shard.lookup_by_stream(42);
assert_eq!(looked_up, Some(id));

shard.begin_close(id).expect("begin_close");
shard.finalize_closing(id).expect("finalize");
assert_eq!(shard.active_count(), 0);
```

For presets tuned to deployment class:

```rust
use connection_shard::{ConnectionShardBuilder, Preset};

let shard = ConnectionShardBuilder::from_preset(Preset::HundredGbe)
    .reactor_id(0)
    .build()
    .expect("build shard");
```

## Layer map

| Layer | Module | Responsibility |
|-------|--------|----------------|
| 1 | [`error`]        | `ShardError` thiserror enum + `is_fatal` / `is_retryable` |
| 2 | [`config`]       | `ShardConfig`, `Preset`, capacity + backpressure constants |
| 3 | [`id`]           | `ConnectionId` (slot + generation packed in `u64`), `NIL_ID` |
| 4 | [`state`]        | `ConnectionState` machine + `const fn` predicates |
| 5 | [`priority`]     | `ConnectionPriority` (Bulk → RealTime, weighted) |
| 6 | [`backpressure`] | `BackpressureLevel` (None / Soft / Hard) + fill-ratio detection |
| 7 | [`connection`]   | `ConnectionEntry` (`#[align(64)]`), `ConnectionSnapshot` |
| 8 | [`spec`]         | `ConnectionSpec` — creation input |
| 9 | [`stats`]        | `StatsCollector` (atomic), `StatsSnapshot` (`Copy`) |
| 10 | [`strategy`]    | `ShardingStrategy` trait + Local / Modulo / JumpHash / Rendezvous |
| 11 | [`migration`]   | `MigrationRequest` + protocol helpers |
| 12 | [`health`]      | `HealthReport`, `HealthState` (Healthy / Degraded / Critical) |
| 13 | [`shard`]       | `ConnectionShard` facade — fast path + lifecycle + migration |
| 14 | [`builder`]     | `ConnectionShardBuilder` (fluent + `from_preset`) |

## Relationship to other crates

| Crate | Role |
|-------|------|
| `runtime-layer/cqe-processor` | Decodes CQEs and dispatches them; calls `lookup_by_stream` on each Recv to resolve the owning connection. |
| `runtime-layer/send-queue`    | Per-connection send bookkeeping. The shard exposes `touch_send(id, bytes)` so byte counters stay authoritative here. |
| `runtime-layer/task-queue`    | Schedules follow-up work after connection state transitions. |
| `runtime-layer/timer-wheel`   | Drives idle / keep-alive timeouts; the reactor translates timer fires into `begin_close` calls on the shard. |
| `runtime-layer/buffer-pool`   | Owns the byte buffers the shard's connections send and receive. |
| `kernel-layer/kernel-nic`     | Bottom of the stack — the shard never touches it directly. |

The shard is the **single source of truth** for "who owns this
connection?" on a core. Other runtime modules borrow `&mut
ConnectionShard` briefly on the fast path; nothing else stores
connection metadata.

## Design choices

- **Single-threaded per shard.** One `ConnectionShard` per Reactor
  core. The fast path (`lookup_by_stream`, `touch_recv`, `touch_send`)
  uses plain `&mut self` — no atomics, no locks. Only `StatsCollector`
  uses `AtomicU64` so monitoring threads can read counters.
- **Generation-tagged handles.** `ConnectionId` packs a 20-bit slot
  and a 16-bit generation into a `u64`. Each `destroy` bumps the
  slot's generation before returning it to the free list; stale
  handles from a previous generation are rejected as `NotFound`.
  Pattern matches send-queue / task-queue exactly.
- **Cache-aligned slab entries.** `ConnectionEntry` is
  `#[repr(C, align(64))]` so adjacent slots never share a cache line.
  The slab is a flat `Vec<ConnectionEntry>` — O(1) access with no
  pointer chasing.
- **LIFO free list.** Just-recycled slots are reused next, keeping
  them warm in cache. Combined with the slab layout, this makes
  allocation churn cache-friendly.
- **stream_id → slot HashMap, per shard.** `std::collections::HashMap`
  is fine here — only the owning core touches it. Duplicates are
  rejected at `create()` time with `DuplicateStream`.
- **Explicit migration.** No shared queue, no coordinator. Source
  serializes a `MigrationRequest`, target calls `accept_migration`,
  source calls `complete_migration_out`. The connection is frozen in
  `Migrating` state during the handoff; rollback restores `Active`.
- **Backpressure from fill ratio.** Soft at `soft_bp_pct` (default
  80%), Hard at `hard_bp_pct` (default 95%). `recompute_backpressure`
  is cheap and can be called after each `create` / `destroy`.
- **Atomic stats, `Copy` snapshots.** Counters use `Relaxed` ordering
  — single-writer per core on the hot path; the snapshot is a `Copy`
  struct so callers don't borrow from the shard.
- **Sharding strategies are pluggable.** `LocalSharding` (RSS-style
  "this core owns it"), `ModuloSharding`, `JumpHashSharding` (Google
  jump consistent hash), `RendezvousSharding` (HRW — minimal
  re-mapping on shard-count changes).

## Benchmarks

The crate ships a 4-binary Criterion suite covering the hot path,
lifecycle churn, slab allocation, and migration overhead.

```sh
cargo bench -p connection-shard                              # full Criterion suite
cargo bench -p connection-shard --bench lookup               # stream_id lookup
cargo bench -p connection-shard --bench slot_map -- \
    --sample-size 10 --measurement-time 1                    # quick smoke
```

### Bench inventory

| Binary | Concern |
|--------|---------|
| `lookup`    | `lookup_by_stream` at 1K / 16K / 64K fill + stream miss |
| `lifecycle` | `create_single`, `create_destroy_cycle`, `touch_recv` / `touch_send` |
| `slot_map`  | `ConnectionId` encode/decode, `view` (valid + stale), recycle churn at 100 / 1K |
| `migration` | `prepare_migration` + `accept_migration` round-trip overhead |

[`error`]: src/error.rs
[`config`]: src/config.rs
[`id`]: src/id.rs
[`state`]: src/state.rs
[`priority`]: src/priority.rs
[`backpressure`]: src/backpressure.rs
[`connection`]: src/connection.rs
[`spec`]: src/spec.rs
[`stats`]: src/stats.rs
[`strategy`]: src/strategy.rs
[`migration`]: src/migration.rs
[`health`]: src/health.rs
[`shard`]: src/shard.rs
[`builder`]: src/builder.rs
