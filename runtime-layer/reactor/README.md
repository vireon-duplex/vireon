# reactor

> Thread-per-core reactor orchestrator that owns one `io_uring`
> ring and drives every Vireon runtime-layer primitive per tick.

**Status: Phase 0 — architecture + skeleton.** See
[DESIGN.md](./DESIGN.md) for the full specification.

---

## What this crate is

The **Reactor** is the runtime-orchestration layer of Vireon. It
owns one CPU core, one `io_uring` ring, and a complete set of
runtime-layer primitives. Each call to `EventLoop::run_one_tick`
drains CQEs, fires timers, flushes the send queue, runs tasks,
and reclaims closed connections — all in a fixed, latency-optimal
order.

The Reactor is **protocol-agnostic**. QUIC-specific logic lives
in `transport-layer/quic-engine`, which hooks in via the
`ReactorDriver` callback trait. The reactor calls the driver
directly — no queue hop on the hot path.

## Architecture (one-line)

```text
park_on_ring → cqe_proc.drain_cq → timer_wheel.drain_expired
  → send_queue.flush → task_queue.tick → shard.drain_closed
  → driver.on_tick_complete → check_shutdown
```

See [DESIGN.md §3](./DESIGN.md#3-per-tick-event-loop) for the
canonical loop ordering.

## Position in the Vireon stack

```text
Application
    │
    ▼
transport-layer/quic-engine   ← implements ReactorDriver
    │
    ▼
runtime-layer/reactor         ← THIS CRATE
    │
    ├─► runtime-layer/cqe-processor
    ├─► runtime-layer/timer-wheel
    ├─► runtime-layer/buffer-pool
    ├─► runtime-layer/connection-shard
    ├─► runtime-layer/send-queue
    ├─► runtime-layer/task-queue
    │
    ▼
io-layer/io                   ← io_uring, socket, ops
```

## Quick start (Phase 0 stub)

```rust
use reactor::{ReactorBuilder, ReactorPreset, ReactorError};

let _reactor = ReactorBuilder::from_preset(ReactorPreset::BareMetal, 0)
    .build()?;
// Phase 1 will add: reactor.start(driver) → run loop
# Ok::<(), ReactorError>(())
```

The reactor compiles today but does not run an event loop yet —
that lands in Phase 1.

## Module map (17 source modules)

| # | Module | Phase | Responsibility |
|---|--------|-------|----------------|
| 1 | [`error`]    | 0 ✅ | `ReactorError` enum |
| 2 | [`config`]   | 0 ✅ | `ReactorConfig`, `ReactorPreset` |
| 3 | [`tick`]     | 0 ✅ | `TickResult`, `TickBudget` |
| 4 | [`budget`]   | 0 ✅ | `BudgetConfig` |
| 5 | [`idle`]     | 0 ✅ | `IdleStrategy` |
| 6 | [`pin`]      | 0 ✅ | `CpuPinning` (`sched_setaffinity`) |
| 7 | [`stats`]    | 0 ✅ | `ReactorStats`, `StatsSnapshot` |
| 8 | [`health`]   | 0 ✅ | `HealthReport` |
| 9 | [`driver`]   | 1 | `ReactorDriver` trait (callback interface) |
| 10| [`reactor`]  | 1 | `Reactor` facade |
| 11| [`event_loop`]| 1 | `EventLoop::run_one_tick` |
| 12| [`ring`]     | 2 | `RingOwner` (wraps `IoUringCtx`) |
| 13| [`submission`]| 2 | `RingSubmissionSink` (SEND_ZC SQEs) |
| 14| [`wake`]     | 3 | `EventfdWake` (cross-thread wake) |
| 15| [`buf_ring`] | 4 | `ProvidedBufferRing` (zero-copy RX) |
| 16| [`shutdown`] | 5 | `ShutdownCoordinator` |
| 17| [`builder`]  | 1 | `ReactorBuilder` (fluent) |

[`error`]: src/error.rs
[`config`]: src/config.rs
[`tick`]: src/tick.rs
[`budget`]: src/budget.rs
[`idle`]: src/idle.rs
[`pin`]: src/pin.rs
[`stats`]: src/stats.rs
[`health`]: src/health.rs
[`driver`]: src/driver.rs
[`reactor`]: src/reactor.rs
[`event_loop`]: src/event_loop.rs
[`ring`]: src/ring.rs
[`submission`]: src/submission.rs
[`wake`]: src/wake.rs
[`buf_ring`]: src/buf_ring.rs
[`shutdown`]: src/shutdown.rs
[`builder`]: src/builder.rs

## Design choices

- **Thread-per-core, shared-nothing.** No `Arc<Mutex>`, no work
  stealing. One reactor per CPU core, pinned via
  `sched_setaffinity`. Matches Pingora / Seastar / Glommio.
- **Direct callback driver.** The protocol layer implements
  `ReactorDriver`; the reactor calls it directly on every event.
  Lowest possible latency.
- **eventfd cross-thread wake.** Pure io_uring — other threads
  write 8 bytes to an eventfd registered via `IORING_OP_POLL_ADD`.
  No `mio`, no epoll on the wake path.
- **Provided buffer rings** for zero-copy RX.
- **SEND_ZC** for zero-copy TX with two-CQE lifecycle.
- **Deterministic tail latency.** Every tick bounded by
  `TickBudget`; overruns observable in `StatsSnapshot`.
- **No allocation on hot path.** Every per-tick structure is
  stack-allocated or pre-allocated scratch.

## Bench inventory

| Bench | File | Phase | Measures |
|-------|------|-------|----------|
| `event_loop` | [`benches/event_loop.rs`](benches/event_loop.rs) | 1 | per-tick latency under CQE / timer / task mixes |
| `submission` | [`benches/submission.rs`](benches/submission.rs) | 2 | SEND_ZC SQE push throughput vs batch size |
| `wake` | [`benches/wake.rs`](benches/wake.rs) | 3 | eventfd wake-to-CQE latency |
| `pin` | [`benches/pin.rs`](benches/pin.rs) | any | `sched_setaffinity` overhead |

Phase 0 ships the Criterion harnesses; bodies land with their
respective phases.

## Configuration presets

| Preset | Use case | Ring | Idle | SQPOLL | ZC send |
|--------|----------|------|------|--------|---------|
| `Cloud` | VM, unknown NUMA | 1024 | Hybrid 50 μs | off | off |
| `BareMetal` | Dedicated cores | 4096 | Hybrid 50 μs | on | on |
| `HundredGbe` | 100 GbE NIC, multi-queue | 8192 | Block | on | on |
| `LowLatency` | Trading / gaming | 2048 | Spin | off | off |
| `MaxThroughput` | Bulk transfer, CDN | 8192 | Block | on | on |

## Relationship to other Vireon crates

The reactor is the **apex** of the runtime layer. It depends on:

- [`io`](../../io-layer/io) — io_uring core
- [`cqe-processor`](../cqe-processor) — CQE decode + dispatch
- [`timer-wheel`](../timer-wheel) — hierarchical timers
- [`buffer-pool`](../buffer-pool) — memory allocation
- [`connection-shard`](../connection-shard) — connection ownership
- [`send-queue`](../send-queue) — egress batching
- [`task-queue`](../task-queue) — cooperative task execution

Nothing depends on the reactor (yet) — the future
`quic-engine` will be the first consumer.

## Roadmap

See [DESIGN.md §12](./DESIGN.md#12-15-phase-roadmap) for the
15-phase implementation roadmap. Each phase is its own
conversation / PR; Phase 0 is this round.

## License

GNU AFFERO GENERAL PUBLIC LICENSE v3.0.
