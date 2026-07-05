# cqe-processor

Production-grade **reactor-local CQE dispatcher** for Vireon — the
central event loop that drains io_uring completion events, decodes
them via the IO Layer's `CompletionDecoder`, and fans outcomes out to
the Buffer Pool, Send Queue, Task Queue, and Timer Wheel. One
processor per reactor; no sharing, no locks.

See [`DESIGN.md`](DESIGN.md) for the full architecture specification.

## Architecture

```text
+-------------------------------------------------------------------+
|                  CQE PROCESSOR (Reactor Core N)                   |
|                                                                   |
|  ── Inputs ────────────────────────────────────────────────────   |
|  io_uring CQ ──► CqDrainer ──► [Entry] ──► CompletionDecoder      |
|                  (scratch Vec,                                    |
|                   single head-advance)      (in io-layer)         |
|                                                                   |
|  ── Dispatch ──────────────────────────────────────────────────   |
|  decode user_data high nibble ──► DispatchTable[opkind]           |
|                                   (fn-pointer, no match)          |
|                                          │                        |
|  ── Handlers emit Outcomes ─────────────────────────────────────► |
|  process_recv   ─► Outcome { rearm_multishot, return_buffer }     |
|  process_send   ─► Outcome { forward: SendCqe(_) }                |
|  process_reclaim─► Outcome { return_buffer }                      |
|  process_timeout► Outcome { retire_timer }                        |
|  process_cancel ─► Outcome { retire_timer }                       |
|                                                                   |
|  ── Coalescing buffers (drained by reactor at end of tick) ──     |
|  RecyclePipeline   SendQueueEvent queue   CqeScheduler   timers   |
|  (batched buf returns)                                            |
+-------------------------------------------------------------------+
```

## Quick start

```no_run
use cqe_processor::{CqeProcessorBuilder, CompletionDescriptor};
use io::completion::Completion;
use io::ops::{OpKind, encode};

let mut proc = CqeProcessorBuilder::new()
    .batch_max_cqes(256)
    .reactor_id(0)
    .build()?;

// Dispatch a synthetic completion without touching the ring.
let c = Completion::Timeout { id: 42 };
let outcome = proc.dispatch_one(encode(OpKind::Timeout, 42), &c, 0);
assert!(outcome.consumed);
assert_eq!(outcome.retire_timer, Some(42));
# Ok::<(), cqe_processor::CqeError>(())
```

For the full per-tick path against a real ring:

```no_run
use cqe_processor::CqeProcessorBuilder;
use io::context::IoUringCtx;

let mut proc = CqeProcessorBuilder::new().reactor_id(0).build()?;
let mut ring = IoUringCtx::init_ring(256, false).expect("ring");
// arm multishot recv, send_zc, timeout, cancel …
let result = proc.drain_cq(ring.ring_mut(), 0);
// reactor forwards `result` to BufferPool / SendQueue / TaskQueue / TimerWheel
# Ok::<(), cqe_processor::CqeError>(())
```

## Layer map

| Layer | Module | Responsibility |
|-------|--------|----------------|
| 1 | [`error`]     | `CqeError` thiserror enum + `is_fatal` |
| 2 | [`config`]    | `CqeProcessorConfig`, defaults, validation, `OverflowPolicy` |
| 3 | [`user_data`] | `OpKind`-aware helpers (re-export of `io::ops`) |
| 4 | [`completion`]| `CompletionDescriptor` (CQE + decoded view) |
| 5 | [`cache`]     | `HotState` cache-aligned (`#[align(128)]`) counters |
| 6 | [`dispatcher`]| `DispatchTable`, `Handler` fn-pointer, default handlers |
| 7 | [`drain`]     | `CqDrainer` (raw ring read + head advance) |
| 8 | [`sendzc`]    | `SendZcTracker` (id → buf_id + debug pairing) |
| 9 | [`multishot`] | `MultishotState` (re-arm policy + cooldown) |
| 10 | [`recycle`]  | `RecyclePipeline` (batched buffer returns) |
| 11 | [`lifecycle`]| `Outcome` bitset, `SendQueueEvent` enum |
| 12 | [`scheduler`]| `CqeScheduler` (Task Queue bridge accumulator) |
| 13 | [`stats`]    | `StatsCollector` (atomic), `StatsSnapshot` (`Copy`) |
| 14 | [`processor`]| `CqeProcessor` facade, `TickResult`, `drain_cq` / `dispatch_one` |
| 15 | [`builder`]  | `CqeProcessorBuilder` (fluent) |

## Relationship to other crates

| Crate | Role |
|-------|------|
| `io-layer/io`         | Owns the `IoUring` ring + `CompletionDecoder`. CQE Processor borrows `&mut IoUring` per tick. |
| `runtime-layer/buffer-pool` | Receives recycled buffer IDs from `drain_pending_recycles()`. |
| `runtime-layer/send-queue`  | Consumes `SendQueueEvent`s from `drain_pending_sendq_events()` (Send CQE, Reclaim CQE, Send-timeout). |
| `runtime-layer/task-queue`  | Drains scheduled tasks from `drain_pending_tasks()` after each tick. |
| `runtime-layer/timer-wheel` | Receives `retire_timer` IDs for cancelled / fired timers. |

The CQE Processor **never** owns runtime modules. It emits `Outcome`s
into per-tick scratch buffers; the reactor applies them at the end of
the tick. This decoupling keeps the processor unit-testable without
the full runtime.

## Design choices

- **Function-pointer dispatch table.** `[Handler; 16]` indexed by the
  high nibble of `user_data`. No `match` statement in the hot path —
  a single indirect call per CQE. Edition 2024 quirk: `default_table()`
  is a non-const `fn` because filling a `[fn-pointer; N]` from named
  function items requires explicit type ascription.
- **High-nibble `user_data` tagging.** Bits `[63:60]` = `OpKind`,
  bits `[59:0]` = id. Zero-overhead decode via shifts. Shared format
  with the IO Layer's `CompletionDecoder`.
- **Single head-advance per batch.** `CqDrainer::drain` uses the
  io-uring crate's `by_ref()` iterator; head is advanced in the
  iterator's `Drop` after the batch is consumed — exactly one Release
  store per drain, regardless of batch size.
- **Outcome-based handler contracts.** Handlers return `Outcome`
  instead of touching runtime modules directly. The reactor polls
  `drain_pending_*` at end-of-tick to apply effects in batch. This
  keeps the processor free of cross-module borrows on the hot path.
- **SEND_ZC dual-CQE pairing.** `SendZcTracker` maps `id → buf_id`
  so the Reclaim CQE (which arrives later, with `IORING_CQE_F_NOTIF`)
  can resolve the buffer to recycle. Debug builds track per-id phase
  (`Pending | SendSeen | ReclaimSeen`) to detect orphan CQEs.
- **Coalesced recycle pipeline.** Buffer returns are batched in
  `RecyclePipeline` until `recycle_coalesce_threshold` is hit or the
  reactor explicitly drains — fewer `BufferPool::return_buffer` calls.
- **Multishot re-arm cooldown.** `MultishotState` suppresses re-arm
  spam after `-ENOBUFS` bursts; the reactor drains pending requests
  via `take_pending()` at the next submission boundary.
- **Cache-aligned hot state.** `HotState` is `#[repr(align(128))]` so
  its per-OpKind counters don't share a line with adjacent fields.
  Counters use `AtomicU64` with `Relaxed` ordering — the processor is
  single-threaded per reactor, so Relaxed is sufficient and free on
  x86/ARM.
- **Overflow policies.** `Log` (default) records the event;
  `Shutdown` flags the tick so the reactor can tear down cleanly;
  `Panic` calls `tracing::error!` then `std::process::abort()` (panic!
  is denied by workspace lints).
- **Starvation detection.** `consecutive_empty` ticks above
  `starvation_threshold_ticks` flip `TickResult::starvation_detected`
  so the reactor can yield or back off submission.

## Benchmarks

The crate ships a 5-binary Criterion suite covering per-OpKind
dispatch, batched drain scaling, recycle pipeline, stats, and
sustained pressure.

```sh
cargo bench -p cqe-processor                              # full Criterion suite
cargo bench -p cqe-processor --bench dispatch             # per-OpKind dispatch
cargo bench -p cqe-processor --bench stats -- \
    --sample-size 10 --measurement-time 1                 # quick smoke
```

### Bench inventory

| Binary | Concern |
|--------|---------|
| `drain`    | `dispatch_at_depth` at 16–1024 + `dispatch_mixed` (60/20/10/10 Recv/Send/Reclaim/Timeout) |
| `dispatch` | Per-`OpKind` single-CQE dispatch latency for Recv, Send, Reclaim, Timeout, Cancel |
| `recycle`  | `RecyclePipeline` push/drain at 16–1024 |
| `stats`    | `StatsCollector` increment + snapshot |
| `batch`    | `large_batch` 1k–64k drain scaling + `sustained_dispatch` 256×64 with drain between rounds |

### Smoke-test numbers (single run, your Mileage Will Vary)

| Bench | Result |
|-------|--------|
| `stats_increment/1024`     | ~5.6 µs (~183 Melem/s) |
| `stats_snapshot/single`    | ~19 ns / call |
| `dispatch_recv/single`     | sub-µs (criterion-reported) |

[`error`]: src/error.rs
[`config`]: src/config.rs
[`user_data`]: src/user_data.rs
[`completion`]: src/completion.rs
[`cache`]: src/cache.rs
[`dispatcher`]: src/dispatcher.rs
[`drain`]: src/drain.rs
[`sendzc`]: src/sendzc.rs
[`multishot`]: src/multishot.rs
[`recycle`]: src/recycle.rs
[`lifecycle`]: src/lifecycle.rs
[`scheduler`]: src/scheduler.rs
[`stats`]: src/stats.rs
[`processor`]: src/processor.rs
[`builder`]: src/builder.rs
