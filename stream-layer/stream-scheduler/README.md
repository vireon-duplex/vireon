# stream-scheduler

> Per-tick stream selection: RFC 9218 prioritization, fair queuing,
> starvation prevention.

**Crate**: `stream-layer/stream-scheduler`
**Phase**: 0 (architecture + skeleton)
**Full spec**: [DESIGN.md](DESIGN.md)

---

## Architecture

```text
┌─────────────────────────────────────────────────────────────┐
│  Stream Manager                                             │
│    ▼ ready-stream notifications                             │
├─────────────────────────────────────────────────────────────┤
│  ★ Stream Scheduler ★                                       │
│    ├── ReadyQueue       (8-bucket urgency)                  │
│    ├── BitmapScheduler  (O(1) trailing_zeros lookup)        │
│    ├── StreamSelector   (policy-driven pick)                │
│    ├── AgingTracker     (starvation prevention)             │
│    ├── FairnessMetrics  (Jain's index)                      │
│    ├── BatchComposer    (GSO-friendly flush)                │
│    ├── BackpressureTracker (flow-control skip)              │
│    └── IncrementalScheduler (RFC 9218 cycle-share)          │
│    ▼ Vec<Selection>                                         │
├─────────────────────────────────────────────────────────────┤
│  Send Queue / quic-engine                                   │
└─────────────────────────────────────────────────────────────┘
```

The scheduler reads stream priority and readiness from
[`stream-manager`](../stream-manager/) and emits selections the
send-queue packetizes into STREAM frames.

---

## Quick Start (Phase 0)

```rust
use stream_scheduler::{Preset, SchedulerBuilder};

// Build from a preset
let builder = SchedulerBuilder::from_preset(Preset::LatencyFirst);
let cfg = builder.config();
cfg.validate().expect("preset valid");

// Build custom
let builder = SchedulerBuilder::new()
    .max_streams_per_tick(128)
    .batch_size_max(32);
```

Phase 1 will deliver `StreamScheduler::new(cfg)` with functional
`tick()`.

---

## Module Map

| # | Module | Phase | Responsibility |
|---|--------|-------|----------------|
| 1 | [`error`](src/error.rs) | 0 ✅ | `SchedulerError` (7 variants) |
| 2 | [`config`](src/config.rs) | 0 ✅ | `SchedulerConfig` + `Preset` (4 presets) |
| 3 | [`policy`](src/policy.rs) | 0 ✅ | `SchedulingPolicy` (7 variants) |
| 4 | [`budget`](src/budget.rs) | 0 ✅ | `SchedulingBudget` (per-tick caps) |
| 5 | [`aging`](src/aging.rs) | 0 ✅ | `AgingTracker` + `AgingEntry` |
| 6 | [`fairness`](src/fairness.rs) | 0 ✅ | `FairnessMetrics` (Jain's index) |
| 7 | [`stats`](src/stats.rs) | 0 ✅ | `SchedulerStats` (8 atomics + per-band) |
| 8 | [`health`](src/health.rs) | 0 ✅ | `HealthReport` (Healthy/Degraded/Critical) |
| 9 | [`queue`](src/queue.rs) | 1 | `ReadyQueue` (8-bucket urgency) |
| 10 | [`bitmap`](src/bitmap.rs) | 1 | `BitmapScheduler` (O(1) band lookup) |
| 11 | [`selector`](src/selector.rs) | 1 | `StreamSelector` |
| 12 | [`batch`](src/batch.rs) | 2 | `BatchComposer` |
| 13 | [`backpressure`](src/backpressure.rs) | 2 | `BackpressureTracker` |
| 14 | [`incremental`](src/incremental.rs) | 2 | `IncrementalScheduler` |
| 15 | [`event`](src/event.rs) | 1 | `SchedulingEvent` enum |
| 16 | [`builder`](src/builder.rs) | 1 | `SchedulerBuilder` |
| 17 | [`scheduler`](src/scheduler.rs) | 1 | `StreamScheduler` facade |

---

## Policies

| Policy | Fair | Preemptive | Default |
|--------|------|-----------|---------|
| `Fifo` | ✗ | ✗ | |
| `RoundRobin` | ✓ | ✗ | |
| `WeightedRoundRobin` | ✓ | ✗ | |
| `DeficitRoundRobin` | ✓ | ✗ | |
| `WeightedFair` | ✓ | ✗ | |
| `StrictPriority` | ✗ | ✓ | |
| `Hybrid` | ✓ | ✓ | ✅ default |

The default **Hybrid** policy: strict priority across RFC 9218
urgency bands, round-robin within each band, aging for starvation
prevention.

---

## Presets

| Preset | Policy | Batch | Use case |
|--------|--------|-------|----------|
| `Fair` | Hybrid | 16 | General workloads. |
| `PriorityHeavy` | StrictPriority | 32 | Priority-dominant. |
| `LatencyFirst` | Hybrid | 4 | Low-latency (trading). |
| `ThroughputFirst` | WeightedFair | 64 | High-throughput (GSO). |

---

## Benchmarks

| Bench | Phase | Measures |
|-------|-------|----------|
| `select` | 1 | Per-tick selection cost across policies |
| `queue` | 1 | ReadyQueue enqueue / dequeue |
| `fairness` | 1 | Jain's index computation |
| `incremental` | 2 | RFC 9218 incremental cycle-share |

Run: `cargo bench -p stream-scheduler`

---

## Key Design Decisions

1. **Bitmap band lookup** — `trailing_zeros()` gives O(1) highest-band
   selection (one CPU instruction).
2. **Aging tracker** — promotes starving streams after configurable
   threshold (default 10 ms).
3. **Jain's fairness index** — quantitative fairness measurement over
   a sliding window.
4. **Budget-capped ticks** — per-tick work bounded regardless of
   queue depth.
5. **GSO-ready batching** — `BatchComposer` accumulates selections
   for single-syscall flushes.

See [DESIGN.md](DESIGN.md) for the full architecture specification.
