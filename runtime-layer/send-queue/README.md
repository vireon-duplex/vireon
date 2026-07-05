# send-queue

Production-grade **thread-local transmit queue** for Vireon — the seam
between the application stream layer and the IO Layer's `send_zc` path.
A lock-free SPSC ring with tracking-slot lifecycle, graded backpressure,
batch coalescing, and retry tracking. No `io_uring` access; the IO
boundary is the [`SubmissionSink`] trait.

## Architecture

```text
+-------------------------------------------------------------------+
|                       SEND QUEUE (CPU Core N)                     |
|                                                                   |
|  ── Producer (application thread) ──────────────────────────────  |
|  submit(buf_index, len, dest, meta)                               |
|    │                                                              |
|    ▼                                                              |
|  BackpressureGate ──► TrackingSlotAlloc ──► IngressRing (SPSC)    |
|  (admit / reject)      (free-list + gen)      (lock-free push)    |
|                                                                   |
|  ── Reactor tick ───────────────────────────────────────────────  |
|  flush(sink, now_ns)                                              |
|    │                                                              |
|    ▼                                                              |
|  Scheduler ──► BatchComposer ──► SubmissionSink (IO Layer)       |
|  (FIFO drain)   (count/bytes/age)   (SEND_ZC SQEs)                |
|    │                                          │                   |
|    ▼                                          ▼                   |
|  InFlightTable ◄──── on_send_cqe / on_reclaim_cqe ──────────────  |
|  (generation-verified state machine)                              |
|    │                                                              |
|    ▼                                                              |
|  TrackingSlotAlloc (free on reclaim)                              |
+-------------------------------------------------------------------+
```

## Quick start

```no_run
use send_queue::{
    BatchEntry, PacketMeta, SendQueueBuilder, SendQueueError,
    SchedulePolicyKind, SubmissionSink,
};

/// The integration layer provides this — it bridges to the IO Layer's
/// `Sender::send_zc` and pushes `SEND_ZC` SQEs.
struct MySink;
impl SubmissionSink for MySink {
    fn submit_batch(&mut self, entries: &[BatchEntry])
        -> Result<usize, SendQueueError>
    {
        // Push SEND_ZC SQEs via io_uring here.
        Ok(entries.len())
    }
}

let mut sq = SendQueueBuilder::new()
    .capacity(4096)
    .policy(SchedulePolicyKind::Fifo)
    .batch_max_packets(64)
    .build()?;

// Producer side.
let id = sq.submit(0, 64, "10.0.0.1:8080".parse()?, PacketMeta::new())?;

// Reactor flush.
let mut sink = MySink;
sq.flush(&mut sink, 0)?;

// Two-CQE lifecycle (send status, then buffer reclaim).
sq.on_send_cqe(id.raw());
sq.on_reclaim_cqe(id.raw());

// Retransmission timeout (driven by `runtime-layer/timer-wheel`).
match sq.on_timeout(id.raw()) {
    Ok(rto_ns) => { /* re-arm timer */ }
    Err(reason) => { /* drop packet */ }
}
# Ok::<(), Box<dyn std::error::Error>>(())
```

## Layer map

| Layer | Module | Responsibility |
|-------|--------|----------------|
| 1 | [`error`]          | `SendQueueError` thiserror enum |
| 2 | [`config`]         | `SendQueueConfig`, `RetryConfig`, constants |
| 3 | [`queue`]          | `IngressRing` (SPSC), `PacketSlot`, `SlotState` |
| 4 | [`scheduler`]      | `SchedulePolicy` trait, `FifoPolicy` |
| 5 | [`batch`]          | `BatchComposer`, `BatchEntry` (count / bytes / age triggers) |
| 6 | [`backpressure`]   | `BackpressureGate`, `PressureLevel`, `PressureObserver` |
| 7 | [`retry`]          | `RetryTracker`, `DropReason` |
| 8 | [`lifecycle`]      | `InFlightTable`, ID encode/decode (slot + generation) |
| 9 | [`submit`]         | `SubmissionSink` trait (IO Layer boundary) |
| 10 | [`stats`]         | `StatsCollector` (atomic), `StatsSnapshot` (`Copy`) |
| 12 | [`api`]           | `SendQueue` facade, `SendQueueBuilder`, `PacketMeta` |

## Relationship to other crates

| Crate | Role |
|-------|------|
| `io-layer/io`      | Implements `SubmissionSink` to translate `BatchEntry` → `SEND_ZC` SQEs. Owns the `Sender` and registered buffers. |
| `runtime-layer/timer-wheel` | Drives retransmission timeouts. On fire, calls `SendQueue::on_timeout(id)` which returns the next RTO or a `DropReason`. |
| `runtime-layer/buffer-pool` | Owns the registered-buffer indices (`buf_index`) passed to `submit()`. The buffer stays pinned until `on_reclaim_cqe` retires the tracking slot. |

The Send Queue **never** touches `io_uring`, `RawFd`, or `Sender`
directly — all kernel interaction goes through [`SubmissionSink`].

## Design choices

- **SPSC lock-free ring.** Power-of-two masking + `Acquire`/`Release`
  atomics on cache-padded head/tail counters. No locks, no allocation
  on the hot path.
- **Tracking-slot generation.** Every submit allocates a tracking slot
  `(index, generation)`. The generation is bumped on free so stale CQEs
  from a previous packet are rejected. Slots are freed on
  `on_reclaim_cqe`, not on `on_send_cqe` — the buffer is live until the
  kernel reports reclamation.
- **Two-CQE lifecycle.** `SEND_ZC` produces two completions: `Send`
  (status, `InFlight → Accepted`) and `SendBufferReclaim` (buffer may
  be recycled, retire + free tracking slot).
- **Graded backpressure with hysteresis.** Dual watermarks (`low` /
  `high`) plus a hysteresis band prevent flapping. Producers receive a
  `PressureLevel` signal (`Normal` / `Elevated` / `Saturated`) and a
  `QueueFull` error when `depth ≥ high`.
- **Batch composer.** Accumulates drained packets and flushes on count,
  byte, or age triggers. Partial sink submission (SQ full) pushes back
  un-submitted entries for the next tick.
- **Thread-local.** Phase 1 uses `&mut self` for all mutation methods.
  The ring's `Sync` impl supports cross-thread SPSC; full cross-thread
  `&self` submit is Phase 2.

## Benchmarks

The crate ships an 11-binary Criterion suite covering hot-path latency,
sustained-load stability, multi-producer scaling, cache behaviour, and
component-level micro-benchmarks.

```sh
cargo bench -p send-queue                              # full Criterion suite
cargo bench -p send-queue --bench submit               # single bench
cargo bench -p send-queue --bench submit -- \
    --sample-size 20 --measurement-time 0.5            # quick smoke

# Sustained-load soak — detect resource leaks over minutes or hours.
cargo bench -p send-queue --bench soak -- \
    --measurement-time 60 --sample-size 5

# MPSC scaling — thread-local vs shared-Mutex vs false-sharing.
cargo bench -p send-queue --bench mpsc
```

### Bench inventory

| Binary | Concern |
|--------|---------|
| `submit`        | `submit()` latency + burst throughput (16 / 64 / 256) + packet-size sweep |
| `flush`         | `flush()` at depths 16–1024, empty fast-path, `force_flush` |
| `lifecycle`     | End-to-end submit → flush → `send_cqe` → `reclaim_cqe` |
| `backpressure`  | `admit` / `post_change` / `level` overhead |
| `stats`         | Atomic-counter increments + `snapshot` |
| `saturation`    | Queue-full latency at fill levels (50–99%), rejection path, backpressure cycle |
| `retry`         | `on_timeout` at scale (1–10 000 inflight), RTO lookup, retry-until-drop, `SendQueue::on_timeout` |
| `cache`         | Submit / flush / lifecycle vs ring capacity (256–16 384) and packet size (64 B–16 KiB); proves zero-copy design |
| `mpsc`          | Thread-local scaling (1–16 threads), shared-Mutex contention, false-sharing micro-bench (padded vs unpadded atomics) |
| `soak`          | Sustained submit → flush → reclaim; detects tracking-slot / inflight leaks via throughput degradation |
| `scheduler`     | FIFO drain at depth, `build_policy` construction, partial (batched) drain |

### Leak detection

The `soak` bench reuses a single queue across all criterion iterations.
If tracking slots, inflight records, or batch entries leak, later
iterations slow down as data structures grow. Cross-check RSS with:

```sh
/usr/bin/time -v cargo bench -p send-queue --bench soak -- \
    --measurement-time 300 --sample-size 3
```

### NUMA notes

For NUMA-aware benchmarking (two-socket systems), pin the bench process
and verify memory locality with `numactl`:

```sh
numactl --cpunodebind=0 --membind=0 cargo bench -p send-queue --bench mpsc
```

All benches use [`NullSink`](benches/common.rs) (accepts all entries,
discards them) so measurements reflect only queue machinery, not
IO-Layer noise.

[`error`]: src/error.rs
[`config`]: src/config.rs
[`queue`]: src/queue/mod.rs
[`scheduler`]: src/scheduler/mod.rs
[`batch`]: src/batch/mod.rs
[`backpressure`]: src/backpressure/mod.rs
[`retry`]: src/retry/mod.rs
[`lifecycle`]: src/lifecycle/mod.rs
[`submit`]: src/submit/mod.rs
[`stats`]: src/stats/mod.rs
[`api`]: src/api/mod.rs
[`SubmissionSink`]: src/submit/mod.rs
