# timer-wheel

Production-grade, lock-free, thread-per-core **hierarchical timing wheel** for
`Vireon` — the timing substrate for QUIC ACK/PTO timers, connection idle
timeouts, and retransmission deadlines.

## Design

- **Thread-per-core sharding (share-nothing).** Each worker thread owns one
  `Shard` (wheel + slab + command queue + expired-out queue). No global lock.
- **Lock-free ingestion.** Application threads hash a `task_id` to a `ShardId`
  and push `Start`/`Cancel` commands through a bounded Vyukov MPSC queue.
- **Hierarchical wheel.** Power-of-two levels with bitwise slot lookup.
  Defaults: 6 bits/level (64 slots), 4 levels → 256 buckets, coverage `2^24`
  ticks (≈4.66 h at a 1 ms tick).
- **Slab allocation.** `TimerEntry` is exactly 24 bytes; the slab hands out
  `NonZeroU32` indices from an O(1) free-list. No per-timer heap allocation.

## Quick start

```no_run
use std::time::Duration;
use timer_wheel::{TimerWheel, TimerWheelBuilder};

let wheel = TimerWheelBuilder::new().shards(4).spawn()?;

// Schedule a timer; the worker fires it into the drain queue.
let handle = wheel.start(42, Duration::from_millis(50))?;

// ... later, from the application thread:
let fired = wheel.drain_expired();
assert!(fired.contains(&42));

wheel.shutdown()?;
# Ok::<(), timer_wheel::TimerError>(())
```

## Layer map

| Module | Responsibility |
|--------|----------------|
| `error` | `TimerError` enum |
| `config` | `TimerWheelConfig`, `TimerWheelBuilder` |
| `core` | Slab allocator, 24-byte `TimerEntry`, intrusive buckets, hierarchical wheel |
| `concurrent` | Bounded lock-free MPSC queue, shard router |
| `scheduler` | Monotonic clock, per-core `Shard`, worker event loop |
| `api` | `TimerWheel` facade, `TimerHandler` trait |

## Benchmarks

The crate ships a production-grade bench suite covering throughput, tail
latency, sustained soak, memory footprint, and hardware-counter workflows.
See [`benches/README.md`](benches/README.md) for the full guide. Headlines:

```sh
cargo bench -p timer-wheel                          # full Criterion suite
cargo bench -p timer-wheel --bench stress           # standalone soak binary
cargo bench -p timer-wheel --bench stress -- 300 8 500000  # 5-min, 8-thread soak
```

| Binary | Concern |
|--------|---------|
| `insert`   | `start` throughput (single, bulk up to 1 M, short vs long deadline) |
| `cancel`   | `cancel` throughput (random-half, all) |
| `tick`     | Worker `tick()` cost (empty, full, cascade, with-expire) |
| `expire`   | `drain_expired()` throughput (100 / 1k / 100k) |
| `mixed`    | Realistic load shapes (insert+cancel, burst, short/long streams) |
| `latency`  | Per-op tail latency (p50/p90/p99 via Criterion HTML report) |
| `stress`   | Standalone soak — p99/max latency drift, `VmPeak`/`VmRSS`, leak check |


