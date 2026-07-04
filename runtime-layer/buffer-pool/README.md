# buffer-pool

Production-grade memory pool for Vireon — a layered allocator modelled on
jemalloc / mimalloc / tcmalloc.

The pool owns raw aligned slots from the OS, classifies requests into 15
power-of-2 size classes (64 B – 1 MiB), and serves them through a three-tier
cache hierarchy (thread-local → global sharded → arena/slab). An RAII `Buffer`
guard returns the slot on drop, so a buffer can never be forgotten.

## Architecture

```text
BufferPool::get(size)
  -> classifier           size -> ClassId / Huge
  -> thread-local cache   fast path, per-class bucket
  -> global shard cache   batch refill, sharded by thread id
  -> arena / slab         fresh allocation, power-of-2 size classes
  -> huge allocator       sizes above the class table (>= 1 MiB)

  backpressure gate       rejects or parks when bytes_outstanding > cap
  maintenance worker      background shrink / tune / telemetry
```

Release is the mirror image, driven by an RAII `Buffer` guard so a slot can
never be forgotten.

## Quick start

### Zero-config

```rust
use buffer_pool::BufferPool;

let pool = BufferPool::default();

// Acquire a buffer — DerefMut to &mut [u8].
let mut buf = pool.get(1024)?;
buf[..5].copy_from_slice(b"hello");

// into_bytes copies once into a refcounted Bytes.
let bytes = buf.into_bytes();
assert_eq!(&bytes[..5], b"hello");
```

### Tuned construction

```rust
use buffer_pool::{BufferPool, ZeroingPolicy};

let pool = BufferPool::builder()
    .local_cache(128)            // per-class thread-local capacity
    .global_cache(2048)          // per-shard global capacity
    .shards(16)                  // global shards per class
    .batch(32)                   // refill/drain batch size
    .huge_threshold(1 << 20)     // 1 MiB
    .max_memory(512 << 20)       // 512 MiB byte ceiling
    .zeroing(ZeroingPolicy::Clear)
    .build()?;
```

### Non-blocking acquire with backpressure

```rust
// try_get returns Err(Rejected) when bytes_outstanding + size > capacity.
match pool.try_get(4096) {
    Ok(buf) => { /* use buf */ }
    Err(buffer_pool::BufferPoolError::Rejected { size }) => {
        // apply backpressure to the caller
    }
    _ => unreachable!(),
}
```

### Statistics

```rust
let stats = pool.stats();
println!(
    "hit rate: {:.1}%  reuse rate: {:.1}%  outstanding: {}  p99: {} ns",
    stats.hit_rate() * 100.0,
    stats.reuse_rate() * 100.0,
    stats.counters.outstanding,
    stats.latency.percentiles.p99_ns,
);
```

## Features

All off by default; a release build depends only on `bytes`, `thiserror`,
and `tracing`.

| Feature | Description |
|---------|-------------|
| `prometheus` | Registers counters/gauges on a Prometheus registry. |
| `otel` | Emits metrics via the OpenTelemetry SDK. |
| `numa` | NUMA-aware allocation via `getcpu` / `mbind` (Linux). |
| `leak` | Captures allocation-site backtraces for the leak detector. |

## Configuration

| Field | Default | Description |
|-------|---------|-------------|
| `local_cache` | 64 | Per-class thread-local cache capacity (buffer count). |
| `global_cache` | 1024 | Per-shard global cache capacity (buffer count). |
| `shards` | 8 | Number of global shards per size class. |
| `batch` | 16 | Refill/drain batch size for thread-local ↔ global transfers. |
| `huge_threshold` | 1 MiB | Sizes above this bypass caches and go to the huge allocator. |
| `max_memory` | 256 MiB | Hard byte ceiling before backpressure engages. |
| `zeroing` | `Clear` | How buffers are zeroed on release (`None`/`Clear`/`Secure`/`Lazy`). |
| `shrink` | `OnPressure` | When the background shrinker runs (`Never`/`OnPressure`/`Aggressive`). |
| `maintenance_interval_ms` | 1000 | Background maintenance tick interval. |

## Size classes

15 power-of-2 classes from 64 B to 1 MiB. Requests are classified to the
smallest class that fits; sizes above 1 MiB go through the huge allocator.

| Class | Slot size | Alignment |
|-------|-----------|-----------|
| 0 | 64 B | 64 B |
| 1 | 128 B | 64 B |
| 2 | 256 B | 64 B |
| 3 | 512 B | 64 B |
| 4 | 1 KiB | 64 B |
| 5 | 2 KiB | 64 B |
| 6 | 4 KiB | 4 KiB |
| 7 | 8 KiB | 4 KiB |
| 8 | 16 KiB | 4 KiB |
| 9 | 32 KiB | 4 KiB |
| 10 | 64 KiB | 4 KiB |
| 11 | 128 KiB | 4 KiB |
| 12 | 256 KiB | 4 KiB |
| 13 | 512 KiB | 4 KiB |
| 14 | 1 MiB | 4 KiB |

## Benchmarks

Criterion benchmarks measure the two hot operations: **acquire** (`get`) and
**release** (`drop`). Payload sizes are adapted from the FluxIO workload
generator — 11 realistic sizes spanning 64 B (Heartbeat) to 320 KiB
(VideoFrame).

### Running

```bash
# All benchmarks
cargo bench -p buffer-pool --bench pool_bench

# Single group
cargo bench -p buffer-pool --bench pool_bench -- get/per_size

# Single case
cargo bench -p buffer-pool --bench pool_bench -- get/per_size/heartbeat/64
```

### Groups

| Group | What it measures |
|-------|-----------------|
| `get/per_size` | `get(size)` + immediate `drop`, per realistic size (11 sizes) |
| `get/warm` | Pure thread-local cache hit (fast path) |
| `get/cold` | Arena allocation (fresh pool each batch) |
| `drop/per_size` | Release path only — zeroing + cache push |
| `roundtrip/workload` | All 11 sizes back-to-back (FluxIO batch pattern) |
| `concurrent/roundtrip` | N-thread get+drop stress (1 / 2 / 4 / 8 threads) |

### Sample results

Measured on a single run (your numbers will vary):

```
get/per_size/heartbeat/64         376 ns        162 MiB/s
get/per_size/tool_call/4096       370 ns       10.3 GiB/s
get/per_size/audio_frame/80000    1.60 µs      46.5 GiB/s
get/per_size/video_frame/320000   5.23 µs      57.0 GiB/s

get/warm/64B                      370 ns
get/warm/4KiB                     364 ns
get/warm/128KiB                   2.35 µs

drop/per_size/heartbeat/64        315 ns

roundtrip/workload/fluxio_batch   21.3 µs      45.0 GiB/s

concurrent/roundtrip/1            385 µs
concurrent/roundtrip/8            764 µs
```

## Layer map

| Layer | Responsibility |
|-------|---------------|
| 1-2 | Pool handle + fluent builder |
| 3-6 | Size-class table, arena, slab, huge allocator |
| 7-9 | Thread-local cache, global sharded cache, adaptive batch |
| 10-11 | Zeroing & recycle preparation |
| 12 | RAII buffer guard |
| 13-14 | Atomic counters, latency histogram |
| 15 | Telemetry exporters (Prometheus / OTel) |
| 16-19 | Pressure monitor, shrinker, tuner, maintenance worker |
| 20-21 | Leak tracker, validator, per-op tracing |
| 22 | NUMA node detection & memory binding |
| 23 | Backpressure gate + parking waiter |
| 24-25 | Config, policy, error types |
