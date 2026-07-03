# buffer-pool

Production-grade memory pool for Vireon — a layered allocator modelled on
jemalloc / mimalloc / tcmalloc.

## Architecture

```text
BufferPool::get(size)
  -> classifier           size -> ClassId / Huge
  -> thread-local cache   fast path, per-class bucket
  -> global shard cache   batch refill, sharded by thread id
  -> arena / slab         fresh allocation, power-of-2 size classes
  -> huge allocator       sizes above the class table

  backpressure gate       rejects or parks when bytes_outstanding > cap
  maintenance worker      background shrink / tune / telemetry
```

Release is the mirror image, driven by an RAII `Buffer` guard so a slot can
never be forgotten.

## Quick start

```rust
use buffer_pool::BufferPool;

let pool = BufferPool::default();

let mut buf = pool.get(1024)?;
buf[..5].copy_from_slice(b"hello");

// into_bytes copies once into a refcounted Bytes.
let bytes = buf.into_bytes();
assert_eq!(&bytes[..5], b"hello");
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

## Layer map

| Layer | Responsibility |
|-------|---------------|
| 1-2 | Pool handle + fluent builder |
| 3-6 | Size-class table, arena, slab, huge allocator |
| 7-9 | Thread-local cache, global sharded cache, adaptive batch |
| 10-11 | Zeroing & recycle preparation |
| 12 | RAII buffer guard |
| 13-14 | Atomic counters, latency histogram |
| 15 | Telemetry exporters |
| 16-19 | Pressure monitor, shrinker, tuner, maintenance worker |
| 20-21 | Leak tracker, validator, per-op tracing |
| 22 | NUMA node detection & memory binding |
| 23 | Backpressure gate + parking waiter |
| 24-25 | Config, policy, error types |
