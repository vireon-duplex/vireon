# timer-wheel benchmarks

Production-grade benchmark suite for `runtime-layer/timer-wheel`. Mirrors the layout
and conventions of `runtime-layer/buffer-pool/benches/` — one `[[bench]]` binary per
concern, a shared `common.rs`, and a standalone soak binary for stability
metrics that Criterion cannot capture.

## Quick start

```sh
# Run every Criterion bench (takes several minutes; writes HTML to
# target/criterion/<group>/report/index.html).
cargo bench -p timer-wheel

# Quick iteration on a single bench file (10 samples, 2s measurement).
cargo bench -p timer-wheel --bench insert -- \
    --sample-size 10 --measurement-time 2 --warm-up-time 1

# Standalone soak binary — 60s, 4 producer threads, ~100k ops/s target.
cargo bench -p timer-wheel --bench stress

# 5-minute soak with 8 threads at 500k ops/s.
cargo bench -p timer-wheel --bench stress -- 300 8 500000
```

## Bench files

| Binary | Level | What it measures |
|--------|-------|------------------|
| `insert`   | Shard  | `start`-path throughput: single, bulk `{1k,10k,100k,1M}`, short-deadline (level 0), long-deadline (level 2). |
| `cancel`   | Shard  | `cancel`-path throughput: random-half and all-timers, parameterised by `BULK_SIZES`. |
| `tick`     | Shard  | Worker `tick()` cost: empty, full (n live, none expiring), with-cascade (level-0 wrap), with-expire (n timers fire in one bucket). |
| `expire`   | Facade | End-to-end `drain_expired()` throughput at `{100, 1k, 100k}` fired timers. |
| `mixed`    | Facade | Realistic patterns: 50/50 insert+cancel, 10k-timer burst, short-timeout stream, long-timeout stream. |
| `latency`  | Both   | Per-op tail latency (p50/p90/p99 via Criterion HTML report) for `start`, `cancel`, `drain_expired` (empty + full), and `Shard::tick()`. |
| `stress`   | Facade | **Standalone binary** — sustained soak with periodic telemetry (throughput, outstanding, p50/p99/max, `VmPeak`/`VmRSS`) and a final leak check. Not Criterion. |

`common.rs` is the shared helper module (PRNG, workload constants, wheel
builders, seeders, `/proc/self/status` reader). Every bench pulls it in via
`#[path = "common.rs"] mod common;`.

## Production-metrics → bench mapping

The bench suite was designed against the production-grade metric checklist.
Each requested metric maps to a specific bench:

| Metric | Where it lives |
|--------|----------------|
| Insert 1 timer              | `insert/single` |
| Insert 1 M timers           | `insert/bulk/1000000` |
| **Insert throughput**       | `Throughput::Elements(n)` on every `insert/*` bench (Criterion reports elem/s) |
| Cancel random               | `cancel/random/{n}` |
| Cancel all                  | `cancel/all/{n}` |
| **Cancel throughput**       | `Throughput::Elements(n)` on `cancel/*` |
| Tick empty                  | `tick/empty` |
| Tick full                   | `tick/full/{1,1k,100k}` |
| Tick with cascade           | `tick/with_cascade` |
| **Tick latency**            | `latency/tick_shard` (per-tick p50/p99) |
| Expire 100 / 1k / 100k      | `expire/{100,1000,100000}` and `expire/drain_only/{…}` |
| **Expire throughput**       | `Throughput::Elements(n)` on `expire/*` |
| Mixed insert + cancel       | `mixed/insert_cancel_50_50` |
| Burst                       | `mixed/burst` |
| Long-timeout stream         | `mixed/long_timeout` |
| Short-timeout stream        | `mixed/short_timeout` |
| Steady state                | `stress` (sustained soak; telemetry every 5 s) |
| **Worst-case latency**      | `stress` final report (`max`) + Criterion outlier view in HTML |
| **99th percentile**         | Criterion HTML report on `latency/*` + `stress` periodic `p99` |
| **Memory usage**            | `stress` reports `VmPeak`/`VmRSS` every 5 s; or `/usr/bin/time -v` (see below) |
| **Cache miss rate**         | `perf stat` (see below) |
| **Branch miss rate**        | `perf stat` (see below) |

## Hardware performance counters (Linux only)

Criterion cannot see cache misses or branch misses — those live in the CPU's
PMU. Use `perf stat` against a compiled bench binary. The binary path is
printed when you run `cargo bench --no-run`:

```sh
# Compile the bench without running it.
cargo bench -p timer-wheel --bench insert --no-run

# Run perf stat with the full event set.
perf stat -e instructions,cycles,cache-references,cache-misses,\
branches,branch-misses,L1-dcache-loads,L1-dcache-load-misses,\
LLC-loads,LLC-load-misses,context-switches,page-faults,cpu-migrations \
    target/release/deps/insert-<hash> --bench insert/bulk/1000000
```

For a single sub-bench, pass a filter:

```sh
perf stat -e cache-misses,LLC-load-misses \
    target/release/deps/insert-<hash> --bench insert/bulk/1000000
```

IPC (instructions per cycle) is the headline number for compute-bound code;
cache-miss rate (`cache-misses / cache-references`) and branch-miss rate
(`branch-misses / branches`) are the headlines for the wheel's linked-list
traversal.

### Flamegraph

```sh
cargo install cargo-flamegraph
cargo flamegraph -p timer-wheel --bench insert -- --bench insert/bulk/1000000
```

Open `flamegraph.svg` in a browser.

## Memory profiling

```sh
# Peak RSS via the kernel's own accounting.
/usr/bin/time -v target/release/deps/stress-<hash> 60 4 100000
# Look for "Maximum resident set size (kbytes)".

# Valgrind massif for the full allocation timeline.
valgrind --tool=massif target/release/deps/stress-<hash> 30 2 50000
ms_print massif.out.<pid>
```

For periodic in-process RSS during a soak, the `stress` binary already
reads `/proc/self/status` every 5 s and prints `VmPeak`/`VmRSS`.

## Quick iteration

To smoke-test a bench without waiting for Criterion's full statistical run:

```sh
cargo bench -p timer-wheel --bench <name> -- \
    --sample-size 10 --measurement-time 2 --warm-up-time 1
```

To compare two implementations:

```sh
cargo install critcmp
cargo bench -p timer-wheel -- --save baseline
# ... make changes ...
cargo bench -p timer-wheel -- --save changed
critcmp baseline changed
```

## Reading the output

Criterion prints three numbers per bench: `[low mean high]` at 95% confidence.
For throughput-annotated benches (`Throughput::Elements(n)`), it also prints
`[low thrpt high]` in elem/s. Open `target/criterion/<group>/report/index.html`
for the per-bench distribution (p50/p90/p99/max, density plot, regression
analysis).
