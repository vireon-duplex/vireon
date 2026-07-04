# Buffer Pool Benchmark Suite

A production-grade benchmark suite modelled on how mimalloc, jemalloc,
tcmalloc, Tokio, and Bytes evaluate allocator quality.

## Directory layout

```text
benches/
  common.rs           Shared workload sizes, xorshift PRNG, helpers
  pool_bench.rs       Basic get / drop / concurrent (baseline)
  alloc_paths.rs      Cat 1-2: path isolation (local / global / fresh / huge)
  batch.rs            Cat 3-4: refill & drain batch sizing
  size_class.rs       Cat 5: all 15 power-of-2 classes
  latency.rs          Cat 6-7: warm & cold tail latency percentiles
  mixed_workload.rs   Cat 8: random size distributions
  scalability.rs      Cat 9-10: multi-thread scaling & contention
  pressure.rs         Cat 12-13: memory pressure & backpressure policies
  zeroing.rs          Cat 14: None / Clear / Secure / Lazy comparison
  huge.rs             Cat 15: 2-16 MiB huge allocation paths
  workload.rs         Cat 16-19: QUIC / streaming / event-bus / codec
  stress.rs           Cat 20: long-running stability binary
  numa.rs             Cat 11: NUMA detection / bind / migrate (feature-gated)
  README.md           This file (Cat 21-23: perf, bandwidth, regression)
```

## Running benchmarks

```bash
# All benchmarks (takes ~30-60 min for full suite)
cargo bench -p buffer-pool

# Single benchmark file
cargo bench -p buffer-pool --bench alloc_paths

# Single group within a file
cargo bench -p buffer-pool --bench alloc_paths -- acquire/local_hit

# Quick run (fewer samples for faster iteration)
cargo bench -p buffer-pool --bench latency -- --sample-size 10 --measurement-time 2 --warm-up-time 1

# Stress test (standalone binary, not Criterion)
cargo bench -p buffer-pool --bench stress -- 60 4    # 60s, 4 threads

# NUMA benchmarks (requires numa feature)
cargo bench -p buffer-pool --features numa --bench numa
```

---

## Category 21 — Hardware performance counters

Use `perf stat` to collect CPU-level metrics alongside any benchmark.

### Running with perf

```bash
# Build release benches first
cargo bench -p buffer-pool --no-run

# Run a specific bench under perf stat
perf stat -e instructions,cycles,cache-references,cache-misses,\
branches,branch-misses,L1-dcache-loads,L1-dcache-load-misses,\
LLC-loads,LLC-load-misses,context-switches,page-faults,cpu-migrations \
target/release/deps/alloc_paths-<hash> --bench acquire/local_hit

# Record for flamegraph
perf record -g target/release/deps/alloc_paths-<hash> --bench acquire/local_hit
perf script | stackcollapse-perf.pl | flamegraph.pl > flamegraph.svg
```

### Key counters and what they tell you

| Counter | What it reveals |
|---------|-----------------|
| `instructions`, `cycles`, IPC | Raw work done per operation. IPC < 1 suggests memory stalls. |
| `cache-references`, `cache-misses` | L1/L2 efficiency. High miss rate → poor locality. |
| `LLC-loads`, `LLC-load-misses` | Last-level cache pressure. High → working set exceeds L3. |
| `branches`, `branch-misses` | Branch prediction quality. High miss rate → unpredictable control flow. |
| `context-switches` | Thread scheduling overhead. Non-zero → contention or parking. |
| `page-faults` | OS-level memory commits. High in cold paths, should be ~0 in warm paths. |
| `cpu-migrations` | Thread moved between cores. Non-zero → scheduler pressure. |

### Flamegraph

```bash
cargo install flamegraph
cargo flamegraph -p buffer-pool --bench alloc_paths -- --acquire/local_hit
```

---

## Category 22 — Memory bandwidth

Throughput in GiB/s is already reported by benchmarks that set
`Throughput::Bytes(size)`. To isolate raw memory bandwidth:

### Pool bandwidth vs theoretical DDR bandwidth

```bash
# Measure pool-acquired write bandwidth
cargo bench -p buffer-pool --bench huge -- huge/write

# Compare against raw memset (theoretical ceiling)
perf bench memset -m 1G
```

| Metric | How to get it |
|--------|---------------|
| **Pool GiB/s** | `cargo bench --bench huge -- huge/write` |
| **Pool write + drop** | `cargo bench --bench size_class -- class/get_write_drop` |
| **Raw memset ceiling** | `perf bench memset` or `sysbench memory run` |
| **DDR theoretical** | `dmidecode -t memory` → transfer rate × channels |

### Cache efficiency

```bash
# Cache-line-stride read pattern
cargo bench -p buffer-pool --bench numa --features numa -- numa/cross_node_read

# Under perf to see actual cache behaviour
perf stat -e cache-references,cache-misses,LLC-load-misses \
  cargo bench -p buffer-pool --bench size_class -- class/get_write_drop
```

---

## Category 23 — Regression detection

Criterion has built-in baseline comparison. Use it to detect performance
regressions between commits.

### Workflow

```bash
# 1. Save a baseline on the "good" commit
git checkout main
cargo bench -p buffer-bench -- --save-baseline main

# 2. Switch to your feature branch
git checkout feature/my-change

# 3. Run benches comparing against the saved baseline
cargo bench -p buffer-pool -- --baseline main

# Criterion reports % change and p-value for each benchmark.
# Look for: "regressed" (red) or "improved" (green)
```

### CI integration

```yaml
# .github/workflows/bench.yml
- name: Run benchmarks
  run: |
    cargo bench -p buffer-pool --bench alloc_paths -- --save-baseline ${{ github.base_ref }}

- name: Compare with PR
  run: |
    cargo bench -p buffer-pool --bench alloc_paths -- --baseline ${{ github.base_ref }}
```

### Regression thresholds

Criterion reports regressions with confidence intervals. Common thresholds:

| Change | Severity |
|--------|----------|
| > 10 % regression | **Critical** — investigate immediately |
| 5–10 % regression | **Warning** — review in PR |
| < 5 % | Noise — usually not actionable |

### Programmatic comparison

```bash
# JSON output for CI dashboards
cargo bench -p buffer-pool --bench alloc_paths -- --message-format=json > results.json

# Or use critcmp for cross-run comparison
cargo install critcmp
critcmp main feature/my-change
```

---

## Interpreting results

### Expected performance targets

| Benchmark | Target |
|-----------|--------|
| `acquire/local_hit` | < 500 ns |
| `release/local_push` | < 500 ns |
| `class/get_drop/64B` | < 500 ns, > 100 MiB/s |
| `class/get_drop/4KiB` | < 500 ns, > 8 GiB/s |
| `warm/sustained/64B` | p99 < 1000 ns |
| `scaling/roundtrip/8` | > 6× single-thread throughput |
| `contention/single_shard/8` | > 2× single-thread throughput |

These are ballpark figures on modern x86-64 hardware. Your numbers will vary
based on CPU, memory speed, and kernel version.

### Key questions this suite answers

1. **Where is time spent?** → `alloc_paths` (path breakdown)
2. **Does it scale?** → `scalability` (thread scaling curve)
3. **Is memory reused?** → `mixed_workload` (reuse rate from stats)
4. **What's the tail latency?** → `latency` (p50/p90/p99)
5. **Is it production-ready?** → `stress` (24h soak: no leaks, stable p99)
