# io benchmarks

Production-grade benchmark suite for the `io` crate. Mirrors the layout
and conventions of `runtime-layer/timer-wheel/benches/` and `runtime-layer/buffer-pool/benches/`
— one `[[bench]]` binary per concern, a shared `common.rs`, and Criterion's
`iter_batched` for setup-heavy paths.

## Quick start

```sh
# Run every Criterion bench (writes HTML to
# target/criterion/<group>/report/index.html).
cargo bench -p io

# Quick iteration on a single bench file (20 samples, 0.5s measurement).
cargo bench -p io --bench udp_roundtrip -- \
    --sample-size 20 --measurement-time 0.5 --warm-up-time 0.1
```

## Bench files

| Binary | Level | What it measures |
|--------|-------|------------------|
| `ring_init`     | Kernel  | `IoUring` ring setup cost (entries=4…1024), SQPOLL path, `register_buffers` (16/64/256 iovecs), `register_files_sparse` (64/256/1024 fds). |
| `buffer_pool`   | Kernel  | `BufferPool::init_buffer_ring` (count=16…1024, size=64…4096), `refill_buffer` hot path, `get_buffer_address` lookup, `with_defaults` convenience path. |
| `timer_ops`     | Kernel  | `TimerWheel::add_timeout` (1ms…10s), `add_periodic_tick`, `cancel_timeout` SQE submit, `retire` HashMap remove, `is_active` HashMap lookup. |
| `connection`    | Kernel  | `Socket::bind` (ephemeral, REUSEPORT), `Receiver::arm`, `Receiver::parse` (64B…2048B synthetic `io_uring_recvmsg_out`), `CompletionDecoder` decode, `Sender::send_zc` SQE submit. |
| `metrics`       | Compute | `MetricsCollector` atomic-counter overhead: `record_cqe_completion`, `record_bytes_{rx,tx}`, `record_*` backpressure paths, `snapshot` (fresh + after 10k ops). |
| `udp_roundtrip` | E2E     | End-to-end UDP latency: `send_to` → `submit_and_wait(1)` → drain CQE → `Receiver::parse` → refill, at 73B…2057B packet sizes. |

`common.rs` is the shared helper module (PRNG, packet-size constants,
ring/socket/pool fixture builders). Every bench pulls it in via
`#[path = "common.rs"] mod common;`.

## Headline numbers

Measured on the development host with `--sample-size 20
--measurement-time 0.5`:

| Bench | Typical |
|-------|---------|
| `ring_init/init_ring/entries=256`        | ~60 µs |
| `ring_init/register_sparse_files/1024`   | ~5 µs  |
| `buffer_pool/init_buffer_ring/count=256` | ~5.7 µs |
| `buffer_pool/refill_buffer`              | ~1.1 ns |
| `buffer_pool/get_buffer_address/random`  | ~1.8 ns |
| `timer_ops/retire/hit`                   | ~66 ns |
| `timer_ops/is_active/active`             | ~8 ns  |
| `connection/completion_decode/recv_success` | ~232 ps |
| `connection/send_zc/2048B`              | ~3.7 µs |
| `metrics/record_cqe_completion/rx_success` | ~4 ns |
| `metrics/snapshot/after_10k_ops`         | ~930 ps |
| `udp_roundtrip/1033B`                    | ~10 µs |

Numbers are host-dependent and intended for regression tracking, not
absolute comparison.

## Hardware performance counters (Linux only)

Criterion cannot see cache misses or branch misses — those live in the
CPU's PMU. Use `perf stat` against a compiled bench binary:

```sh
# Compile the bench without running it.
cargo bench -p io --bench buffer_pool --no-run

# Run perf stat with the full event set.
perf stat -e instructions,cycles,cache-references,cache-misses,\
branches,branch-misses,L1-dcache-loads,L1-dcache-load-misses,\
LLC-loads,LLC-load-misses,context-switches,page-faults,cpu-migrations \
    target/release/deps/buffer_pool-<hash> --bench refill_buffer
```

## Flamegraph

```sh
cargo install cargo-flamegraph
cargo flamegraph -p io --bench buffer_pool -- --bench refill_buffer
```

Open `flamegraph.svg` in a browser.

## Quick iteration

```sh
# Smoke-test a single bench without Criterion's full statistical run.
cargo bench -p io --bench <name> -- \
    --sample-size 10 --measurement-time 2 --warm-up-time 1
```

To compare two implementations:

```sh
cargo install critcmp
cargo bench -p io -- --save baseline
# ... make changes ...
cargo bench -p io -- --save changed
critcmp baseline changed
```

## Reading the output

Criterion prints three numbers per bench: `[low mean high]` at 95 %
confidence. Open
`target/criterion/<group>/report/index.html` for the per-bench
distribution (p50/p90/p99/max, density plot, regression analysis).
