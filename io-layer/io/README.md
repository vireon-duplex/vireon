# io

Production-grade **io_uring IO layer** for Vireon — the kernel boundary
for the QUIC engine. A thread-per-core reactor built on Linux `io_uring`,
targeting 1 M+ concurrent connections with sub-ms tail latency.

## Architecture

```text
+-------------------------------------------------------------------+
|                       REACTOR (CPU Core N)                        |
|  ┌─────────────────────────────────────────────────────────────┐ |
|  │                  Event Loop (run())                         │ |
|  │   submit SQEs ─── io_uring_submit_and_wait ─── reap CQEs    │ |
|  └─────────────────────────────────────────────────────────────┘ |
|       │                │                 │              │        |
|       ▼                ▼                 ▼              ▼        |
|  IoUringCtx        BufferPool        TimerWheel     Metrics      |
|  (ring setup,      (PBUF_RING,       (IORING_OP_    (atomic      |
|   fixed bufs,       refill)           TIMEOUT +      counters)   |
|   sparse files)                       CANCEL)                    |
|       │                                                          |
|       ▼                                                          |
|  Socket + Receiver + Sender (UDP SO_REUSEPORT, multishot, send_zc)|
+-------------------------------------------------------------------+
```

## Quick start

```no_run
use io::Reactor;

let features = kernel_nic::KernelFeatures::detect()?;
let busy_poll = kernel_nic::BusyPollPolicy::default();
let mut reactor = Reactor::init(0, 8080, 0, false, features, busy_poll)?;
reactor.run()?;
# Ok::<(), io::IoError>(())
```

## Layer map

| Layer | Module | Responsibility |
|-------|--------|----------------|
| 1 | [`common`]        | Constants, protocol opcodes |
| 2 | [`error`]         | `IoError` thiserror enum |
| 3 | [`features`]      | `IoFeatures` bitmask + kernel version detection |
| 4 | [`context`]       | `IoUringCtx` ring setup + `IoRegistry` |
| 5 | [`memory`]        | `MemoryRegion` + `BufferPool` (`PBUF_RING`) |
| 6 | [`ops`]           | `OpKind` user_data encoding + `Receiver`/`Sender` + SQE builders |
| 7 | [`socket`]        | UDP socket bind + fd ownership |
| 8 | [`completion`]    | `Completion` enum + `CompletionDecoder` |
| 9 | [`registry`]      | Explicit resource unregister + `Drop` cleanup |
| 10 | [`timer_wheel`]  | Kernel-side io_uring timeouts (`TIMEOUT` + `CANCEL`) |
| 11 | [`metrics`]      | Atomic counters + snapshot |
| 12 | [`reactor`]      | Thread-per-core event loop |

## Relationship to `runtime-layer/`

The `memory::BufferPool` and `timer_wheel` modules here are **kernel-interface
adapters**, not duplicates of `runtime-layer/buffer-pool` and `runtime-layer/timer-wheel`:

| `runtime-layer/` crate | `io` module | Difference |
|---------------|-------------|------------|
| `buffer-pool`  | `io::memory::BufferPool`  | `runtime-layer/` is a userspace object pool; `io` registers a kernel `PBUF_RING` so the kernel posts recv'd datagrams directly. |
| `timer-wheel`  | `io::timer_wheel`  | `runtime-layer/` is a userspace hierarchical wheel with worker threads; `io::timer_wheel` submits `IORING_OP_TIMEOUT` / `TIMEOUT_REMOVE` SQEs and tracks active kernel timers. |

Both `runtime-layer/` crates will be used **on top of** the reactor for
application-level pooling and userspace deadlines that don't belong in
the kernel ring.

## Design choices

- **Typed `io-uring` v0.7 wrapper.** Avoids hand-rolling SQE/CQE
  structs; provides `RecvMsgMulti`, `SendZc`, `Timeout`,
  `BufRingEntry` helpers.
- **Thread-per-core** — no shared state between reactors; each owns a
  private ring, buffer pool, and UDP socket bound with `SO_REUSEPORT`.
- **`IORING_SETUP_SINGLE_ISSUER | DEFER_TASKRUN | COOP_TASKRUN`** —
  halves context-switch cost vs. the legacy task-work path.
- **Provided buffer rings** (`IORING_REGISTER_PBUF_RING`) for zero-copy
  RX — the kernel posts recv'd datagrams directly into our pool, no
  per-SQE buffer pointer.
- **`SEND_ZC`** for zero-copy TX with a two-CQE lifecycle (send status
  + buffer-reclaim notification).
- **High-bit `user_data` tagging** — bits `[63:60]` encode `OpKind`,
  bits `[59:0]` encode caller ID. The `CompletionDecoder` classifies
  CQEs with zero-overhead shift+match, no HashMap lookup.
- **Explicit resource lifetime** — `IoRegistry` and `BufferPool` both
  unregister kernel resources in `Drop` before freeing memory.

## Kernel requirements

- **Linux ≥ 6.0** for `recvmsg_multishot`, `send_zc`, and
  `IORING_REGISTER_PBUF_RING`.
- **`IORING_SETUP_DEFER_TASKRUN`** requires the kernel to support
  deferred task work (Linux ≥ 5.19).
- **`SQPOLL`** is optional and requires `CAP_SYS_NICE` (or
  `RLIMIT_NOFILE` relaxation). The bench suite silently skips the SQPOLL
  path if the kernel denies it.

Check support at runtime:

```sh
# Kernel version.
uname -r

# io_uring feature matrix.
grep -i io_uring /proc/kallsyms | head
```

## Benchmarks

The crate ships a Criterion suite covering ring init, buffer-pool ops,
timer ops, connection setup/parse, metrics overhead, and end-to-end UDP
round-trip latency. See [`benches/README.md`](benches/README.md) for the
full guide. Headlines:

```sh
cargo bench -p io                                    # full Criterion suite
cargo bench -p io --bench udp_roundtrip              # single bench
cargo bench -p io --bench udp_roundtrip -- \
    --sample-size 20 --measurement-time 0.5          # quick smoke
```

| Binary | Concern |
|--------|---------|
| `ring_init`     | Ring setup, fixed-buffer/file registration |
| `buffer_pool`   | `PBUF_RING` init, refill, address lookup |
| `timer_ops`     | Timeout SQE submit, retire/is_active HashMap ops |
| `connection`    | Socket bind, recv arm, buffer parse, `send_zc`, completion decode |
| `metrics`       | Atomic-counter overhead + snapshot |
| `udp_roundtrip` | End-to-end UDP latency (send → recv → parse) |

[`common`]: src/common.rs
[`error`]: src/error.rs
[`features`]: src/features.rs
[`context`]: src/context.rs
[`memory`]: src/memory/mod.rs
[`ops`]: src/ops/mod.rs
[`socket`]: src/socket.rs
[`completion`]: src/completion.rs
[`registry`]: src/registry.rs
[`timer_wheel`]: src/timer_wheel.rs
[`metrics`]: src/metrics.rs
[`reactor`]: src/reactor.rs
