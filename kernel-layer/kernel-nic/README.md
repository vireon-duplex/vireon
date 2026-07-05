# kernel-nic

Production-grade **Kernel & NIC Layer** for Vireon — the lowest
software layer above the Linux kernel and below the IO Layer. It is
**generic networking infrastructure**: UDP sockets, `SO_REUSEPORT`
groups, NIC discovery, RSS / XPS / IRQ affinity, busy poll, GRO/GSO,
NUMA awareness, and kernel capability detection.

This layer **does not know** about QUIC, RPC, HTTP, streams,
sessions, frames, or any application protocol. It speaks `RawFd`,
`sockaddr_in`, and `ethtool` channels — nothing higher.

See [`DESIGN.md`](DESIGN.md) for the full architecture specification.

## Architecture

```text
+-------------------------------------------------------------------+
|                      KERNEL & NIC LAYER                            |
|                                                                   |
|  ── Discovery (read-only, frozen at startup) ─────────────────    |
|  CpuTopology  ─► KernelFeatures  ─► NicDiscovery  ─► IrqMap        |
|  (CPU/NUMA,                        (/sys/class/net, (/proc/irq,    |
|   cache ids)                        driver, queues)   MSI-X map)   |
|                                                                   |
|  ── Slow-path configuration (writes, gated by CAP_NET_ADMIN) ──   |
|  IrqPolicy   ─► RSS / XPS indirection  ─► SysctlProfile            |
|  (PerCpu/                                              (rmem,     |
|   PerLlc/                                              wmem,      |
|   Balanced)                                            busy_poll) |
|                                                                   |
|  ── Socket lifecycle ──────────────────────────────────────────   |
|  SocketBuilder ─► ConfiguredSocket ─► PerCoreSocketRegistry       |
|                   (owns RawFd)       (!Sync, per-reactor)          |
|                                                                   |
|  ── REUSEPORT groups ─────────────────────────────────────────   |
|  SocketGroup { generation, members[] } ─► rolling upgrade         |
+-------------------------------------------------------------------+
```

## Quick start

```no_run
use kernel_nic::{NicManagerBuilder, Preset, SocketSpec};

let mut manager = NicManagerBuilder::new()
    .preset(Preset::BareMetal)
    .build()?;

// Build a single bound socket for the IO Layer.
let spec = SocketSpec::for_reuseport(8080, 0, 1);
let sock = manager.build_socket(spec)?;
let fd = sock.into_raw_fd();  // IO Layer takes ownership
unsafe { libc::close(fd); }   // (demo cleanup)
# Ok::<(), kernel_nic::KernelNicError>(())
```

## Layer map

| Layer | Module | Responsibility |
|-------|--------|----------------|
| 1 | [`error`]     | `KernelNicError` thiserror enum + `is_fatal` |
| 2 | [`config`]    | `NicConfig`, `Preset`, defaults, validation |
| 3 | [`features`]  | `KernelFeatures` bitmask + probe-based `detect()` |
| 4 | [`topology`]  | `CpuTopology`, `CpuInfo`, `NumaNode`, cache ids |
| 5 | [`cpuset`]    | Fixed-size `CpuSet` (1024 bits) |
| 6 | [`nic`]       | `NicInfo`, `NicDiscovery`, offloads, queues |
| 7 | [`irq`]       | `IrqMap`, `IrqInfo`, IRQ ↔ NIC mapping |
| 8 | [`affinity`]  | IRQ affinity application + `IrqPolicy` |
| 9 | [`rss`]       | RSS indirection table + hash key + `RssHashTypes` |
| 10 | [`xps`]      | XPS per-TX-queue CPU masks |
| 11 | [`busy_poll`] | `BusyPollPolicy` + `BusyPollMode` |
| 12 | [`sysctl`]   | Sysctl reader/writer + `SysctlProfile` presets |
| 13 | [`socket`]   | `SocketBuilder`, `SocketGroup`, `PerCoreSocketRegistry` |
| 14 | [`manager`]  | `NicManager` facade + `NicManagerBuilder` |

## Relationship to other crates

| Crate | Role |
|-------|------|
| `io-layer/io`         | Borrows configured `RawFd`s from this layer via `SocketBuilder`; never performs socket policy itself. |
| `runtime-layer/*`     | Receives the `NicManager` at reactor startup; no direct kernel-nic calls from runtime hot paths. |

The Kernel & NIC Layer **never** depends on anything above it in the
stack. It has no async runtime, no io_uring calls, and no protocol
knowledge.

## Design choices

- **Probe-based feature detection.** `KernelFeatures::detect()` attempts
  `setsockopt(2)` on a transient socket for each option rather than
  parsing `uname(2)`. A probe is authoritative: if `SO_BUSY_POLL` is
  accepted, the feature is available regardless of kernel version.
- **Read-mostly metadata.** `CpuTopology`, `NicInfo`, `IrqMap`,
  `KernelFeatures` are frozen at startup and immutable thereafter —
  freely shareable by `&`. No locks on the fast path.
- **Per-core registry, `!Sync` by design.** `PerCoreSocketRegistry`
  is owned by exactly one reactor thread. Mutation requires
  `cpu_id == current_cpu()` (debug-asserted).
- **`SocketBuilder` is the single seam.** All `socket(2)` /
  `bind(2)` / `setsockopt(2)` policy lives there. The IO Layer calls
  `manager.build_socket(spec)` → `ConfiguredSocket::into_raw_fd()`.
- **`SO_REUSEPORT` groups with generation tracking.** Rolling
  upgrades bump generation; old members transition `Bound → Armed →
  Draining` while new members arm — non-dropping rotation.
- **1024-bit fixed `CpuSet`.** Bounds the CPU count to keep
  per-core structures off the heap allocator. Hex format renders
  directly to `/proc/irq/<n>/smp_affinity`.
- **Workspace lints honored.** `unwrap_used` / `expect_used` /
  `panic` / `print_stdout` / `print_stderr` all denied. Logging via
  `tracing`. Errors via `thiserror`. Abort (not panic) on
  unrecoverable init failure.
- **`Preset` profiles.** Cloud / BareMetal / HundredGbe /
  TwentyFiveGbe / LowLatency / MaxThroughput — each populates a
  `NicConfig` with sensible values for the deployment class.

## Benchmarks

The crate ships a 5-binary Criterion suite covering discovery latency,
feature detection, socket creation, and CpuSet operations.

```sh
cargo bench -p kernel-nic                            # full Criterion suite
cargo bench -p kernel-nic --bench socket_create      # single bench
cargo bench -p kernel-nic --bench cpuset -- \
    --sample-size 10 --measurement-time 1            # quick smoke
```

### Bench inventory

| Binary | Concern |
|--------|---------|
| `topology`         | `CpuTopology::discover` latency + CPU lookup overhead. |
| `features_detect`  | `KernelFeatures::detect` (probe set) + bitmask `contains`. |
| `nic_scan`         | `NicDiscovery::scan` latency (sysfs walk). |
| `socket_create`    | Single UDP socket create+bind+configure (full options). |
| `cpuset`           | `CpuSet` set/contains/first at 16–1024-bit fan-out. |

### Smoke-test numbers (single run, your Mileage Will Vary)

| Bench | Result |
|-------|--------|
| `cpuset_set/1024`         | ~1.0 µs (~988 Melem/s) |
| `cpuset_contains/1024`    | ~328 ns (~3.1 Gelem/s) |
| `cpuset_first/set_with_6_bits` | ~362 ps |
| `socket_create/single`    | sub-µs (criterion-reported) |

[`error`]: src/error.rs
[`config`]: src/config.rs
[`features`]: src/features.rs
[`topology`]: src/topology.rs
[`cpuset`]: src/cpuset.rs
[`nic`]: src/nic.rs
[`irq`]: src/irq.rs
[`affinity`]: src/affinity.rs
[`rss`]: src/rss.rs
[`xps`]: src/xps.rs
[`busy_poll`]: src/busy_poll.rs
[`sysctl`]: src/sysctl.rs
[`socket`]: src/socket.rs
[`manager`]: src/manager.rs
