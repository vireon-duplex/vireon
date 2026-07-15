<div align="center">
  <h1>Vireon</h1>

  <p>
    <strong>A high-performance realtime runtime built on QUIC and io_uring for multi-stream, full-duplex communication at massive scale.</strong>
  </p>

  <p>
    <a href="https://github.com/vireon-duplex/Vireon/actions"><img src="https://img.shields.io/github/actions/workflow/status/vireon-duplex/Vireon/ci.yml?style=flat-square" alt="Build Status" /></a>
    <a href="https://github.com/vireon-duplex/Vireon/releases"><img src="https://img.shields.io/github/v/release/vireon-duplex/Vireon?style=flat-square" alt="Release" /></a>
    <a href="./LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg?style=flat-square" alt="License" /></a>
    <a href="https://rust-lang.org"><img src="https://img.shields.io/badge/Rust-1.90+-orange.svg?style=flat-square" alt="Rust Version" /></a>
  </p>
</div>

---

## 📖 About The Project

Vireon is a next-generation realtime runtime engineered for massive scale. Built entirely in **Rust** and leveraging the power of **QUIC** alongside **io_uring**, it provides an ultra-low latency, full-duplex communication layer capable of multiplexing millions of concurrent streams without breaking a sweat.

### ✨ Key Features

- **Blazing Fast I/O:** Utilizes `io_uring` for true asynchronous file and network operations, bypassing traditional kernel bottlenecks.
- **Next-Gen Transport:** Powered by QUIC for multiplexed, 0-RTT, full-duplex communication over UDP.
- **Clean Architecture:** Domain-centric design with pluggable ports for underlying storage and transport engines.
- **Highly Scalable:** Asynchronous at its core built on Tokio, operating exclusively with non-blocking, bounded channels.
- **Cloud-Native Ready:** Designed for containerized deployments with out-of-the-box observability (Prometheus metrics, OpenTelemetry tracing).

## 🏗 Architecture

Vireon employs a strict Clean Architecture strategy, isolating the core domain logic from infrastructure details. Transport and storage mechanisms are treated as interchangeable ports to allow for deep deployment flexibility.

<div align="center">
  <img src="resources/Vireon.png" alt="Vireon Architecture Diagram" />
</div>

For a deep dive into the design decisions and broker domain model, please consult our [Architecture Document](./ARCHITECTURE.md).

## 🚀 Getting Started

To get a local development instance of Vireon running, follow these steps.

### Prerequisites

Ensure you have the latest stable version of Rust installed:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
# Specific required components:
rustup component add rustfmt clippy
```

### Installation

1. Clone the repository
   ```bash
   git clone https://github.com/vireon-duplex/Vireon.git
   cd Vireon
   ```
2. Run the project in release mode for optimal performance
   ```bash
   cargo run --release
   ```

## 🧪 Testing & Quality Assurance

Vireon enforces strict code quality gates to maintain performance and safety. Before contributing or opening a PR, ensure all checks pass:

```bash
# Format and linting
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings

# Tests
cargo test --workspace

# Security and dependency audit
cargo audit
cargo deny check
```

For comprehensive details on our specific testing coverage and layers, refer to [TESTING.md](./TESTING.md).

## 📊 Profiler

Vireon ships with a **production-grade function profiler** (`#[profile]` attribute macro) that instruments hot-path functions across all modules — frame encode/decode, CRC32C, io_uring CQE processing, buffer pool allocation, timer wheel, stream scheduling, consistent hashing, GSO batching, and more.

- **< 30 ns overhead** per instrumented call when active
- **Zero-cost** when disabled (Guard compiles to a no-op; the compiler eliminates it entirely)
- Thread-local accumulation with lock-free global aggregation — no mutex on the hot path
- 512-bucket logarithmic histogram for P50 / P95 / P99 percentile reporting
- Background flush thread with configurable interval
- Multiple export formats: text report, JSON, CSV, Chrome Trace, flamegraph (folded stack), Graphviz DOT

See [`profiler/DESIGN.md`](./profiler/DESIGN.md) for the full architecture.

### Quick Start

```rust,no_run
use profiler::{Profiler, ProfilerConfig};

// 1. Initialize with defaults (or use the builder for custom config).
Profiler::init(ProfilerConfig::default());

// 2. Enable profiling globally.
Profiler::enable();

// 3. Start the background flush thread.
Profiler::start();

// ... run your workload — instrumented functions are timed automatically ...

// 4. Print a formatted report.
println!("{}", Profiler::report());

// 5. Stop + flush remaining data.
Profiler::stop();
```

### Enabling / Disabling

There are two independent layers of control:

#### Compile-time (feature flag)

By default the profiler is compiled in **zero-cost disabled** mode — the `Guard` is a zero-sized type with an empty `Drop`, so the optimizer removes it entirely. No binary bloat, no runtime overhead.

To activate instrumentation, build with the `profiling` feature:

```bash
# Enable for the server binary.
cargo run --release -p quic-server --features profiling -- --cert cert.pem --key key.pem --port 4433

# Enable for the client binary.
cargo run --release -p quic-client --features profiling -- ...

# Run profiler's own tests (require the feature).
cargo test -p profiler --features enabled
```

#### Runtime (API toggle)

Even when compiled with `enabled`, you can toggle profiling at runtime without recompiling:

```rust
use profiler::Profiler;

// Pause profiling — existing guards skip recording (no data lost).
Profiler::disable();

// Resume profiling.
Profiler::enable();
```

### Instrumenting a Function

Add `#[profile]` to any free function, method, async function, generic function, or trait-impl method:

```rust,ignore
use profiler::profile;

#[profile]
fn process_packet(data: &[u8]) -> usize {
    data.len()
}

#[profile]
async fn send_stream(id: u64) -> Result<(), Error> {
    // ...
}
```

The macro expands to:

```rust,ignore
fn process_packet(data: &[u8]) -> usize {
    let _profiler_guard = ::profiler::Guard::enter("process_packet");
    data.len()
} // Guard::drop → compute elapsed → collector::record("process_packet", ns)
```

> `const fn` is not supported (Drop is non-const). The attribute is silently ignored on `const fn`.

### Configuration

```rust
use profiler::ProfilerConfig;

let config = ProfilerConfig::builder()
    .enabled()                   // start enabled (default: false)
    .sampling(1)                 // sample every N-th call (1 = all)
    .flush_interval_ms(500)      // background flush period
    .thread_local_capacity(2048) // flush threshold per thread
    .build();

profiler::Profiler::init(config);
```

| Field | Default | Description |
|---|---|---|
| `enabled` | `false` | Whether profiling is active on init |
| `sampling` | `1` | Record every N-th call (1 = every call) |
| `export_path` | `None` | Reserved for file-based export |
| `flush_interval_ms` | `1000` | Background thread tick period |
| `histogram_precision` | `16` | Sub-buckets per octave (512 total buckets) |
| `thread_local_capacity` | `1024` | Thread-local entries before forced flush |

### Export Formats

```rust
// Formatted text table (function, calls, avg, min, max, p50, p95, p99).
let text = Profiler::report();

// Machine-readable.
let json  = Profiler::export_json();           // JSON array of snapshots
let csv   = Profiler::export_csv();            // CSV with header row

// Visualization.
let trace = Profiler::export_chrome_trace();   // chrome://tracing
let folded = Profiler::export_flamegraph();    // flamegraph.pl input
let dot    = Profiler::export_dot();           // Graphviz call graph
```

### Viewing Reports (Server / Client Binaries)

The `quic-server` and `quic-client` binaries dump the profiler report automatically on shutdown.
Pass `--profile-output <file>` to select the destination and format (selected by file extension):

```bash
# Run the server — report is dumped to report.json on shutdown.
cargo run --release -p quic-server --features profiling -- \
    --cert cert.pem --key key.pem --port 4433 \
    --profile-output report.json
```

Supported extensions:

| Extension | Format |
|---|---|
| `.json` | JSON array of function snapshots |
| `.csv` | CSV with header row |
| `.ctrace` | Chrome Trace (`chrome://tracing`) |
| `.folded` | Folded stacks for `flamegraph.pl` |
| `.dot` | Graphviz call graph |
| *(other / none)* | Formatted text table |

#### Shutdown behavior

- **Graceful (one Ctrl-C / SIGTERM):** the drain begins, connections close, and once the reactor exits the report is written to the `--profile-output` path (or logged — see below).
- **Forced (second Ctrl-C / SIGTERM):** the process exits immediately with code 130, but the report is **still dumped** via the signal handler before `process::exit` — so you never lose data by force-quitting.
- **No `--profile-output`:** the top-10 functions by total time are printed to the log via `tracing::info!`, and a hint suggests rerunning with `--profile-output` for the full report.

```bash
# Example: graceful shutdown with no file → log summary
RUST_LOG=quic_server=info cargo run --release -p quic-server --features profiling -- \
    --cert cert.pem --key key.pem --port 4433
# Ctrl-C once → log lines like:
#   profiler: top functions by total time (of 30):
#     encode_into        calls=  12345  total=  4567890 ns  avg=  370 ns  p99=  1200 ns
#     ...
```

### Already-Instrumented Hot Paths

The following functions carry `#[profile]` across the workspace:

| Crate | Key Functions |
|---|---|
| `frame` | `encode_into`, `decode_with_len`, `decode_frame`, `crc32c` |
| `cqe-processor` | `drain_cq`, `dispatch_one`, `CqDrainer::drain` |
| `buffer-pool` | `get`, `try_get`, `acquire`, `release_buffer`, `flush` |
| `timer-wheel` | `advance`, `add`, `next_deadline` |
| `quic-engine` | `build_quiche_config`, `GsoBatcher::push/flush`, `Crc32cHasher::write` |
| `stream-scheduler` | `tick`, `submit_batch`, `select`, `jain_index` |
| `session-registry` | `ConsistentHash::add_node`, `lookup`, `lookup_n` |
| `reactor` | `EventLoop::run_one_tick` |

## 🤝 Contributing

Contributions are what make the open-source community an amazing place to learn, inspire, and build robust software. Any contributions to Vireon are **greatly appreciated**.

Please review our [Contributing Guidelines](./CONTRIBUTING.md) and [Engineering Standards](./SKILLS.md) to understand our workflow and expectations before submitting your PR.

## 🛡 Security

If you discover a security vulnerability within Vireon, please do **NOT** open a public issue. Review our [Security Policy](./SECURITY.md) for the correct reporting procedures.

## 📄 License

Distributed under the MIT License. See [`LICENSE`](./LICENSE) for more information.
