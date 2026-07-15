# Profiler — Design Document

Production-grade function profiler for Rust. Instruments any function via
`#[profile]` attribute macro with <30ns overhead per call.

## Architecture

```
profiler/
├── macro/           crate: profiler-macro   (proc-macro = true)
│   └── src/lib.rs   #[profile] expansion
└── runtime/         crate: profiler          (library)
    ├── src/lib.rs           Profiler facade + re-exports
    ├── src/guard.rs         RAII timing guard (cfg-gated)
    ├── src/collector.rs     thread-local accumulator + flush
    ├── src/registry.rs      global RwLock<HashMap> registry
    ├── src/stats.rs         FunctionStats (atomics) + LocalEntry
    ├── src/histogram.rs     512-bucket lock-free histogram
    ├── src/context.rs       thread-local call-tree stack
    ├── src/report.rs        formatted text report
    ├── src/exporter.rs      JSON / CSV / Chrome Trace
    ├── src/flamegraph.rs    folded-stack + DOT export
    ├── src/config.rs        ProfilerConfig + builder
    ├── benches/overhead.rs  criterion benchmarks
    └── tests/integration.rs end-to-end tests
```

## Data Flow

```
#[profile] fn foo() { ... }
          │
          ▼  (macro expansion at compile time)
fn foo() {
    let _guard = Guard::enter("foo");   // Instant::now() + context::push
    ...                                  // function body
}  // Guard::drop → compute elapsed → collector::record("foo", ns)
          │
          ▼  (thread-local, no locks)
LocalCollector::record("foo", ns)
    entries["foo"].record(ns)            // FxHashMap lookup, u64 arithmetic
    calls_since_flush += 1
    if calls_since_flush >= 1024 → flush()
          │
          ▼  (every 1024 calls or background tick)
flush() → for each entry: registry::merge(name, &local)
    │           │
    │           ├─ read lock (fast path): stats.merge_local(&local)
    │           └─ write lock (first time): insert new FunctionStats
    │
    └─ context::drain_edges() → flamegraph::merge_edges()
```

Background thread ("profiler-bg") increments `TICK_GENERATION` every
`flush_interval_ms`. Each thread-local collector checks this on every
`record()` call — a single `Relaxed` atomic load — and flushes if the
generation changed.

## Hot-Path Design

Every instrumented call does:

1. **Guard::enter** (~5ns): `Instant::now()`, check `ENABLED` atomic,
   `context::push` (thread-local Vec)
2. **Function body** executes
3. **Guard::drop** (~10ns): `Instant::elapsed()`, `context::pop`,
   `collector::record()` (thread-local HashMap lookup + u64 arithmetic)

**No locks on the hot path.** The global `RwLock` is touched only during
flush (every 1024 calls), and even then it takes a **read lock** — the
`FunctionStats` entries use atomic counters for merging.

**No heap allocation after warm-up.** Each function's `LocalEntry` is
created once (first call), then reused. The `&'static str` function name
comes from the macro at compile time.

## Statistics

### Per-function (`FunctionStats`)

Global, atomically updated:

| Field | Type | Update |
|-------|------|--------|
| `call_count` | `AtomicU64` | `fetch_add(1, Relaxed)` |
| `total_ns` | `AtomicU64` | `fetch_add(ns, Relaxed)` |
| `min_ns` | `AtomicU64` | CAS loop (fetch_min) |
| `max_ns` | `AtomicU64` | CAS loop (fetch_max) |
| `last_ns` | `AtomicU64` | `swap(ns, Relaxed)` |
| `histogram` | `[AtomicU64; 512]` | `fetch_add(1, Relaxed)` |

All operations use `Ordering::Relaxed` — sufficient for statistics where
exact ordering doesn't matter.

### Histogram

512-bucket logarithmic histogram covering 1ns to ~4.3s:

- 32 octaves × 16 sub-buckets per octave
- Relative error: ~4.4% per bucket (factor 2^(1/16))
- Bucket assignment: `ns.ilog2()` for octave + 4 high bits for sub-bucket
- Percentile: walk buckets accumulating counts, return geometric mean of
  containing bucket

### Thread-local (`LocalEntry`)

Plain `u64` counters + histogram bucket counts (`Vec<u64>`). No atomics
needed — single-threaded by construction. Flushed to global every 1024
calls or on background tick.

## Proc-Macro

`#[profile]` expands to:

```rust,ignore
// Input
#[profile]
fn process(data: &[u8]) -> usize { data.len() }

// Output
fn process(data: &[u8]) -> usize {
    let _profiler_guard = ::profiler::Guard::enter("process");
    data.len()
}
```

Supported function kinds:
- Free functions, generic functions, lifetimes
- `async fn` (guard lives in state machine, persists across `.await`)
- `unsafe fn`
- Methods in `impl` blocks, trait impl methods
- Any visibility (`pub`, `pub(crate)`, private)

Unsupported:
- `const fn` — Drop makes it non-const (silently passes through)
- Trait method declarations (no body) — don't apply to declarations

## Zero-Cost Disable

When the `enabled` feature is off:

```rust,ignore
pub struct Guard;  // zero-sized

impl Guard {
    #[inline(always)]
    pub fn enter(_: &'static str) -> Self { Guard }
}

impl Drop for Guard {
    fn drop(&mut self) {}  // optimized away
}
```

The compiler eliminates the guard entirely — no overhead, no binary bloat.

## Async Support

No runtime dependency. The guard placed at `async fn` body entry
persists across `.await` points because it's part of the state machine's
saved frame. It measures wall-clock time from first poll to completion.

If a future is dropped (cancelled), the guard's `Drop` still fires and
records the elapsed duration. If the thread is panicking, recording is
skipped to avoid corrupting state during unwind.

## Call Tree / Flamegraph

Thread-local call stack tracks (parent → child) edges:

```
CALL_STACK: Vec<&'static str>     // per thread
LOCAL_EDGES: HashMap<(parent, child), u64>  // per thread
```

On `Guard::enter`: push name, record edge (parent → this).
On `Guard::drop`: pop.
On flush: drain edges → merge into global edge map.

Export formats:
- **Folded stack** (`parent;child;grandchild count`) — for `flamegraph.pl`
- **Graphviz DOT** — call graph visualization

## Export Formats

| Format | Method | Content |
|--------|--------|---------|
| Text report | `Profiler::report()` | Aligned table: name, calls, avg, min, max, p50, p95, p99 |
| JSON | `Profiler::export_json()` | Array of function snapshots |
| CSV | `Profiler::export_csv()` | Header + rows |
| Chrome Trace | `Profiler::export_chrome_trace()` | `chrome://tracing` JSON |
| Flamegraph | `Profiler::export_flamegraph()` | Folded stack format |
| DOT | `Profiler::export_dot()` | Graphviz call graph |

## API

```rust
use profiler::{Profiler, ProfilerConfig, profile};

// Initialize
Profiler::init(ProfilerConfig::default());
Profiler::enable();
Profiler::start();  // background flush thread

// Instrument
#[profile]
fn hot_function(x: u64) -> u64 { x * 2 }

// Report
println!("{}", Profiler::report());
let json = Profiler::export_json();

// Cleanup
Profiler::stop();
Profiler::reset();
```

### Config

```rust
pub struct ProfilerConfig {
    pub enabled: bool,                // default: false
    pub sampling: u32,                // 1 = every call (default)
    pub export_path: Option<PathBuf>, // not yet used
    pub flush_interval_ms: u64,       // default: 1000
    pub histogram_precision: u8,      // sub-buckets per octave (16)
    pub thread_local_capacity: usize, // flush threshold (1024)
}
```

## Design Constraints

| Constraint | How |
|-----------|-----|
| Stable Rust, Edition 2024 | No nightly features |
| No global mutex on hot path | Thread-local accumulation + RwLock read on flush |
| No heap alloc on hot path | HashMap entries created once, reused |
| `&'static str` names | From macro at compile time |
| No tokio dependency | Guard-based timing works with any runtime |
| <30ns overhead | TLS + FxHashMap + u64 arithmetic |
| Strict lints | No `unwrap`/`expect`/`panic` in non-test code |

## Verification

```bash
# Build both crates
cargo build -p profiler -p profiler-macro

# Run all tests (74 unit + 14 integration + 10 macro)
cargo test -p profiler --features enabled
cargo test -p profiler-macro

# Run benchmarks
cargo bench -p profiler --features enabled

# Verify zero-cost disable
cargo build -p profiler --no-default-features
```
