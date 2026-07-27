# quic-server

Production-grade **QUIC Server** for the Vireon runtime — the composition
root that wires all 8 architectural layers into a single, top-down
layered stack driven by io_uring.

Each worker owns one `io::Reactor` (io-layer) and one `VireonStack`
that processes QUIC events through a nested ownership chain:

```
Application → Session → Stream → Message → Transport → Runtime → IO → Kernel
```

## Architecture

```text
┌──────────────────────────────────────────────────────────────────┐
│                           Worker                                  │
│                                                                  │
│  ┌─────────────┐    ┌──────────────────────────────────────────┐│
│  │   Reactor    │    │              VireonStack                  ││
│  │  (io-layer)  │───►│  impls IoHandler                          ││
│  │              │    │                                          ││
│  │  io_uring    │    │  ┌────────────────────────────────────┐  ││
│  │  recv/send   │    │  │     UPPER HALF (logical protocol)   │  ││
│  │  multishot   │    │  │                                    │  ││
│  │  ZC          │    │  │  ApplicationLayer                   │  ││
│  └─────────────┘    │  │    └─ SessionLayer                   │  ││
│                      │  │         └─ StreamLayer              │  ││
│                      │  │              └─ MessageLayer        │  ││
│                      │  │    └─ dispatcher: LoggingDispatcher  │  ││
│                      │  │         (owns AiStreamPipeline)      │  ││
│                      │  └────────────────────────────────────┘  ││
│                      │                                          ││
│                      │  ┌────────────────────────────────────┐  ││
│                      │  │     LOWER HALF (physical I/O)       │  ││
│                      │  │                                    │  ││
│                      │  │  RuntimeLayer                       │  ││
│                      │  │    └─ ConnectionShard               │  ││
│                      │  │    └─ TimerWheel (idle reaping)     │  ││
│                      │  │    └─ BufferPool (scratch)          │  ││
│                      │  │  QuicEngine (transport-layer)       │  ││
│                      │  │  ConnectionRegistry (cross-layer)   │  ││
│                      │  └────────────────────────────────────┘  ││
│                      └──────────────────────────────────────────┘│
└──────────────────────────────────────────────────────────────────┘
          │                                  │
     [kernel-layer]                    [kernel-layer]
     io_uring syscalls              kernel-nic CpuTopology
     UDP socket                     (CPU/NUMA discovery in config.rs)
```

### Layer Dependency Chain (Top-Down Ownership)

Each layer struct **owns** the layer struct below it via a nested
field. Frame processing flows downward through delegation; the
routing decision (`FrameAction`) flows back upward through return
values.

```text
ApplicationLayer                          // application-layer
  ├─ SessionLayer                         // session-layer
  │    └─ StreamLayer                     // stream-layer
  │         └─ MessageLayer               // message-layer
  └─ LoggingDispatcher
       └─ AiStreamPipeline                // ai-streaming integration
```

| Layer | Struct | Crate | Owns |
|-------|--------|-------|------|
| kernel | *(direct crate usage)* | `kernel-nic` | — |
| io | *(owned by Worker)* | `io` | Reactor (io_uring ring) |
| runtime | `RuntimeLayer` | `connection-shard`, `timer-wheel`, `buffer-pool` | Shard + Timer + Pool |
| transport | *(QuicEngine)* | `quic-engine` | QUIC protocol core (quiche) |
| message | `MessageLayer` | `frame` | Frame classification |
| stream | `StreamLayer` | `stream-manager` | MessageLayer + StreamSlab |
| session | `SessionLayer` | `session-manager` | StreamLayer + SessionTable |
| application | `ApplicationLayer` | `ai-streaming` | SessionLayer + Dispatcher |

## Module Structure

```text
quic-server/src/
├── main.rs              Entry point: CLI parse, tracing init, boot server
├── cli.rs               clap-based CLI argument definitions
├── conf.rs              TOML config file parser (ServerConf + QuicConf)
├── config.rs            ServerConfig resolution: CLI > config file > default
├── server.rs            QuicServer: orchestrates workers (single/multi/distributed)
├── worker.rs            Worker: owns Reactor + VireonStack, runs event loop
├── stack.rs             VireonStack: composition root, impls IoHandler
├── connection.rs        ConnectionRegistry: cross-layer conn_idx → ConnState
├── error.rs             ServerError enum
├── distributed.rs       Distributed mode stub (future inter-node coordination)
└── layers/
    ├── mod.rs           Module declarations + re-exports
    ├── runtime.rs       RuntimeLayer: ConnectionShard + TimerWheel + BufferPool
    ├── message.rs       MessageLayer: frame classification → FrameAction
    ├── stream.rs        StreamLayer: StreamSlab + per-conn stream tracking
    ├── session.rs       SessionLayer: SessionTable + delegates to StreamLayer
    └── application.rs   ApplicationLayer: AppDispatcher trait + LoggingDispatcher
```

## Data Flow

### Incoming Datagram (bottom-up physical, then top-down logical)

```text
Kernel ──► io_uring CQE
               │
               ▼
Reactor.on_datagram(data, peer)
               │
               ▼
VireonStack.on_datagram ──► engine.on_datagram(data, peer)
                                    │
                          quiche::Connection::recv()
                          decode frame via frame::codec
                                    │
                                    ▼
                          engine accumulates QuicEvents
```

### Event Processing (on each reactor tick)

```text
VireonStack.on_tick(ctx)
    │
    ├── engine.on_tick(ctx)           // flush sends, cleanup closed conns
    ├── events = engine.drain_events()
    │
    └── for each QuicEvent:
        │
        ├── ConnectionEstablished { conn_idx, peer }
        │     [runtime]     runtime.on_connect(conn_idx, peer) → conn_id
        │     [app→session] application.on_connect(conn_idx, peer) → session_id
        │     connections.insert(conn_idx, ConnState { conn_id, session_id, peer })
        │
        ├── FrameReceived { conn_idx, frame }
        │     [runtime]     runtime.on_frame(conn_idx, conn_id, frame_len)
        │                    // touch_recv + reset idle timer + acquire buffer
        │     [app→ses→str→msg] application.on_frame(conn_idx, session_id, &frame)
        │                    //   → session.on_frame → stream.on_frame → message.classify
        │                    //   → if data: dispatcher.on_data_frame (AI token routing)
        │                    //   → if control: stream slab reserve/release
        │
        ├── ConnectionClosed { conn_idx, peer, reason }
        │     [app→sen→str] application.on_close(conn_idx, session_id, reason)
        │                    //   → session.on_close → stream.on_close (release streams)
        │                    //   → dispatcher.on_connection_closed
        │     [runtime]     runtime.on_close(conn_idx, conn_id)
        │                    //   cancel timer + destroy connection
        │     connections.remove(conn_idx)
        │
        └── StreamFinished { conn_idx, stream_id }
              (logged for now)

    [runtime] reap_idle()
              // drain_expired timers → close idle connections
```

### Delegation Chain

Each layer delegates to the one it owns:

```text
ApplicationLayer::on_frame
    │
    ├── session.on_frame(conn_idx, &frame)     // [session-layer]
    │       │
    │       └── stream.on_frame(conn_idx, &frame)  // [stream-layer]
    │               │
    │               ├── message.classify(&frame)    // [message-layer]
    │               │     → FrameAction::DispatchToApp or Consumed
    │               │
    │               └── if control: slab.try_reserve / release
    │
    └── if FrameAction::DispatchToApp:
            dispatcher.on_data_frame(session_id, conn_idx, &frame)
              // TextToken → AiStreamPipeline TokenChunk routing
```

## Key Types

### VireonStack (`stack.rs`)

The composition root — owns all layers and implements `io::IoHandler`.

```rust
pub struct VireonStack {
    application: ApplicationLayer,   // upper half
    runtime: RuntimeLayer,           // lower half
    engine: QuicEngine,              // transport-layer
    connections: ConnectionRegistry, // cross-layer state
}

impl IoHandler for VireonStack {
    fn on_datagram(&mut self, data: &[u8], peer: SocketAddr);
    fn on_tick(&mut self, ctx: &mut TickContext);
}
```

### Layer Structs

| Struct | Key Methods | Responsibility |
|--------|------------|----------------|
| `RuntimeLayer` | `on_connect`, `on_frame`, `on_close`, `drain_expired` | Connection create/destroy, idle timer, buffer pool |
| `MessageLayer` | `classify(&Frame) → FrameAction` | Data vs control frame routing |
| `StreamLayer` | `on_connect`, `on_frame`, `on_close` | Stream slab allocation, per-conn stream tracking |
| `SessionLayer` | `on_connect`, `on_frame`, `on_close` | Session table slot management |
| `ApplicationLayer` | `on_connect`, `on_frame`, `on_close` | App dispatcher notification, AI streaming routing |

### AppDispatcher Trait (`layers/application.rs`)

Implemented by application services to receive data-plane events:

```rust
pub trait AppDispatcher {
    fn on_connection_established(&mut self, session_id: SessionId, peer: SocketAddr);
    fn on_data_frame(&mut self, session_id: SessionId, conn_idx: usize, frame: &Frame);
    fn on_connection_closed(&mut self, session_id: SessionId, reason: Option<&str>);
}
```

`LoggingDispatcher` is the default implementation — routes `TextToken`
frames through `AiStreamPipeline` as `TokenChunk` values.

### ConnectionRegistry (`connection.rs`)

Cross-layer bridge — maps QUIC engine `conn_idx` to runtime and
session identifiers:

```rust
pub struct ConnState {
    pub conn_id: ConnectionId,   // runtime-layer
    pub session_id: SessionId,   // session-layer
    pub peer_addr: SocketAddr,
}
```

Each layer manages its own internal state (timer handles in
`RuntimeLayer`, stream sets in `StreamLayer`). This registry only
stores the identifiers needed to bridge the upper and lower halves.

## Quick Start

### Prerequisites

- TLS certificate and private key (PEM format)

```bash
# Generate a self-signed certificate for testing
openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem \
    -days 365 -nodes -subj "/CN=localhost"
```

### Run

```bash
# Single-worker mode (default) — benchmarks, development
cargo run -p quic-server --release -- \
    --cert cert.pem --key key.pem --port 4433 --mode single

# Benchmark echo mode (for bench-harness rr/multi-rr/pipeline)
cargo run -p quic-server --release -- \
    --cert cert.pem --key key.pem --port 4433 --mode single --echo

# Multi-worker mode (thread-per-core, production)
cargo run -p quic-server --release -- \
    --cert cert.pem --key key.pem --port 4433 \
    --mode multi --preset bare-metal

# With WAL durability (enables Fetch/replay)
cargo run -p quic-server --release -- \
    --cert cert.pem --key key.pem --port 4433 \
    --mode multi --wal-root /var/lib/vireon/wal

# Custom worker count + queue depth
cargo run -p quic-server --release -- \
    --cert cert.pem --key key.pem --port 4433 \
    --mode multi --workers 4 --queue-depth 512 -v

# Using a TOML config file (like redis.conf / server.properties)
cp quic-server/vireon.conf.example my.conf
# edit my.conf: set port, mode, workers, [quic] overrides, etc.
cargo run -p quic-server --release -- \
    --config my.conf --cert cert.pem --key key.pem

# CLI flags override config file values
cargo run -p quic-server --release -- \
    --config my.conf --port 44332   # overrides port from my.conf
```

### CLI Arguments

All flags can also be set via a [config file](#configuration-file). CLI
flags always override config file values.

#### Connection / networking

| Flag | Default | Description |
|------|---------|-------------|
| `--config` | *(none)* | Path to a TOML config file. See [Configuration File](#configuration-file). |
| `--port` | `4433` | UDP port to bind |
| `--bind` | `0.0.0.0` | Bind address (IP only; port from `--port`) |
| `--cert` | *(required)* | PEM certificate chain path |
| `--key` | *(required)* | PEM private key path |

#### Worker / threading

| Flag | Default | Description |
|------|---------|-------------|
| `--mode` | `single` | `single` (1 reactor on main thread), `multi` (thread-per-core), `distributed` (multi + inter-node, stub) |
| `--workers N` | `0` (auto) | Worker thread count. `0` = one per CPU core. Ignored in `single` mode (always 1). |
| `--queue-depth N` | `1024` | io_uring submission queue depth (0 = default). Higher = more in-flight I/O but more memory. |

#### Tuning presets

| Flag | Default | Description |
|------|---------|-------------|
| `--preset` | `bare-metal` | Tune QUIC defaults for a deployment class. See [Presets](#presets) below. |

#### Benchmark / debug

| Flag | Default | Description |
|------|---------|-------------|
| `--echo` | off | Bounce every received frame back on the same QUIC stream. Required for `bench-harness` `rr` / `multi-rr` / `pipeline` modes. Bypasses the application layer. |
| `--skip-crc` | **on** | Skip CRC32C verification on received frames (default ON). QUIC's AEAD already guarantees frame integrity end-to-end; skipping saves ~5–8% of decode CPU. Use `--enforce-crc` to opt back into verification (debugging). |
| `--sqpoll` | off | Enable the io_uring SQPOLL kernel thread (kernel ≥ 5.11, needs CAP_SYS_NICE). The kernel polls the SQ ring, eliminating submit syscalls. Auto-falls-back if denied. |
| `-v` | off | Enable verbose (debug-level) tracing. |
| `--profile-output FILE` | off | Dump profiler report on shutdown. Requires `--features profiling`. Extension selects format: `.json`, `.csv`, `.ctrace`, `.folded`, `.dot`, else text. |

#### Pub/Sub features

| Flag | Default | Description |
|------|---------|-------------|
| `--acl-rules FILE` | off | Load tenant-isolation ACL rules from a TOML file, replacing the default permissive `allow("*.*")` table. |
| `--wal-root DIR` | off | Enable the write-ahead log. Every accepted publish is appended under `<DIR>/worker_<id>/`. Subscribers can `Fetch(offset)` to replay missed messages. Off = fire-and-forget. |
| `--cluster-node-id N` | off | (P6) Join a multi-node cluster as node `N`. Must be >0 and appear in `--cluster-peers`. |
| `--cluster-peers LIST` | off | (P6) Cluster peer list: `"1=host:port,2=host:port,..."`. All nodes including self must be listed. Requires `--cluster-node-id`. |

### Presets

| Preset | Use case |
|--------|----------|
| `cloud` | Multi-tenant cloud workloads |
| `bare-metal` | General production server |
| `hundred-gbe` | 100 GbE+ high-throughput |
| `low-latency` | Sub-ms tail latency (RPC, gaming) |
| `max-throughput` | Bulk-transfer max throughput |

## Configuration File

All CLI flags can alternatively be set in a TOML config file (like
`redis.conf` or Kafka's `server.properties`). This is the recommended
way to configure production deployments.

### Precedence

```
CLI flag (explicit)  >  config file value  >  hardcoded default
```

If `--config` is not given, behavior is 100% backward-compatible.

### Usage

```bash
# Copy the template and edit
cp quic-server/vireon.conf.example my.conf

# Run with the config file
quic-server --config my.conf --cert cert.pem --key key.pem

# CLI flags override config file values
quic-server --config my.conf --port 44332    # CLI wins
```

### Example (`vireon.conf`)

```toml
# ── Network ───────────────────────────────────────────────────
port = 4433
bind = "0.0.0.0"
cert = "cert.pem"
key = "key.pem"

# ── Workers ───────────────────────────────────────────────────
mode = "multi"                  # single | multi | distributed
workers = 0                     # 0 = auto-detect (one per CPU core)
queue_depth = 0                 # 0 = default (1024)

# ── Preset ────────────────────────────────────────────────────
preset = "bare-metal"

# ── CRC ───────────────────────────────────────────────────────
skip_crc = true                 # default ON (QUIC AEAD guarantees integrity)

# ── WAL ───────────────────────────────────────────────────────
wal_root = "/var/lib/vireon/wal"
wal_retention_secs = 3600       # entries older than 1h are truncated
wal_max_bytes = 1073741824      # 1 GiB per topic

# ── Metrics ───────────────────────────────────────────────────
metrics_port = 9090

# ── Security ──────────────────────────────────────────────────
max_handshakes_per_ip_per_sec = 100

# ── QUIC engine overrides (optional — preset defaults apply) ──
[quic]
max_connections = 131072
initial_max_data = "100MiB"         # human-readable: 100MiB, 1GiB, 256KiB
idle_timeout = "30s"                 # human-readable: 30s, 5m, 1h
congestion = "bbr"                   # reno | cubic | bbr | bbr2
```

A fully commented template is at
[`quic-server/vireon.conf.example`](vireon.conf.example).

### Human-Readable Values

The `[quic]` section accepts human-readable strings for byte sizes and
durations:

| Type | Examples |
|------|----------|
| Bytes | `"100MiB"`, `"1GiB"`, `"256KiB"`, `1073741824` (raw) |
| Duration | `"30s"`, `"5m"`, `"1h"`, `"500ms"`, `30` (raw seconds) |

## Worker Modes

### Single (`--mode single`)

One reactor on the main thread. Simplest mode — all connections
served by a single io_uring ring.

### Multi (`--mode multi`)

N reactors, one per CPU core (thread-per-core architecture). Each
worker:
- Pins to a specific core (`vireon-reactor-{core_id}`)
- Owns its own `Reactor` + `VireonStack` (shared-nothing)
- Binds to the same UDP port (SO_REUSEPORT)

The kernel steers incoming datagrams to workers via RSS/RPS.

> **Consumer-group caveat:** `group_locals` (the per-connection
> membership map used for round-robin fan-out) lives inside each
> worker's `ApplicationLayer`. It is **not shared across workers**.
> In `--mode multi`, a publisher and its group members may land on
> different workers → deliveries are silently lost. For consumer-group
> benchmarks (`bench-harness -m group`), use `--mode single` until
> cross-worker group sharing lands (tracked as a P5/P6 follow-up).

### Distributed (`--mode distributed`)

Currently delegates to multi-worker mode. Planned: inter-node
session coordination via gossip, consistent hashing, and health
probes (via `session-registry` crate).

## Threading Model

```text
Main thread
  ├── parse CLI, init tracing
  ├── create QuicServer
  └── QuicServer::run()
        ├── [single] Worker::create + run (on main thread)
        └── [multi]  Worker::spawn × N
                      │
                      ├── Thread "vireon-reactor-0" ── Worker { reactor, stack }
                      ├── Thread "vireon-reactor-1" ── Worker { reactor, stack }
                      └── ...
                      │
                      └── install_signal_handler (Ctrl-C sets all stop flags)
                          join all threads
```

**Not `Send`** — `VireonStack` holds raw pointers (QuicEngine) and
non-Send strategy boxes (ConnectionShard). Each worker must stay on
the thread that created it.

## Testing

```bash
# Per-layer unit tests (message classification, stream lifecycle)
cargo test -p quic-server

# Full workspace
cargo test --workspace

# Lint
cargo clippy -p quic-server -- -D warnings
cargo fmt --all
```

## Design Principles

1. **Top-down ownership** — each layer owns the layer below it.
   The dependency chain flows Application → Session → Stream →
   Message, expressed structurally through nested fields.

2. **Delegation over dispatch** — events flow through method calls
   on each layer (`layer.on_frame()` delegates to `child.on_frame()`),
   not a centralized match block.

3. **Encapsulated state** — each layer manages its own internal
   state. Timer handles live in `RuntimeLayer`, stream sets live in
   `StreamLayer`. No cross-layer state leaking.

4. **Shared-nothing workers** — each worker owns a complete stack
   with no shared mutable state. Cross-thread communication only
   for shutdown signaling.

5. **Best-effort error handling** — layer API calls that can fail
   on capacity (`shard.create`, `session_table.try_reserve_slot`,
   `stream_slab.try_reserve`) are logged and skipped, not fatal.
