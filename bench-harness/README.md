# bench-harness

Network load-test harness for Vireon `quic-server`. Opens N concurrent
QUIC connections and reports throughput, latency, flow-control, and
resource-efficiency statistics.

Five benchmark modes cover distinct performance questions:

- **`stream`** — one-way TX throughput (all connections send)
- **`rr` / `multi-rr` / `pipeline`** — round-trip throughput + latency
  (server echoes back)
- **`group`** — P5 consumer-group round-robin delivery (1 publisher +
  N-1 members; measures fan-out rate + delivery fairness)

Binary name: **`loadtest`**

```
cargo run -p bench-harness --release -- --target 127.0.0.1:4433 --conns 4 --duration 10
```

## Prerequisites

### 1. TLS Certificate

```bash
# Generate a self-signed certificate for testing
openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem \
    -days 365 -nodes -subj "/CN=localhost"
```

### 2. Start the QUIC Server

The server must be running **before** launching the benchmark.

**For `stream` mode** (one-way throughput — no echo needed):

```bash
cargo run -p quic-server --release -- \
    --cert cert.pem \
    --key key.pem \
    --port 4433 \
    --mode single
```

**For `rr` / `multi-rr` / `pipeline` modes** (echo required — add `--echo`):

```bash
cargo run -p quic-server --release -- \
    --cert cert.pem \
    --key key.pem \
    --port 4433 \
    --mode single \
    --echo
```

`--echo` makes the server bounce every received frame back on the same
QUIC stream, bypassing the full application layer chain. This isolates
transport-layer round-trip performance.

**For `group` mode** (consumer-group delivery — no echo needed; groups
are always-on):

```bash
cargo run -p quic-server --release -- \
    --cert cert.pem \
    --key key.pem \
    --port 4433 \
    --mode single
```

The server's consumer-group coordinator evicts idle members after 3 s
(`DEFAULT_MAX_IDLE_NS`). The harness refreshes membership by re-sending
`ConsumerGroupJoin` every 1 s, so no special server flag is needed.

### 3. Build the Benchmark

```bash
# Debug build (fast compile)
cargo build -p bench-harness

# Release build (required for meaningful performance numbers)
cargo build -p bench-harness --release
```

Release binaries are placed at:

```
target/x86_64-unknown-linux-gnu/release/loadtest
```

## Usage

```
loadtest [OPTIONS]
```

### CLI Flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--target HOST:PORT` | `-t` | `127.0.0.1:4433` | Server address |
| `--conns N` | `-c` | `1` | Concurrent QUIC connections. In `group` mode: conn 0 = publisher, conns 1..N = members. |
| `--duration SECS` | `-d` | `10` | Measurement window in seconds |
| `--mode MODE` | `-m` | `stream` | Benchmark mode: `stream`, `rr`, `multi-rr`, `pipeline`, `group` |
| `--pipeline-depth N` | `-p` | `4` | In-flight batches for `pipeline` mode |
| `--sweep MODE` | — | `none` | Sweep mode: `conn` (sweep connection counts), `payload` (sweep payload sizes), `none` |
| `--egress-policy POLICY` | — | `none` | Client send policy: `none`, `reliable_ordered`, `reliable_unordered`, `realtime_drop_old`, `latest_only` |
| `--reactors N` | — | auto | Max reactor threads (one per core by default). Lower this when client + server share a machine so the server gets CPU too. |
| `--help` | `-h` | — | Show help |

### Benchmark Modes

| Mode | Flag | Requires `--echo` | Min conns | Description |
|------|------|:-:|:-:|-------------|
| `stream` | `-m stream` | No | 1 | **Full-duplex streaming** — fire continuously, drain in background. Maximises one-way TX throughput. Default. |
| `rr` | `-m rr` | Yes | 1 | **Request-Response** — send 1 batch on stream 0, wait for full echo, repeat. Pure latency baseline. |
| `multi-rr` | `-m multi-rr` | Yes | 1 | **Multi-stream R-R** — 15 streams fired concurrently, wait all echoes. |
| `pipeline` | `-m pipeline` | Yes | 1 | **Pipelined R-R** — keep N batches in-flight (`--pipeline-depth`) before waiting for echo. Balances throughput + latency. |
| `group` | `-m group` | No | 2 | **P5 consumer-group delivery** — conn 0 = publisher streaming `Publish` to topic `bench.group`; conns 1..N = members that join group `bench`. Server round-robin-delivers each publish to exactly one member. Measures fan-out rate + fairness. |

### What each mode measures

| Mode | Question it answers | Headline metric |
|------|---------------------|-----------------|
| `stream` | How fast can N connections push data one-way? | TX MiB/s (aggregate) |
| `rr` | What's the RTT floor for a single-stream request? | latency p50/p99 |
| `multi-rr` | How does RTT change with 15 concurrent streams? | latency p50/p99 |
| `pipeline` | How much throughput can in-flight batching recover? | req/sec at depth N |
| `group` | How fast can the server fan out publishes to a consumer group, and how fair is round-robin? | deliveries/sec + member fairness (min/avg/max) |

> **`stream` vs `group` throughput:** `stream` aggregate TX scales with
> `-c` (all connections send). `group` has only **1 sender** (the
> publisher), so aggregate TX ≈ a single connection's throughput — this
> is structural, not a regression. The group metric is
> **deliveries/sec** (server → members), which should approach
> `publishes/sec` when the server keeps up.

### Workload

In `stream` / `rr` / `multi-rr` / `pipeline` modes, each connection
sends a **~1 MiB batch** of 15 frame types (×32
coalesced copies = ~32 MiB wire per batch):

| Type | Hex | Payload | Data/Control |
|------|-----|---------|:---:|
| TextToken | `0x01` | 100 KB | data |
| AudioFrame | `0x02` | 80 KB | data |
| VideoFrame | `0x03` | 320 KB | data |
| ImageFrame | `0x04` | 256 KB | data |
| ToolCall | `0x05` | 4 KB | data |
| ToolResult | `0x06` | 148 KB | data |
| StateUpdate | `0x07` | 4 KB | data |
| Error | `0x08` | 2 KB | data |
| Heartbeat | `0x09` | 41 B | data |
| StreamOpen | `0x0A` | 1 KB | control |
| StreamClose | `0x0B` | 37 B | control |
| StreamReset | `0x0C` | 255 B | control |
| Handshake | `0x0D` | 2 KB | control |
| Fragment | `0x0E` | 120 KB | control |
| Ack | `0x0F` | 48 B | control |

Each logical stream maps to a distinct QUIC stream: `stream_id = i × 4`
(client bidi: 0, 4, 8, ..., 56).

In `group` mode the shared workload is ignored. Instead:

- **Publisher** (conn 0) continuously streams coalesced `Publish` frames
  to topic `bench.group` (~4 KB each) on QUIC stream 0.
- **Members** (conns 1..N) send `ConsumerGroupJoin` (topic `bench.group`,
  group `bench`, consumer id `c<idx>`, 4 partitions) on stream 0, then
  count incoming `Publish` deliveries via the recv path.

## Example Commands

```bash
# ── Throughput test (no echo needed) ──────────────────────────────

# Single connection, 10 seconds
loadtest -m stream -c 1 -d 10

# 4 concurrent connections, 30 seconds
loadtest -m stream -c 4 -d 30

# ── Pipelined request-response (requires --echo) ─────────────────

# Depth 8, 1 connection
loadtest -m pipeline -c 1 -d 30 -p 8

# Depth 16, 4 connections
loadtest -m pipeline -c 4 -d 30 -p 16

# ── Latency baseline (requires --echo) ───────────────────────────

# Single-stream request-response
loadtest -m rr -c 1 -d 10

# Multi-stream concurrent request-response
loadtest -m multi-rr -c 1 -d 10

# ── Consumer-group delivery (P5) ──────────────────────────────────
# 1 publisher + 7 members (round-robin delivery), 10 seconds
loadtest -m group -c 8 -d 10

# 1 publisher + 1 member (single-member baseline)
loadtest -m group -c 2 -d 10

# Connections mode
RUST_LOG=bench_harness=warn cargo run -p bench-harness -- -m pipeline -c 1 -d 5 -p 800 --sweep conn
```

## Output Metrics

The summary prints two or three sections (group mode adds a third):

### Core Metrics

| Metric | Description |
|--------|-------------|
| `requests` | Completed request-response cycles (pipeline / rr modes only) |
| `frames` | Total frames sent across all connections |
| `bytes` | Total wire bytes sent |
| `req/sec` | Request-response cycles per second |
| `frames/sec` | Frames sent per second |
| `latency (us)` | min / p50 / p90 / p99 / p99.9 / max RTT (pipeline / rr modes only) |
| `throughput` | TX throughput in MiB/s |
| `handshake` | TLS handshake time percentiles |

### Group (P5) Metrics — only in `-m group`

| Metric | Description |
|--------|-------------|
| `publishers` | Publisher connection count (always 1) |
| `members` | Member connection count (round-robin consumers) |
| `publishes/sec` | Rate of Publish frames sent by the publisher |
| `deliveries/sec` | Rate of Publish frames delivered to members by the server. Should approach `publishes/sec` when the server keeps up. |
| `member fairness` | min / avg / max frames received per member. Round-robin should keep these within a few percent; wide skew signals delivery imbalance. |

### Extended Stats

| Metric | Description |
|--------|-------------|
| `TX Frames/s` | Frames sent per second |
| `RX Frames/s` | Data frames received per second (echo) |
| `TX MiB/s` | Wire bytes sent per second |
| `RX MiB/s` | Data payload bytes received per second |
| `Frames In Flight` | Peak in-flight frames (pipeline / stream) |
| `Avg Queue Depth` | Average in-flight frames per poll iteration |
| `Blocked Writes` | Times `stream_send` returned `Error::Done` (flow-control) |
| `Flow Control Waits` | Drain loops entered to free flow-control credit |
| `Write Syscalls/s` | UDP send syscalls per second |
| `Read Syscalls/s` | UDP recv syscalls per second |

## Performance Notes

- **Always use `--release`** for benchmarks. Debug builds are 50-100× slower.
- **`stream` mode** measures one-way TX throughput — the server does not
  need `--echo`. This is the primary throughput benchmark.
- **`pipeline` / `rr` modes** measure round-trip throughput and latency.
  RX throughput is inherently lower than TX because the server must
  process and echo every byte.
- **Pipeline depth** controls how many batches are in-flight before
  blocking. Higher depth = more pipeline fill = higher throughput, but
  also more memory. Start with `-p 4` and increase.
- On loopback, `cubic` congestion control outperforms `bbr` (BBR
  collapses its window with near-zero RTT). The server defaults to
  `cubic`.
- **Sharing a machine with the server:** pin the harness to a disjoint
  CPU set with `taskset` and cap reactors so the server isn't starved:
  ```bash
  taskset -c 8-15 loadtest --reactors 6 -c 8 -d 10 -m group
  ```
- **`group` mode** measures consumer-group fan-out, not raw throughput.
  Aggregate TX is inherently lower than `stream` because only 1 of N
  connections sends. The headline metric is `deliveries/sec` +
  `member fairness`. If `deliveries/sec` ≪ `publishes/sec`, the server
  can't keep up with fan-out (check for stale-member eviction in server
  logs — the harness re-joins every 1 s to prevent this).

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `handshake timed out` | Server not running or wrong port | Verify server is up: `ps aux \| grep quic-server` |
| `RX = 0` in echo modes | Server started without `--echo` | Restart server with `--echo` flag |
| Very low throughput | Debug build | Rebuild with `--release` |
| `connection closed during handshake` | Cert/key mismatch or expired | Regenerate certs |
| Port already in use | Previous server still running | `kill $(pgrep quic-server)` |
| `group` deliveries/sec ≪ publishes/sec | Multiple server instances sharing port 4433 (SO_REUSEPORT) — each has independent group state | Kill all stale servers: `pkill -f quic-server`, restart one |
| `group` member fairness skewed (one member ≈ 0) | Member's `ConsumerGroupJoin` was delayed by an overwhelmed server | Reduce publish rate or add server workers; verify single server instance |
