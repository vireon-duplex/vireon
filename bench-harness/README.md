# bench-harness

Network load-test harness for Vireon `quic-server`. Opens N concurrent
QUIC connections, each sending the full 15-frame realistic workload
(text / audio / video / image / tool / state / control planes), and
reports throughput, latency, and flow-control statistics.

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
| `--conns N` | `-c` | `1` | Concurrent QUIC connections (each = 1 user = 15 streams) |
| `--duration SECS` | `-d` | `10` | Measurement window in seconds |
| `--mode MODE` | `-m` | `stream` | Benchmark mode (see below) |
| `--pipeline-depth N` | `-p` | `4` | In-flight batches for pipeline mode |
| `--help` | `-h` | — | Show help |

### Benchmark Modes

| Mode | Flag | Requires `--echo` | Description |
|------|------|:-:|-------------|
| `stream` | `-m stream` | No | **Full-duplex streaming** — fire continuously, drain echoes in background. Maximises TX throughput. Default. |
| `pipeline` | `-m pipeline` | Yes | **Pipelined R-R** — keep N batches in-flight before waiting for echo. Balances throughput + latency measurement. |
| `rr` | `-m rr` | Yes | **Request-Response** — send 1 batch on stream 0, wait for full echo, repeat. Pure latency baseline. |
| `multi-rr` | `-m multi-rr` | Yes | **Multi-stream R-R** — 15 streams fired concurrently, wait all echoes. |

### Workload

Each connection sends a **~1 MiB batch** of 15 frame types (×32
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

# Connections mode
RUST_LOG=bench_harness=warn cargo run -p bench-harness -- -m pipeline -c 1 -d 5 -p 800 --sweep conn
```

## Output Metrics

The summary prints two sections:

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

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `handshake timed out` | Server not running or wrong port | Verify server is up: `ps aux \| grep quic-server` |
| `RX = 0` in echo modes | Server started without `--echo` | Restart server with `--echo` flag |
| Very low throughput | Debug build | Rebuild with `--release` |
| `connection closed during handshake` | Cert/key mismatch or expired | Regenerate certs |
| Port already in use | Previous server still running | `kill $(pgrep quic-server)` |
