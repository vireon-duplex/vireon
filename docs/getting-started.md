# Getting Started with Vireon

Vireon is a QUIC-native pub/sub runtime for high-throughput, low-latency
messaging. This guide walks you through building the server, writing your
first publisher and subscriber, and understanding the core concepts.

## Prerequisites

| Requirement | Version | Check |
|---|---|---|
| **Rust** | 1.85+ (edition 2024) | `rustc --version` |
| **Linux** | kernel 5.6+ (io_uring) | `uname -r` |
| **OpenSSL/BoringSSL** | system dev headers | bundled by `quiche` |

```bash
git clone https://github.com/vireon-duplex/vireon.git
cd vireon
cargo build --release
```

## 1. Start the server

Generate a self-signed certificate (dev only):

```bash
openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem \
  -days 365 -nodes -subj "/CN=localhost"
```

Launch the server:

```bash
./target/release/quic-server \
  --bind 127.0.0.1 \
  --cert cert.pem \
  --key key.pem
```

You should see:

```
[vireon] listening on 127.0.0.1:4433
[vireon] workers: 8 (multi)
```

### Configuration via TOML

For anything beyond quick testing, use a config file:

```toml
# vireon.conf
bind = "0.0.0.0"
cert = "/etc/vireon/cert.pem"
key = "/etc/vireon/key.pem"
mode = "multi"          # "single" = 1 worker, "multi" = auto-detect
workers = 0             # 0 = auto (CPU count)
metrics_port = 9090     # Prometheus /metrics endpoint

[delivery]
logtail_threshold = 256  # Use LogTail for 256+ subscribers
```

```bash
./target/release/quic-server --config vireon.conf
```

See [`vireon.conf`](../vireon.conf) for all options.

## 2. Add the SDK to your project

```bash
cargo add vireon-sdk
```

```toml
[dependencies]
vireon-sdk = "0.1"
```

## 3. Write a publisher

```rust
use vireon_sdk::ClientBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ClientBuilder::new("127.0.0.1:4433")
        .tls_verify(vireon_sdk::TlsVerify::DangerAcceptInvalid)
        .connect()
        .await?;

    // Fire-and-forget publish (highest throughput).
    for i in 0..1000 {
        client.try_publish("sensor.temp", format!("{{\"v\":{i}}}".as_bytes())?;
    }

    // Acknowledged publish (blocks until server confirms).
    client.publish("sensor.temp", b"final").await?;

    client.close().await?;
    Ok(())
}
```

## 4. Write a subscriber

```rust
use vireon_sdk::ClientBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ClientBuilder::new("127.0.0.1:4433")
        .tls_verify(vireon_sdk::TlsVerify::DangerAcceptInvalid)
        .connect()
        .await?;

    // Wildcard subscription: matches "sensor.temp", "sensor.humid", etc.
    let mut sub = client.subscribe("sensor.*").await?;

    while let Some(msg) = sub.recv().await {
        println!("topic: {}  payload: {} bytes",
            msg.topic, msg.payload.len());
    }

    Ok(())
}
```

## 5. Core concepts

### Topics and patterns

- Topics are **opaque strings** — the server does not parse payload content.
- Subscriptions use **wildcard patterns**: `sensor.*` matches `sensor.temp`,
  `sensor.humid`, etc.
- The default ACL requires **two-segment topics** (`foo.bar`). Single-segment
  topics like `cursor` are silently denied unless the ACL is configured.

### Delivery policies

Each QUIC stream can have its own delivery policy:

| Policy | Use case | Behavior |
|---|---|---|
| `ReliableOrdered` | Command streams, RPC | FIFO, no gaps |
| `LatestOnly` | Sensor sampling (newest only) | Drops old if consumer is slow |
| `RealtimeDropOld` | Live video/audio | Drops oldest to maintain real-time |

```rust
use vireon_sdk::{ClientBuilder, StreamSpec, DeliveryPolicy};

let client = ClientBuilder::new("127.0.0.1:4433").connect().await?;
let mut stream = client.open_stream(
    StreamSpec::new(DeliveryPolicy::RealtimeDropOld)
        .with_topic("video.frame")
).await?;
```

### Connection pool (high-throughput publishing)

A single QUIC connection's command channel caps `try_publish` throughput.
`ClientPool` multiplexes across N connections:

```rust
use vireon_sdk::{ClientBuilder, ClientPool, TlsVerify};

let builder = ClientBuilder::new("127.0.0.1:4433")
        .tls_verify(TlsVerify::DangerAcceptInvalid);

let pool = ClientPool::connect(builder, 4).await?;  // 4 connections

for _ in 0..100_000 {
    pool.try_publish("bench.data", payload)?;
}
```

### Consumer groups

Load-balanced delivery across N consumers (each message delivered to
exactly one member):

```rust
let mut group = client.subscribe_group("jobs.queue", "workers", "worker-1").await?;
while let Some(job) = group.recv().await {
    process(job).await;
}
```

### WAL and persistence

Enable WAL for durable delivery and LogTail mode (high-fan-out):

```toml
wal_root = "/var/lib/vireon/wal"

[wal_sync]
mode = "batched"  # "never" | "batched" | "fsync"
```

### Cluster mode

Run multiple nodes with replication:

```bash
quic-server --node-id 1 --peers "2=127.0.0.1:4434,3=127.0.0.1:4435" \
  --replication-factor 2
```

## 6. Production checklist

- [ ] Replace dev cert with proper CA-signed certificate
- [ ] Enable mTLS (`client_ca` in config)
- [ ] Configure ACL rules (`acl_rules` in config)
- [ ] Set `metrics_port` for Prometheus scraping
- [ ] Configure WAL for durable delivery
- [ ] Set `max_connections` and `max_handshakes_per_ip_per_sec` limits
- [ ] Test graceful drain (`SIGTERM` → wait for pending sends)

## 7. Examples

The [`client-sdk/examples/`](../client-sdk/examples/) directory contains
15 runnable scenarios:

| Example | What it demonstrates |
|---|---|
| `quickstart` | All 3 delivery policies in one run |
| `s07_hol_congestion` | Head-of-line blocking isolation |
| `s11_ordering` | Frame ordering verification |
| `s12_full_limit` | Maximum fan-out throughput |
| `s13_aggregate_throughput` | Multi-connection aggregate bandwidth |
| `s14_pool_multiplex` | ClientPool publish-side multiplexing |
| `s15_consumer_group` | Load-balanced consumer groups |
| `s18_cross_node_fanout` | 3-node cluster cross-node delivery |
| `s19_connection_migration` | QUIC connection migration (network switch) |
| `s20_logtail_delivery` | LogTail (WAL + NotifyOffset) verification |

```bash
cargo run -p vireon-sdk --release --example quickstart
```

## Next steps

- [Architecture](../ARCHITECTURE.md) — layered design and data flow
- [Operations](../OPERATIONS.md) — deployment, tuning, troubleshooting
- [Deployment](../DEPLOYMENT.md) — Docker, systemd, bare metal
- [Security](../SECURITY.md) — threat model, reporting, hardening
- [CHANGELOG](../CHANGELOG.md) — version history
