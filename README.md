<div align="center">
  <h1>Vireon</h1>

  <p>
    <strong>A QUIC-native pub/sub runtime with per-stream delivery semantics,
    WAL persistence, consumer groups, and cluster mode.</strong>
  </p>

  <p>
    <a href="https://github.com/vireon-duplex/Vireon/actions"><img src="https://img.shields.io/github/actions/workflow/status/vireon-duplex/Vireon/ci.yml?style=flat-square" alt="Build Status" /></a>
    <a href="https://github.com/vireon-duplex/Vireon/releases"><img src="https://img.shields.io/github/v/release/vireon-duplex/Vireon?style=flat-square" alt="Release" /></a>
    <a href="https://github.com/vireon-duplex/Vireon/pkgs/container/vireon"><img src="https://img.shields.io/badge/docker-ghcr.io-blue.svg?style=flat-square" alt="Docker" /></a>
    <a href="./LICENSE"><img src="https://img.shields.io/badge/license-AGPL--3.0-blue.svg?style=flat-square" alt="License" /></a>
    <a href="https://rust-lang.org"><img src="https://img.shields.io/badge/Rust-1.85+-orange.svg?style=flat-square" alt="Rust Version" /></a>
  </p>
</div>

---

## About

Vireon is a pub/sub runtime built in Rust on **QUIC** (`quiche`) and **io_uring**.
Each subscriber opens a dedicated QUIC stream with its own flow-control and
loss-recovery window — a congested topic never blocks lighter topics. The
server is a single binary (`quic-server`); clients connect via the Rust SDK
(`vireon-sdk`) or one of 17 language bindings.

## Install

### Option A — Docker (fastest)

```bash
docker pull ghcr.io/vireon-duplex/vireon:latest
docker run --rm -p 4433:4433/udp -p 9090:9090/tcp \
  -v "$PWD/cert.pem:/cert.pem:ro" \
  -v "$PWD/key.pem:/key.pem:ro" \
  ghcr.io/vireon-duplex/vireon:latest \
  --cert /cert.pem --key /key.pem --port 4433
```

### Option B — System install (bare-metal, like nginx/redis)

Build, then install as a systemd service:

```bash
cargo build --release -p quic-server
sudo ./scripts/install.sh --gen-cert   # installs binary + config + service + dev cert
sudo systemctl enable --now vireon
```

Or without the dev cert (use your own TLS certs):

```bash
sudo make install
```

This creates a `vireon` user, installs to `/usr/local/bin/`, `/etc/vireon/`,
`/var/lib/vireon/`, and registers `vireon.service`. Uninstall with
`sudo make uninstall`.

### Option C — Prebuilt binary

Download `quic-server-linux-amd64` from the
[Releases page](https://github.com/vireon-duplex/Vireon/releases), then:

```bash
./quic-server --cert cert.pem --key key.pem --port 4433
```

### Option D — Build from source

Requires Rust 1.85+ and Linux kernel 5.6+ (io_uring). The `quiche` build
needs `cmake`, `golang`, `ninja-build`, `perl`, `python3`, `pkg-config`.

```bash
git clone https://github.com/vireon-duplex/Vireon.git
cd Vireon
cargo build --release
# Binary: target/release/quic-server
```

## Quick start

```bash
# Generate a dev cert (skip if you used --gen-cert above)
openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem \
  -days 365 -nodes -subj "/CN=localhost"

# Start the server
quic-server --cert cert.pem --key key.pem --port 4433

# Run an example (separate terminal)
cargo run -p vireon-sdk --release --example quickstart
```

For the full tutorial see [`docs/getting-started.md`](./docs/getting-started.md).

## Configuration

Use a TOML config for anything beyond quick testing:

```toml
# vireon.conf
bind = "0.0.0.0"
port = 4433
cert = "/etc/vireon/cert.pem"
key  = "/etc/vireon/key.pem"
mode = "multi"          # "single" | "multi"
workers = 0             # 0 = auto
metrics_port = 9090     # Prometheus /metrics
wal_root = "/var/lib/vireon/wal"
[wal_sync]
mode = "batched"
```

```bash
quic-server --config vireon.conf
```

Key CLI flags (override config file):

| Flag | Purpose |
|---|---|
| `--cert` / `--key` | TLS certificate + private key |
| `--client-ca` | Enable mTLS (require client certs) |
| `--mode single\|multi` | Worker thread model |
| `--workers N` | Worker count (0 = auto) |
| `--wal-root PATH` | Enable WAL at this directory |
| `--metrics-port N` | Prometheus scrape port |
| `--cluster-node-id N` | Cluster node identity (enables cluster mode) |
| `--cluster-peers MAP` | `id=host:port,id=host:port,...` |
| `--skip-crc` / `--enforce-crc` | Toggle CRC32C verification (default skip) |
| `--config FILE` | Load TOML config |

Run `quic-server --help` for the full list.

## Examples

Runnable scenarios in [`client-sdk/examples/`](./client-sdk/examples/) — each
auto-spawns its own server on an ephemeral port:

`quickstart` · `s07_hol_congestion` · `s09_reconnect` · `s10_mtls` ·
`s11_ordering` · `s14_pool_multiplex` · `s15_consumer_group` ·
`s16_cluster_replication` · `s19_connection_migration` · `s20_logtail_delivery`

```bash
cargo run -p vireon-sdk --release --example <name>
```

## Further reading

- [Architecture](./ARCHITECTURE.md) — layered design and crate breakdown
- [Operations](./OPERATIONS.md) — production runbook
- [Deployment](./DEPLOYMENT.md) — cluster topology, capacity planning
- [Contributing](./CONTRIBUTING.md) · [Security](./SECURITY.md) · [Testing](./TESTING.md)

## License

GNU Affero General Public License v3.0 (AGPL-3.0). See [`LICENSE`](./LICENSE).
