# quic-engine

Production-grade **QUIC Transport Layer** for the Vireon runtime.
Wraps Cloudflare's [`quiche`] for the protocol core (TLS 1.3, AEAD,
packet parsing, loss recovery, congestion control, ACK processing,
flow control, stream scheduling) and adds the Vireon-native
integration substrate: CID-keyed connection table, io_uring-native
packet I/O, GSO batching, pacing, path validation, timer binding,
and thread-per-core reactor integration.

[`quiche`]: https://github.com/cloudflare/quiche

## Status — Phase 0 (architecture review + skeleton)

This crate ships **only the architecture contract** and the module
skeleton. Functional QUIC behavior lands in Phases 1–15 as defined
in [`DESIGN.md`](DESIGN.md). Each stub module declares its target
phase and links back to the corresponding DESIGN.md section.

| Phase | Subsystem | Primary modules |
|-------|-----------|-----------------|
| 0 | Architecture review + skeleton | `DESIGN.md`, `lib.rs`, `error.rs`, `config.rs` |
| 1 | Connection Manager | `cid`, `token`, `conn_id_table`, `connection`, `state` |
| 2 | Packet Engine | `packet` |
| 3 | Crypto Engine | `crypto` |
| 4 | Loss Recovery | `loss_recovery` |
| 5 | Congestion Control | `congestion` |
| 6 | ACK Manager | `ack_manager` |
| 7 | Flow Control | `flow_control` |
| 8 | Stream Manager | `stream`, `stream_scheduler` |
| 9 | Frame Engine | `frame` |
| 10 | Packet Scheduler | `packet_scheduler`, `pacing`, `gso` |
| 11 | Timer System | `timer_binding` |
| 12 | Path Manager | `path`, `migration` |
| 13 | Runtime Integration | `shard_binding`, `engine`, `builder` |
| 14 | Observability | `stats`, `health` |
| 15 | Production Readiness Review | cross-cutting |

## Architecture

```text
+--------------------------------------------------------------------+
|                  QuicEngine (Reactor Core N)                       |
|                                                                    |
|  ── Ingress (per CQE from cqe-processor) ───────────────────────   |
|  UDP datagram ──► PacketRouter ──► ConnIdTable ──► QuicConnection  |
|                                                       │            |
|                                          quiche::Connection::recv  |
|                                                       │            |
|                                          Stream events surfaced     |
|                                                       ▼            |
|                                          Application dispatcher     |
|                                                                    |
|  ── Egress (per tick) ──────────────────────────────────────────   |
|  Tick fires ──► PacketScheduler ──► StreamScheduler ──► Pacer     |
|                                                       │            |
|                                          quiche::Connection::send   |
|                                                       │            |
|                                          GsoBatcher coalesces      |
|                                                       │            |
|                                          send_queue.submit_gso()   |
|                                                                    |
|  ── Timeouts (from timer-wheel) ────────────────────────────────   |
|  timer_fire ──► TimerBinding ──► QuicConnection.timeout()         |
|                                                                    |
|  ── Per-connection state ──────────────────────────────────────   |
|  slab: Vec<QuicConnection> (cache-aligned, 64 B)                  |
|  Each QuicConnection owns:                                        |
|    • quiche::Connection (heap)                                    |
|    • LossRecovery, CongestionController, FlowController           |
|    • PathManager, StreamScheduler, TimerBinding                   |
|    • ShardBinding (cross-ref to runtime-layer ConnectionShard)    |
|    • StatsCollector handle                                        |
+--------------------------------------------------------------------+
```

See [`DESIGN.md`](DESIGN.md) §2 for the full Vireon-stack diagram
and §5 for the per-packet hot-path breakdown.

## Quick start (target API — Phase 13+)

```no_run
use quic_engine::{QuicEngineBuilder, QuicPreset};

// One engine per Reactor core (thread-per-core, shared-nothing).
let mut engine = QuicEngineBuilder::from_preset(QuicPreset::BareMetal)
    .reactor_id(0)
    .max_connections(8192)
    .build()
    .expect("engine");
```

The snippet above compiles today (Phase 0) but `engine` is a
placeholder — there is no UDP socket, no listeners, no streams
until Phase 13.

## Layer map

| Layer | Module | Phase | Responsibility |
|-------|--------|-------|----------------|
| 1 | [`error`]               | —   | `QuicError` enum + `is_fatal` / `is_retryable` |
| 2 | [`config`]              | —   | `QuicConfig`, `QuicPreset`, capacity / timeout constants |
| 3 | [`cid`]                 | 1   | `QuicCid` — connection identifier + rotation policy |
| 4 | [`token`]               | 1   | `StatelessResetToken`, `RetryToken` |
| 5 | [`conn_id_table`]       | 1   | `ConnIdTable` — CID → QuicConnection lookup |
| 6 | [`connection`]          | 1   | `QuicConnection` — wraps `quiche::Connection` |
| 7 | [`state`]               | 1   | `QuicConnState` — handshake / connected / closing / draining |
| 8 | [`packet`]              | 2   | `PacketRouter` — long / short header routing, coalescing |
| 9 | [`crypto`]              | 3   | `CryptoContext` — TLS 1.3 secrets handoff, keylog, 0-RTT |
| 10 | [`loss_recovery`]      | 4   | `LossRecovery` — wraps quiche loss detection (RFC 9002 §6) |
| 11 | [`congestion`]         | 5   | `CongestionController` — CUBIC / Reno / BBR / BBRv2 + ECN |
| 12 | [`ack_manager`]        | 6   | `AckManager` — delayed ACK, ACK Frequency extension |
| 13 | [`flow_control`]       | 7   | `FlowController` — MAX_DATA / MAX_STREAM_DATA / blocked |
| 14 | [`stream`]             | 8   | `Stream`, `StreamId` — bidirectional / unidirectional |
| 15 | [`stream_scheduler`]   | 8   | `StreamScheduler` — priority + fair queuing across streams |
| 16 | [`frame`]              | 9   | `Frame` — extension frame policy, DATAGRAM (RFC 9221) |
| 17 | [`packet_scheduler`]   | 10  | `PacketScheduler` — egress ordering: ACK > probe > app |
| 18 | [`pacing`]             | 10  | `Pacer` — token-bucket integration with `send-queue` |
| 19 | [`gso`]                | 10  | `GsoBatcher` — `UDP_SEGMENT` batching |
| 20 | [`timer_binding`]      | 11  | `TimerBinding` — quiche timeout → `timer-wheel` |
| 21 | [`path`]               | 12  | `PathManager` — PATH_CHALLENGE / RESPONSE, NAT rebinding |
| 22 | [`migration`]          | 12  | `Migration` — connection migration + multipath hooks |
| 23 | [`shard_binding`]      | 13  | `ShardBinding` — QuicConnection ↔ `connection_shard::ConnectionShard` |
| 24 | [`engine`]             | 13  | `QuicEngine` facade — one per Reactor core |
| 25 | [`builder`]            | 13  | `QuicEngineBuilder` (fluent + `from_preset`) |
| 26 | [`stats`]              | 14  | `StatsCollector` (atomic), `StatsSnapshot` (`Copy`) |
| 27 | [`health`]             | 14  | `HealthReport`, `HealthState` |

## Relationship to other Vireon crates

| Crate | Role |
|-------|------|
| `runtime-layer/cqe-processor` | Drains io_uring CQEs and delivers UDP datagrams to the engine's `on_recv`. |
| `runtime-layer/send-queue`    | Accepts GSO-batched outgoing packets for the next `sendmmsg` submission. |
| `runtime-layer/timer-wheel`   | Drives per-connection loss / idle / handshake / keepalive timers. |
| `runtime-layer/buffer-pool`   | Allocates zero-alloc receive and send buffers. |
| `runtime-layer/task-queue`    | Schedules deferred work (e.g. migration handoff completion). |
| `runtime-layer/connection-shard` | Owns the connection lifecycle / ownership mapping; reconciled via `shard_binding`. |
| `kernel-layer/kernel-nic`     | Bottom of the stack — never touched directly by the engine. |

## Design choices

- **Wrap quiche.** TLS, AEAD, packet parsing, loss recovery, CC,
  ACK, flow control, stream scheduling all live in `quiche`. Vireon
  builds the production integration substrate. Matches the Cloudflare
  server pattern and avoids re-implementing audited crypto.
- **Thread-per-core, shared-nothing.** One `QuicEngine` per Reactor
  core. No `Arc<Mutex<...>>` on the hot path. Stats counters are the
  only cross-thread coordination point (atomic, monitor-readable).
- **io_uring native.** All UDP I/O flows through `cqe-processor` and
  `send-queue`. `mio` is used only for socket construction and the
  engine's `Waker` for shutdown coordination.
- **8-byte server CIDs.** Enables inline hash keys in the CID table,
  u64 hash with no length-prefix branch, and SIMD-friendly compare.
- **Cache-aligned connection slab.** `QuicConnection` is
  `#[repr(C, align(64))]` so adjacent slots never share a cache line.
- **Explicit migration.** No shared queue, no coordinator.
  Source serializes, target deserializes.
- **RFC extension hook points.** DATAGRAM (RFC 9221), ACK
  Frequency, Multipath (RFC 9464), BBRv2 — hook points designed
  now, impl deferred to their phases per DESIGN.md §12.
- **Zero allocation hot path.** Receive buffer from `buffer-pool`,
  returned on send CQE. All connection state in pre-allocated slab.
  No `Vec::push` on the per-packet path.

## RFC compliance

| RFC | Status |
|-----|--------|
| RFC 9000 — QUIC Transport | Required (Phases 1–13) |
| RFC 9001 — TLS for QUIC | Required (Phase 3, via quiche+BoringSSL) |
| RFC 9002 — Loss Detection & CC | Required (Phases 4–5, via quiche) |
| RFC 9221 — DATAGRAM | Designed, impl Phase 9 |
| RFC 9287 — Grease Bit | quiche default-on |
| RFC 9464 — Multipath | Hook points Phase 12 |
| draft-ietf-quic-ack-frequency | Hook points Phase 6 |
| draft-ietf-quic-bbrv2-congestion-control | Plugin hook Phase 5 |

See [`DESIGN.md`](DESIGN.md) §3 for the section-by-section compliance
matrix.

## Benchmarks

The crate ships a 5-binary Criterion suite. Phase 0 ships only
skeletons; bench bodies land alongside each phase's implementation.

```sh
cargo bench -p quic-engine --no-run                  # compile all benches
cargo bench -p quic-engine --bench cid_lookup        # Phase 1
cargo bench -p quic-engine --bench packet            # Phase 2
cargo bench -p quic-engine --bench stream            # Phase 8
cargo bench -p quic-engine --bench scheduler         # Phase 10
cargo bench -p quic-engine --bench migration         # Phase 12
```

### Bench inventory

| Binary | Phase | Concern |
|--------|-------|---------|
| `cid_lookup` | 1   | `ConnIdTable` lookup at 10K / 100K / 1M fill |
| `packet`     | 2   | `PacketRouter` latency + coalescing |
| `stream`     | 8   | `stream_send` / `recv` + scheduler next_ready |
| `scheduler`  | 10  | `PacketScheduler` + `Pacer` + `GsoBatcher` |
| `migration`  | 12  | `PathManager` validation round-trip |

## Target performance numbers (per DESIGN.md §1.2)

| Metric | Target |
|--------|--------|
| Concurrent connections / host | 1 M (10 M with multi-tier) |
| Handshake latency (p99 LAN) | < 50 ms |
| Steady-state throughput | 80 Gbps on 100 GbE |
| Packet processing latency (p99) | < 50 µs |
| Connection setup rate | 100K / sec / core |
| Memory / connection (steady) | ≤ 4 KiB resident |

## Further reading

- [`DESIGN.md`](DESIGN.md) — full architecture review (RFC compliance
  matrix, per-subsystem specs, hot-path analysis, scalability review
  from 10K → 10M connections, production readiness scorecard,
  comparison against quiche / MsQuic / Google / ngtcp2 / quinn /
  aioquic, 15-phase roadmap).
- RFC 9000, RFC 9001, RFC 9002 — base QUIC specs.
- RFC 9221, RFC 9287, RFC 9464 — extension RFCs covered.

[`error`]: src/error.rs
[`config`]: src/config.rs
[`cid`]: src/cid.rs
[`token`]: src/token.rs
[`conn_id_table`]: src/conn_id_table.rs
[`connection`]: src/connection.rs
[`state`]: src/state.rs
[`packet`]: src/packet.rs
[`crypto`]: src/crypto.rs
[`loss_recovery`]: src/loss_recovery.rs
[`congestion`]: src/congestion.rs
[`ack_manager`]: src/ack_manager.rs
[`flow_control`]: src/flow_control.rs
[`stream`]: src/stream.rs
[`stream_scheduler`]: src/stream_scheduler.rs
[`frame`]: src/frame.rs
[`packet_scheduler`]: src/packet_scheduler.rs
[`pacing`]: src/pacing.rs
[`gso`]: src/gso.rs
[`timer_binding`]: src/timer_binding.rs
[`path`]: src/path.rs
[`migration`]: src/migration.rs
[`shard_binding`]: src/shard_binding.rs
[`engine`]: src/engine.rs
[`builder`]: src/builder.rs
[`stats`]: src/stats.rs
[`health`]: src/health.rs
