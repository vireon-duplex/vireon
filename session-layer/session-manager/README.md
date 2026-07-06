# session-manager

The **Session Orchestrator** for the Vireon QUIC runtime. Binds
application-level sessions to QUIC connections, streams, and reactor
cores. Not just CRUD — this crate coordinates authentication,
authorization, resource quotas, connection migration, and
end-to-end backpressure.

## Architecture (one-line)

```text
Application ─► [Session Manager] ─► Stream Manager / Transport
                     │
                     ├─ SessionTable      (slab + index)
                     ├─ SessionRegistry    (local lookup)
                     ├─ SessionRouter      (SessionId → Connection → Core)
                     ├─ AuthenticationPipeline (TLS/JWT/OAuth)
                     ├─ AuthorizationSystem    (RBAC/ABAC)
                     ├─ MigrationManager   (QUIC path migration)
                     ├─ SessionScheduler   (per-class scheduling)
                     ├─ ResourceManager    (quotas + token bucket)
                     ├─ BackpressureTracker(end-to-end)
                     └─ SessionLifecycle   (10-state FSM)
```

See [`DESIGN.md`](./DESIGN.md) for the full 20-part specification.

## Layer map

| #  | Module          | Phase | Responsibility |
|----|-----------------|-------|----------------|
| 1  | `error`         | 0 ✅  | `SessionManagerError` thiserror enum |
| 2  | `config`        | 0 ✅  | `SessionManagerConfig`, `Preset` |
| 3  | `id`            | 0 ✅  | `SessionId` (128-bit application-level) |
| 4  | `state`         | 0 ✅  | `SessionState` (10-state FSM) |
| 5  | `priority`      | 0 ✅  | `SessionClass` (Latency/Interactive/Streaming/Bulk) |
| 6  | `quota`         | 0 ✅  | `ResourceQuota` (memory/buffer/stream/bandwidth) |
| 7  | `stats`         | 0 ✅  | `SessionStats` atomics, `StatsSnapshot` |
| 8  | `health`        | 0 ✅  | `HealthReport`, `HealthState` |
| 9  | `session`       | 1    | `Session` struct (metadata + state) |
| 10 | `table`         | 1    | `SessionTable` (slab + index) |
| 11 | `registry`      | 1    | `SessionRegistry` (local lookup) |
| 12 | `routing`       | 1    | `SessionRouter` (SessionId → Connection → Core) |
| 13 | `auth`          | 2    | `AuthenticationPipeline` (TLS/JWT/OAuth) |
| 14 | `authz`         | 2    | `AuthorizationSystem` (RBAC/ABAC) |
| 15 | `migration`     | 3    | `MigrationManager` (QUIC migration) |
| 16 | `scheduler`     | 2    | `SessionScheduler` (per-class) |
| 17 | `resource`      | 2    | `ResourceManager` (token bucket) |
| 18 | `backpressure`  | 3    | `BackpressureTracker` (end-to-end) |
| 19 | `lifecycle`     | 3    | `SessionLifecycle` (FSM controller) |
| 20 | `event`         | 1    | `SessionEvent` enum |
| 21 | `builder`       | 1    | `SessionManagerBuilder` (fluent) |
| 22 | `manager`       | 1    | `SessionManager` facade |

## Quick start (Phase 0)

```rust
use session_manager::{SessionManager, SessionManagerConfig, Preset};

// From preset
let cfg = Preset::Hyperscale.to_config();

// Or build custom
use session_manager::SessionManagerBuilder;
let cfg = SessionManagerBuilder::new()
    .max_sessions(10_000)
    .default_class(session_manager::SessionClass::Streaming)
    .config();

let mgr = SessionManager::new(cfg).expect("valid config");
println!("health: {:?}", mgr.health());
```

## Session identity (three layers)

| Identifier | Layer | Size | Purpose |
|------------|-------|------|---------|
| `SessionId`    | Application | 128-bit (`u64`+`u64`) | Stable across reconnects, unguessable |
| `ConnectionId` | Runtime     | 64-bit (slot+gen)     | Per-connection within a shard |
| `QuicCid`      | Protocol    | 1–20 bytes            | QUIC wire format (RFC 9000 §17.2) |

## 10-state session FSM

```text
  ┌──────────┐  handshake  ┌────────────────┐  auth ok  ┌───────┐
  │ Creating │ ──────────► │ Authenticating │ ────────► │ Ready │
  └──────────┘             └────────────────┘           └───┬───┘
       │ auth fail              │                           │ activate
       ▼                        ▼                           ▼
  ┌──────────┐             ┌──────┐         ┌───────┐  ┌────────┐
  │Destroyed │             │Closed│         │ Idle  │◄─┤ Active │
  └──────────┘             └──────┘         └───┬───┘  └────┬───┘
                                            timeout│          │ migrate
                                                   ▼          ▼
                                              ┌────────┐  ┌──────────┐
                                              │ Active │  │Migrating │
                                              └────────┘  └────┬─────┘
                                                              │ ok
                                                              ▼
                                                         ┌────────┐
                                                         │ Active │
                                                         └────────┘
```

Plus `Draining` and `Closing` for graceful shutdown.

## Session classes

| Class            | Urgency | Weight | Incremental | Example |
|------------------|---------|--------|-------------|---------|
| LatencySensitive | 0–1     | 8      | ✗           | Trading, gaming, RPC |
| Interactive      | 2–3     | 4      | ✗           | Chat, web, APIs |
| Streaming        | 4–5     | 2      | ✓           | Video, bulk file |
| Bulk             | 6–7     | 1      | ✗           | Backups, replication |

## Presets

| Preset | Sessions | Default Class | Resume | Migration |
|--------|----------|---------------|--------|-----------|
| `CloudRpc`         | 100K  | Interactive     | ✓ | ✓ |
| `MessagingGateway` | 500K  | Interactive     | ✓ | ✓ |
| `RealTime`         | 10K   | LatencySensitive| ✗ | ✓ |
| `Hyperscale`       | 1M    | Streaming       | ✓ | ✓ |
| `Edge`             | 1K    | Interactive     | ✓ | ✗ |

## Bench inventory

| Bench | Target |
|-------|--------|
| `lookup`    | `SessionTable::get(id)` throughput |
| `lifecycle` | create → activate → idle → destroy cycle |
| `quota`     | `try_acquire_stream()` / `try_acquire_memory()` throughput |
| `routing`   | `SessionRouter::resolve(id)` throughput |

All four ship as Phase-0 stubs (compile, no measurement).

## Status

Phase 0 — **architecture contract** + type system + skeletons.
Functional table + auth + migration land in Phases 1–3+.
