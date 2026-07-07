# session-replication-service

[![crates.io](https://img.shields.io/crates/v/session-replication-service.svg)](https://crates.io/crates/session-replication-service)
[![docs.rs](https://docs.rs/session-replication-service/badge.svg)](https://docs.rs/session-replication-service)

The **Metadata Replication Service** for the Vireon QUIC runtime.
Provides high-availability replication of session-layer metadata
using Gossip, Raft, CRDT, or Sync strategies with conflict detection
and quorum management.

## Architecture

```
  SessionMetadata ──► [ReplicationStrategy] ──► Replicas
                              │
                              ├─ GossipEngine   (eventual consistency)
                              ├─ RaftEngine     (strong consistency)
                              ├─ CrdtEngine     (conflict-free)
                              └─ SyncEngine     (synchronous)
```

See [DESIGN.md](DESIGN.md) for the full 20-section specification.

## Layer Map

| #  | Module         | Phase | Responsibility |
|----|----------------|-------|----------------|
| 1  | `error`        | 0     | `ReplicationError` thiserror enum |
| 2  | `config`       | 0     | `ReplicationConfig`, `Preset` |
| 3  | `strategy`     | 0     | `ReplicationStrategy`, `ConsistencyLevel` |
| 4  | `replica`      | 0     | `ReplicaId`, `ReplicaInfo` |
| 5  | `liveness`     | 0     | `ReplicaLiveness`, `ReplicaHealth` |
| 6  | `stats`        | 0     | `ReplicationStats` atomics, `StatsSnapshot` |
| 7  | `health`       | 0     | `HealthReport`, `HealthState` |
| 8  | `event`        | 0     | `ReplicationEvent` enum |
| 9  | `builder`      | 0     | `ReplicationServiceBuilder` (fluent) |
| 10 | `gossip`       | 1     | `GossipEngine` skeleton |
| 11 | `raft`         | 1     | `RaftEngine` skeleton |
| 12 | `crdt`         | 1     | `CrdtEngine` skeleton |
| 13 | `sync`         | 1     | `SyncEngine` skeleton |
| 14 | `conflict`     | 1     | `ConflictResolver` skeleton |

## Quick Start

```rust
use session_replication_service::{
    ReplicationServiceBuilder, Preset, ReplicationStrategy, ConsistencyLevel,
};

let cfg = ReplicationServiceBuilder::from_preset(Preset::StrongConsistency)?
    .strategy(ReplicationStrategy::Raft)
    .consistency(ConsistencyLevel::Quorum)
    .replica_count(5)
    .write_timeout_ms(3_000)
    .build()?;

// Use cfg to initialize the replication engine (Phase 1+).
```

## Presets

| Preset | Strategy | Consistency | Replicas | Use Case |
|--------|----------|-------------|----------|----------|
| `StrongConsistency` | Raft | Quorum | 3 | Critical metadata |
| `EventuallyConsistent` | Gossip | One | 5 | Edge / IoT |
| `Hybrid` | CRDT | Quorum | 3 | Multi-region balance |
| `EdgeReplication` | Gossip | One | 7 | Constrained networks |

## Strategy Comparison

| Dimension | Gossip | Raft | CRDT | Sync |
|-----------|--------|------|------|------|
| Latency | Low | Medium | Low | Medium |
| Consistency | Eventual | Strong | Eventual | Strong |
| Availability | Maximum | Medium | High | Low |
| Conflicts | Possible | None | None | None |
| Scalability | 100+ | 3–9 | 50+ | 2–5 |

**Default recommendation**: Raft for session metadata (strong
consistency). CRDT for derived data. Gossip for edge deployments.

## ReplicaId

`ReplicaId` wraps `NodeId` to distinguish replica identity from
node identity at the type level:

```rust
use session_replication_service::ReplicaId;
use session_registry::NodeId;

let node = NodeId::new(42);
let replica = ReplicaId::from_node(node);
assert_eq!(replica.raw(), 42);
assert_eq!(replica.to_node().raw(), 42);
```

## Consistency Levels

```rust
use session_replication_service::ConsistencyLevel;

// Quorum: majority of replicas must ack.
assert_eq!(ConsistencyLevel::Quorum.min_acks(3), 2);
assert_eq!(ConsistencyLevel::Quorum.min_acks(5), 3);

// One: only one replica needs to ack.
assert_eq!(ConsistencyLevel::One.min_acks(5), 1);

// All: every replica must ack.
assert_eq!(ConsistencyLevel::All.min_acks(3), 3);

// Stale reads allowed only at One.
assert!(ConsistencyLevel::One.allows_stale_reads());
assert!(!ConsistencyLevel::Quorum.allows_stale_reads());
```

## Observability

```rust
use session_replication_service::{ReplicationStats, HealthReport};

let stats = ReplicationStats::new();
stats.record_success();
stats.record_success();
stats.record_failure();
stats.set_lag_ns(1_000_000);

let snapshot = stats.snapshot();
assert_eq!(snapshot.success_rate_bps(), 6_666); // 2/3 = 66.66%

let health = HealthReport::derive(&snapshot);
// health.state tells you Healthy / Degraded / Critical
```

## Phase 0 Status

Modules 1–9 are **real implementations** (~85 unit tests).
Modules 10–14 ship as typed skeletons. Functional engines land in
Phase 1+.

## Dependencies

| Crate | Purpose |
|-------|---------|
| `session-registry` | `NodeId`, `Region`, `Zone`, `Epoch`, `Generation` |
| `session-manager` | `SessionId` |
| `thiserror` | Typed errors |
| `tracing` | Structured logging |

## License

MIT OR Apache-2.0
