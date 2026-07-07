# session-registry

[![crates.io](https://img.shields.io/crates/v/session-registry.svg)](https://crates.io/crates/session-registry)
[![docs.rs](https://docs.rs/session-registry/badge.svg)](https://docs.rs/session-registry)

The **Distributed Session Directory** for the Vireon QUIC runtime.
Maps application-level `SessionId` to the cluster node that owns it,
using consistent hashing for uniform distribution and minimal data
movement on topology changes.

## Architecture

```
  SessionID ──► [Consistent Hash Ring] ──► Owner Node
                      │
                      ├─ MembershipList   (node join/leave/suspicion)
                      ├─ OwnershipTable   (SessionId → NodeId + generation)
                      ├─ LeaseManager     (per-session lease with TTL)
                      └─ SessionDirectory (facade)
```

See [DESIGN.md](DESIGN.md) for the full 20-section specification.

## Layer Map

| #  | Module         | Phase | Responsibility |
|----|----------------|-------|----------------|
| 1  | `error`        | 0     | `RegistryError` thiserror enum |
| 2  | `config`       | 0     | `RegistryConfig`, `Preset` |
| 3  | `node`         | 0     | `NodeId`, `NodeInfo`, `NodeState`, `Region`, `Zone` |
| 4  | `version`      | 0     | `Generation`, `Epoch`, `MonotonicVersion` |
| 5  | `lease`        | 0     | `LeaseInfo`, `LeaseState` |
| 6  | `locator`      | 0     | `SessionLocator` (resolved location) |
| 7  | `ownership`    | 0     | `OwnershipEntry`, `OwnershipTable` skeleton |
| 8  | `ring`         | 0     | `ConsistentHash` ring (real impl) |
| 9  | `stats`        | 0     | `RegistryStats` atomics, `StatsSnapshot` |
| 10 | `health`       | 0     | `HealthReport`, `HealthState` |
| 11 | `event`        | 0     | `RegistryEvent` enum |
| 12 | `directory`    | 1     | `SessionDirectory` facade |
| 13 | `membership`   | 1     | `MembershipList` (cluster tracker) |
| 14 | `rebalance`    | 1     | `RebalancePlanner` |
| 15 | `gc`           | 1     | `RegistryGc` (expired lease cleanup) |
| 16 | `builder`      | 0     | `RegistryBuilder` (fluent) |

## Quick Start

```rust
use session_registry::{RegistryBuilder, Preset, NodeId, Region, Zone};

let cfg = RegistryBuilder::from_preset(Preset::MultiRegion, NodeId::new(1))?
    .region(Region::new(1))
    .zone(Zone::new(1))
    .lease_ttl_ms(45_000)
    .build()?;

let mut dir = session_registry::SessionDirectory::new(cfg)?;
let health = dir.health();
println!("Health: {:?}", health.state);
```

## Presets

| Preset | Topology | TTL | Vnodes | Sessions |
|--------|----------|-----|--------|----------|
| `SingleRegion` | 1 region | 30s | 150 | 100 M |
| `MultiRegion` | Multi-region | 45s | 200 | 10 M |
| `Hyperscale` | Many regions | 20s | 256 | 100 M |
| `Edge` | Many small regions | 60s | 100 | 1 M |

## Consistent Hash Ring

The ring uses **virtual nodes** (150 by default) for uniform
distribution. Adding or removing a node only moves ~1/N of sessions:

```rust
use session_registry::{ConsistentHash, NodeId};
use session_manager::SessionId;

let mut ring = ConsistentHash::default();
ring.add_node(NodeId::new(1));
ring.add_node(NodeId::new(2));
ring.add_node(NodeId::new(3));

let owner = ring.lookup(&SessionId::from_pair(42, 99));
assert!(owner.is_some());
```

## Lease-Based Ownership

Every ownership has a bounded TTL. Only one node can hold a live
lease at a time, preventing split-brain:

```rust
use session_registry::{LeaseInfo, NodeId, Generation};

let lease = LeaseInfo::new(
    NodeId::new(1),
    Generation::INITIAL,
    0,           // now_ns
    30_000,      // 30s TTL
);

assert!(!lease.is_expired(0));
assert!(lease.is_expired(31_000_000_000));
assert_eq!(lease.state(0), LeaseState::Active);
```

## Phase 0 Status

Modules 1–11 are **real implementations** (146 unit tests).
Modules 12–16 ship as typed skeletons. Functional directory,
membership, and lease renewal land in Phase 1+.

## Dependencies

| Crate | Purpose |
|-------|---------|
| `session-manager` | `SessionId` |
| `connection-shard` | `ConnectionId` |
| `thiserror` | Typed errors |
| `tracing` | Structured logging |

## License

MIT OR Apache-2.0
