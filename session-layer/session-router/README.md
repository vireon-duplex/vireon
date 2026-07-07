# session-router

The **Session Routing Layer** for the Vireon QUIC runtime. Resolves a
`SessionId` to a `RoutingDecision` — the next hop for a datagram: local
delivery, remote forward, anycast, broadcast, or multicast group fan-out.

## Layer Map

| #  | Module        | Phase | Responsibility                              |
|----|---------------|-------|---------------------------------------------|
| 1  | `error`       | 0     | `RouterError` thiserror enum (9 variants)   |
| 2  | `config`      | 0     | `RouterConfig`, `Preset`                    |
| 3  | `decision`    | 0     | `RoutingDecision`, `RoutingMode`, `MulticastGroup` |
| 4  | `endpoint`    | 0     | `NodeEndpoint`, `EndpointProtocol`          |
| 5  | `cache`       | 0     | `RoutingCacheStats`, `RoutingCache` skeleton|
| 6  | `stats`       | 0     | `RouterStats` atomics, `StatsSnapshot`      |
| 7  | `health`      | 0     | `HealthReport`, `HealthState`               |
| 8  | `local`       | 1     | `LocalRouter`                               |
| 9  | `remote`      | 1     | `RemoteRouter`                              |
| 10 | `anycast`     | 1     | `AnycastRouter`                             |
| 11 | `broadcast`   | 1     | `BroadcastRouter`                           |
| 12 | `multicast`   | 1     | `MulticastRouter`                           |
| 13 | `resolver`    | 1     | `RouteResolver` facade                      |
| 14 | `builder`     | 1     | `RouterBuilder`                             |
| 15 | `event`       | 1     | `RouterEvent` enum                          |

## Presets

| Preset       | Cache TTL | Max Entries | RPC Timeout | Retries | Use Case                    |
|--------------|-----------|-------------|-------------|---------|-----------------------------|
| `LowLatency` | 5 s       | 10 K        | 2 ms        | 0       | Gaming, real-time bidding   |
| `Balanced`   | 15 s      | 50 K        | 10 ms       | 2       | General-purpose production  |
| `CacheHeavy` | 60 s      | 500 K       | 20 ms       | 3       | Read-heavy, stable sessions |
| `Cacheless`  | 0 (off)   | 0           | 50 ms       | 1       | High-churn environments     |

## Quick Start

```rust
use session_router::{RouterBuilder, Preset, RoutingDecision};

// Build from preset.
let resolver = RouterBuilder::from_preset(Preset::Balanced)
    .cache_ttl_ms(20_000)
    .rpc_timeout_ms(5)
    .build_resolver()?;

// Resolve a session.
let decision = resolver.resolve(session_id)?;
match decision {
    RoutingDecision::Local => { /* deliver in-process */ }
    RoutingDecision::Remote(node) => { /* forward to node */ }
    RoutingDecision::Anycast => { /* anycast selected */ }
    RoutingDecision::Broadcast => { /* fan-out to all */ }
    RoutingDecision::Multicast(group) => { /* fan-out to group */ }
}

// Check health.
let health = resolver.health();
if health.needs_attention() {
    // alert operations
}
```

## Routing Decision Examples

```rust
// Local: this node owns the session.
let d = RoutingDecision::Local;
assert!(d.is_local());
assert!(!d.requires_rpc());

// Remote: forward to a specific node.
let d = RoutingDecision::Remote(NodeId::new(7));
assert!(d.is_remote());
assert_eq!(d.target_node(), Some(NodeId::new(7)));

// Anycast: pick the healthiest node.
let d = RoutingDecision::Anycast;
assert!(d.requires_rpc());

// Multicast: fan-out to a group.
let d = RoutingDecision::Multicast(MulticastGroup::new(5));
assert!(d.requires_rpc());
```

## Phase 0 Status

Modules 1-7 are fully implemented (real atomic counters, health derivation,
config validation, error classification). Modules 8-15 ship as typed skeletons
returning stub errors. Functional routing (cache, registry integration, RPC)
lands in Phase 1+.

**Test count**: ~60 unit tests across 15 modules.

## Dependencies

| Dependency         | Source             | Purpose                      |
|--------------------|--------------------|------------------------------|
| `session-manager`  | workspace path     | `SessionId` type             |
| `session-registry` | workspace path     | `NodeId`, `NodeInfo`, ring   |
| `connection-shard` | workspace path     | `ConnectionId` re-export     |
| `thiserror`        | workspace          | Error derive macro           |
| `tracing`          | workspace          | Structured logging           |
| `criterion`        | dev-dependency     | Benchmark framework          |

## Verification

```bash
cargo check
cargo clippy -- -D warnings
cargo fmt --check
cargo test
cargo bench --no-run
```

See `DESIGN.md` for the full 17-section specification.
