# session-migration-manager

[![crates.io](https://img.shields.io/crates/v/session-migration-manager.svg)](https://crates.io/crates/session-migration-manager)
[![docs.rs](https://docs.rs/session-migration-manager/badge.svg)](https://docs.rs/session-migration-manager)

The **Cross-Node Session Migration Coordinator** for the Vireon QUIC
runtime. Coordinates the migration of active sessions between cluster
nodes with zero message loss, using a four-phase protocol:

```
  Initiate -> Snapshot -> Transfer -> Restore -> Finalize
```

## Architecture

```
  Operator / Rebalancer
          |
          v
  +---------------------+
  | MigrationCoordinator|
  +---------+-----------+
            |
            +- MigrationPlan      (source -> target + deadline + flags)
            +- MigrationPhase     (state machine)
            +- SnapshotMetadata   (session state descriptor)
            +- StateTransfer      (chunked/streaming transfer)
            +- StateRestore       (reconstruct on target)
            +- OrderingPreserver  (sequence + replay buffer)
            +- CheckpointManager  (crash recovery)
            +- MigrationStats     (observability)
```

See [DESIGN.md](DESIGN.md) for the full 20-section specification.

## Layer Map

| #  | Module        | Phase | Responsibility |
|----|---------------|-------|----------------|
| 1  | `error`       | 0     | `MigrationError` thiserror enum |
| 2  | `config`      | 0     | `MigrationConfig`, `Preset` |
| 3  | `phase`       | 0     | `MigrationPhase` state machine |
| 4  | `plan`        | 0     | `MigrationPlan`, `MigrationFlags` |
| 5  | `snapshot`    | 0     | `SnapshotMetadata`, `MigrationSnapshotRef` |
| 6  | `coordinator` | 0     | `MigrationRecord`, `MigrationCoordinator` skeleton |
| 7  | `stats`       | 0     | `MigrationStats` atomics, `StatsSnapshot` |
| 8  | `health`      | 0     | `HealthReport`, `HealthState` |
| 9  | `event`       | 0     | `MigrationEvent` enum |
| 10 | `builder`     | 0     | `MigrationServiceBuilder` (fluent) |
| 11 | `transfer`    | 1     | `StateTransfer` (chunked/streaming) |
| 12 | `restore`     | 1     | `StateRestore` (reconstruct) |
| 13 | `ordering`    | 1     | `OrderingPreserver` (sequence + replay) |
| 14 | `checkpoint`  | 1     | `CheckpointManager` (crash recovery) |

## Quick Start

```rust
use session_migration_manager::{
    MigrationServiceBuilder, MigrationPlan, MigrationFlags, Preset,
};
use session_manager::SessionId;
use session_registry::{NodeId, Generation};

// Configure the migration service.
let cfg = MigrationServiceBuilder::from_preset(Preset::Balanced)?
    .timeout_ms(45_000)
    .max_concurrent(128)
    .zero_downtime(true)
    .build()?;

// Create a migration plan.
let plan = MigrationPlan::new(
    SessionId::from_pair(1, 1),
    NodeId::new(1),        // source
    NodeId::new(2),        // target
    10_000_000_000,        // deadline_ns
    0,                     // initiated_ns
    MigrationFlags::zero_downtime(),
    Generation::INITIAL,
);

assert!(!plan.is_expired(5_000_000_000));
assert_eq!(plan.remaining_ns(5_000_000_000), 5_000_000_000);
```

## Presets

| Preset | Timeout | Concurrent | Retries | Zero-Downtime |
|--------|---------|-----------|---------|---------------|
| `Conservative` | 60s | 16 | 5 | off |
| `Balanced` | 30s | 64 | 3 | off |
| `Aggressive` | 10s | 256 | 2 | on |
| `Streaming` | 120s | 32 | 4 | on |

## Phase State Machine

```
  Initiated -> SnapshotCaptured -> Transferred -> Restored -> Finalized

  At any pre-terminal phase:
    -> Aborted  (operator or policy)
    -> Failed   (error)
```

```rust
use session_migration_manager::MigrationPhase;

assert!(MigrationPhase::Initiated.is_in_progress());
assert!(!MigrationPhase::Finalized.is_in_progress());
assert!(MigrationPhase::Finalized.is_terminal());
assert!(MigrationPhase::Initiated.can_transition_to(MigrationPhase::SnapshotCaptured));
assert!(!MigrationPhase::Finalized.can_transition_to(MigrationPhase::Initiated));
```

## Stats and Health

```rust
use session_migration_manager::{MigrationStats, HealthReport};

let stats = MigrationStats::new();
stats.record_started();
stats.record_completed(5_000_000_000); // 5s duration

let snap = stats.snapshot();
assert_eq!(snap.migrations_completed, 1);
assert_eq!(snap.success_rate_bps(), 10_000); // 100%

let health = HealthReport::derive(&snap);
assert!(health.is_healthy());
```

## Error Classification

```rust
use session_migration_manager::{MigrationError, NodeId};
use session_manager::SessionId;

// Fatal errors: abort, don't retry.
assert!(MigrationError::ShuttingDown.is_fatal());
assert!(MigrationError::InvalidConfig("bad".into()).is_fatal());

// Retryable errors: backoff + retry.
assert!(MigrationError::TransferFailed("net").is_retryable());
assert!(MigrationError::SourceUnavailable(NodeId::new(1)).is_retryable());

// Recoverable errors: rollback + retry.
assert!(MigrationError::AlreadyInProgress(SessionId::nil()).is_recoverable());
```

## Phase 0 Status

Modules 1-10 are **real implementations** (~60 unit tests). Modules
11-14 ship as typed skeletons. Functional coordinator, transfer,
and restore land in Phase 1+.

## Dependencies

| Crate | Purpose |
|-------|---------|
| `session-manager` | `SessionId` |
| `session-registry` | `NodeId`, `Generation`, `Epoch`, `Region` |
| `connection-shard` | `ConnectionId` |
| `thiserror` | Typed errors |
| `tracing` | Structured logging |

## License

GNU AFFERO GENERAL PUBLIC LICENSE v3.0
