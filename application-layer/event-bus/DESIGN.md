# event-bus Design Document

This document is the authoritative specification for the `event-bus` crate
within the Vireon application layer. It covers architecture, data structures,
algorithms, failure modes, observability, configuration, presets, performance,
API reference, testing strategy, ops runbook, security, and future directions.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Data Structures](#3-data-structures)
4. [Algorithms](#4-algorithms)
5. [Failure Modes](#5-failure-modes)
6. [Observability](#6-observability)
7. [Configuration](#7-configuration)
8. [Presets](#8-presets)
9. [Performance Characteristics](#9-performance-characteristics)
10. [API Reference](#10-api-reference)
11. [Testing Strategy](#11-testing-strategy)
12. [Ops Runbook](#12-ops-runbook)
13. [Security](#13-security)
14. [Future Directions](#14-future-directions)

---

## 1. Overview

### 1.1 Purpose

The `event-bus` crate provides a **partitioned event bus** for the Vireon
application layer. It is the primary intra-process pub/sub mechanism that
connects producers (modules that emit events) with consumers (modules that
react to events). The design is inspired by the partitioned log model
popularized by Apache Kafka and Apache Pulsar, but adapted for the
single-process, io_uring-driven Vireon runtime.

### 1.2 Goals

- **Ordered delivery within a partition.** Events published to the same
  partition are delivered to consumers in the order they were published.
- **Parallel consumption across partitions.** Different partitions of the
  same topic can be consumed independently and in parallel by different
  consumer group members.
- **Consumer group semantics.** Multiple consumers can form a group; the
  group as a whole sees every message, and partitions are distributed among
  group members.
- **Offset tracking.** Each consumer group maintains its own offset per
  partition, enabling at-least-once delivery semantics.
- **Rebalancing.** When consumers join or leave a group, partitions are
  redistributed automatically.
- **Typed errors.** Every fallible API returns a small, exhaustive
  `EventError` enum so callers can branch precisely on failure modes.
- **Observability.** Atomic counters and a derived health report are
  available at all times with zero allocation on the hot path.
- **Configuration-driven.** All operational knobs live in
  `EventBusConfig` and can be materialized from presets or built fluently.

### 1.3 Non-Goals

- **Cross-process communication.** The event bus is intra-process. For
  cross-process messaging, use the `pubsub-engine` crate (which layers
  on top of QUIC streams).
- **Persistent storage.** Events are held in memory. Durability is the
  responsibility of higher layers that checkpoint to disk or replicated
  stores.
- **Exactly-once delivery.** The bus provides at-least-once semantics.
  Exactly-once requires transactional support in the consumer.
- **Schema registry.** Message payloads are opaque `u64` hashes (Phase 0)
  or byte slices (Phase 1+). Schema management is out of scope.

### 1.4 Phase Status

The crate is developed in phases:

| Phase | Scope | Status |
|-------|-------|--------|
| 0 | Core types, config, stats, health, builder, typed skeletons | Complete |
| 1 | Functional `EventBus`, `OffsetStore`, `PartitionRebalancer` | Planned |
| 2 | Durability hooks, WAL integration | Future |
| 3 | Cross-process replication via pubsub-engine | Future |

This document describes Phase 0 in detail and outlines the Phase 1+ design
where skeletons already exist.

### 1.5 Crate Layout

```
event-bus/
  Cargo.toml
  DESIGN.md           ← this document
  README.md
  src/
    lib.rs            ← crate root, re-exports
    config.rs         ← EventBusConfig, Preset
    topic.rs           ← TopicId, TopicConfig
    partition.rs       ← PartitionId, Offset, PartitionState
    consumer.rs        ← ConsumerGroupId, ConsumerId, SubscriptionMode
    error.rs           ← EventError
    stats.rs           ← EventBusStats, StatsSnapshot
    health.rs          ← HealthReport, HealthState
    builder.rs         ← EventBusBuilder
    bus.rs             ← EventBus facade (skeleton)
    offset_store.rs    ← OffsetStore facade (skeleton)
    rebalancer.rs      ← PartitionRebalancer facade (skeleton)
  benches/
    publish.rs
    consume.rs
    rebalance.rs
    ack.rs
```

### 1.6 Dependencies

| Dependency | Purpose |
|------------|---------|
| `configuration-system` | Declarative config loading, validation hooks |
| `middleware-pipeline` | Pre/post-publish middleware hooks (Phase 1+) |
| `session-manager` | `SessionId` re-export; session-scoped consumers |
| `thiserror` | Derive `Error` for `EventError` |
| `tracing` | Structured logging spans for publish/consume/rebalance |
| `criterion` (dev) | Criterion benchmark harness |

---

## 2. Architecture

### 2.1 One-Line Summary

```
Event --> [Event Bus] --> Consumer Group
              |
              +-- TopicRegistry    (TopicId -> Topic)
              +-- PartitionManager  (partition assignment)
              +-- OffsetStore       (consumer progress)
              +-- Rebalancer        (partition distribution)
              +-- ConsumerGroups    (group membership)
```

### 2.2 Component Diagram

```
                    +---------------------------------+
                    |           EventBus              |
                    |                                 |
  Producer -------> |  +-----------+   +-----------+  |
                    |  | TopicReg  |   | Partition |  |
                    |  |           |   | Manager   |  |
                    |  +-----------+   +-----------+  |
                    |  +-----------+   +-----------+  |
                    |  | Offset    |   | Rebalancer|  |
                    |  | Store     |   |           |  |
                    |  +-----------+   +-----------+  |
                    |  +--------------------------+   |
                    |  |    Consumer Groups        |   |
                    |  +--------------------------+   |
                    |  +--------------------------+   |
                    |  |    EventBusStats         |   |
                    |  +--------------------------+   |
                    +---------------------------------+
                                       |
                    +---------------------------------+
                    |        Consumer Group           |
                    |  Consumer A: partitions 0,1     |
                    |  Consumer B: partitions 2,3     |
                    +---------------------------------+
```

### 2.3 Module Dependency Graph

```
lib.rs
  |
  +-- config.rs      (depends on error.rs)
  +-- topic.rs       (depends on error.rs)
  +-- partition.rs   (no internal deps)
  +-- consumer.rs    (no internal deps)
  +-- error.rs       (depends on topic.rs, partition.rs, consumer.rs)
  +-- stats.rs       (no internal deps)
  +-- health.rs      (depends on stats.rs)
  +-- builder.rs     (depends on config.rs)
  +-- bus.rs         (depends on config, consumer, error, health, partition, stats, topic)
  +-- offset_store.rs(depends on config, consumer, error, health, partition, stats)
  +-- rebalancer.rs  (depends on config, consumer, error, health, partition, stats)
```

### 2.4 Data Flow

#### 2.4.1 Publish Flow

```
1. Producer calls EventBus::publish(topic, partition, msg_hash)
2. EventBus validates topic existence and partition state
3. If partition is Active, the message is appended to the partition log
4. EventBusStats::record_publish(bytes) is called
5. Any subscribed consumers are notified (Phase 1+)
6. On failure, EventBusStats::record_publish_error() is called
7. EventError variant is returned to the caller
```

#### 2.4.2 Consume Flow

```
1. Consumer calls EventBus::subscribe(group, topic, mode)
2. EventBus registers the consumer in the consumer group
3. Partitions are assigned to the consumer by the Rebalancer
4. Consumer polls for messages from its assigned partitions
5. On delivery, EventBusStats::record_consume() is called
6. Consumer processes the message
7. Consumer calls EventBus::ack(group, partition, offset)
8. EventBusStats::record_ack() is called
9. OffsetStore commits the offset
```

#### 2.4.3 Rebalance Flow

```
1. Consumer joins or leaves a group
2. EventBus triggers PartitionRebalancer::plan(group, partitions)
3. Rebalancer computes a new partition assignment
4. PartitionRebalancer::execute(group) applies the assignment
5. Affected partitions enter Reassigning state
6. Consumers are notified of their new assignments
7. Partitions return to Active state
8. EventBusStats::record_rebalance() is called
```

### 2.5 Concurrency Model

The event bus is designed for the Vireon single-process, multi-task runtime:

- **No threads.** The bus is driven by async tasks scheduled by the Vireon
  reactor. All operations are non-blocking.
- **Interior mutability.** Atomic counters (`AtomicU64`) are used for
  stats with `Ordering::Relaxed`, which is sufficient because stats are
  advisory and do not participate in synchronization.
- **No locks on the hot path (Phase 1+).** The partition log will use
  lock-free MPSC channels for producer/consumer handoff. The offset store
  will use a lock-free skip list.
- **`Send + Sync`.** All public types are `Send + Sync` (Phase 0 types
  are `Copy` or contain only `AtomicU64`).

### 2.6 Relationship to Adjacent Layers

```
+--------------------------------------------------+
|              Application Layer                   |
|                                                  |
|  +------------+   +----------+   +------------+  |
|  | event-bus  |<--| scheduler|   | pubsub-    |  |
|  |            |   | -hooks   |   | engine     |  |
|  +-----+------+   +----------+   +------+-----+  |
|        |                               |        |
|  +-----+------+               +-------+------+  |
|  | middleware |               | session-     |  |
|  | -pipeline  |               | manager      |  |
|  +------------+               +--------------+  |
+--------------------------------------------------+
         |
+--------------------------------------------------+
|              Session Layer                        |
+--------------------------------------------------+
         |
+--------------------------------------------------+
|              Transport Layer (QUIC)               |
+--------------------------------------------------+
```

- **scheduler-hooks**: The event bus emits lifecycle hooks (on-publish,
  on-consume, on-rebalance) that the scheduler-hooks crate can subscribe
  to for scheduling decisions.
- **middleware-pipeline**: Pre-publish and post-consume middleware
  (e.g., validation, transformation, rate limiting) are applied via the
  middleware-pipeline crate.
- **session-manager**: Consumer identities can be tied to sessions. The
  `SessionId` type is re-exported for convenience.
- **pubsub-engine**: For cross-process or cross-node messaging, the
  pubsub-engine builds on top of the event bus, adding serialization,
  network transport, and replication.

---

## 3. Data Structures

### 3.1 TopicId

**File**: `src/topic.rs`

```rust
pub struct TopicId(u64);
```

A `TopicId` is a 64-bit unsigned integer that uniquely identifies a topic
within the event bus. The value `0` is reserved as `TopicId::NIL` and
serves as a sentinel for "no topic."

**Trait implementations**: `Clone`, `Copy`, `Debug`, `PartialEq`, `Eq`,
`Hash`, `Default` (yields `NIL`), `Display` (formats as `topic#{n}`).

**API**:

| Method | Signature | Description |
|--------|-----------|-------------|
| `NIL` | `pub const NIL: Self` | The reserved nil value (0). |
| `new` | `pub const fn new(id: u64) -> Self` | Construct a new topic ID. |
| `nil` | `pub const fn nil() -> Self` | Construct the nil topic ID. |
| `is_nil` | `pub const fn is_nil(&self) -> bool` | Whether this is the nil ID. |
| `raw` | `pub const fn raw(self) -> u64` | Get the raw underlying value. |

**Design rationale**: Using a newtype around `u64` rather than a string
topic name avoids allocation, enables O(1) hashing, and makes topic IDs
suitable as keys in `HashMap` and `HashSet` without wrapper overhead.
Topic name-to-ID resolution is handled by the configuration system.

**Nil sentinel**: The nil value (`0`) is used to indicate "no topic" in
optional contexts (e.g., when a consumer has not yet been assigned a
topic). It is never a valid topic for publishing or subscribing.

### 3.2 TopicConfig

**File**: `src/topic.rs`

```rust
pub struct TopicConfig {
    pub partition_count: u8,
    pub retention_ms: u64,
    pub max_message_bytes: u32,
}
```

Per-topic configuration. Created via `TopicConfig::new(partition_count)`
and customized with builder-style `with_*` methods.

**Defaults**: 7-day retention (`604_800_000` ms), 1 MB max message size
(`1_000_000` bytes).

**Fields**:

| Field | Type | Default | Range |
|-------|------|---------|-------|
| `partition_count` | `u8` | (required) | 1..=255 |
| `retention_ms` | `u64` | 604_800_000 (7 days) | >0 |
| `max_message_bytes` | `u32` | 1_000_000 | >0 |

**Validation**: `validate()` returns `EventError::PublishFailed` if any
field is zero or otherwise out of range.

**API**:

| Method | Signature | Description |
|--------|-----------|-------------|
| `new` | `pub const fn new(partition_count: u8) -> Self` | Create with defaults. |
| `with_retention_ms` | `pub const fn with_retention_ms(self, ms: u64) -> Self` | Set retention. |
| `with_max_message_bytes` | `pub const fn with_max_message_bytes(self, bytes: u32) -> Self` | Set max msg size. |
| `validate` | `pub fn validate(&self) -> Result<(), EventError>` | Validate config. |

### 3.3 PartitionId

**File**: `src/partition.rs`

```rust
pub struct PartitionId(u32);
```

A `PartitionId` identifies a partition within a topic. Partitions are
numbered `0..N` where `N` is the topic's `partition_count`.

**Trait implementations**: `Clone`, `Copy`, `Debug`, `PartialEq`, `Eq`,
`Hash`, `PartialOrd`, `Ord`, `Display` (formats as `partition#{n}`).

**API**:

| Method | Signature | Description |
|--------|-----------|-------------|
| `new` | `pub const fn new(id: u32) -> Self` | Construct a new partition ID. |
| `raw` | `pub const fn raw(self) -> u32` | Get the raw underlying value. |

**Design rationale**: `u32` is chosen over `u8` (which is the type of
`partition_count`) to allow future expansion beyond 255 partitions
without breaking the API. The `Ord` implementation enables sorting
partitions for deterministic assignment in the rebalancer.

### 3.4 Offset

**File**: `src/partition.rs`

```rust
pub struct Offset(u64);
```

An `Offset` is a monotonically increasing identifier for an event within
a partition. Each new event gets the next offset. The value `u64::MAX`
is reserved as an invalid sentinel.

**Trait implementations**: `Clone`, `Copy`, `Debug`, `PartialEq`, `Eq`,
`Hash`, `PartialOrd`, `Ord`, `Default` (yields `ZERO`).

**Constants**:

| Constant | Value | Description |
|----------|-------|-------------|
| `ZERO` | `Offset(0)` | The starting offset. |

**API**:

| Method | Signature | Description |
|--------|-----------|-------------|
| `new` | `pub const fn new(offset: u64) -> Self` | Construct a new offset. |
| `raw` | `pub const fn raw(self) -> u64` | Get the raw value. |
| `is_valid` | `pub const fn is_valid(self) -> bool` | True if not `u64::MAX`. |
| `next` | `pub const fn next(self) -> Self` | Get the next offset (wrapping). |

**Wrapping semantics**: `next()` uses `wrapping_add(1)` so that
`Offset::new(u64::MAX).next()` yields `Offset::new(0)`. This is safe
because `u64::MAX` is reserved as invalid and will never appear in a
valid offset sequence. In practice, a partition would need to process
18 quintillion messages before this matters.

### 3.5 PartitionState

**File**: `src/partition.rs`

```rust
#[repr(u8)]
pub enum PartitionState {
    Active = 0,
    Reassigning = 1,
    Sealed = 2,
}
```

The lifecycle state of a partition.

**Trait implementations**: `Clone`, `Copy`, `Debug`, `Default` (yields
`Active`), `PartialEq`, `Eq`, `Hash`.

**Variants**:

| Variant | Value | Description |
|---------|-------|-------------|
| `Active` | 0 | Partition is accepting writes and reads. |
| `Reassigning` | 1 | Partition is being reassigned to another consumer. |
| `Sealed` | 2 | Partition is read-only (no new writes). |

**API**:

| Method | Signature | Description |
|--------|-----------|-------------|
| `is_active` | `pub const fn is_active(&self) -> bool` | True if `Active`. |
| `accepts_writes` | `pub const fn accepts_writes(&self) -> bool` | True if `Active`. |
| `as_str` | `pub const fn as_str(&self) -> &'static str` | Human-readable name. |

**State transitions**:

```
Active ----rebalance---> Reassigning ----complete---> Active
Active ----seal---------> Sealed
Reassigning ----seal-----> Sealed
Sealed (terminal)
```

### 3.6 ConsumerGroupId

**File**: `src/consumer.rs`

```rust
pub struct ConsumerGroupId(u64);
```

A `ConsumerGroupId` uniquely identifies a consumer group within the event
bus. The value `0` is reserved as `ConsumerGroupId::NIL`.

**Trait implementations**: `Clone`, `Copy`, `Debug`, `PartialEq`, `Eq`,
`Hash`, `Default` (yields `NIL`), `Display` (formats as `group#{n}`).

**API**:

| Method | Signature | Description |
|--------|-----------|-------------|
| `NIL` | `pub const NIL: Self` | The reserved nil value (0). |
| `new` | `pub const fn new(id: u64) -> Self` | Construct a new group ID. |
| `nil` | `pub const fn nil() -> Self` | Construct the nil group ID. |
| `is_nil` | `pub const fn is_nil(&self) -> bool` | Whether this is nil. |
| `raw` | `pub const fn raw(self) -> u64` | Get the raw value. |

### 3.7 ConsumerId

**File**: `src/consumer.rs`

```rust
pub struct ConsumerId(u64);
```

A `ConsumerId` uniquely identifies a consumer within a consumer group.

**Trait implementations**: `Clone`, `Copy`, `Debug`, `PartialEq`, `Eq`,
`Hash`.

**API**:

| Method | Signature | Description |
|--------|-----------|-------------|
| `new` | `pub const fn new(id: u64) -> Self` | Construct a new consumer ID. |
| `raw` | `pub const fn raw(self) -> u64` | Get the raw value. |

### 3.8 SubscriptionMode

**File**: `src/consumer.rs`

```rust
#[repr(u8)]
pub enum SubscriptionMode {
    Earliest = 0,
    Latest = 1,
    FromOffset(u64) = 2,
}
```

Determines where a consumer starts reading from a partition.

**Trait implementations**: `Clone`, `Copy`, `Debug`, `Default` (yields
`Earliest`), `PartialEq`, `Eq`, `Hash`.

**Variants**:

| Variant | Description |
|---------|-------------|
| `Earliest` | Start from the earliest available offset. |
| `Latest` | Start from the latest offset (only new messages). |
| `FromOffset(u64)` | Start from a specific offset. |

**API**:

| Method | Signature | Description |
|--------|-----------|-------------|
| `as_str` | `pub const fn as_str(&self) -> &'static str` | Human-readable name. |
| `offset` | `pub const fn offset(&self) -> Option<u64>` | Get the offset if `FromOffset`. |

**Note on `repr(u8)`**: The `FromOffset(u64)` variant carries a payload,
so `as u8` is not available. The `repr(u8)` attribute is retained for
future FFI compatibility and to signal intent that the discriminant
should be compact.

### 3.9 EventBusConfig

**File**: `src/config.rs`

```rust
pub struct EventBusConfig {
    pub max_topics: u32,
    pub default_partitions: u8,
    pub retention_ms: u64,
    pub max_message_bytes: u32,
    pub consumer_group_timeout_ms: u64,
}
```

Declarative configuration for the entire event bus.

**Trait implementations**: `Clone`, `Copy`, `Debug`, `PartialEq`, `Eq`,
`Default` (yields the same values as `new()`).

See [Section 7](#7-configuration) for full details on fields, ranges,
and validation.

### 3.10 EventBusStats

**File**: `src/stats.rs`

```rust
pub struct EventBusStats {
    pub messages_published: AtomicU64,
    pub messages_consumed: AtomicU64,
    pub bytes_published: AtomicU64,
    pub publish_errors: AtomicU64,
    pub consumer_lag_ns: AtomicU64,
    pub rebalance_count: AtomicU64,
    pub ack_total: AtomicU64,
}
```

Atomic counters for the event bus. All fields are public so any subsystem
can record events directly.

See [Section 6](#6-observability) for full details.

### 3.11 StatsSnapshot

**File**: `src/stats.rs`

```rust
pub struct StatsSnapshot {
    pub messages_published: u64,
    pub messages_consumed: u64,
    pub bytes_published: u64,
    pub publish_errors: u64,
    pub consumer_lag_ns: u64,
    pub rebalance_count: u64,
    pub ack_total: u64,
}
```

A point-in-time snapshot of all counters. Obtained via
`EventBusStats::snapshot()`.

### 3.12 HealthReport

**File**: `src/health.rs`

```rust
pub struct HealthReport {
    pub state: HealthState,
    pub messages_published: u64,
    pub publish_error_rate_bps: u16,
    pub active_rebalances: bool,
}
```

### 3.13 HealthState

**File**: `src/health.rs`

```rust
pub enum HealthState {
    Healthy,
    Degraded,
    Critical,
}
```

Tri-state health classification.

### 3.14 EventError

**File**: `src/error.rs`

```rust
pub enum EventError {
    TopicNotFound(TopicId),
    TopicExists(TopicId),
    PublishFailed(&'static str),
    ConsumerLag(u64),
    OffsetOutOfRange(u64),
    PartitionRebalance(PartitionId),
    ConsumerGroupNotFound(ConsumerGroupId),
    ShuttingDown,
}
```

See [Section 5](#5-failure-modes) for full details.

---

## 4. Algorithms

### 4.1 Partition Selection

When a producer publishes a message, the partition is either explicitly
specified or derived from a message hash.

**Explicit partition (Phase 0)**: The caller provides a `PartitionId`
directly to `EventBus::publish(topic, partition, msg_hash)`.

**Hash-based partition selection (Phase 1+)**: When the caller does not
specify a partition, the bus computes:

```
partition = hash(key) % partition_count
```

where `hash` is a fast, uniform hash function (e.g., FxHash or ahash).
This ensures that messages with the same key always go to the same
partition, preserving ordering per key.

**Partition selection algorithm (Phase 1+)**:

```
fn select_partition(topic: TopicId, key: Option<u64>, msg_hash: u64) -> PartitionId {
    let config = topic_registry.get(topic)?;
    let count = config.partition_count as u32;
    let hash = key.unwrap_or(msg_hash);
    PartitionId::new((hash % count as u64) as u32)
}
```

**Rationale for `u8` partition_count**: 255 partitions per topic is
sufficient for intra-process parallelism. The Vireon runtime typically
has 8-128 tasks, so partition counts above 255 would only add overhead
without improving parallelism.

### 4.2 Offset Tracking

Each consumer group maintains a committed offset per partition. The
offset represents the last successfully processed message.

**Commit protocol**:

```
1. Consumer receives message at offset O from partition P.
2. Consumer processes the message.
3. Consumer calls ack(group, P, O).
4. OffsetStore commits O for (group, P).
5. On next poll, consumer receives message at offset O+1.
```

**At-least-once semantics**: If a consumer crashes after receiving a
message but before committing the offset, the message will be
redelivered to the next consumer that takes over the partition. This
means consumers must be idempotent or use deduplication.

**Offset validation (Phase 1+)**:

```
fn validate_offset(group, partition, requested_offset) -> Result<(), EventError> {
    let committed = offset_store.fetch(group, partition)?;
    let log_start = partition_log.start_offset(partition)?;
    let log_end = partition_log.end_offset(partition)?;

    if requested_offset < log_start {
        return Err(EventError::OffsetOutOfRange(requested_offset));
    }
    if requested_offset > log_end {
        return Err(EventError::OffsetOutOfRange(requested_offset));
    }
    Ok(())
}
```

**Offset compaction (Phase 2+)**: Old offsets can be compacted to save
memory. The compaction strategy retains only the latest offset per
(group, partition) pair and discards historical offset records.

### 4.3 Consumer Group Rebalancing

Rebalancing is the process of redistributing partitions among consumers
in a consumer group. It is triggered when:

1. A new consumer joins the group.
2. An existing consumer leaves the group (graceful or crash).
3. The partition count of a topic changes.
4. An administrator triggers a manual rebalance.

**Rebalance algorithm (range assignment, Phase 1+)**:

```
fn plan_rebalance(group, partitions) -> Assignment {
    let consumers = consumer_groups.get(group)?;
    let n_consumers = consumers.len();
    let n_partitions = partitions.len();

    // Sort partitions and consumers for deterministic assignment.
    let sorted_partitions = partitions.sorted();
    let sorted_consumers = consumers.sorted();

    // Divide partitions evenly among consumers.
    let per_consumer = n_partitions / n_consumers;
    let remainder = n_partitions % n_consumers;

    let mut assignment = Assignment::new();
    let mut idx = 0;
    for (i, consumer) in sorted_consumers.iter().enumerate() {
        let count = per_consumer + if i < remainder { 1 } else { 0 };
        let slice = &sorted_partitions[idx..idx + count];
        assignment.assign(consumer, slice);
        idx += count;
    }
    assignment
}
```

**Example**: 8 partitions, 3 consumers:
- Consumer A: partitions 0, 1, 2 (3 partitions)
- Consumer B: partitions 3, 4, 5 (3 partitions)
- Consumer C: partitions 6, 7 (2 partitions)

**Sticky assignment (Phase 2+)**: The sticky assignment strategy
minimizes partition movement during rebalancing. It preserves existing
assignments where possible and only moves partitions that must change
hands. This reduces duplicate processing during rebalance.

**Cooperative rebalancing (Phase 3+)**: Instead of revoking all
partitions and reassigning (eager rebalancing), cooperative rebalancing
revokes only the partitions that need to move. This allows consumers to
continue processing on unaffected partitions during the rebalance.

**State transitions during rebalance**:

```
1. All affected partitions enter Reassigning state.
2. Consumers stop polling on affected partitions.
3. New assignment is computed.
4. Consumers are notified of new assignments.
5. Partitions return to Active state.
6. Consumers begin polling on new assignments.
```

### 4.4 Message Delivery Semantics

The event bus provides **at-least-once** delivery:

- Messages are not deleted after delivery; they remain in the partition
  log until retention expires.
- Consumers track their progress via offsets. If a consumer crashes, the
  next consumer resumes from the last committed offset.
- If a consumer processes a message but crashes before committing, the
  message is redelivered.

**Implications for consumers**:
- Consumers must be idempotent or implement deduplication.
- Side effects (e.g., writing to a database) should be made idempotent
  by using the (partition, offset) tuple as a deduplication key.
- Transactions that span multiple messages should use a higher-level
  transaction coordinator.

---

## 5. Failure Modes

### 5.1 Error Taxonomy

The `EventError` enum is the typed failure surface for the entire event
bus. Every fallible API returns one of these variants.

```rust
#[non_exhaustive]
pub enum EventError {
    TopicNotFound(TopicId),
    TopicExists(TopicId),
    PublishFailed(&'static str),
    ConsumerLag(u64),
    OffsetOutOfRange(u64),
    PartitionRebalance(PartitionId),
    ConsumerGroupNotFound(ConsumerGroupId),
    ShuttingDown,
}
```

### 5.2 Error Categories

Errors are classified into three categories:

| Category | Variants | Caller Action |
|----------|----------|---------------|
| **Fatal** | `TopicExists`, `ShuttingDown` | Abort; do not retry. |
| **Retryable** | `PublishFailed`, `ConsumerLag`, `PartitionRebalance` | Back off and retry. |
| **Capacity** | `OffsetOutOfRange` | Wait or reset offset. |
| **Not Found** | `TopicNotFound`, `ConsumerGroupNotFound` | Check configuration. |

### 5.3 Variant Details

#### 5.3.1 TopicNotFound(TopicId)

**When**: An operation references a topic ID that has not been created.

**Likely causes**:
- The topic was never created.
- The topic was deleted.
- The topic ID is wrong (typo, configuration mismatch).

**Caller action**: Check topic configuration and create the topic if
needed. This is not retryable without corrective action.

#### 5.3.2 TopicExists(TopicId)

**When**: An attempt is made to create a topic that already exists.

**Likely causes**:
- Duplicate topic creation call.
- Race condition between two creators.

**Caller action**: This is a fatal error indicating a programming
mistake. Log it and fix the duplicate creation logic. Do not retry.

#### 5.3.3 PublishFailed(&'static str)

**When**: Publishing a message fails for any reason (buffer full,
validation failure, internal error).

**Likely causes**:
- Message exceeds `max_message_bytes`.
- Partition is in `Sealed` state.
- Internal buffer is full (backpressure).

**Caller action**: Retry with exponential backoff. If the error persists,
investigate the error message for the specific cause.

#### 5.3.4 ConsumerLag(u64)

**When**: A consumer is falling behind the producer. The payload is the
number of messages the consumer is behind.

**Likely causes**:
- Consumer is slow (CPU-bound processing).
- Consumer is overloaded (too many partitions).
- Producer burst (sudden spike in message rate).

**Caller action**: Retry. Consider scaling the consumer group by adding
more consumers. Monitor the lag trend; if it grows monotonically, the
consumer cannot keep up and action is needed.

#### 5.3.5 OffsetOutOfRange(u64)

**When**: A consumer requests an offset that is outside the valid range
of the partition log.

**Likely causes**:
- The requested offset has been compacted away (older than retention).
- The requested offset is beyond the log end (future offset).
- The offset was corrupted or miscomputed.

**Caller action**: Reset the consumer's offset to `Earliest` or `Latest`
and resume. This may result in duplicate or skipped messages, so log
the event for audit.

#### 5.3.6 PartitionRebalance(PartitionId)

**When**: An operation targets a partition that is currently being
reassigned to another consumer.

**Likely causes**:
- A rebalance is in progress.
- The partition is in `Reassigning` state.

**Caller action**: Retry after a short backoff. The rebalance should
complete within seconds.

#### 5.3.7 ConsumerGroupNotFound(ConsumerGroupId)

**When**: An operation references a consumer group that does not exist.

**Likely causes**:
- The group was never created.
- The group expired (timeout).
- The group ID is wrong.

**Caller action**: Create the consumer group or check the ID. Not
retryable without corrective action.

#### 5.3.8 ShuttingDown

**When**: The event bus is in the process of shutting down and no longer
accepts new operations.

**Likely causes**:
- Graceful shutdown was initiated.
- The runtime is terminating.

**Caller action**: Stop all publish/consume activity. This is fatal and
not retryable. Clean up any consumer resources.

### 5.4 Error Predicates

```rust
impl EventError {
    pub const fn is_fatal(&self) -> bool {
        matches!(self, Self::TopicExists(_) | Self::ShuttingDown)
    }

    pub const fn is_retryable(&self) -> bool {
        matches!(self,
            Self::PublishFailed(_) | Self::ConsumerLag(_) | Self::PartitionRebalance(_))
    }
}
```

**Design rationale**: The predicates allow callers to write generic
retry logic without matching on every variant:

```rust
match result {
    Ok(v) => handle(v),
    Err(e) if e.is_retryable() => retry_with_backoff(e),
    Err(e) if e.is_fatal() => abort(e),
    Err(e) => handle_non_retryable(e),
}
```

### 5.5 Non-Exhaustive

The `EventError` enum is `#[non_exhaustive]`, meaning new variants may
be added in future versions without a breaking change. Callers should
always include a catch-all arm in their match expressions.

---

## 6. Observability

### 6.1 Atomic Counters

The `EventBusStats` struct provides lock-free atomic counters for
real-time observability. All counters use `Ordering::Relaxed` because
they are advisory and do not participate in synchronization.

| Counter | Type | Description |
|---------|------|-------------|
| `messages_published` | `AtomicU64` | Total messages published (counter). |
| `messages_consumed` | `AtomicU64` | Total messages consumed (counter). |
| `bytes_published` | `AtomicU64` | Total bytes published (counter). |
| `publish_errors` | `AtomicU64` | Total publish errors (counter). |
| `consumer_lag_ns` | `AtomicU64` | Consumer lag in nanoseconds (gauge). |
| `rebalance_count` | `AtomicU64` | Rebalance events triggered (counter). |
| `ack_total` | `AtomicU64` | ACKs processed (counter). |

### 6.2 Recording Methods

| Method | Effect |
|--------|--------|
| `record_publish(bytes)` | Increments `messages_published` and `bytes_published`. |
| `record_consume()` | Increments `messages_consumed`. |
| `record_publish_error()` | Increments `publish_errors`. |
| `record_consumer_lag(lag_ns)` | Sets `consumer_lag_ns` (gauge, not counter). |
| `record_rebalance()` | Increments `rebalance_count`. |
| `record_ack()` | Increments `ack_total`. |

### 6.3 Snapshot

`EventBusStats::snapshot()` returns a `StatsSnapshot` containing all
counter values at a single point in time. The snapshot is a `Copy` struct
of plain `u64` values, making it cheap to pass around and compare.

**Consistency**: Because each counter is loaded independently with
`Relaxed` ordering, the snapshot is not atomically consistent — a
counter may change between two loads. This is acceptable for advisory
monitoring. If strict consistency is needed, a `Mutex<StatsSnapshot>`
could be used, but the overhead is not justified for this use case.

### 6.4 Derived Metrics

`StatsSnapshot` provides two derived metrics:

| Method | Returns | Description |
|--------|---------|-------------|
| `publish_error_rate_bps()` | `u16` | Publish error rate in basis points (0..=10_000). |
| `has_active_rebalances()` | `bool` | True if `rebalance_count > 0`. |

**Basis points**: 1 basis point (bp) = 0.01%. An error rate of 1% =
100 bps. Using basis points avoids floating-point arithmetic while
providing sufficient precision for monitoring thresholds.

**Error rate formula**:

```
error_rate_bps = (publish_errors * 10_000) / (messages_published + publish_errors)
```

If the denominator is zero (no publishes yet), the rate is 0.

### 6.5 Health Report

`HealthReport::derive(&snapshot)` produces a tri-state health
classification from a stats snapshot.

**Health states**:

| State | Description | Operator Action |
|-------|-------------|-----------------|
| `Healthy` | All signals nominal. | None. |
| `Degraded` | Performance or availability degraded. | Investigate. |
| `Critical` | Severe impact. | Page on-call. |

**Classification thresholds**:

| Signal | Degraded | Critical |
|--------|----------|----------|
| Publish error rate | > 100 bps (1%) | > 500 bps (5%) |
| Active rebalances | Yes | — |

**Worst signal wins**: If any signal is `Critical`, the overall state is
`Critical`. Otherwise, if any signal is `Degraded`, the overall state is
`Degraded`. Otherwise, `Healthy`.

### 6.6 Export to External Monitoring (Phase 1+)

The `StatsSnapshot` struct is designed to be easily exported to external
monitoring systems:

- **Prometheus**: Each field maps to a Prometheus counter or gauge.
  The `as_str()` methods on enums provide label values.
- **OpenTelemetry**: The snapshot can be converted to OTLP metrics.
- **JSON**: The `Debug` implementation provides a human-readable
  representation that can be serialized to JSON.

### 6.7 Tracing

The `tracing` crate is used for structured logging. Key spans:

| Span | Fields | When |
|------|--------|------|
| `publish` | `topic`, `partition`, `bytes` | On each publish. |
| `consume` | `group`, `topic`, `partition`, `offset` | On each consume. |
| `rebalance` | `group`, `partition_count` | On each rebalance. |
| `ack` | `group`, `partition`, `offset` | On each ack. |

---

## 7. Configuration

### 7.1 EventBusConfig

```rust
pub struct EventBusConfig {
    pub max_topics: u32,
    pub default_partitions: u8,
    pub retention_ms: u64,
    pub max_message_bytes: u32,
    pub consumer_group_timeout_ms: u64,
}
```

### 7.2 Fields

| Field | Type | Default | Min | Max | Description |
|-------|------|---------|-----|-----|-------------|
| `max_topics` | `u32` | 100 | 1 | 100,000 | Maximum number of topics. |
| `default_partitions` | `u8` | 8 | 1 | 255 | Default partitions per topic. |
| `retention_ms` | `u64` | 604,800,000 (7 days) | 3,600,000 (1 hour) | 31,536,000,000 (365 days) | Message retention time. |
| `max_message_bytes` | `u32` | 1,000,000 | 512 | 50,000,000 | Maximum message size. |
| `consumer_group_timeout_ms` | `u64` | 60,000 (60s) | 5,000 (5s) | 3,600,000 (1 hour) | Consumer group timeout. |

### 7.3 Constants

```rust
pub const MIN_TOPICS: u32 = 1;
pub const MAX_TOPICS: u32 = 100_000;
pub const MIN_PARTITIONS: u8 = 1;
pub const MAX_PARTITIONS: u8 = 255;
pub const MIN_RETENTION_MS: u64 = 3_600_000;       // 1 hour
pub const MAX_RETENTION_MS: u64 = 31_536_000_000;  // 365 days
pub const MIN_MESSAGE_BYTES: u32 = 512;
pub const MAX_MESSAGE_BYTES: u32 = 50_000_000;
pub const MIN_GROUP_TIMEOUT_MS: u64 = 5_000;       // 5 seconds
pub const MAX_GROUP_TIMEOUT_MS: u64 = 3_600_000;   // 1 hour
```

### 7.4 Builder API

```rust
impl EventBusConfig {
    pub fn new() -> Self { ... }

    pub const fn with_max_topics(self, topics: u32) -> Self { ... }
    pub const fn with_default_partitions(self, partitions: u8) -> Self { ... }
    pub const fn with_retention_ms(self, ms: u64) -> Self { ... }
    pub const fn with_max_message_bytes(self, bytes: u32) -> Self { ... }
    pub const fn with_consumer_group_timeout_ms(self, ms: u64) -> Self { ... }

    pub fn validate(&self) -> Result<(), EventError> { ... }
}
```

All `with_*` methods are `const fn`, enabling compile-time configuration:

```rust
const CFG: EventBusConfig = EventBusConfig::new()
    .with_max_topics(50)
    .with_default_partitions(16);
```

### 7.5 Validation

`validate()` checks every field against its min/max bounds. If any field
is out of range, it returns `EventError::PublishFailed` with a
descriptive message.

**Why `PublishFailed`?**: In Phase 0, `EventError` has a limited set of
variants. A dedicated `ConfigInvalid` variant will be added in Phase 1.
For now, `PublishFailed` is used as a catch-all for validation errors,
which is acceptable because validation errors are programming errors,
not runtime failures.

### 7.6 EventBusBuilder

The `EventBusBuilder` provides a fluent API for constructing
`EventBusConfig`:

```rust
let cfg = EventBusBuilder::new()
    .max_topics(500)
    .default_partitions(16)
    .retention_ms(1_209_600_000)
    .max_message_bytes(8_000_000)
    .consumer_group_timeout_ms(90_000)
    .build()?;
```

Or from a preset:

```rust
let cfg = EventBusBuilder::from_preset(Preset::HighThroughput)
    .max_topics(500)  // override
    .build()?;
```

`build()` calls `validate()` before returning the config.

---

## 8. Presets

### 8.1 Overview

Presets encode deployment archetypes studied in the Kafka/Pulsar
literature. Each preset is a self-consistent starting point for a
specific workload profile.

```rust
#[non_exhaustive]
pub enum Preset {
    Local,
    Clustered,
    HighThroughput,
}
```

### 8.2 Preset Table

| Preset | Topics | Partitions | Retention | Message Size | Group Timeout |
|--------|--------|------------|-----------|--------------|---------------|
| `Local` | 10 | 8 | 1 day | 1 MB | 30 s |
| `Clustered` | 100 | 8 | 7 days | 1 MB | 60 s |
| `HighThroughput` | 1000 | 128 | 30 days | 16 MB | 120 s |

### 8.3 Local

```rust
Preset::Local.to_config()
```

- **Use case**: Development, testing, single-node deployments.
- **Rationale**: Few topics and partitions keep memory usage low. Short
  retention (1 day) is sufficient for development iteration. Small
  message size (1 MB) covers most test payloads.
- **Group timeout**: 30 seconds is short enough to detect dead consumers
  quickly during development.

### 8.4 Clustered

```rust
Preset::Clustered.to_config()
```

- **Use case**: Production clusters with moderate throughput.
- **Rationale**: 100 topics and 8 partitions per topic provide good
  parallelism without excessive overhead. 7-day retention covers most
  debugging scenarios. 1 MB message size is the industry standard.
- **Group timeout**: 60 seconds balances fast failure detection with
  tolerance for transient network issues.
- **Note**: The `Clustered` preset matches the default `EventBusConfig`
  values exactly.

### 8.5 HighThroughput

```rust
Preset::HighThroughput.to_config()
```

- **Use case**: High-volume data pipelines, streaming analytics.
- **Rationale**: 1000 topics and 128 partitions per topic maximize
  parallelism. 30-day retention allows for long-range replay. 16 MB
  message size supports large batch payloads.
- **Group timeout**: 120 seconds allows for longer processing times
  without false timeout signals.
- **Memory considerations**: With 1000 topics x 128 partitions, the bus
  manages 128,000 partitions. At ~100 bytes per partition metadata, this
  is ~12.8 MB of metadata, which is acceptable.

### 8.6 Customization

Presets are starting points, not constraints. After materializing a
preset, use builder methods to override individual fields:

```rust
let cfg = Preset::Local.to_config()
    .with_max_topics(50)
    .with_retention_ms(604_800_000); // override to 7 days
```

Or via the builder:

```rust
let cfg = EventBusBuilder::from_preset(Preset::HighThroughput)
    .max_topics(500)  // reduce from 1000
    .build()?;
```

### 8.7 Validation

All presets produce valid configurations. This is verified by the test
`all_presets_produce_valid_configs`.

---

## 9. Performance Characteristics

### 9.1 Phase 0 Performance

Phase 0 types are lightweight newtypes around integers. The performance
characteristics are:

| Operation | Cost |
|-----------|------|
| `TopicId::new(n)` | 1 move (8 bytes) |
| `PartitionId::new(n)` | 1 move (4 bytes) |
| `Offset::new(n)` | 1 move (8 bytes) |
| `Offset::next()` | 1 arithmetic op |
| `EventBusStats::record_publish(bytes)` | 2 atomic fetch-add |
| `EventBusStats::snapshot()` | 7 atomic loads |
| `HealthReport::derive(&snapshot)` | ~10 comparisons + arithmetic |
| `EventBusConfig::validate()` | 5 comparisons |
| `Preset::to_config()` | 1 match + 5 field assignments |

All operations are O(1) and allocation-free.

### 9.2 Expected Phase 1 Performance

| Operation | Target | Notes |
|-----------|--------|-------|
| `publish()` | < 100 ns | Lock-free MPSC append |
| `subscribe()` | < 1 us | Group registration + partition assignment |
| `consume()` | < 50 ns | Offset check + log read |
| `ack()` | < 50 ns | Atomic store |
| `rebalance.plan()` | < 10 us | Sort + partition (8 partitions) |
| `rebalance.execute()` | < 100 us | State transitions + notifications |

### 9.3 Memory Footprint

| Structure | Size (bytes) | Notes |
|-----------|-------------|-------|
| `TopicId` | 8 | `u64` newtype |
| `TopicConfig` | 24 | 3 fields, no padding |
| `PartitionId` | 4 | `u32` newtype |
| `Offset` | 8 | `u64` newtype |
| `PartitionState` | 1 | `repr(u8)` enum |
| `ConsumerGroupId` | 8 | `u64` newtype |
| `ConsumerId` | 8 | `u64` newtype |
| `SubscriptionMode` | 16 | Enum with `u64` payload |
| `EventBusConfig` | 40 | 5 fields |
| `EventBusStats` | 56 | 7 `AtomicU64` |
| `StatsSnapshot` | 56 | 7 `u64` |
| `HealthReport` | 24 | State + 3 fields |
| `EventError` | 32 | Largest variant (TopicId + discriminant) |

### 9.4 Cache Behavior

- All ID types (`TopicId`, `PartitionId`, `ConsumerGroupId`,
  `ConsumerId`) are small enough to be passed by value, avoiding
  pointer chasing.
- `EventBusStats` is 56 bytes, fitting in a single cache line (64
  bytes). This ensures that counter updates don't cause false sharing.
- `StatsSnapshot` is also 56 bytes and `Copy`, so it can be passed
  around without allocation.

### 9.5 Scalability

- **Topics**: Scales to `MAX_TOPICS` (100,000). Topic lookup is O(1)
  via `HashMap<TopicId, Topic>` (Phase 1+).
- **Partitions per topic**: Scales to `MAX_PARTITIONS` (255). Partition
  operations are O(1) via direct indexing.
- **Consumer groups**: No hard limit. Group management is O(1) for
  join/leave, O(P log P) for rebalance (P = partitions).
- **Messages**: Limited by available memory. Retention-based eviction
  ensures bounded memory usage (Phase 1+).

### 9.6 Benchmark Harness

Criterion benchmarks are provided in the `benches/` directory:

| Bench | File | What it measures |
|-------|------|------------------|
| `publish` | `benches/publish.rs` | Cost of `EventBus::publish()` |
| `consume` | `benches/consume.rs` | Cost of `EventBus::subscribe()` |
| `rebalance` | `benches/rebalance.rs` | Cost of `PartitionRebalancer::plan()` |
| `ack` | `benches/ack.rs` | Cost of `EventBus::ack()` |

In Phase 0, these benches measure the skeleton implementations (which
return `Err(ShuttingDown)`). They establish a baseline for measuring
the overhead of Phase 1+ functionality.

---

## 10. API Reference

### 10.1 Crate-Level Exports

```rust
// Configuration
pub use config::{EventBusConfig, Preset};
pub use builder::EventBusBuilder;

// Topic
pub use topic::{TopicConfig, TopicId};

// Partition
pub use partition::{Offset, PartitionId, PartitionState};

// Consumer
pub use consumer::{ConsumerGroupId, ConsumerId, SubscriptionMode};

// Error
pub use error::EventError;

// Stats
pub use stats::{EventBusStats, StatsSnapshot};

// Health
pub use health::{HealthReport, HealthState};

// Bus (Phase 1+)
pub use bus::EventBus;
pub use offset_store::OffsetStore;
pub use rebalancer::PartitionRebalancer;

// Adjacent-layer re-export
pub use session_manager::SessionId;
```

### 10.2 EventBus

```rust
impl EventBus {
    pub fn new(cfg: EventBusConfig) -> Self;

    pub fn create_topic(&mut self, config: TopicConfig) -> Result<TopicId, EventError>;
    pub fn publish(&self, topic: TopicId, partition: PartitionId, msg_hash: u64) -> Result<(), EventError>;
    pub fn subscribe(&self, group: ConsumerGroupId, topic: TopicId, mode: SubscriptionMode) -> Result<(), EventError>;
    pub fn ack(&self, group: ConsumerGroupId, partition: PartitionId, offset: Offset) -> Result<(), EventError>;

    pub fn health(&self) -> HealthReport;
}
```

### 10.3 OffsetStore

```rust
impl OffsetStore {
    pub fn new(cfg: EventBusConfig) -> Self;

    pub fn commit(&self, group: ConsumerGroupId, partition: PartitionId, offset: Offset) -> Result<(), EventError>;
    pub fn fetch(&self, group: ConsumerGroupId, partition: PartitionId) -> Result<Offset, EventError>;

    pub fn health(&self) -> HealthReport;
}
```

### 10.4 PartitionRebalancer

```rust
impl PartitionRebalancer {
    pub fn new(cfg: EventBusConfig) -> Self;

    pub fn plan(&self, group: ConsumerGroupId, partitions: &[PartitionId]) -> Result<(), EventError>;
    pub fn execute(&self, group: ConsumerGroupId) -> Result<(), EventError>;

    pub fn health(&self) -> HealthReport;
}
```

### 10.5 EventBusBuilder

```rust
impl EventBusBuilder {
    pub fn new() -> Self;
    pub fn from_preset(preset: Preset) -> Self;

    pub fn max_topics(self, topics: u32) -> Self;
    pub fn default_partitions(self, partitions: u8) -> Self;
    pub fn retention_ms(self, ms: u64) -> Self;
    pub fn max_message_bytes(self, bytes: u32) -> Self;
    pub fn consumer_group_timeout_ms(self, ms: u64) -> Self;

    pub fn build(self) -> Result<EventBusConfig, EventError>;
}
```

### 10.6 EventBusStats

```rust
impl EventBusStats {
    pub const fn new() -> Self;

    pub fn record_publish(&self, bytes: u64);
    pub fn record_consume(&self);
    pub fn record_publish_error(&self);
    pub fn record_consumer_lag(&self, lag_ns: u64);
    pub fn record_rebalance(&self);
    pub fn record_ack(&self);

    pub fn snapshot(&self) -> StatsSnapshot;
}
```

### 10.7 StatsSnapshot

```rust
impl StatsSnapshot {
    pub const fn publish_error_rate_bps(&self) -> u16;
    pub const fn has_active_rebalances(&self) -> bool;
}
```

### 10.8 HealthReport

```rust
impl HealthReport {
    pub const ERROR_DEGRADED_BPS: u16 = 100;  // 1%
    pub const ERROR_CRITICAL_BPS: u16 = 500;  // 5%

    pub fn derive(stats: &StatsSnapshot) -> Self;

    pub const fn is_healthy(&self) -> bool;
    pub const fn needs_attention(&self) -> bool;
}
```

### 10.9 HealthState

```rust
impl HealthState {
    pub const fn is_healthy(&self) -> bool;
    pub const fn needs_attention(&self) -> bool;
    pub const fn is_critical(&self) -> bool;
    pub const fn as_str(&self) -> &'static str;
}
```

### 10.10 EventError

```rust
impl EventError {
    pub const fn is_fatal(&self) -> bool;
    pub const fn is_retryable(&self) -> bool;
}
```

---

## 11. Testing Strategy

### 11.1 Test Counts

The crate has **110 tests** across all modules:

| Module | Test Count | Coverage |
|--------|------------|----------|
| `config.rs` | 14 | Config construction, builder, validation, presets, boundaries |
| `topic.rs` | 11 | TopicId, TopicConfig construction and validation |
| `partition.rs` | 13 | PartitionId, Offset, PartitionState |
| `consumer.rs` | 13 | ConsumerGroupId, ConsumerId, SubscriptionMode |
| `error.rs` | 11 | All variants, predicates, Display |
| `stats.rs` | 13 | All counters, snapshot, derived metrics |
| `health.rs` | 12 | All states, classification, thresholds |
| `builder.rs` | 8 | Builder construction, presets, overrides, validation |
| `bus.rs` | 6 | Bus creation, skeleton behavior, health |
| `offset_store.rs` | 4 | Store creation, skeleton behavior, health |
| `rebalancer.rs` | 4 | Rebalancer creation, skeleton behavior, health |
| **Total** | **~109-110** | |

### 11.2 Test Categories

#### 11.2.1 Construction Tests

Verify that types can be constructed correctly:
- `new()` produces the expected values.
- `default()` matches `new()`.
- `nil()` matches the `NIL` constant.

#### 11.2.2 Equality and Hashing Tests

Verify that types implement `PartialEq`, `Eq`, and `Hash` correctly:
- Equal values are equal.
- Different values are not equal.
- Hashing equal values produces the same hash.

#### 11.2.3 Validation Tests

Verify that `validate()` accepts valid configs and rejects invalid ones:
- Boundary values (min, max) are accepted.
- Out-of-range values are rejected.
- Zero values are rejected where applicable.

#### 11.2.4 Builder Tests

Verify that builder methods chain correctly and override defaults:
- Each `with_*` method sets the correct field.
- Chaining multiple methods works.
- `build()` validates before returning.

#### 11.2.5 Preset Tests

Verify that presets produce valid configs:
- Each preset has the expected field values.
- All presets pass validation.
- Builder overrides work on top of presets.

#### 11.2.6 Error Classification Tests

Verify that `is_fatal()` and `is_retryable()` correctly classify each
variant, and that the classifications are non-overlapping.

#### 11.2.7 Stats Tests

Verify that counters increment correctly and that the snapshot is
consistent:
- Each `record_*` method increments the correct counter.
- `snapshot()` returns the current values.
- Derived metrics (`publish_error_rate_bps`, `has_active_rebalances`)
  are computed correctly.

#### 11.2.8 Health Tests

Verify that health classification follows the "worst signal wins"
policy:
- Empty stats → Healthy.
- Low error rate → Healthy.
- Moderate error rate → Degraded.
- High error rate → Critical.
- Active rebalances → Degraded.
- Critical + degraded → Critical (worst wins).

#### 11.2.9 Skeleton Tests

Verify that Phase 0 skeleton implementations return the expected
`Err(ShuttingDown)` for all operations, and that `health()` returns
`Healthy` on a fresh instance.

### 11.3 Test Style

- Tests use the `#[test]` attribute with `assert!` and `assert_eq!`.
- Tests do NOT use `unwrap()` or `expect()` (denied by workspace lints).
  Instead, they use `assert!(result.is_ok())` or pattern matching.
- Each test function has a descriptive name like `topic_id_new` or
  `validate_rejects_bad_topic_count`.
- Tests are organized in a `#[cfg(test)] mod tests` block at the bottom
  of each source file.

### 11.4 Coverage Gaps

- No fuzzing (planned for Phase 1+).
- No property-based tests (planned for Phase 1+ with `proptest`).
- No integration tests (the crate is a library; integration tests live
  in the consuming crate).

---

## 12. Ops Runbook

### 12.1 Health Monitoring

Check the health of the event bus at any time:

```rust
let report = bus.health();
match report.state {
    HealthState::Healthy => { /* all good */ }
    HealthState::Degraded => { /* investigate */ }
    HealthState::Critical => { /* page on-call */ }
}
```

### 12.2 Common Alerts

#### 12.2.1 High Publish Error Rate

**Symptom**: `HealthReport.publish_error_rate_bps > 500` (5%)

**Investigation**:
1. Check `StatsSnapshot.publish_errors` for the trend.
2. Look at `tracing` logs for `EventError::PublishFailed` messages.
3. Check if any topics have `Sealed` partitions.
4. Check if message sizes exceed `max_message_bytes`.

**Mitigation**:
- Reduce publish rate.
- Increase `max_message_bytes` if messages are legitimately large.
- Unseal partitions if they were sealed by mistake.

#### 12.2.2 Consumer Lag

**Symptom**: `StatsSnapshot.consumer_lag_ns` is growing.

**Investigation**:
1. Identify which consumer groups are lagging.
2. Check if consumers are CPU-bound or I/O-bound.
3. Check if the consumer group has enough members.
4. Check if rebalancing is frequent (thrashing).

**Mitigation**:
- Add more consumers to the group.
- Optimize consumer processing (batching, async I/O).
- Increase `consumer_group_timeout_ms` if timeouts are too aggressive.
- Trigger a manual rebalance if partition assignment is uneven.

#### 12.2.3 Frequent Rebalances

**Symptom**: `StatsSnapshot.rebalance_count` is increasing rapidly.

**Investigation**:
1. Check if consumers are joining and leaving frequently.
2. Check for consumer crashes (look at logs).
3. Check if `consumer_group_timeout_ms` is too short.

**Mitigation**:
- Fix consumer crashes.
- Increase `consumer_group_timeout_ms`.
- Use sticky assignment (Phase 2+) to minimize partition movement.

### 12.3 Capacity Planning

| Resource | Formula | Example |
|----------|---------|---------|
| Topic metadata | `max_topics * sizeof(Topic)` | 100 topics * ~200 bytes = ~20 KB |
| Partition metadata | `topics * partitions * sizeof(Partition)` | 100 * 8 * ~100 bytes = ~80 KB |
| Offset table | `groups * partitions * sizeof(Offset)` | 10 * 800 * 8 bytes = ~64 KB |
| Message buffer | `sum(per-partition buffer)` | 800 * 1 MB = ~800 MB |

### 12.4 Startup

```rust
use event_bus::{EventBusBuilder, Preset};

let cfg = EventBusBuilder::from_preset(Preset::Clustered)
    .max_topics(200)
    .build()?;

let bus = EventBus::new(cfg);
```

### 12.5 Shutdown

The event bus does not have an explicit shutdown method in Phase 0. In
Phase 1+, `EventBus::shutdown()` will:

1. Stop accepting new publish/subscribe calls (return `ShuttingDown`).
2. Wait for in-flight messages to be consumed.
3. Flush offset commits.
4. Release all resources.

Callers should handle `EventError::ShuttingDown` gracefully by stopping
all activity and cleaning up.

### 12.6 Debugging

**Enable debug logging**:

```rust
RUST_LOG=event_bus=debug
```

**Key log messages**:

| Message | Level | When |
|---------|-------|------|
| `topic created: {topic_id}` | INFO | On `create_topic` success. |
| `publish failed: {error}` | WARN | On `publish` failure. |
| `consumer joined: {group_id}/{consumer_id}` | INFO | On `subscribe`. |
| `rebalance triggered: {group_id}` | INFO | On `rebalance.plan`. |
| `rebalance complete: {group_id}` | INFO | On `rebalance.execute` success. |
| `offset committed: {group_id}/{partition}/{offset}` | DEBUG | On `ack`. |

---

## 13. Security

### 13.1 Threat Model

The event bus is an intra-process component. It does not expose a
network interface and is not directly accessible to external actors.
The primary security concerns are:

1. **Unauthorized topic access**: A module publishing to or subscribing
   to a topic it should not have access to.
2. **Message tampering**: A module modifying messages in the partition
   log.
3. **Denial of service**: A module flooding the bus with messages,
   exhausting memory or CPU.
4. **Information leakage**: A consumer reading messages from a topic
   it should not have access to.

### 13.2 Mitigations

#### 13.2.1 Topic Access Control (Phase 2+)

Topic access will be controlled via the `auth-framework` crate. Each
topic will have an ACL (Access Control List) that specifies which
modules can publish and subscribe.

#### 13.2.2 Resource Quotas (Phase 1+)

The `resource-quotas` crate will enforce per-module limits on:
- Number of topics that can be created.
- Number of messages per second that can be published.
- Maximum message size.
- Number of consumer groups.

#### 13.2.3 Message Integrity (Phase 2+)

Messages will be checksummed (CRC32 or similar) to detect tampering.
The checksum is verified on consumption; a mismatch results in
`EventError::PublishFailed` (to be replaced by a dedicated
`MessageCorrupted` variant in Phase 1+).

#### 13.2.4 Audit Logging (Phase 2+)

All administrative operations (topic creation, deletion, rebalancing)
will be logged with the operator identity and timestamp. The audit log
will be tamper-evident (hash-chained).

### 13.3 Current State (Phase 0)

In Phase 0, there is no access control. Any module within the process
can create topics, publish, and subscribe. This is acceptable for the
current single-process, trusted-execution model. Phase 2+ will add
the mitigations described above.

### 13.4 Payload Handling

Message payloads are opaque to the event bus. In Phase 0, the payload
is a `u64` hash. In Phase 1+, it will be a byte slice (`&[u8]`). The
bus does not deserialize, inspect, or modify payloads. Payload
encryption is the responsibility of the producer and consumer.

---

## 14. Future Directions

### 14.1 Phase 1: Functional Implementation

- **EventBus**: Implement topic registry, partition logs, publish/subscribe.
- **OffsetStore**: Implement offset commit/fetch with in-memory storage.
- **PartitionRebalancer**: Implement range assignment algorithm.
- **Message payload**: Upgrade from `u64` hash to `&[u8]` byte slices.
- **Backpressure**: Implement flow control when consumers are slow.

### 14.2 Phase 2: Durability and Security

- **WAL integration**: Write-ahead logging for durability across restarts.
- **Offset persistence**: Persist offsets to disk or replicated store.
- **Access control**: Per-topic ACLs via `auth-framework`.
- **Resource quotas**: Per-module limits via `resource-quotas`.
- **Message checksums**: CRC32 integrity verification.
- **Sticky assignment**: Minimize partition movement during rebalancing.
- **Audit logging**: Tamper-evident log of administrative operations.

### 14.3 Phase 3: Cross-Process and Replication

- **pubsub-engine integration**: Cross-process and cross-node messaging.
- **Replication**: Replicate partition logs across nodes for fault tolerance.
- **Cooperative rebalancing**: Incremental rebalancing without full revocation.
- **Schema registry**: Integrate with a schema registry for payload validation.
- **Exactly-once**: Transactional consumer support for exactly-once semantics.
- **Tiered storage**: Move old messages to disk or object storage.

### 14.4 Phase 4: Advanced Features

- **Stream processing**: Built-in windowing, joins, and aggregations.
- **Compaction**: Log compaction to retain only the latest value per key.
- **Transactions**: Multi-partition transactions with two-phase commit.
- **Federation**: Cross-cluster topic mirroring and federation.
- **Smart routing**: Content-based routing and message filtering.

### 14.5 Research Directions

- **Lock-free partition logs**: Explore lock-free data structures for
  the partition log to eliminate contention on hot partitions.
- **Zero-copy delivery**: Explore zero-copy message delivery using
  `bytes::Bytes` or io_uring's registered buffers.
- **Adaptive partitioning**: Dynamically adjust partition counts based
  on load, without requiring a restart.
- **Backpressure algorithms**: Explore advanced backpressure algorithms
  (e.g., window-based, credit-based) to prevent consumer overload.
- **Predictive rebalancing**: Use machine learning to predict consumer
  failures and proactively rebalance before they occur.

### 14.6 Compatibility

The crate maintains semantic versioning. Phase 0 types are stable and
will not change in Phase 1. New methods and variants may be added, but
existing APIs will remain compatible:

- `EventError` is `#[non_exhaustive]`, so new variants can be added.
- `Preset` is `#[non_exhaustive]`, so new presets can be added.
- `HealthState` is `#[non_exhaustive]`, so new states can be added.
- `PartitionState` is `#[non_exhaustive]`, so new states can be added.
- All public structs have public fields, which will not be removed
  (though new fields may be added in major versions).

### 14.7 Deprecation Policy

When a Phase 1+ implementation replaces a Phase 0 skeleton:
1. The skeleton method will be deprecated with a `#[deprecated]`
   attribute pointing to the new method.
2. The skeleton will remain functional for one minor version.
3. The skeleton will be removed in the next major version.

---

## Appendix A: Glossary

| Term | Definition |
|------|------------|
| **Topic** | A named stream of events. |
| **Partition** | An ordered subset of a topic's events. |
| **Offset** | A monotonically increasing position within a partition. |
| **Consumer Group** | A group of consumers that jointly consume a topic. |
| **Rebalance** | The process of redistributing partitions among consumers. |
| **Retention** | How long messages are kept before being deleted. |
| **At-least-once** | Delivery semantic where messages may be redelivered but never skipped. |
| **Basis point (bp)** | 1/100 of a percent. 100 bps = 1%. |
| **Backpressure** | Flow control mechanism to slow down producers when consumers are slow. |
| **WAL** | Write-Ahead Log. A durability mechanism that writes to disk before acknowledging. |

## Appendix B: Type Size Reference

| Type | Size (bytes) | Alignment |
|------|-------------|-----------|
| `TopicId` | 8 | 8 |
| `TopicConfig` | 24 | 8 |
| `PartitionId` | 4 | 4 |
| `Offset` | 8 | 8 |
| `PartitionState` | 1 | 1 |
| `ConsumerGroupId` | 8 | 8 |
| `ConsumerId` | 8 | 8 |
| `SubscriptionMode` | 16 | 8 |
| `EventBusConfig` | 40 | 8 |
| `EventBusStats` | 56 | 8 |
| `StatsSnapshot` | 56 | 8 |
| `HealthReport` | 24 | 8 |
| `HealthState` | 1 | 1 |
| `EventError` | 32 | 8 |

## Appendix C: Constant Reference

| Constant | Module | Value |
|----------|--------|-------|
| `MIN_TOPICS` | config | 1 |
| `MAX_TOPICS` | config | 100,000 |
| `MIN_PARTITIONS` | config | 1 |
| `MAX_PARTITIONS` | config | 255 |
| `MIN_RETENTION_MS` | config | 3,600,000 (1 hour) |
| `MAX_RETENTION_MS` | config | 31,536,000,000 (365 days) |
| `MIN_MESSAGE_BYTES` | config | 512 |
| `MAX_MESSAGE_BYTES` | config | 50,000,000 |
| `MIN_GROUP_TIMEOUT_MS` | config | 5,000 (5 seconds) |
| `MAX_GROUP_TIMEOUT_MS` | config | 3,600,000 (1 hour) |
| `TopicId::NIL` | topic | 0 |
| `ConsumerGroupId::NIL` | consumer | 0 |
| `Offset::ZERO` | partition | 0 |
| `ERROR_DEGRADED_BPS` | health | 100 (1%) |
| `ERROR_CRITICAL_BPS` | health | 500 (5%) |

---

*End of document.*
