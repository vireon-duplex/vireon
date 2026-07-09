# event-bus

**Partitioned event bus** for the Vireon application layer. Provides topics,
partitions, consumer groups, offset tracking, and partition rebalancing.

## Overview

The event bus is the primary intra-process pub/sub mechanism in Vireon. It
follows the partitioned log model popularized by Kafka and Pulsar, adapted
for the single-process, io_uring-driven Vireon runtime.

Key features:

- Ordered delivery within a partition
- Parallel consumption across partitions
- Consumer group semantics with offset tracking
- Automatic partition rebalancing
- Typed errors (`EventError`) with retryable/fatal predicates
- Atomic stats counters and tri-state health reports
- Configuration presets (Local, Clustered, HighThroughput)

## Layer Map

| #  | Module          | Phase | Responsibility                          |
|----|-----------------|-------|-----------------------------------------|
| 1  | `error`         | 0     | `EventError` thiserror enum             |
| 2  | `config`        | 0     | `EventBusConfig`, `Preset`              |
| 3  | `topic`         | 0     | `TopicId`, `TopicConfig`                |
| 4  | `partition`     | 0     | `PartitionId`, `Offset`, `PartitionState` |
| 5  | `consumer`      | 0     | `ConsumerGroupId`, `ConsumerId`, `SubscriptionMode` |
| 6  | `stats`         | 0     | `EventBusStats` atomics, `StatsSnapshot` |
| 7  | `health`        | 0     | `HealthReport`, `HealthState`           |
| 8  | `builder`       | 0     | `EventBusBuilder` (fluent)              |
| 9  | `bus`           | 1     | `EventBus` facade                       |
| 10 | `offset_store`  | 1     | `OffsetStore` (consumer progress)       |
| 11 | `rebalancer`    | 1     | `PartitionRebalancer` (distribution)    |

Phase 0 modules are fully implemented. Modules 9-11 ship as typed skeletons
and return `Err(EventError::ShuttingDown)` until Phase 1.

## Quick Start

```rust
use event_bus::{EventBusBuilder, EventBus, Preset, TopicConfig};

// Build config from a preset with overrides.
let cfg = EventBusBuilder::from_preset(Preset::Clustered)
    .max_topics(200)
    .build()?;

// Create the event bus.
let mut bus = EventBus::new(cfg);

// Create a topic with 8 partitions.
let topic = bus.create_topic(TopicConfig::new(8))?;

// Check health.
let report = bus.health();
assert!(report.is_healthy());
```

## Presets

| Preset           | Topics | Partitions | Retention | Message Size | Group Timeout |
|------------------|--------|------------|-----------|--------------|---------------|
| `Local`          | 10     | 8          | 1 day     | 1 MB         | 30 s          |
| `Clustered`      | 100    | 8          | 7 days    | 1 MB         | 60 s          |
| `HighThroughput` | 1000   | 128        | 30 days   | 16 MB        | 120 s         |

All presets produce validated configs. Override any field with builder
methods after materializing:

```rust
let cfg = Preset::Local.to_config()
    .with_max_topics(50)
    .with_retention_ms(604_800_000);
```

## Hook Triggers (Event Flow)

The event bus drives the following event flow through the Vireon
application layer:

```
Producer                EventBus                Consumer Group
   |                       |                        |
   |--- publish --------->>|                        |
   |                       |--- partition select    |
   |                       |--- append to log       |
   |                       |--- stats.record_publish|
   |                       |                        |
   |                       |<<-- poll -------------<<|
   |                       |--- deliver message --->>|
   |                       |--- stats.record_consume|
   |                       |                        |
   |                       |<<-- ack --------------<<|
   |                       |--- offset commit       |
   |                       |--- stats.record_ack    |
   |                       |                        |
   |                       |<<-- join/leave -------<<|
   |                       |--- rebalancer.plan     |
   |                       |--- rebalancer.execute  |
   |                       |--- stats.record_rebal  |
```

Lifecycle hooks (consumed by `scheduler-hooks`):

| Trigger           | When                          | Hook Subscriber        |
|-------------------|-------------------------------|------------------------|
| `on_publish`      | After message is appended     | scheduler-hooks        |
| `on_consume`      | After message is delivered    | scheduler-hooks        |
| `on_ack`          | After offset is committed     | scheduler-hooks        |
| `on_rebalance`    | After rebalance completes     | scheduler-hooks        |

## Error Handling

All fallible APIs return `Result<T, EventError>`. Use the built-in
predicates for retry logic:

```rust
match bus.publish(topic, partition, hash) {
    Ok(()) => { /* success */ }
    Err(e) if e.is_retryable() => { /* back off and retry */ }
    Err(e) if e.is_fatal() => { /* abort */ }
    Err(e) => { /* handle */ }
}
```

## Observability

```rust
let snapshot = bus.stats_snapshot();
println!("published: {}", snapshot.messages_published);
println!("error rate: {} bps", snapshot.publish_error_rate_bps());

let health = bus.health();
match health.state {
    HealthState::Healthy => { /* nominal */ }
    HealthState::Degraded => { /* investigate */ }
    HealthState::Critical => { /* page on-call */ }
}
```

## Testing

The crate has 110 tests covering all Phase 0 modules. Run them with:

```sh
cargo test
```

Benchmarks are provided via Criterion:

```sh
cargo bench
```

## Design Document

See `DESIGN.md` for the full 14-section specification covering architecture,
data structures, algorithms, failure modes, observability, configuration,
presets, performance, API reference, testing strategy, ops runbook,
security, and future directions.
