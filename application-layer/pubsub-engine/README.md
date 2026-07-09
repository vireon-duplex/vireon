# pubsub-engine

**Pub/Sub engine** for the Vireon realtime runtime. Provides topic
pattern matching, subscription management, and message delivery.

## Overview

The `pubsub-engine` crate sits above the `event-bus` and below the
application runtime, exposing a pattern-based publish/subscribe API.
Subscribers register wildcard patterns (e.g., `sensor.*.high`) and
receive messages whose concrete topic matches.

## Quick Start

```rust
use pubsub_engine::{PubSubBuilder, PubSubEngine, PubSubMessage, TopicPattern};

let cfg = PubSubBuilder::new()
    .max_subscriptions(500)
    .delivery_timeout_ms(15_000)
    .build()?;

let engine = PubSubEngine::new(cfg);

// Phase 0: skeleton — these return ShuttingDown errors
// Phase 1: real implementation
let pattern = TopicPattern::new("sensor.*")?;
let msg = PubSubMessage::new("sensor.temp", vec![1, 2, 3]);

// engine.subscribe(pattern)?;   // Phase 1
// engine.publish(msg)?;          // Phase 1

let health = engine.health();
assert!(health.is_healthy());
```

## Topic Patterns

Patterns use dot-delimited segments with `*` wildcards:

| Pattern         | Topic              | Match |
|-----------------|--------------------|-------|
| `sensor.temp`   | `sensor.temp`      | yes   |
| `sensor.*`      | `sensor.temp`      | yes   |
| `sensor.*`      | `sensor.temp.high` | no    |
| `*.*.*`         | `a.b.c`            | yes   |
| `sensor.*.high` | `sensor.temp.high` | yes   |

The `*` wildcard matches exactly one segment (MQTT single-level).

## Configuration

Three presets are available:

| Preset           | Max Subs | Patterns | Timeout | Backlog  |
|------------------|----------|----------|---------|----------|
| `Local`          | 128      | 64       | 5 s     | 1,024    |
| `Clustered`      | 1,024    | 256      | 10 s    | 10,000   |
| `HighThroughput` | 10,000   | 1,024    | 30 s    | 100,000  |

```rust
use pubsub_engine::{Preset, PubSubBuilder};

let cfg = PubSubBuilder::from_preset(Preset::HighThroughput)
    .max_subscriptions(5_000)
    .build()?;
```

## Observability

Stats are tracked with atomic counters and exported as `StatsSnapshot`:

```rust
let health = engine.health();
println!("state: {}", health.state.as_str());
println!("delivery success: {} bps", health.delivery_success_rate_bps);
```

Health states: `Healthy`, `Degraded` (>1% failures), `Critical` (>5% failures).

## Phase Status

- **Phase 0** (current): Foundational types, typed skeletons, docs, benchmarks.
- **Phase 1**: Real engine + trie implementation.
- **Phase 2**: Async delivery, backpressure, sharding.

See `DESIGN.md` for the full specification.

## Crate Layout

| Module           | Phase | Responsibility                          |
|------------------|-------|-----------------------------------------|
| `error`          | 0     | `PubSubError` typed failure surface     |
| `config`         | 0     | `PubSubConfig`, `Preset`                |
| `subscription`   | 0     | `SubscriptionId`, `TopicPattern`        |
| `message`        | 0     | `PubSubMessage`                         |
| `stats`          | 0     | `PubSubStats`, `StatsSnapshot`          |
| `health`         | 0     | `HealthReport`, `HealthState`           |
| `builder`        | 0     | `PubSubBuilder`                         |
| `engine`         | 1     | `PubSubEngine` facade                   |
| `trie`           | 1     | `TopicTrie` pattern matcher             |

## Verification

```bash
cargo check
cargo clippy -- -D warnings
cargo fmt --check
cargo test
cargo bench --no-run
```

All gates pass with 98 tests.

## Error Handling

All fallible APIs return `Result<_, PubSubError>`. Errors are classified
as fatal, retryable, or neither:

| Variant                | Fatal | Retryable | Action                    |
|------------------------|-------|-----------|---------------------------|
| `SubscriptionNotFound` | no    | no        | Check subscription ID     |
| `SubscriptionExists`   | yes   | no        | Abort; don't retry        |
| `PublishFailed`        | no    | yes       | Back off and retry        |
| `TopicMismatch`        | no    | no        | Verify topic/pattern      |
| `ShuttingDown`         | yes   | no        | Abort; don't retry        |

```rust
use pubsub_engine::PubSubError;

match result {
    Ok(_) => { /* success */ }
    Err(PubSubError::PublishFailed(msg)) => { /* retry with backoff */ }
    Err(PubSubError::ShuttingDown) => { /* abort */ }
    Err(e) if e.is_fatal() => { /* fatal: log and abort */ }
    Err(e) if e.is_retryable() => { /* retry */ }
    Err(_) => { /* handle other errors */ }
}
```

## Dependencies

| Crate                  | Type     | Purpose                          |
|------------------------|----------|----------------------------------|
| `configuration-system` | path     | Configuration loading            |
| `event-bus`            | path     | Lower-level topic management     |
| `thiserror`            | workspace| Error derive macro               |
| `tracing`              | workspace| Structured logging (Phase 1+)    |
| `criterion`            | dev      | Benchmark framework              |

## Re-exports

The crate re-exports its public types and `event_bus::TopicId` for
convenience:

```rust
use pubsub_engine::{
    HealthReport, Preset, PubSubBuilder, PubSubConfig, PubSubEngine,
    PubSubError, PubSubMessage, PubSubStats, StatsSnapshot,
    SubscriptionId, TopicId, TopicPattern, TopicTrie,
};
```

## License

GNU AFFERO GENERAL PUBLIC LICENSE v3.0
