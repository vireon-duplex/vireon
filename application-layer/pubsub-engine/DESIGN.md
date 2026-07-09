# DESIGN.md — pubsub-engine

> **Pub/Sub Engine** for the Vireon realtime runtime.
>
> This document specifies the architecture, data structures, algorithms,
> error model, observability surface, configuration system, and phased
> implementation plan for the `pubsub-engine` crate.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Goals & Non-Goals](#2-goals--non-goals)
3. [Architecture](#3-architecture)
4. [Subscriptions & Topic Patterns](#4-subscriptions--topic-patterns)
5. [Topic Pattern Matching Algorithm](#5-topic-pattern-matching-algorithm)
6. [Topic Trie](#6-topic-trie)
7. [Message Model](#7-message-model)
8. [Delivery Model](#8-delivery-model)
9. [Engine Facade](#9-engine-facade)
10. [Topic Trie Internals](#10-topic-trie-internals)
11. [Failure Modes](#11-failure-modes)
12. [Observability](#12-observability)
13. [Configuration & Presets](#13-configuration--presets)
14. [Builder Pattern](#14-builder-pattern)
15. [Concurrency Model](#15-concurrency-model)
16. [Performance Targets](#16-performance-targets)
17. [Security Considerations](#17-security-considerations)
18. [Integration Points](#18-integration-points)
19. [Testing Strategy](#19-testing-strategy)
20. [Phased Implementation Plan](#20-phased-implementation-plan)

---

## 1. Overview

The `pubsub-engine` crate provides a **publish/subscribe** messaging
abstraction for the Vireon application layer. It sits above the
`event-bus` crate and below the application-runtime, providing a
higher-level messaging API focused on:

- **Topic pattern matching** — subscribers register wildcard patterns
  (e.g., `sensor.*.high`) and receive messages whose concrete topic
  matches the pattern.
- **Subscription management** — create, track, and remove subscriptions
  with unique IDs, enforcing configurable limits.
- **Message delivery** — route published messages to all matching
  subscriptions with backpressure and backlog management.
- **Observability** — atomic counters, health derivation, and rate
  calculations for operational monitoring.

### Positioning in the Vireon Stack

```text
┌───────────────────────────────────────────────────┐
│              Application Runtime                   │
├───────────────────────────────────────────────────┤
│            Pub/Sub Engine  ←── this crate          │
├───────────────────────────────────────────────────┤
│              Event Bus                             │
├───────────────────────────────────────────────────┤
│         Session / Stream / Transport               │
├───────────────────────────────────────────────────┤
│               Kernel / NIC                         │
└───────────────────────────────────────────────────┘
```

The pub/sub engine uses the event-bus for lower-level topic and partition
management, while exposing a simpler pattern-matching API to the
application layer.

### Design Principles

1. **Typed errors** — Every fallible API returns `Result<_, PubSubError>`
   with an exhaustive, `#[non_exhaustive]` enum. No string matching.
2. **Copy config** — `PubSubConfig` is `Copy`, making it cheap to pass
   around and impossible to accidentally mutate.
3. **Atomic stats** — All counters use `AtomicU64` with `Relaxed` ordering
   for minimal overhead on hot paths.
4. **Worst-signal-wins health** — Health derivation takes the most severe
   signal across all dimensions.
5. **Skeleton-first** — Phase 0 ships typed skeletons that compile and
   test, returning `ShuttingDown` errors. Phase 1+ fills in real logic.

---

## 2. Goals & Non-Goals

### Goals

- Provide a clean, typed pub/sub API for Vireon applications.
- Support wildcard topic pattern matching with `*` (single segment).
- Manage subscriptions with unique IDs and configurable limits.
- Track delivery statistics with atomic counters for lock-free reads.
- Derive health from stats using a deterministic, worst-signal-wins policy.
- Compile and pass all verification gates in Phase 0 (skeleton facades).
- Benchmark hot paths (publish, subscribe, match, deliver) with Criterion.

### Non-Goals (Phase 0)

- Persistent message storage (future phase, possibly event-bus).
- Ordered delivery guarantees (Phase 2+).
- At-least-once / exactly-once delivery semantics (Phase 2+).
- Cross-cluster message replication (future phase).
- Recursive wildcard `#` matching (Phase 2+).
- Message compression or transformation (middleware pipeline concern).
- Authentication / authorization (auth-framework concern).

### Non-Goals (Permanent)

- Replacing the event-bus — pubsub-engine builds on top of it.
- Direct QUIC transport — handled by transport layer.
- Schema registry — separate crate if needed.
- Message serialization format — payload is opaque `Vec<u8>`.

---

## 3. Architecture

### Module Map

| #  | Module           | Phase | Responsibility                                      |
|----|------------------|-------|-----------------------------------------------------|
| 1  | `error`          | 0 ✅  | `PubSubError` thiserror enum with `is_fatal`/`is_retryable` |
| 2  | `config`         | 0 ✅  | `PubSubConfig`, `Preset`, validation, MIN/MAX constants |
| 3  | `subscription`   | 0 ✅  | `SubscriptionId` newtype, `TopicPattern` with wildcard matching |
| 4  | `message`        | 0 ✅  | `PubSubMessage` struct with topic, payload, timestamp, headers |
| 5  | `stats`          | 0 ✅  | `PubSubStats` atomic counters, `StatsSnapshot` with rate calculations |
| 6  | `health`         | 0 ✅  | `HealthReport`, `HealthState` tri-state with worst-signal-wins |
| 7  | `builder`        | 0 ✅  | `PubSubBuilder` fluent builder with validation on build |
| 8  | `engine`         | 1    | `PubSubEngine` facade — publish, subscribe, unsubscribe, health |
| 9  | `trie`           | 1    | `TopicTrie` — O(k) pattern insertion and lookup |

### Data Flow

```text
                    ┌──────────────┐
                    │  Publisher   │
                    └──────┬───────┘
                           │ PubSubMessage
                           ▼
                    ┌──────────────┐
                    │ PubSubEngine │
                    │  .publish()  │
                    └──────┬───────┘
                           │
                           ▼
                    ┌──────────────┐
                    │  TopicTrie   │
                    │  .lookup()   │
                    └──────┬───────┘
                           │ Vec<SubscriptionId>
                           ▼
                    ┌──────────────┐
                    │  Delivery    │
                    │  (per-sub)   │
                    └──────────────┘
```

### Module Dependencies

```text
error ←── config
error ←── subscription
error ←── stats (indirectly, via health)
config ←── builder
config ←── engine
config ←── trie
stats ←── health
stats ←── engine
stats ←── trie
health ←── engine
health ←── trie
subscription ←── engine
message ←── engine
```

The `error` module is the leaf dependency — it depends only on
`subscription` (for `SubscriptionId` used in error variants) and
`thiserror`.

---

## 4. Subscriptions & Topic Patterns

### SubscriptionId

A `SubscriptionId` is a `u64` newtype with the following properties:

- **NIL constant** — `SubscriptionId::NIL` (value 0) is the reserved nil
  value, used for "no subscription" semantics.
- **Factory methods** — `new(u64)`, `nil()`, `is_nil()`, `raw()`.
- **Display** — Formats as `sub#{id}` for human-readable error messages.
- **Derives** — `Clone, Copy, Debug, PartialEq, Eq, Hash` for use as map
  keys and cheap passing.
- **Default** — `Default::default()` returns `NIL`.

```rust
pub struct SubscriptionId(u64);

impl SubscriptionId {
    pub const NIL: Self = Self(0);
    pub const fn new(id: u64) -> Self { Self(id) }
    pub const fn nil() -> Self { Self::NIL }
    pub const fn is_nil(&self) -> bool { self.0 == 0 }
    pub const fn raw(self) -> u64 { self.0 }
}
```

### TopicPattern

A `TopicPattern` is an owned string with validation and matching logic:

- **Validation** — Non-empty, max 256 bytes.
- **Matching** — Simple wildcard `*` matching on dot-delimited segments.
- **Ownership** — Owns the pattern string for zero-copy matching.
- **Derives** — `Clone, Debug, PartialEq, Eq, Hash`.

#### Matching Rules

| Pattern         | Topic           | Match? | Reason                          |
|-----------------|-----------------|--------|---------------------------------|
| `sensor.temp`   | `sensor.temp`   | ✅     | Exact match                     |
| `sensor.temp`   | `sensor.humidity` | ❌   | Literal segment mismatch        |
| `sensor.*`      | `sensor.temp`   | ✅     | `*` matches single segment      |
| `sensor.*`      | `sensor.temp.high` | ❌  | `*` matches one segment only    |
| `*`             | `sensor`        | ✅     | `*` matches single segment      |
| `*`             | `sensor.temp`   | ❌     | `*` matches one segment only    |
| `*.*.*`         | `a.b.c`         | ✅     | Three wildcards, three segments |
| `sensor.*.high` | `sensor.temp.high` | ✅  | Mixed wildcard + literal        |

The wildcard `*` matches exactly one dot-delimited segment. There is no
recursive `#` wildcard in Phase 0. This matches the MQTT single-level
wildcard semantics.

#### Implementation

```rust
pub fn matches(&self, topic: &str) -> bool {
    let pat_segments: Vec<&str> = self.pattern.split('.').collect();
    let topic_segments: Vec<&str> = topic.split('.').collect();
    if pat_segments.len() != topic_segments.len() {
        return false;
    }
    for (pat, topic) in pat_segments.iter().zip(topic_segments.iter()) {
        if *pat != "*" && *pat != *topic {
            return false;
        }
    }
    true
}
```

This is O(k) where k is the number of segments. In Phase 1, the
`TopicTrie` will provide the same matching semantics but with O(1)
amortized lookup per segment via tree traversal, avoiding the repeated
allocation of `split('.')`.

---

## 5. Topic Pattern Matching Algorithm

### Phase 0: Linear Match

In Phase 0, pattern matching is done via `TopicPattern::matches()`, which
splits both pattern and topic by `.` and compares segment-by-segment.

**Complexity**: O(k) per match, where k = number of segments.

**Allocation**: Each call allocates two `Vec<&str>` for the split
segments. This is acceptable for Phase 0 (bench stubs) but will be
replaced in Phase 1.

### Phase 1: Trie-Based Match

In Phase 1, the `TopicTrie` will provide O(1) amortized per-segment
traversal:

1. Insert: Walk the trie along pattern segments, creating nodes as
   needed. At the leaf, store the `SubscriptionId`.
2. Lookup: Walk the trie along topic segments. At each node, follow both
   the literal segment child and the `*` child. Collect all subscription
   IDs at matching leaves.

**Complexity**: O(k * w) per lookup, where k = segments, w = wildcard
fan-out (typically 1-2, since `*` is the only wildcard).

### Phase 2+: Recursive Wildcard

A future `#` wildcard will match zero or more segments (MQTT multi-level
wildcard). This requires a different trie traversal strategy (depth-first
with backtracking) and is deferred to Phase 2+.

### Matching Semantics Summary

| Feature              | Phase 0 | Phase 1 | Phase 2+ |
|----------------------|---------|---------|----------|
| Exact match          | ✅      | ✅      | ✅       |
| Single `*` wildcard  | ✅      | ✅      | ✅       |
| Multiple `*` wildcards | ✅    | ✅      | ✅       |
| Trie-based lookup    | ❌      | ✅      | ✅       |
| Recursive `#` wildcard | ❌    | ❌      | ✅       |
| Pattern validation   | ✅      | ✅      | ✅       |

---

## 6. Topic Trie

### Purpose

The `TopicTrie` provides efficient topic pattern matching for the
pub/sub engine. It stores patterns as paths in a trie, allowing O(k)
insertion and lookup where k is the number of dot-delimited segments.

### Structure (Phase 1 Design)

```text
Root
├── "sensor" (literal)
│   ├── "temp" (literal)
│   │   └── [leaf: sub#1]
│   ├── "*" (wildcard)
│   │   └── [leaf: sub#2]
│   └── "humidity" (literal)
│       └── "high" (literal)
│           └── [leaf: sub#3]
├── "alert" (literal)
│   └── "*" (wildcard)
│       └── [leaf: sub#4]
└── "*" (wildcard)
    └── "*" (wildcard)
        └── [leaf: sub#5]
```

### Node Structure (Planned)

```rust
struct TrieNode {
    children: HashMap<String, TrieNode>,  // literal segment → child
    wildcard: Option<Box<TrieNode>>,       // "*" segment → child
    subscriptions: Vec<SubscriptionId>,    // subscriptions at this node
}
```

### Operations

- **insert(pattern, sub_id)**: Split pattern by `.`, walk/create nodes
  for each segment. At the final node, append `sub_id` to the
  subscription list.
- **lookup(topic)**: Split topic by `.`. At each level, follow both the
  literal child and the wildcard child. Collect all subscription IDs
  found at matching leaf nodes.

### Phase 0 Skeleton

In Phase 0, `TopicTrie` is a typed skeleton:

```rust
pub struct TopicTrie {
    cfg: PubSubConfig,
    stats: PubSubStats,
}

impl TopicTrie {
    pub fn new(cfg: PubSubConfig) -> Self { ... }
    pub fn insert(&mut self, _pattern: &str, _sub_id: SubscriptionId) -> Result<(), PubSubError> {
        // TODO(phase-1)
        Err(PubSubError::ShuttingDown)
    }
    pub fn lookup(&self, _topic: &str) -> Result<Vec<SubscriptionId>, PubSubError> {
        // TODO(phase-1)
        Err(PubSubError::ShuttingDown)
    }
    pub fn health(&self) -> HealthReport { ... }
}
```

All methods return `Err(PubSubError::ShuttingDown)` to indicate the
skeleton state. The `health()` method works immediately since it only
depends on the stats counters.

---

## 7. Message Model

### PubSubMessage

A `PubSubMessage` carries:

| Field           | Type                    | Description                          |
|-----------------|-------------------------|--------------------------------------|
| `topic`         | `String`                | The concrete topic (e.g., `sensor.temp`) |
| `payload`       | `Vec<u8>`               | Binary payload (opaque to engine)    |
| `timestamp_ms`  | `u64`                   | Milliseconds since epoch             |
| `headers`       | `BTreeMap<String, String>` | Optional key-value headers       |

### Design Choices

- **Owned topic** — The topic is an owned `String` because the engine
  needs to retain it for pattern matching during delivery.
- **Owned payload** — `Vec<u8>` is used instead of `bytes::Bytes` to
  avoid an external dependency. In Phase 1+, if buffer pooling is
  available, the payload type may be changed.
- **BTreeMap headers** — Ordered map for deterministic iteration and
  reproducible serialization. The map is small (typically <10 entries).
- **No type erasure** — The message is a plain struct, not a trait
  object. This keeps it `Copy`-friendly for benchmarks and avoids dynamic
  dispatch.

### Construction

```rust
// Minimal
let msg = PubSubMessage::new("sensor.temp", vec![1, 2, 3]);

// With timestamp
let msg = PubSubMessage::with_timestamp("sensor.temp", vec![1, 2, 3], 1_700_000_000_000);

// With headers
let msg = PubSubMessage::new("sensor.temp", vec![1, 2, 3])
    .with_header("source", "sensor-1")
    .with_header("priority", "high");
```

### Accessors

- `topic()` — Returns `&str` for the topic.
- `payload_size()` — Returns `usize` for the payload byte count.
- `header(key)` — Returns `Option<&str>` for a header value.

---

## 8. Delivery Model

### Phase 0: No Delivery

In Phase 0, `PubSubEngine::publish()` returns
`Err(PubSubError::ShuttingDown)`. No messages are delivered.

### Phase 1: Synchronous Delivery

In Phase 1, delivery will be synchronous:

1. Publisher calls `engine.publish(msg)`.
2. Engine looks up matching subscriptions via `TopicTrie::lookup()`.
3. For each matching subscription, the message is enqueued in the
   subscription's backlog.
4. Subscribers poll their backlog via `engine.poll(sub_id)`.

### Phase 2: Async Delivery

In Phase 2, delivery will be async with backpressure:

1. Subscribers register a callback or channel.
2. The engine spawns delivery tasks for each matching subscription.
3. Backpressure is applied via the backlog limit — if a subscription's
   backlog is full, the engine applies backpressure to the publisher.

### Backlog Management

Each subscription has a backlog (message queue) with a configurable
limit (`PubSubConfig::backlog_limit`). When the backlog is full:

- **Drop oldest** — Remove the oldest message and enqueue the new one.
  This is the default policy.
- **Reject** — Return an error to the publisher. (Future policy.)
- **Block** — Apply backpressure. (Phase 2+.)

### Delivery Timeout

Each message has a delivery timeout (`PubSubConfig::delivery_timeout_ms`).
If a message is not delivered within the timeout, it is considered
failed and a delivery failure is recorded.

---

## 9. Engine Facade

### PubSubEngine

The `PubSubEngine` is the main entry point for the pub/sub API.

```rust
pub struct PubSubEngine {
    cfg: PubSubConfig,
    stats: PubSubStats,
}
```

### API

| Method                          | Returns                        | Phase |
|---------------------------------|--------------------------------|-------|
| `new(cfg: PubSubConfig)`        | `Self`                         | 0 ✅  |
| `publish(msg: PubSubMessage)`   | `Result<(), PubSubError>`      | 1    |
| `subscribe(pattern: TopicPattern)` | `Result<SubscriptionId, PubSubError>` | 1 |
| `unsubscribe(id: SubscriptionId)` | `Result<(), PubSubError>`    | 1    |
| `health()`                      | `HealthReport`                 | 0 ✅  |

### Phase 0 Behavior

- `publish()` → `Err(PubSubError::ShuttingDown)`
- `subscribe()` → `Err(PubSubError::ShuttingDown)`
- `unsubscribe()` → `Err(PubSubError::ShuttingDown)`
- `health()` → Derives from stats (always `Healthy` initially)

### Debug Formatting

The engine implements `Debug` manually to avoid requiring `Debug` on
`PubSubStats` (which contains atomics that don't implement `Debug`
in the same way):

```rust
impl fmt::Debug for PubSubEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PubSubEngine")
            .field("cfg", &self.cfg)
            .field("stats", &"<stats>")
            .finish()
    }
}
```

---

## 10. Topic Trie Internals

### Node Design (Phase 1)

Each trie node contains:

1. **Children map** — Maps literal segment strings to child nodes.
   Uses `HashMap<String, TrieNode>` for O(1) average-case lookup.
2. **Wildcard child** — An optional `Box<TrieNode>` for the `*` segment.
   This is separate from the children map to avoid key collision with
   a literal segment named `*`.
3. **Subscription set** — A `Vec<SubscriptionId>` of subscriptions that
   terminate at this node.

### Insertion Algorithm

```text
insert(pattern, sub_id):
    node = root
    for segment in pattern.split('.'):
        if segment == "*":
            if node.wildcard is None:
                node.wildcard = Some(Box::new(TrieNode::new()))
            node = node.wildcard
        else:
            if segment not in node.children:
                node.children[segment] = TrieNode::new()
            node = node.children[segment]
    node.subscriptions.push(sub_id)
```

### Lookup Algorithm

```text
lookup(topic):
    results = []
    frontier = [(root, 0)]  // (node, segment_index)
    segments = topic.split('.')
    while frontier is not empty:
        (node, i) = frontier.pop()
        if i == segments.len():
            results.extend(node.subscriptions)
            continue
        segment = segments[i]
        // Follow literal child
        if segment in node.children:
            frontier.push((node.children[segment], i + 1))
        // Follow wildcard child
        if node.wildcard is Some:
            frontier.push((node.wildcard, i + 1))
    return results
```

### Complexity

| Operation | Time Complexity         | Space Complexity  |
|-----------|------------------------|-------------------|
| Insert    | O(k)                   | O(k) per pattern  |
| Lookup    | O(k * w) where w ≤ 2  | O(k * w) frontier |
| Remove    | O(k) + cleanup         | O(1) per removed  |

### Memory

Each node is approximately:
- 48 bytes for `HashMap` (empty)
- 8 bytes for `Option<Box<TrieNode>>`
- 24 bytes for `Vec<SubscriptionId>` (empty)
- Total: ~80 bytes per node (empty)

For a typical deployment with 256 patterns averaging 3 segments:
- 768 nodes × 80 bytes = ~60 KB
- Negligible compared to message buffers.

---

## 11. Failure Modes

### PubSubError

```rust
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum PubSubError {
    SubscriptionNotFound(SubscriptionId),
    SubscriptionExists(SubscriptionId),
    PublishFailed(&'static str),
    TopicMismatch,
    ShuttingDown,
}
```

### Error Categories

| Category     | Variants                          | `is_fatal` | `is_retryable` | Caller Action                    |
|--------------|-----------------------------------|------------|----------------|----------------------------------|
| Fatal        | `SubscriptionExists`, `ShuttingDown` | ✅        | ❌             | Abort; don't retry               |
| Retryable    | `PublishFailed`                   | ❌         | ✅             | Back off + retry                 |
| Not-found    | `SubscriptionNotFound`            | ❌         | ❌             | Check subscription ID            |
| Mismatch     | `TopicMismatch`                   | ❌         | ❌             | Verify topic/pattern alignment   |

### Error Messages

Each variant has a human-readable message via `#[error("...")]`:

| Variant                  | Message Format                                    |
|--------------------------|---------------------------------------------------|
| `SubscriptionNotFound(id)` | `subscription not found: sub#{id}`             |
| `SubscriptionExists(id)`   | `subscription already exists: sub#{id}`        |
| `PublishFailed(msg)`       | `publish failed: {msg}`                        |
| `TopicMismatch`          | `topic mismatch: expected pattern does not match topic` |
| `ShuttingDown`           | `pub/sub engine is shutting down`               |

### Display Implementation

`SubscriptionId` implements `Display` to produce `sub#{id}` format,
which is used in `#[error("...{0}...")]` thiserror format strings.

### Non-Exhaustive

The enum is `#[non_exhaustive]` to allow adding new variants in future
phases without breaking downstream match expressions. Callers must
include a `_ =>` arm in match expressions.

### Error Handling Guidelines

1. **Fatal errors** — The caller must not retry. These indicate
   programming mistakes (duplicate subscription) or terminal states
   (shutdown in progress).
2. **Retryable errors** — The caller may retry after a backoff.
   Typically transient (publish failed due to resource pressure).
3. **Not-found errors** — The caller should verify the subscription ID
   and handle gracefully (e.g., log a warning and skip).
4. **Mismatch errors** — The caller should verify the topic and pattern
   are compatible before retrying.

---

## 12. Observability

### PubSubStats

Atomic counters for the pub/sub engine:

| Counter                 | Type      | Description                              |
|-------------------------|-----------|------------------------------------------|
| `messages_published`    | AtomicU64 | Total messages published (counter)       |
| `messages_delivered`    | AtomicU64 | Total messages delivered (counter)       |
| `delivery_failures`     | AtomicU64 | Delivery failures (counter)              |
| `subscriptions_active`  | AtomicU64 | Currently active subscriptions (gauge)   |
| `subscriptions_created` | AtomicU64 | Total subscriptions created (counter)    |

### Recording Methods

- `record_publish()` — Increment `messages_published`.
- `record_deliver()` — Increment `messages_delivered`.
- `record_delivery_failure()` — Increment `delivery_failures`.
- `record_subscription_created()` — Increment both `subscriptions_created`
  and `subscriptions_active`.
- `record_subscription_removed()` — Decrement `subscriptions_active`.
- `set_active_subscriptions(count)` — Set `subscriptions_active` directly.

### StatsSnapshot

A `Copy` struct for point-in-time reads:

```rust
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    pub messages_published: u64,
    pub messages_delivered: u64,
    pub delivery_failures: u64,
    pub subscriptions_active: u64,
    pub subscriptions_created: u64,
}
```

### Rate Calculations

Rates are in **basis points** (bps), where 10,000 bps = 100%.

#### Delivery Success Rate

```rust
pub const fn delivery_success_rate_bps(&self) -> u16 {
    let total = self.messages_delivered.saturating_add(self.delivery_failures);
    if total == 0 {
        return 10_000; // 100% — no failures observed
    }
    let rate = (self.messages_delivered.saturating_mul(10_000)) / total;
    if rate > u16::MAX as u64 { u16::MAX } else { rate as u16 }
}
```

- Returns 10,000 (100%) when there are no deliveries — since no failures
  have occurred, the system is "healthy by default."
- Uses `saturating_mul` and `saturating_add` to prevent overflow.
- Integer division truncates: 2*10000/3 = 6666, not 6667.

#### Delivery Failure Rate

```rust
pub const fn delivery_failure_rate_bps(&self) -> u16 {
    let total = self.messages_delivered.saturating_add(self.delivery_failures);
    if total == 0 {
        return 0;
    }
    let rate = (self.delivery_failures.saturating_mul(10_000)) / total;
    if rate > u16::MAX as u64 { u16::MAX } else { rate as u16 }
}
```

- Returns 0 when there are no deliveries.
- Complementary to `delivery_success_rate_bps`, but integer truncation
  may cause `success + failure ≠ 10_000` for non-evenly-divisible totals.

### HealthReport

Derived from `StatsSnapshot` using a worst-signal-wins policy:

```rust
pub struct HealthReport {
    pub state: HealthState,
    pub messages_published: u64,
    pub delivery_success_rate_bps: u16,
    pub delivery_failure_rate_bps: u16,
    pub active_subscriptions: u64,
}
```

### HealthState

Tri-state classification:

| State      | Meaning                          | `as_str()`  |
|------------|----------------------------------|-------------|
| `Healthy`  | All systems nominal              | `"healthy"` |
| `Degraded` | Performance or availability degraded | `"degraded"` |
| `Critical` | Severe impact; page on-call      | `"critical"`|

### Health Thresholds

| Threshold                   | Value  | Meaning                          |
|-----------------------------|--------|----------------------------------|
| `FAILURE_DEGRADED_BPS`      | 100    | >1% failure rate → Degraded      |
| `FAILURE_CRITICAL_BPS`      | 500    | >5% failure rate → Critical      |

### Derivation Algorithm

```text
derive(stats):
    failure_rate = stats.delivery_failure_rate_bps()
    if failure_rate > FAILURE_CRITICAL_BPS:
        state = Critical
    else if failure_rate > FAILURE_DEGRADED_BPS:
        state = Degraded
    else:
        state = Healthy
    return HealthReport { state, ... }
```

Currently, only the delivery failure rate is used as a health signal.
In future phases, additional signals may include:
- Backlog saturation (queued messages / backlog limit).
- Active subscription count vs. max.
- Delivery latency (p99).

---

## 13. Configuration & Presets

### PubSubConfig

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PubSubConfig {
    pub max_subscriptions: u32,
    pub max_topic_patterns: u32,
    pub delivery_timeout_ms: u64,
    pub backlog_limit: u32,
}
```

### Field Reference

| Field                  | Type  | Min    | Max        | Default  | Description                          |
|------------------------|-------|--------|------------|----------|--------------------------------------|
| `max_subscriptions`    | u32   | 1      | 1,000,000  | 1,024    | Max concurrent subscriptions         |
| `max_topic_patterns`   | u32   | 1      | 100,000    | 256      | Max topic patterns tracked           |
| `delivery_timeout_ms`  | u64   | 100    | 300,000    | 10,000   | Delivery timeout (ms)                |
| `backlog_limit`        | u32   | 16     | 1,000,000  | 10,000   | Max queued messages per subscription |

### Constants

```rust
pub const MIN_MAX_SUBSCRIPTIONS: u32 = 1;
pub const MAX_MAX_SUBSCRIPTIONS: u32 = 1_000_000;
pub const MIN_MAX_TOPIC_PATTERNS: u32 = 1;
pub const MAX_MAX_TOPIC_PATTERNS: u32 = 100_000;
pub const MIN_DELIVERY_TIMEOUT_MS: u64 = 100;
pub const MAX_DELIVERY_TIMEOUT_MS: u64 = 300_000;
pub const MIN_BACKLOG_LIMIT: u32 = 16;
pub const MAX_BACKLOG_LIMIT: u32 = 1_000_000;
```

### Validation

`PubSubConfig::validate()` checks each field against its MIN/MAX
constants. Returns `Err(PubSubError::PublishFailed("..."))` with a
descriptive message if any field is out of range.

### Builder Methods

All builder methods are `const fn` and return `Self` for chaining:

```rust
pub const fn with_max_subscriptions(mut self, max: u32) -> Self;
pub const fn with_max_topic_patterns(mut self, max: u32) -> Self;
pub const fn with_delivery_timeout_ms(mut self, ms: u64) -> Self;
pub const fn with_backlog_limit(mut self, limit: u32) -> Self;
```

### Presets

Three deployment archetypes:

| Preset            | Max Subs | Max Patterns | Delivery Timeout | Backlog Limit |
|-------------------|----------|--------------|------------------|---------------|
| `Local`           | 128      | 64           | 5,000 ms         | 1,024         |
| `Clustered`       | 1,024    | 256           | 10,000 ms       | 10,000        |
| `HighThroughput`  | 10,000   | 1,024         | 30,000 ms       | 100,000       |

#### Local

For single-node deployments and development:
- Low subscription count (128).
- Short delivery timeout (5 seconds).
- Small backlog (1,024 messages).

#### Clustered

Default preset for production clusters:
- Moderate subscription count (1,024).
- Standard delivery timeout (10 seconds).
- Moderate backlog (10,000 messages).

#### HighThroughput

For high-volume data pipelines:
- High subscription count (10,000).
- Long delivery timeout (30 seconds).
- Large backlog (100,000 messages).

---

## 14. Builder Pattern

### PubSubBuilder

A fluent builder for `PubSubConfig`:

```rust
pub struct PubSubBuilder {
    cfg: PubSubConfig,
}

impl PubSubBuilder {
    pub fn new() -> Self;
    pub fn from_preset(preset: Preset) -> Self;
    pub fn max_subscriptions(self, max: u32) -> Self;
    pub fn max_topic_patterns(self, max: u32) -> Self;
    pub fn delivery_timeout_ms(self, ms: u64) -> Self;
    pub fn backlog_limit(self, limit: u32) -> Self;
    pub fn build(self) -> Result<PubSubConfig, PubSubError>;
}
```

### Usage

```rust
// Default config
let cfg = PubSubBuilder::new().build().unwrap();

// From preset with overrides
let cfg = PubSubBuilder::from_preset(Preset::HighThroughput)
    .max_subscriptions(5_000)  // override
    .build()
    .unwrap();

// Full custom
let cfg = PubSubBuilder::new()
    .max_subscriptions(500)
    .max_topic_patterns(128)
    .delivery_timeout_ms(15_000)
    .backlog_limit(5_000)
    .build()
    .unwrap();
```

### Validation on Build

`build()` calls `cfg.validate()` and returns `Err(PubSubError)` if any
field is out of range. This ensures that invalid configurations are
caught at construction time, not at runtime.

### Default Implementation

`PubSubBuilder` implements `Default` which delegates to `new()`:

```rust
impl Default for PubSubBuilder {
    fn default() -> Self { Self::new() }
}
```

---

## 15. Concurrency Model

### Phase 0: Single-Threaded

In Phase 0, all types are single-threaded:
- `PubSubEngine` owns its stats and config.
- `TopicTrie` owns its stats and config.
- Stats use `AtomicU64` for future multi-threaded support, but the
  engine itself is not `Sync` in Phase 0 (no `Arc`, no locks).

### Phase 1: Read-Heavy Concurrent

In Phase 1, the engine will support concurrent reads and serialized
writes:
- **Reads** (lookup, health): Lock-free via atomic snapshots.
- **Writes** (publish, subscribe, unsubscribe): Serialized via a
  `RwLock` on the trie and subscription map.

```rust
pub struct PubSubEngine {
    cfg: PubSubConfig,
    stats: PubSubStats,
    trie: RwLock<TopicTrie>,
    subscriptions: RwLock<HashMap<SubscriptionId, Subscription>>,
}
```

### Phase 2: Sharded

In Phase 2, the engine may be sharded by topic prefix for higher
throughput:
- Each shard owns a subset of subscriptions and a local trie.
- Publishing routes to the appropriate shard based on topic prefix.
- Sharding is transparent to the caller.

### Memory Ordering

All atomic operations use `Ordering::Relaxed`:
- Counters are approximate; exact consistency is not required.
- Health derivation reads multiple counters; minor inconsistencies
  between reads are acceptable (a few microseconds of drift).
- If stronger consistency is needed in future phases, use
  `Ordering::SeqCst` or a `SeqLock` pattern.

### Thread Safety

In Phase 0, types implement the following:
- `PubSubConfig`: `Copy + Send + Sync` (no interior mutability).
- `PubSubStats`: `Send + Sync` (atomics are `Sync`).
- `PubSubEngine`: `Send + Sync` (contains only `Send + Sync` fields).
- `TopicTrie`: `Send + Sync` (same reason).

---

## 16. Performance Targets

### Phase 0 (Skeleton)

| Benchmark        | Target  | Notes                              |
|------------------|---------|------------------------------------|
| `publish`        | < 100ns | Skeleton returns error immediately |
| `subscribe`      | < 100ns | Skeleton returns error immediately |
| `match_pattern`  | < 50ns  | Real matching on 3-segment pattern |
| `deliver`        | < 100ns | Skeleton returns error immediately |

### Phase 1 (Real Implementation)

| Benchmark        | Target  | Notes                                    |
|------------------|---------|------------------------------------------|
| `publish`        | < 1µs   | Trie lookup + enqueue per subscription   |
| `subscribe`      | < 500ns | Trie insert + subscription creation      |
| `match_pattern`  | < 100ns | Trie traversal, 3 segments               |
| `deliver`        | < 2µs   | Per-subscription delivery with backpressure |

### Phase 2 (Optimized)

| Benchmark        | Target  | Notes                                    |
|------------------|---------|------------------------------------------|
| `publish`        | < 500ns | Sharded trie, lock-free reads            |
| `subscribe`      | < 200ns | Sharded trie insert                      |
| `match_pattern`  | < 50ns  | Cache-optimized trie                     |
| `deliver`        | < 1µs   | Batch delivery, zero-copy                |

### Benchmark Setup

All benchmarks use Criterion with:
- `black_box` to prevent compiler optimizations.
- Realistic message sizes (4-8 bytes payload).
- 3-segment topic patterns (`sensor.temp.high`).
- Default `PubSubConfig` (no extreme values).

### Measurement Methodology

- Criterion's default 100 samples per benchmark.
- Warm-up: 3 seconds.
- Measurement: 5 seconds.
- Outlier detection: Criterion's built-in robust statistics.

---

## 17. Security Considerations

### Input Validation

- **Topic patterns** are validated on construction (`TopicPattern::new`):
  non-empty, max 256 bytes. This prevents excessively long patterns from
  consuming memory in the trie.
- **PubSubConfig** is validated on build (`PubSubBuilder::build`): all
  fields must be within MIN/MAX bounds. This prevents misconfiguration
  from causing resource exhaustion.
- **Topic names** in `PubSubMessage` are not validated in Phase 0 (they
  are just strings). In Phase 1, topic names will be validated against
  a max length and allowed character set.

### Resource Limits

- `max_subscriptions` prevents unbounded subscription creation.
- `max_topic_patterns` prevents trie memory exhaustion.
- `backlog_limit` prevents per-subscription memory exhaustion.
- `delivery_timeout_ms` prevents indefinite message retention.

### No Authentication

The pub/sub engine does not perform authentication or authorization.
Callers must ensure that only authorized code can:
- Create subscriptions.
- Publish messages.
- Remove subscriptions.

This is handled by the `auth-framework` crate at a higher layer.

### No Encryption

Message payloads are not encrypted by the pub/sub engine. If encryption
is needed, it should be applied at the transport layer (QUIC TLS) or
by the publisher/subscriber.

### Audit Trail

The `PubSubStats` counters provide a basic audit trail:
- Total messages published.
- Total messages delivered.
- Delivery failures.
- Subscription creation/removal.

For detailed audit logging, the `observability-sdk` crate should be
used.

---

## 18. Integration Points

### Upstream (Consumers of pubsub-engine)

| Crate                  | Usage                                    |
|------------------------|------------------------------------------|
| `application-runtime`  | Provides pub/sub API to applications     |
| `rpc-framework`        | Uses pub/sub for notification patterns   |
| `media-streaming`      | Uses pub/sub for stream event routing    |
| `ai-streaming`         | Uses pub/sub for model event routing     |
| `agent-runtime`        | Uses pub/sub for agent message routing   |
| `ops-dashboard`        | Reads health and stats for monitoring    |

### Downstream (Dependencies of pubsub-engine)

| Crate                  | Usage                                    |
|------------------------|------------------------------------------|
| `event-bus`            | Lower-level topic/partition management   |
| `configuration-system` | Configuration loading and validation     |
| `thiserror`            | Error derive macro                       |
| `tracing`              | Structured logging (Phase 1+)            |

### Re-exports

The pubsub-engine re-exports key types for convenience:
- All public types from its own modules.
- `event_bus::TopicId` — for callers that need to reference both
  pub/sub subscriptions and event-bus topics.

### API Stability

Phase 0 APIs are stable within the 0.x version. Breaking changes
require a minor version bump. The `#[non_exhaustive]` attribute on
enums and the `pub(crate)` visibility on internal fields ensure that
adding new variants or fields is non-breaking.

---

## 19. Testing Strategy

### Unit Tests

Each module has comprehensive unit tests:

| Module           | Tests | Coverage                                      |
|------------------|-------|-----------------------------------------------|
| `error`          | 8     | All variants, predicates, Display             |
| `config`         | 12    | Validation, presets, builder, boundaries      |
| `subscription`   | 21    | SubscriptionId, TopicPattern matching         |
| `message`        | 9     | Construction, headers, equality, clone        |
| `stats`          | 18    | Counters, rates, snapshots, edge cases        |
| `health`         | 12    | All states, thresholds, derivation            |
| `builder`        | 7     | Defaults, presets, overrides, validation      |
| `engine`         | 5     | Creation, skeleton returns, health            |
| `trie`           | 4     | Creation, skeleton returns, health            |
| **Total**        | **98** |                                               |

### Test Categories

1. **Construction tests** — Verify that types can be constructed with
   valid inputs and rejected with invalid inputs.
2. **Validation tests** — Verify that config validation accepts
   boundary values and rejects out-of-range values.
3. **Matching tests** — Verify topic pattern matching for all wildcard
   combinations.
4. **Rate calculation tests** — Verify basis-point calculations,
   including truncation behavior and edge cases (0 deliveries, 100%
   failure, complementary rates).
5. **Health derivation tests** — Verify worst-signal-wins policy
   across all threshold combinations.
6. **Skeleton tests** — Verify that Phase 0 skeletons return the
   correct error variants.
7. **Display tests** — Verify human-readable error messages.
8. **Equality and clone tests** — Verify value semantics.

### Property Tests (Future)

In Phase 1+, property-based tests will verify:
- Pattern matching is reflexive (same pattern matches itself).
- Pattern matching is symmetric for wildcards (reordering segments
  doesn't unexpectedly match).
- Rate calculations are bounded (0..=10_000).
- Health derivation is monotonic (worse stats → worse health).

### Integration Tests (Future)

In Phase 1+, integration tests will verify:
- End-to-end publish → match → deliver flow.
- Concurrent publish and subscribe.
- Backlog overflow behavior.
- Delivery timeout behavior.

### Benchmark Tests

Four Criterion benchmarks are defined:
1. `publish` — Measures the cost of `PubSubEngine::publish()`.
2. `subscribe` — Measures the cost of `PubSubEngine::subscribe()`.
3. `match_pattern` — Measures the cost of `TopicPattern::matches()`.
4. `deliver` — Measures the cost of message delivery.

In Phase 0, all benchmarks measure skeleton overhead (error returns)
except `match_pattern` which tests real matching logic.

### Test Conventions

- Tests use `assert!` and `assert_eq!` — no `unwrap()` or `expect()`
  in test bodies (per workspace lint rules).
- Pattern construction in tests uses `.unwrap()` only on known-valid
  inputs (e.g., `TopicPattern::new("sensor.*").unwrap()`).
- Each test is independent and does not rely on execution order.
- Tests are named descriptively: `function_behavior_condition`.

---

## 20. Phased Implementation Plan

### Phase 0 (Current)

**Goal**: Foundational types, typed skeletons, documentation, benchmarks.

| Deliverable                          | Status |
|--------------------------------------|--------|
| `PubSubError` with `is_fatal`/`is_retryable` | ✅     |
| `PubSubConfig` with validation + presets | ✅     |
| `SubscriptionId` newtype             | ✅     |
| `TopicPattern` with wildcard matching | ✅     |
| `PubSubMessage` struct               | ✅     |
| `PubSubStats` with atomic counters   | ✅     |
| `StatsSnapshot` with rate calculations | ✅     |
| `HealthReport` / `HealthState`       | ✅     |
| `PubSubBuilder` fluent builder       | ✅     |
| `PubSubEngine` skeleton              | ✅     |
| `TopicTrie` skeleton                 | ✅     |
| 4 Criterion bench stubs              | ✅     |
| DESIGN.md (1000+ lines)              | ✅     |
| README.md (~150 lines)               | ✅     |
| All verification gates pass          | ✅     |

### Phase 1

**Goal**: Real engine and trie implementation.

| Deliverable                          | Description                              |
|--------------------------------------|------------------------------------------|
| `TopicTrie::insert()`                | Trie insertion with wildcard support     |
| `TopicTrie::lookup()`                | Trie traversal for pattern matching      |
| `PubSubEngine::publish()`            | Route message to matching subscriptions  |
| `PubSubEngine::subscribe()`          | Create subscription, insert into trie    |
| `PubSubEngine::unsubscribe()`        | Remove subscription, clean up trie       |
| Backlog management                   | Per-subscription message queue           |
| Delivery timeout                     | Message expiry based on timestamp        |
| Stats recording                      | Wire up counters to real operations      |
| Integration tests                    | End-to-end publish → match → deliver     |
| Benchmarks with real data            | Measure actual trie and delivery cost    |

### Phase 2

**Goal**: Async delivery, backpressure, performance optimization.

| Deliverable                          | Description                              |
|-------------------------------------|------------------------------------------|
| Async delivery                      | Callback-based or channel-based delivery |
| Backpressure                        | Publisher backpressure on full backlog   |
| Sharded engine                      | Topic-prefix sharding for throughput     |
| Batch delivery                      | Deliver multiple messages in one batch   |
| Zero-copy payload                   | Use `bytes::Bytes` or buffer pool        |
| Recursive `#` wildcard              | MQTT multi-level wildcard support        |
| Delivery ordering                   | Per-subscription message ordering        |
| Property-based tests                | QuickCheck or proptest for invariants    |

### Phase 3+

**Goal**: Advanced features and ecosystem integration.

| Deliverable                          | Description                              |
|-------------------------------------|------------------------------------------|
| At-least-once delivery              | Retry on delivery failure                |
| Exactly-once delivery               | Deduplication via message IDs            |
| Cross-cluster replication           | Replicate messages across clusters       |
| Schema validation                   | Integration with schema registry         |
| Message compression                 | Transparent payload compression          |
| Priority subscriptions              | High-priority subscriptions bypass backlog |
| Dead letter queue                   | Failed messages routed to DLQ            |
| Metrics export                      | Prometheus / OpenTelemetry integration   |

---

## Appendix A: Type Reference

### Public Types

| Type                | Module           | Derives                                              |
|---------------------|------------------|------------------------------------------------------|
| `PubSubError`       | `error`          | `Debug, Error`                                       |
| `PubSubConfig`      | `config`         | `Clone, Copy, Debug, PartialEq, Eq`                  |
| `Preset`            | `config`         | `Clone, Copy, Debug, PartialEq, Eq, Hash`            |
| `SubscriptionId`    | `subscription`   | `Clone, Copy, Debug, PartialEq, Eq, Hash`            |
| `TopicPattern`      | `subscription`   | `Clone, Debug, PartialEq, Eq, Hash`                  |
| `PubSubMessage`     | `message`        | `Clone, Debug, PartialEq, Eq`                        |
| `PubSubStats`       | `stats`          | `Debug`                                              |
| `StatsSnapshot`     | `stats`          | `Clone, Copy, Debug, Default, PartialEq, Eq`         |
| `HealthReport`      | `health`         | `Clone, Copy, Debug, Default, PartialEq, Eq`         |
| `HealthState`       | `health`         | `Clone, Copy, Debug, Default, PartialEq, Eq, Hash`   |
| `PubSubBuilder`     | `builder`        | `Debug, Clone`                                       |
| `PubSubEngine`      | `engine`         | `Debug` (manual impl)                                |
| `TopicTrie`         | `trie`           | `Debug` (manual impl)                                |

### Re-exported Types

| Type       | Source Crate  | Module     |
|------------|---------------|------------|
| `TopicId`  | `event-bus`   | `topic`    |

---

## Appendix B: Constant Reference

### Config Constants

| Constant                       | Value    | Module   |
|--------------------------------|----------|----------|
| `MIN_MAX_SUBSCRIPTIONS`        | 1        | `config` |
| `MAX_MAX_SUBSCRIPTIONS`        | 1,000,000| `config` |
| `MIN_MAX_TOPIC_PATTERNS`       | 1        | `config` |
| `MAX_MAX_TOPIC_PATTERNS`       | 100,000  | `config` |
| `MIN_DELIVERY_TIMEOUT_MS`      | 100      | `config` |
| `MAX_DELIVERY_TIMEOUT_MS`      | 300,000  | `config` |
| `MIN_BACKLOG_LIMIT`            | 16       | `config` |
| `MAX_BACKLOG_LIMIT`            | 1,000,000| `config` |

### Health Constants

| Constant                   | Value | Module  |
|----------------------------|-------|---------|
| `FAILURE_DEGRADED_BPS`     | 100   | `health`|
| `FAILURE_CRITICAL_BPS`     | 500   | `health`|

### SubscriptionId Constants

| Constant              | Value | Module         |
|-----------------------|-------|----------------|
| `SubscriptionId::NIL` | 0     | `subscription` |

---

## Appendix C: Rate Calculation Reference

### Basis Points (bps)

- 1 bps = 0.01%
- 100 bps = 1%
- 1,000 bps = 10%
- 10,000 bps = 100%

### Delivery Success Rate

| Delivered | Failures | Total | Success Rate (bps) |
|-----------|----------|-------|--------------------|
| 0         | 0        | 0     | 10,000 (100%)      |
| 1,000     | 0        | 1,000 | 10,000 (100%)      |
| 1,000     | 5        | 1,005 | 9,950 (99.50%)     |
| 1,000     | 10       | 1,010 | 9,900 (99.00%)     |
| 1,000     | 20       | 1,020 | 9,803 (98.03%)     |
| 1,000     | 100      | 1,100 | 9,090 (90.90%)     |
| 0         | 100      | 100   | 0 (0%)             |
| 2         | 1        | 3     | 6,666 (66.66%)     |

### Delivery Failure Rate

| Delivered | Failures | Total | Failure Rate (bps) |
|-----------|----------|-------|--------------------|
| 0         | 0        | 0     | 0 (0%)             |
| 1,000     | 0        | 1,000 | 0 (0%)             |
| 1,000     | 5        | 1,005 | 49 (0.49%)         |
| 1,000     | 10       | 1,010 | 99 (0.99%)         |
| 1,000     | 20       | 1,020 | 196 (1.96%)        |
| 1,000     | 100      | 1,100 | 909 (9.09%)        |
| 0         | 100      | 100   | 10,000 (100%)      |
| 2         | 1        | 3     | 3,333 (33.33%)     |

### Truncation Behavior

Integer division truncates toward zero:
- `2 * 10_000 / 3 = 20_000 / 3 = 6_666` (not 6,667)
- `1 * 10_000 / 3 = 10_000 / 3 = 3_333` (not 3,334)
- `6_666 + 3_333 = 9_999` (not 10,000)

This means `success_rate_bps + failure_rate_bps` may not equal 10,000
when the total does not divide evenly. This is acceptable for
observability purposes — the rates are approximate by design.

### Overflow Protection

All rate calculations use `saturating_mul` and `saturating_add`:
- `saturating_mul(10_000)` prevents overflow when counters are large.
- `saturating_add` prevents overflow when summing delivered + failures.
- The result is clamped to `u16::MAX` (65,535) as a final safety net,
  though the maximum meaningful value is 10,000.

---

## Appendix D: Error Decision Tree

```text
When an operation fails:
│
├─ Is the engine shutting down?
│   └─ Return ShuttingDown (fatal, not retryable)
│
├─ Does the subscription ID already exist?
│   └─ Return SubscriptionExists (fatal, not retryable)
│
├─ Does the subscription ID not exist?
│   └─ Return SubscriptionNotFound (not fatal, not retryable)
│
├─ Does the message topic not match the subscription pattern?
│   └─ Return TopicMismatch (not fatal, not retryable)
│
├─ Did the publish operation fail due to a transient error?
│   └─ Return PublishFailed("reason") (not fatal, retryable)
│
└─ Unknown failure?
    └─ Add a new variant to PubSubError (non-breaking via #[non_exhaustive])
```

---

## Appendix E: Module Dependency Graph

```text
                    ┌─────────┐
                    │  error  │
                    └────┬────┘
                         │
              ┌──────────┼──────────┐
              │          │          │
         ┌────▼───┐ ┌───▼────┐ ┌───▼────┐
         │ config │ │subscription│ │ stats  │
         └────┬───┘ └────────┘ └───┬────┘
              │                     │
         ┌────▼───┐            ┌───▼────┐
         │builder │            │ health │
         └────────┘            └───┬────┘
                                   │
                    ┌──────────────┼──────────────┐
                    │              │              │
               ┌────▼───┐    ┌────▼───┐    ┌────▼───┐
               │ engine │    │  trie  │    │message │
               └────────┘    └────────┘    └────────┘
```

All arrows point downward (dependency direction). The `error` module
is at the top, depending only on `subscription` (for `SubscriptionId`
in error variants) and `thiserror`.

---

## Appendix F: File Inventory

### Source Files

| File                    | Lines (approx) | Phase | Description                          |
|-------------------------|-----------------|-------|--------------------------------------|
| `src/error.rs`          | 180             | 0 ✅  | `PubSubError` enum + tests           |
| `src/config.rs`         | 350             | 0 ✅  | `PubSubConfig`, `Preset` + tests     |
| `src/subscription.rs`   | 330             | 0 ✅  | `SubscriptionId`, `TopicPattern` + tests |
| `src/message.rs`        | 180             | 0 ✅  | `PubSubMessage` + tests              |
| `src/stats.rs`          | 320             | 0 ✅  | `PubSubStats`, `StatsSnapshot` + tests |
| `src/health.rs`         | 280             | 0 ✅  | `HealthReport`, `HealthState` + tests |
| `src/builder.rs`        | 180             | 0 ✅  | `PubSubBuilder` + tests              |
| `src/engine.rs`         | 170             | 1    | `PubSubEngine` skeleton + tests      |
| `src/trie.rs`           | 130             | 1    | `TopicTrie` skeleton + tests         |
| `src/lib.rs`            | 60              | 0 ✅  | Module table + re-exports            |

### Benchmark Files

| File                      | Description                          |
|---------------------------|--------------------------------------|
| `benches/publish.rs`      | Bench: publish message to engine     |
| `benches/subscribe.rs`    | Bench: create subscription           |
| `benches/match_pattern.rs`| Bench: topic pattern matching        |
| `benches/deliver.rs`      | Bench: message delivery              |

### Documentation Files

| File          | Lines (approx) | Description                          |
|---------------|-----------------|--------------------------------------|
| `DESIGN.md`   | 1000+           | Full design specification            |
| `README.md`   | ~150            | User-facing overview                 |

---

## Appendix G: Verification Gates

The following gates must pass before any phase is considered complete:

| Gate                    | Command                          | Description                          |
|-------------------------|----------------------------------|--------------------------------------|
| Type check              | `cargo check`                    | All types compile                    |
| Lint                    | `cargo clippy -- -D warnings`    | No warnings                          |
| Format                  | `cargo fmt --check`              | Code is formatted                    |
| Unit tests              | `cargo test`                     | All tests pass                       |
| Bench compile           | `cargo bench --no-run`           | Benchmarks compile                   |

### Workspace Lint Rules

The workspace enforces the following lint rules (denied):

| Lint              | Reason                                          |
|-------------------|-------------------------------------------------|
| `unwrap_used`     | Panics on `None`/`Err` — use `?` or match       |
| `expect_used`     | Panics with message — use `?` or match          |
| `panic`           | No `panic!()` macro                             |
| `todo`            | No `todo!()` macro (use typed errors instead)   |
| `unimplemented`   | No `unimplemented!()` macro                     |
| `unreachable`     | No `unreachable!()` macro                       |
| `dbg_macro`       | No `dbg!()` macro                               |
| `print_stdout`    | No `println!()` to stdout                       |
| `print_stderr`    | No `eprintln!()` to stderr                      |

### Phase 0 Gate Results

| Gate                    | Status | Notes                                    |
|-------------------------|--------|------------------------------------------|
| `cargo check`           | ✅     | Clean compile                            |
| `cargo clippy -- -D warnings` | ✅ | No warnings                        |
| `cargo fmt --check`     | ✅     | All files formatted                      |
| `cargo test`            | ✅     | 98 tests, all pass                       |
| `cargo bench --no-run`  | ✅     | 4 benchmarks compile                     |

---

## Appendix H: Comparison with Industry Systems

### MQTT

| Feature              | MQTT           | pubsub-engine (Phase 0) |
|----------------------|----------------|-------------------------|
| Single-level `*`     | ✅             | ✅                      |
| Multi-level `#`      | ✅             | ❌ (Phase 2+)           |
| QoS levels           | 0, 1, 2        | N/A (Phase 2+)          |
| Retained messages    | ✅             | ❌ (Phase 2+)           |
| Last Will            | ✅             | ❌ (N/A)                |
| Broker-based         | ✅             | ❌ (embedded)           |

### NATS

| Feature              | NATS           | pubsub-engine (Phase 0) |
|----------------------|----------------|-------------------------|
| Subject wildcards    | `*`, `>`       | `*` only (Phase 2+: `#`)|
| Queue groups         | ✅             | ❌ (Phase 2+)           |
| Request-reply        | ✅             | ❌ (rpc-framework)      |
| JetStream persistence| ✅             | ❌ (event-bus)          |
| Clustering           | ✅             | ❌ (Phase 3+)           |

### Apache Pulsar

| Feature              | Pulsar         | pubsub-engine (Phase 0) |
|----------------------|----------------|-------------------------|
| Topics               | ✅             | ✅                      |
| Subscriptions        | Exclusive, Shared, Failover | Pattern-based   |
| Message ordering     | ✅             | ❌ (Phase 2+)           |
| Dead letter queue    | ✅             | ❌ (Phase 3+)           |
| Schema registry      | ✅             | ❌ (separate crate)     |

### Design Rationale

The pubsub-engine is designed to be:
- **Embedded** — Not a standalone broker, but a library.
- **Opinionated** — Focused on Vireon's use cases, not general-purpose.
- **Phased** — Start simple (Phase 0), add complexity incrementally.
- **Typed** — Exhaustive error types, not string-based.

---

## Appendix I: Glossary

| Term                | Definition                                           |
|---------------------|------------------------------------------------------|
| bps                 | Basis points (1/100 of a percent, 10,000 = 100%)     |
| Backlog             | Per-subscription queue of undelivered messages       |
| Pattern             | A topic pattern with optional `*` wildcards          |
| Segment             | A dot-delimited part of a topic or pattern           |
| Subscription        | A registration to receive messages matching a pattern|
| Topic               | A concrete, dot-delimited message routing key        |
| TopicTrie           | Trie data structure for efficient pattern matching   |
| Wildcard            | `*` character matching any single segment            |

---

*End of DESIGN.md*
