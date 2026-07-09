# Ops Dashboard - Design Document

## Part 1: Overview

### 1.1 Purpose

The Ops Dashboard provides the operational visibility layer for the Vireon QUIC
runtime. It delivers:

- **Widget Rendering**: Real-time rendering of charts, tables, gauges, logs, and
  alerts via a typed widget system.
- **Metrics Collection**: Time-range queries with aggregation functions (avg,
  sum, max, min, p95, p99) against the observability SDK.
- **Health Monitoring**: Tri-state health derivation (Healthy / Degraded /
  Critical) from query success rates, with worst-signal-wins aggregation.
- **Configuration**: Preset-based deployment archetypes (Minimal / Standard /
  Full) with fluent builder overrides.

### 1.2 Design Philosophy

The dashboard follows these principles:

1. **Typed errors**: Every fallible API returns `OpsDashboardError`, enabling
   exhaustive pattern matching without string matching.
2. **Const-friendly**: Core type constructors and predicates work in `const fn`
   contexts for compile-time validation.
3. **Zero-allocation hot path**: Key types (`DashboardWidgetId`, `QueryRange`,
   `QueryAggregation`) are `Copy` and stack-allocated.
4. **Atomic stats**: Counters use `AtomicU64` with `Relaxed` ordering for
   maximum throughput; stats are advisory, never load-bearing.
5. **Worst-signal-wins**: Health aggregation picks the worst state across all
   signals, ensuring degraded components are never masked by healthy ones.
6. **Preset-driven**: Three deployment archetypes encode self-consistent
   starting points; tune via builder afterwards.

### 1.3 Architecture

```text
┌─────────────────────────────────────────────────────────────────────┐
│                        Application Layer                             │
├─────────────────────────────────────────────────────────────────────┤
│  Ops Dashboard                                                       │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  │
│  │ OpsDashboardSvc  │  │ MetricsCollector │  │  WidgetRegistry  │  │
│  │   (Phase 1)      │  │   (Phase 1)      │  │   (Phase 1)      │  │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘  │
│         │                       │                       │            │
│         └───────────────────────┼───────────────────────┘          │
│                                 ▼                                    │
│                    ┌──────────────────────┐                          │
│                    │  Observability SDK   │                          │
│                    │   (adjacent layer)   │                          │
│                    └──────────────────────┘                          │
└─────────────────────────────────────────────────────────────────────┘
```

### 1.4 Layer Position

The ops-dashboard sits at the top of the application layer stack. It depends on:

- **configuration-system**: For configuration key-value types.
- **observability-sdk**: For metric types, tracing, and logging.
- **service-registry**: For service discovery and health tracking.

No lower-layer crate depends on ops-dashboard. It is a pure consumer of
upstream telemetry and configuration data.

### 1.5 Phase 0 Scope

Phase 0 delivers the foundational types and typed skeleton facades:

| Module | Status | Description |
|--------|--------|-------------|
| `error` | Real | `OpsDashboardError` with `is_fatal`/`is_retryable` |
| `config` | Real | `OpsDashboardConfig` with presets and validation |
| `widget` | Real | `DashboardWidgetId` newtype, `WidgetType` enum |
| `query` | Real | `QueryRange`, `QueryAggregation` |
| `stats` | Real | `OpsDashboardStats` atomics, `StatsSnapshot` |
| `health` | Real | `HealthReport`, `HealthState` tri-state |
| `builder` | Real | `OpsDashboardBuilder` fluent builder |
| `service` | Skeleton | `OpsDashboardService` returns `ShuttingDown` |
| `collector` | Skeleton | `MetricsCollector` returns errors |

---

## Part 2: Error Model

### 2.1 Error Type

```rust
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum OpsDashboardError {
    WidgetNotFound,
    QueryFailed(&'static str),
    CollectorUnavailable,
    ShuttingDown,
}
```

### 2.2 Error Categories

Errors are classified into two categories via predicate methods:

| Category | Variants | Caller Action |
|----------|----------|---------------|
| Fatal | `WidgetNotFound`, `ShuttingDown` | Abort; do not retry |
| Retryable | `QueryFailed`, `CollectorUnavailable` | Back off and retry |

The `is_fatal()` and `is_retryable()` predicates are `const fn`, enabling
compile-time branching in generic contexts.

### 2.3 Error Messages

Each variant carries a human-readable message via `#[error("...")]`:

- `WidgetNotFound` -> `"widget not found"`
- `QueryFailed(msg)` -> `"query failed: {msg}"`
- `CollectorUnavailable` -> `"collector unavailable"`
- `ShuttingDown` -> `"ops dashboard is shutting down"`

### 2.4 Non-Exhaustive

The enum is `#[non_exhaustive]` to allow adding new variants in future phases
without breaking downstream match arms. Callers must include a `_` arm.

### 2.5 Design Rationale

- `QueryFailed` carries a `&'static str` (not `String`) to avoid heap
  allocation on the error path. The message is a static description of the
  failure reason (e.g., `"timeout"`, `"connection refused"`).
- `WidgetNotFound` is a unit variant (no payload) because the widget ID is
  already known to the caller; including it in the error would be redundant.
- `ShuttingDown` is fatal because retrying during shutdown is wasteful and
  could delay shutdown.

---

## Part 3: Configuration

### 3.1 Config Structure

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpsDashboardConfig {
    pub max_widgets: u32,
    pub refresh_interval_ms: u64,
    pub query_timeout_ms: u64,
    pub max_query_range_ms: u64,
}
```

### 3.2 Field Semantics

| Field | Type | Unit | Description |
|-------|------|------|-------------|
| `max_widgets` | `u32` | count | Maximum number of widgets the dashboard may render simultaneously. |
| `refresh_interval_ms` | `u64` | ms | Interval at which widgets refresh their data. Lower values increase load. |
| `query_timeout_ms` | `u64` | ms | Maximum time a single query may take before timing out. |
| `max_query_range_ms` | `u64` | ms | Maximum allowed time span for a single ranged query. Prevents excessive data retrieval. |

### 3.3 Validation Bounds

Each field has `MIN` and `MAX` constants:

| Field | Min | Max | Rationale |
|-------|-----|-----|-----------|
| `max_widgets` | 1 | 1,000 | At least 1 widget; 1,000 is a hard cap for memory. |
| `refresh_interval_ms` | 100 | 60,000 | 100ms is the fastest useful refresh; 60s is the slowest. |
| `query_timeout_ms` | 100 | 30,000 | 100ms minimum for fast backends; 30s max for slow ones. |
| `max_query_range_ms` | 1,000 | 86,400,000 | 1s minimum; 24h maximum (86,400,000 ms). |

### 3.4 Presets

Three deployment archetypes are pre-encoded:

#### Minimal

```rust
OpsDashboardConfig {
    max_widgets: 8,
    refresh_interval_ms: 5_000,
    query_timeout_ms: 2_000,
    max_query_range_ms: 3_600_000, // 1 hour
}
```

Use case: Lightweight monitoring on resource-constrained deployments. Only 8
widgets, 5-second refresh, 2-second timeout. Suitable for edge nodes.

#### Standard

```rust
OpsDashboardConfig {
    max_widgets: 32,
    refresh_interval_ms: 2_000,
    query_timeout_ms: 5_000,
    max_query_range_ms: 3_600_000, // 1 hour
}
```

Use case: Production dashboard with balanced settings. 32 widgets cover most
operational views. 2-second refresh provides near-real-time visibility without
overwhelming backends.

#### Full

```rust
OpsDashboardConfig {
    max_widgets: 128,
    refresh_interval_ms: 1_000,
    query_timeout_ms: 10_000,
    max_query_range_ms: 86_400_000, // 24 hours
}
```

Use case: Full NOC dashboard for dedicated operations centers. 128 widgets
for comprehensive views, 1-second refresh for real-time monitoring, 10-second
timeout for complex queries, 24-hour max range for historical analysis.

### 3.5 Builder Pattern

The `OpsDashboardConfig` struct supports builder-style chaining via `with_*`
methods. For full builder functionality (including validation on build), use
`OpsDashboardBuilder`:

```rust
let cfg = OpsDashboardBuilder::new()
    .max_widgets(64)
    .refresh_interval_ms(1_000)
    .query_timeout_ms(3_000)
    .max_query_range_ms(7_200_000)
    .build()?;
```

The builder validates all fields on `build()`, returning
`OpsDashboardError::QueryFailed` if any field is out of range.

### 3.6 Config as Copy

`OpsDashboardConfig` is `Copy` because it contains only primitive types (two
`u32` and two `u64`). This means it can be passed by value without cloning,
which is important for hot-path access in the service and collector.

### 3.7 Const Construction

The `new()` and `with_*` methods are `const fn`, enabling compile-time
configuration:

```rust
const CFG: OpsDashboardConfig = OpsDashboardConfig::new()
    .with_max_widgets(64)
    .with_refresh_interval_ms(1_000);
```

---

## Part 4: Widget Model

### 4.1 DashboardWidgetId

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct DashboardWidgetId(u32);
```

A 32-bit widget identifier. Uses `#[repr(transparent)]` to ensure ABI
compatibility with `u32`.

#### API

| Method | Signature | Description |
|--------|-----------|-------------|
| `NIL` | `pub const NIL: Self` | The nil/unset widget ID (zero). |
| `new` | `pub const fn new(id: u32) -> Self` | Construct from a u32. |
| `nil` | `pub const fn nil() -> Self` | Construct the nil ID. |
| `is_nil` | `pub const fn is_nil(&self) -> bool` | Check if nil. |
| `raw` | `pub const fn raw(&self) -> u32` | Get the raw u32 value. |

#### Design Rationale

- 32-bit is sufficient for 4 billion widgets; far more than any dashboard
  will ever need.
- `NIL = 0` is the conventional "unset" value; `is_nil()` checks for it.
- `#[repr(transparent)]` ensures the type can be passed across FFI boundaries
  as a `u32` if needed.

### 4.2 WidgetType

```rust
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum WidgetType {
    Chart = 0,
    Table = 1,
    Gauge = 2,
    Log = 3,
    Alert = 4,
}
```

#### Variants

| Variant | Value | Description | Streaming |
|---------|-------|-------------|-----------|
| `Chart` | 0 | Time-series graph, bar chart, etc. | No |
| `Table` | 1 | Tabular data display. | No |
| `Gauge` | 2 | Single-value gauge or meter. | No |
| `Log` | 3 | Streaming log viewer. | Yes |
| `Alert` | 4 | Active alert display. | Yes |

#### Streaming vs Polling

Streaming widgets (`Log`, `Alert`) receive data via push (subscription model).
Non-streaming widgets (`Chart`, `Table`, `Gauge`) poll on the refresh interval.

The `is_streaming()` method returns `true` for `Log` and `Alert`. This
determines whether the widget subscribes to a data stream or polls on a timer.

#### `as_str()` Method

Each variant has a human-readable name via `as_str()`:

```rust
assert_eq!(WidgetType::Chart.as_str(), "chart");
assert_eq!(WidgetType::Log.as_str(), "log");
```

These names are used in metrics labels and configuration keys.

#### `#[repr(u8)]`

The enum uses `#[repr(u8)]` so that `WidgetType::Chart as u8` produces the
expected integer value. This works because all variants are unit (no payload).
If future variants carry payloads, the `as u8` cast would no longer work.

---

## Part 5: Query Model

### 5.1 QueryRange

```rust
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct QueryRange {
    pub start_ms: u64,
    pub end_ms: u64,
}
```

Represents a half-open time interval `[start_ms, end_ms)`. All times are epoch
milliseconds.

#### API

| Method | Signature | Description |
|--------|-----------|-------------|
| `new` | `pub const fn new(start_ms: u64, end_ms: u64) -> Self` | Construct a range. Does not validate. |
| `duration_ms` | `pub const fn duration_ms(&self) -> u64` | Duration in ms. Uses `saturating_sub`. |
| `is_valid` | `pub const fn is_valid(&self) -> bool` | True if `end_ms > start_ms`. |

#### Design Rationale

- `duration_ms()` uses `saturating_sub` to handle inverted ranges gracefully,
  returning 0 instead of panicking.
- `is_valid()` checks that the range has positive duration (start < end).
- The struct derives `Default` with both fields zeroed, which is an invalid
  range (`is_valid()` returns `false`). This is intentional: a default range
  should not be usable without explicit initialization.

### 5.2 QueryAggregation

```rust
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum QueryAggregation {
    None = 0,
    Avg = 1,
    Sum = 2,
    Max = 3,
    Min = 4,
    P95 = 5,
    P99 = 6,
}
```

#### Variants

| Variant | Value | Description |
|---------|-------|-------------|
| `None` | 0 | No aggregation; return raw data points. |
| `Avg` | 1 | Arithmetic mean of all data points. |
| `Sum` | 2 | Sum of all data points. |
| `Max` | 3 | Maximum value across all data points. |
| `Min` | 4 | Minimum value across all data points. |
| `P95` | 5 | 95th percentile. |
| `P99` | 6 | 99th percentile. |

#### Default

`QueryAggregation::default()` returns `None`, meaning raw data points are
returned without aggregation. This is the safest default: callers get the
full data and can aggregate client-side if needed.

#### `as_str()` Method

```rust
assert_eq!(QueryAggregation::P95.as_str(), "p95");
assert_eq!(QueryAggregation::None.as_str(), "none");
```

Used in metrics labels, query logging, and configuration keys.

#### Percentile Computation (Phase 1)

In Phase 1, percentile aggregation will use the nearest-rank method:

1. Sort data points.
2. Index = ceil((percentile / 100) * count) - 1
3. Return the value at that index.

For P95 with 100 data points: index = ceil(0.95 * 100) - 1 = 94. The 95th
percentile is the 95th value (0-indexed: 94).

For P99 with 1000 data points: index = ceil(0.99 * 1000) - 1 = 989. The 99th
percentile is the 990th value (0-indexed: 989).

---

## Part 6: Statistics

### 6.1 OpsDashboardStats

Atomic counters for the ops-dashboard. All fields are `pub` for direct access:

```rust
pub struct OpsDashboardStats {
    pub queries_total: AtomicU64,
    pub queries_succeeded: AtomicU64,
    pub queries_failed: AtomicU64,
    pub queries_timed_out: AtomicU64,
    pub widgets_rendered: AtomicU64,
    pub data_points_fetched: AtomicU64,
}
```

#### Counter Semantics

| Counter | Monotonic | Description |
|---------|-----------|-------------|
| `queries_total` | Yes | Total queries attempted since startup. |
| `queries_succeeded` | Yes | Queries that completed successfully. |
| `queries_failed` | Yes | Queries that failed (excluding timeouts). |
| `queries_timed_out` | Yes | Queries that timed out. |
| `widgets_rendered` | Yes | Total widget render operations. |
| `data_points_fetched` | Yes | Total data points fetched across all queries. |

Note: `queries_total = queries_succeeded + queries_failed + queries_timed_out`
should always hold. The dashboard does not enforce this invariant; callers
must record both the total and the outcome.

#### Recording Methods

| Method | Counter Increment |
|--------|-------------------|
| `record_query()` | `queries_total += 1` |
| `record_query_success()` | `queries_succeeded += 1` |
| `record_query_failure()` | `queries_failed += 1` |
| `record_query_timeout()` | `queries_timed_out += 1` |
| `record_widget_render()` | `widgets_rendered += 1` |
| `record_data_points(n)` | `data_points_fetched += n` |

#### Memory Ordering

All atomic operations use `Relaxed` ordering. This is sufficient because:

1. Stats are advisory; they are never used for synchronization.
2. Individual counter increments are independent; there is no ordering
   requirement between different counters.
3. `Relaxed` is the fastest ordering, minimizing overhead on the hot path.

### 6.2 StatsSnapshot

A `Copy` struct that captures a point-in-time view of all counters:

```rust
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    pub queries_total: u64,
    pub queries_succeeded: u64,
    pub queries_failed: u64,
    pub queries_timed_out: u64,
    pub widgets_rendered: u64,
    pub data_points_fetched: u64,
}
```

#### Rate Calculations

Three rate calculations are provided as `const fn`:

| Method | Formula | Default (no queries) |
|--------|---------|----------------------|
| `query_success_rate_bps()` | `succeeded * 10000 / total` | 10,000 (100%) |
| `query_failure_rate_bps()` | `failed * 10000 / total` | 0 |
| `query_timeout_rate_bps()` | `timed_out * 10000 / total` | 0 |

All rates are in basis points (bps), where 10,000 bps = 100%. This avoids
floating-point arithmetic and enables `const fn` evaluation.

#### Saturating Multiplication

Rate calculations use `saturating_mul` to prevent overflow:

```rust
let rate = (self.queries_succeeded.saturating_mul(10_000)) / self.queries_total;
```

Without `saturating_mul`, `queries_succeeded * 10_000` could overflow `u64`
when `queries_succeeded > u64::MAX / 10_000` (approximately 1.8 * 10^15).
While unlikely in practice, `saturating_mul` is the safe choice.

#### Integer Division Truncation

Integer division truncates toward zero. For example:

- `2 * 10000 / 3 = 6666` (not 6667)
- `97 * 10000 / 100 = 9700` (exact)

This is acceptable for advisory stats; the 1-bps error is negligible.

#### Empty State

When `queries_total == 0`:

- `query_success_rate_bps()` returns `10_000` (100%). An idle system with no
  queries is considered healthy — there are no failures to report.
- `query_failure_rate_bps()` returns `0`. No queries means no failures.
- `query_timeout_rate_bps()` returns `0`. No queries means no timeouts.

This design ensures that a freshly-started dashboard reports `Healthy` health
state, not `Critical` (which would happen if the success rate were 0%).

---

## Part 7: Health Model

### 7.1 HealthState

Tri-state health classification:

```rust
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum HealthState {
    #[default]
    Healthy,
    Degraded,
    Critical,
}
```

| State | Meaning | Operator Action |
|-------|---------|-----------------|
| `Healthy` | All systems nominal. | None. |
| `Degraded` | Performance or availability impact. | Investigate during business hours. |
| `Critical` | Severe impact; data loss risk possible. | Page on-call immediately. |

#### Default

`HealthState::default()` returns `Healthy`. This is the correct default for a
freshly-started system with no failures.

#### Predicates

| Method | Returns `true` for |
|--------|---------------------|
| `is_healthy()` | `Healthy` |
| `needs_attention()` | `Degraded`, `Critical` |
| `is_critical()` | `Critical` |

#### `as_str()`

```rust
assert_eq!(HealthState::Healthy.as_str(), "healthy");
assert_eq!(HealthState::Degraded.as_str(), "degraded");
assert_eq!(HealthState::Critical.as_str(), "critical");
```

Used in metrics labels, alert messages, and log records.

### 7.2 worst_of()

The `worst_of` method implements worst-signal-wins aggregation:

```rust
pub const fn worst_of(a: Self, b: Self) -> Self
```

Priority: `Critical` > `Degraded` > `Healthy`.

This ensures that when multiple health signals are combined, the worst signal
determines the overall state. For example, if one subsystem is `Critical` and
another is `Healthy`, the overall state is `Critical`.

### 7.3 Thresholds

Three threshold constants govern the health classification:

| Constant | Value (bps) | Meaning |
|----------|-------------|---------|
| `SUCCESS_HEALTHY_BPS` | 9,900 | Success rate >= this -> Healthy (99%) |
| `SUCCESS_DEGRADED_BPS` | 9,500 | Success rate >= this -> Degraded (95%) |
| `SUCCESS_CRITICAL_BPS` | 9,000 | Success rate < this -> Critical (90%) |

#### Classification Logic

```rust
let state = if rate >= SUCCESS_HEALTHY_BPS {
    HealthState::Healthy
} else if rate >= SUCCESS_CRITICAL_BPS {
    HealthState::Degraded
} else {
    HealthState::Critical
};
```

Note: The `SUCCESS_DEGRADED_BPS` threshold (9,500) is defined as a constant
for documentation and future use, but the current classification logic uses
only `SUCCESS_HEALTHY_BPS` and `SUCCESS_CRITICAL_BPS`. The degraded band
spans from 9,000 to 9,899 bps.

### 7.4 HealthReport

```rust
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HealthReport {
    pub state: HealthState,
    pub queries_total: u64,
    pub widgets_rendered: u64,
    pub data_points_fetched: u64,
    pub query_success_rate_bps: u16,
}
```

The `HealthReport` is derived from a `StatsSnapshot` via `HealthReport::derive()`.
It includes the health state plus key stats for operator context.

#### Derivation

```rust
let report = HealthReport::derive(&stats_snapshot);
```

The derivation:
1. Computes `query_success_rate_bps` from the snapshot.
2. Classifies the health state based on the rate.
3. Copies `queries_total`, `widgets_rendered`, and `data_points_fetched` from
   the snapshot.

#### Rationale for Including Stats

Including key stats in the health report allows operators to see not just the
health state but also the volume of activity. For example, `Critical` with
`queries_total = 1,000,000` is more urgent than `Critical` with
`queries_total = 10`.

---

## Part 8: Builder

### 8.1 OpsDashboardBuilder

```rust
#[derive(Debug, Clone)]
pub struct OpsDashboardBuilder {
    cfg: OpsDashboardConfig,
}
```

A fluent builder for `OpsDashboardConfig`. Starts from defaults or a preset,
then overrides individual fields via chained methods.

#### API

| Method | Description |
|--------|-------------|
| `new()` | Start with defaults (same as `OpsDashboardConfig::new()`). |
| `from_preset(preset)` | Start from a preset configuration. |
| `max_widgets(n)` | Override max widgets. |
| `refresh_interval_ms(ms)` | Override refresh interval. |
| `query_timeout_ms(ms)` | Override query timeout. |
| `max_query_range_ms(ms)` | Override max query range. |
| `build()` | Validate and produce the final `OpsDashboardConfig`. |

#### Validation on Build

The `build()` method calls `cfg.validate()` and returns an error if any field
is out of range:

```rust
pub fn build(self) -> Result<OpsDashboardConfig, OpsDashboardError> {
    self.cfg.validate()?;
    Ok(self.cfg)
}
```

This ensures that no invalid configuration can be constructed through the
builder. Direct construction via `OpsDashboardConfig { ... }` bypasses
validation, but this is acceptable for internal use and const contexts.

#### Const Methods

All setter methods are `const fn`, enabling compile-time builder usage:

```rust
const CFG: OpsDashboardConfig = OpsDashboardBuilder::new()
    .max_widgets(64)
    .refresh_interval_ms(1_000)
    .build()
    .unwrap_or(OpsDashboardConfig::new());
```

Note: `build()` is not `const fn` because it returns `Result`, which requires
runtime error handling. The `unwrap_or` pattern provides a fallback for
const contexts.

---

## Part 9: Service (Phase 1 Skeleton)

### 9.1 OpsDashboardService

```rust
#[derive(Debug)]
pub struct OpsDashboardService {
    cfg: OpsDashboardConfig,
    stats: OpsDashboardStats,
}
```

The service facade for widget rendering and refresh operations. In Phase 0,
it stores config and stats but returns `ShuttingDown` errors for all
operations.

#### Methods

| Method | Phase 0 Behavior | Phase 1 Behavior |
|--------|-------------------|-------------------|
| `new(cfg, stats)` | Stores cfg and stats. | Same. |
| `render_widget(id)` | Returns `Err(ShuttingDown)`. | Renders the widget. |
| `refresh_widget(id)` | Returns `Err(ShuttingDown)`. | Refreshes the widget's data. |
| `health()` | Returns `HealthReport::derive(&stats.snapshot())`. | Same. |
| `health_state()` | Returns `health().state`. | Same. |

#### Why health() Works in Phase 0

Unlike `render_widget` and `refresh_widget`, the `health()` method is fully
functional in Phase 0. It derives health from the stats, which are real atomic
counters. This allows operators to monitor the dashboard's health even before
the rendering pipeline is implemented.

#### Phase 1 Roadmap

In Phase 1, `OpsDashboardService` will:

1. Maintain a widget registry (ID -> Widget mapping).
2. Render widgets by fetching data from `MetricsCollector`.
3. Refresh widgets on the configured interval.
4. Support streaming widgets (Log, Alert) via subscription.
5. Track per-widget render latency and error rates.

---

## Part 10: Collector (Phase 1 Skeleton)

### 10.1 MetricsCollector

```rust
#[derive(Debug)]
pub struct MetricsCollector {
    cfg: OpsDashboardConfig,
    stats: OpsDashboardStats,
}
```

The collector facade for metric collection and ranged queries. In Phase 0, it
stores config and stats but returns errors for all collection operations.

#### Methods

| Method | Phase 0 Behavior | Phase 1 Behavior |
|--------|-------------------|-------------------|
| `new(cfg, stats)` | Stores cfg and stats. | Same. |
| `collect()` | Returns `Err(CollectorUnavailable)`. | Collects current metrics. |
| `query_range(range, agg)` | Returns `Err(QueryFailed("not implemented"))`. | Executes ranged query. |
| `health()` | Returns `HealthReport::derive(&stats.snapshot())`. | Same. |
| `health_state()` | Returns `health().state`. | Same. |

#### Phase 1 Roadmap

In Phase 1, `MetricsCollector` will:

1. Connect to the observability SDK's `MetricsRegistry`.
2. Execute ranged queries with the specified aggregation.
3. Return data points as a typed `DataPoint` collection.
4. Enforce the `max_query_range_ms` limit.
5. Time out queries after `query_timeout_ms`.
6. Record all query outcomes in `OpsDashboardStats`.

#### Query Execution Flow (Phase 1)

```text
query_range(range, agg)
    │
    ├─ Validate range (is_valid, within max_query_range_ms)
    │   └─ Invalid → Err(QueryFailed("invalid range"))
    │
    ├─ Start timeout timer (query_timeout_ms)
    │
    ├─ Execute query against MetricsRegistry
    │   ├─ Fetch raw data points in [start_ms, end_ms)
    │   └─ Apply aggregation (Avg, Sum, Max, Min, P95, P99)
    │
    ├─ Record stats (success/failure/timeout)
    │
    └─ Return result
        ├─ Ok(data_points) — query succeeded
        ├─ Err(QueryFailed("timeout")) — timed out
        └─ Err(QueryFailed("backend error")) — backend failure
```

---

## Part 11: Failure Modes

### 11.1 Error Propagation

All fallible APIs return `Result<T, OpsDashboardError>`. Errors propagate
upward through the call stack. The caller decides how to handle each error
based on the `is_fatal()` and `is_retryable()` predicates.

### 11.2 Retry Strategy

For retryable errors (`QueryFailed`, `CollectorUnavailable`):

1. Wait with exponential backoff: 100ms, 200ms, 400ms, 800ms, 1.6s.
2. Maximum 5 retries.
3. After max retries, escalate to `Critical` health.

For fatal errors (`WidgetNotFound`, `ShuttingDown`):

1. Do not retry.
2. Log the error.
3. If `ShuttingDown`, stop accepting new operations.

### 11.3 Error to Health Mapping

Errors feed into health via stats:

| Error | Counter Increment | Health Impact |
|-------|-------------------|---------------|
| `QueryFailed` | `queries_failed += 1` | Decreases success rate |
| `CollectorUnavailable` | `queries_failed += 1` | Decreases success rate |
| Timeout | `queries_timed_out += 1` | Decreases success rate |
| `WidgetNotFound` | None (not a query) | No direct health impact |
| `ShuttingDown` | None (lifecycle) | No direct health impact |

### 11.4 Shutdown Sequence

1. Set a shutdown flag (atomic boolean).
2. Reject new operations with `ShuttingDown`.
3. Wait for in-flight queries to complete or time out.
4. Flush stats.
5. Release resources.

---

## Part 12: Observability

### 12.1 Self-Monitoring

The ops-dashboard monitors its own health using the same `OpsDashboardStats`
that it uses for operational metrics. This self-referential design ensures
that dashboard health is always visible, even if the rendering pipeline is
not yet implemented.

### 12.2 Metrics Export

In Phase 2+, stats will be exported to Prometheus via the observability SDK:

| Counter | Prometheus Metric |
|---------|-------------------|
| `queries_total` | `ops_dashboard_queries_total` |
| `queries_succeeded` | `ops_dashboard_queries_succeeded_total` |
| `queries_failed` | `ops_dashboard_queries_failed_total` |
| `queries_timed_out` | `ops_dashboard_queries_timed_out_total` |
| `widgets_rendered` | `ops_dashboard_widgets_rendered_total` |
| `data_points_fetched` | `ops_dashboard_data_points_fetched_total` |

### 12.3 Tracing

In Phase 1, the service and collector will create spans for:

- Widget rendering (`render_widget`)
- Widget refresh (`refresh_widget`)
- Metric collection (`collect`)
- Ranged queries (`query_range`)

Each span will include:
- Widget ID (for render/refresh)
- Query range (for query_range)
- Aggregation type (for query_range)
- Duration
- Success/failure status

### 12.4 Logging

Structured logs will be emitted for:

- Configuration changes (INFO)
- Query failures (WARN)
- Collector unavailable (ERROR)
- Health state transitions (WARN for degraded, ERROR for critical)
- Shutdown events (INFO)

---

## Part 13: Configuration & Presets

### 13.1 Configuration Hierarchy

```text
Preset (archetype)
  └─ OpsDashboardBuilder (overrides)
       └─ OpsDashboardConfig (final, validated)
            └─ OpsDashboardService / MetricsCollector (runtime)
```

### 13.2 Preset Selection Guide

| Deployment | Preset | Rationale |
|------------|--------|-----------|
| Edge node | Minimal | Low resource usage, infrequent refresh. |
| Production | Standard | Balanced visibility and overhead. |
| NOC / SOC | Full | Maximum visibility, dedicated hardware. |
| Development | Full | Fast refresh, high widget count for debugging. |
| CI / Staging | Minimal | Minimal overhead, just health checks. |

### 13.3 Runtime Configuration Updates

In Phase 2+, the dashboard will support hot-reloading configuration via the
configuration-system's `ConfigWatcher`. Changes to `max_widgets`,
`refresh_interval_ms`, etc. will be applied without restart.

### 13.4 Configuration Validation

Validation is performed in two places:

1. `OpsDashboardConfig::validate()` — checks all fields against MIN/MAX.
2. `OpsDashboardBuilder::build()` — calls `validate()` and returns `Result`.

Direct struct construction bypasses validation, but this is acceptable for
internal use and const contexts. All public APIs that accept configuration
should call `validate()` before use.

---

## Part 14: Type Safety

### 14.1 Newtype Pattern

`DashboardWidgetId` is a newtype around `u32`:

```rust
#[repr(transparent)]
pub struct DashboardWidgetId(u32);
```

Benefits:
- **Type safety**: Cannot accidentally pass a raw `u32` where a widget ID
  is expected.
- **Method namespace**: Methods like `is_nil()`, `raw()` are scoped to the
  type.
- **Zero overhead**: `#[repr(transparent)]` ensures identical ABI to `u32`.
- **Copy semantics**: The type is `Copy`, so it can be passed by value.

### 14.2 Enum Repr

`WidgetType` and `QueryAggregation` use `#[repr(u8)]`:

```rust
#[repr(u8)]
pub enum WidgetType {
    Chart = 0,
    Table = 1,
    // ...
}
```

This enables:
- `WidgetType::Chart as u8` -> `0u8`
- FFI compatibility with C code expecting `uint8_t`.
- Compact storage (1 byte per value).

### 14.3 Non-Exhaustive Enums

All enums are `#[non_exhaustive]` to allow future expansion:

```rust
#[non_exhaustive]
pub enum OpsDashboardError { ... }
#[non_exhaustive]
pub enum WidgetType { ... }
#[non_exhaustive]
pub enum QueryAggregation { ... }
#[non_exhaustive]
pub enum HealthState { ... }
#[non_exhaustive]
pub enum Preset { ... }
```

Callers must include a `_` arm in match expressions, ensuring forward
compatibility.

### 14.4 Const Functions

Most methods are `const fn`, enabling compile-time evaluation:

- `DashboardWidgetId::new()`, `nil()`, `is_nil()`, `raw()`
- `WidgetType::is_streaming()`, `as_str()`
- `QueryRange::new()`, `duration_ms()`, `is_valid()`
- `QueryAggregation::as_str()`
- `OpsDashboardStats::new()`
- `StatsSnapshot::query_success_rate_bps()`, `query_failure_rate_bps()`,
  `query_timeout_rate_bps()`
- `HealthState::is_healthy()`, `needs_attention()`, `is_critical()`,
  `as_str()`, `worst_of()`
- `OpsDashboardConfig::new()`, `with_*()` methods
- `Preset::to_config()`
- `OpsDashboardBuilder::new()`, `from_preset()`, setter methods
- `OpsDashboardError::is_fatal()`, `is_retryable()`

---

## Part 15: Performance

### 15.1 Design Goals

| Metric | Target | Rationale |
|--------|--------|-----------|
| Widget render latency | < 10ms | Real-time dashboard experience. |
| Query latency (p99) | < 100ms | Fast enough for 1s refresh. |
| Stats recording | < 50ns | Atomic increment, Relaxed ordering. |
| Snapshot capture | < 200ns | 6 atomic loads, Relaxed ordering. |
| Health derivation | < 100ns | Simple arithmetic on Copy struct. |
| Memory per widget | < 1KB | Stack-allocated types, no heap. |

### 15.2 Hot Path Optimization

The hot path is: `record_query() -> record_query_success/failure() ->
snapshot() -> HealthReport::derive()`.

1. `record_*` methods: Single `fetch_add` with `Relaxed` ordering. ~5ns each.
2. `snapshot()`: 6 `load` with `Relaxed` ordering. ~30ns total.
3. `HealthReport::derive()`: Integer division + comparison. ~20ns.

Total hot path: ~60ns per query cycle. At 100K queries/second, this is 6ms
of CPU time per second — negligible.

### 15.3 Memory Layout

| Type | Size | Alignment | Heap? |
|------|------|-----------|-------|
| `DashboardWidgetId` | 4 bytes | 4 | No |
| `WidgetType` | 1 byte | 1 | No |
| `QueryRange` | 16 bytes | 8 | No |
| `QueryAggregation` | 1 byte | 1 | No |
| `OpsDashboardStats` | 48 bytes | 8 | No |
| `StatsSnapshot` | 48 bytes | 8 | No |
| `HealthReport` | 40 bytes | 8 | No |
| `HealthState` | 1 byte | 1 | No |
| `OpsDashboardConfig` | 32 bytes | 8 | No |
| `OpsDashboardBuilder` | 32 bytes | 8 | No |

All types are stack-allocated and `Copy` (except `OpsDashboardStats`, which
contains `AtomicU64` and is therefore not `Copy`).

### 15.4 Cache Friendliness

`StatsSnapshot` is 48 bytes, fitting in a single cache line (64 bytes on
most architectures). This ensures that snapshot capture reads a single cache
line, minimizing cache misses.

`OpsDashboardStats` is also 48 bytes (6 * 8-byte `AtomicU64`). The atomic
operations touch the same cache line, which could cause false sharing if
multiple threads record stats simultaneously. In Phase 1, we may pad
counters to separate cache lines if contention becomes a bottleneck.

### 15.5 Benchmark Stubs

Four Criterion benchmarks are included:

| Bench | File | What it Measures |
|-------|------|------------------|
| `render_widget` | `benches/render_widget.rs` | Widget ID creation + render call. |
| `query_range` | `benches/query_range.rs` | Range construction, duration, validation. |
| `collect_metrics` | `benches/collect_metrics.rs` | Stats recording, snapshot, collector calls. |
| `refresh` | `benches/refresh.rs` | Widget refresh + health derivation. |

In Phase 0, these benchmarks measure the overhead of the skeleton
operations (ID creation, stats recording, health derivation). In Phase 1,
they will measure the full rendering and collection pipeline.

---

## Part 16: Testing Strategy

### 16.1 Test Coverage

Phase 0 includes 72 unit tests across all modules:

| Module | Tests | Coverage |
|--------|-------|----------|
| `error` | 4 | All variants, predicates, messages. |
| `config` | 10 | Defaults, builder, validation, presets, boundaries. |
| `widget` | 7 | Nil, new, copy, repr, streaming, names, const. |
| `query` | 8 | Valid/invalid ranges, duration, aggregation repr/names. |
| `stats` | 14 | All counters, rates, truncation, saturating_mul, consistency. |
| `health` | 12 | All states, thresholds, boundaries, worst_of, report fields. |
| `builder` | 8 | Defaults, chains, validation, presets, boundaries. |
| `service` | 4 | Creation, skeleton errors, health. |
| `collector` | 4 | Creation, skeleton errors, health. |

### 16.2 Test Categories

1. **Construction tests**: Verify that types can be constructed correctly.
2. **Validation tests**: Verify that invalid inputs are rejected.
3. **Predicate tests**: Verify that `is_*` methods return correct values.
4. **Boundary tests**: Verify behavior at threshold boundaries.
5. **Rate tests**: Verify rate calculations, truncation, and saturation.
6. **Skeleton tests**: Verify that skeleton methods return the correct errors.
7. **Const tests**: Verify that const fn methods work in const contexts.

### 16.3 Property-Based Testing (Future)

In Phase 1, we will add property-based tests using `proptest`:

- For any valid `QueryRange`, `duration_ms() == end_ms - start_ms`.
- For any `StatsSnapshot`, `success_rate + failure_rate + timeout_rate <= 10000`.
- For any `HealthState` pair, `worst_of(a, b) == worst_of(b, a)` (commutativity).
- For any `OpsDashboardConfig` with valid fields, `validate()` returns `Ok`.

### 16.4 Integration Testing (Future)

Phase 1 will add integration tests that:

1. Create a real `OpsDashboardService` with a `MetricsCollector`.
2. Register widgets and verify rendering.
3. Execute queries and verify data points.
4. Simulate failures and verify health degradation.
5. Test the full lifecycle: startup -> operation -> shutdown.

---

## Part 17: Dependencies

### 17.1 Crate Dependencies

| Dependency | Type | Purpose |
|------------|------|---------|
| `configuration-system` | path | Config key-value types for runtime configuration. |
| `observability-sdk` | path | Metric types, tracing, logging. |
| `service-registry` | path | Service discovery and health tracking. |
| `thiserror` | workspace | Derive `Error` for `OpsDashboardError`. |
| `tracing` | workspace | Structured logging (Phase 1). |
| `criterion` | dev | Benchmark framework. |

### 17.2 Dependency Rationale

- **configuration-system**: The dashboard needs to read configuration at
  runtime (max_widgets, refresh_interval, etc.). The configuration-system
  provides typed key-value access with layered sources.

- **observability-sdk**: The dashboard collects and displays metrics. The
  observability-sdk provides the metric types (Counter, Gauge, Histogram),
  tracing (TraceId, SpanId), and logging (LogLevel, LogRecord) infrastructure.

- **service-registry**: The dashboard displays service health. The
  service-registry provides service discovery, endpoint management, and
  health tracking.

- **thiserror**: Used for deriving `std::error::Error` on `OpsDashboardError`
  with `#[error("...")]` format strings.

- **tracing**: Used for structured logging in Phase 1. In Phase 0, it's
  included as a dependency but not actively used.

### 17.3 No Optional Dependencies

Phase 0 has no optional dependencies. All dependencies are required. This
simplifies the build and avoids feature-flag complexity.

### 17.4 Re-exports

The crate re-exports key types from adjacent layers for convenience:

- From `configuration-system`: `ConfigKey`, `ConfigSource`, `ConfigValue`.
- From `observability-sdk`: `MetricKey`, `MetricType`, `MetricValue`,
  `TraceId`, `SpanId`, `SpanKind`, `LogLevel`, `LogRecord`,
  `ObservabilityStats` (as `ObservabilityStatsInner`), `HealthReport` (as
  `ObservabilityHealthReport`), `HealthState` (as `ObservabilityHealthState`).
- From `service-registry`: `ServiceId`, `ServiceInfo`, `ServiceState`,
  `ServiceEndpoint`, `NodeId`, `NodeInfo`, `NodeState`, `Region`, `Zone`.

These re-exports allow downstream crates to depend only on `ops-dashboard`
and access types from all three adjacent layers.

---

## Part 18: Future Phases

### 18.1 Phase 1: Functional Implementation

- Implement `OpsDashboardService::render_widget()` and `refresh_widget()`.
- Implement `MetricsCollector::collect()` and `query_range()`.
- Add widget registry (ID -> Widget mapping).
- Add data point types (`DataPoint`, `DataSeries`).
- Add query execution pipeline (validate -> execute -> aggregate -> return).
- Add timeout handling.
- Add tracing spans for all operations.
- Add structured logging.

### 18.2 Phase 2: Integration

- Integrate with `MetricsRegistry` from observability-sdk.
- Integrate with `ServiceRegistry` for service health widgets.
- Add Prometheus metrics export.
- Add configuration hot-reload via `ConfigWatcher`.
- Add WebSocket / SSE streaming for real-time widget updates.
- Add alert rule engine.

### 18.3 Phase 3: Advanced Features

- Custom widget plugins.
- Dashboard templates and sharing.
- Historical data compression and downsampling.
- Multi-tenant dashboard isolation.
- Anomaly detection on metrics.
- SLO tracking and burn rate alerts.

---

## Part 19: Conventions

### 19.1 Naming

- Types: `PascalCase` (e.g., `OpsDashboardError`, `DashboardWidgetId`).
- Functions/methods: `snake_case` (e.g., `render_widget`, `query_range`).
- Constants: `SCREAMING_SNAKE_CASE` (e.g., `SUCCESS_HEALTHY_BPS`).
- Modules: `snake_case` (e.g., `error`, `config`, `widget`).
- Variants: `PascalCase` (e.g., `Chart`, `Table`, `Healthy`).

### 19.2 Documentation

- All public items have doc comments (`///`).
- Modules have `//!` inner doc comments.
- Doc comments include `See DESIGN.md Part X` cross-references.
- Test modules are documented with `#[test]` functions.

### 19.3 Error Handling

- All fallible APIs return `Result<T, OpsDashboardError>`.
- No `unwrap()` or `expect()` in library code (enforced by clippy).
- Errors are propagated with `?` operator.
- Fatal errors are not retried; retryable errors use exponential backoff.

### 19.4 Testing

- Every public method has at least one test.
- Boundary values are tested (MIN, MAX, MIN-1, MAX+1).
- Error cases are tested (invalid inputs, empty states).
- Const functions are tested in const contexts.

### 19.5 Linting

The crate uses workspace-level clippy lints:

- `unwrap_used = deny`
- `expect_used = deny`
- `panic = deny`
- `todo = deny`
- `unimplemented = deny`
- `unreachable = deny`
- `dbg_macro = deny`
- `print_stdout = deny`
- `print_stderr = deny`

These lints ensure that the library code never panics in production and
never produces debug output.

---

## Part 20: API Reference Summary

### 20.1 Error API

```rust
OpsDashboardError::WidgetNotFound         // Fatal
OpsDashboardError::QueryFailed(&str)      // Retryable
OpsDashboardError::CollectorUnavailable   // Retryable
OpsDashboardError::ShuttingDown           // Fatal

// Predicates (const fn)
error.is_fatal()      // -> bool
error.is_retryable()  // -> bool
```

### 20.2 Config API

```rust
// Constants
MIN_MAX_WIDGETS, MAX_MAX_WIDGETS: u32
MIN_REFRESH_INTERVAL_MS, MAX_REFRESH_INTERVAL_MS: u64
MIN_QUERY_TIMEOUT_MS, MAX_QUERY_TIMEOUT_MS: u64
MIN_MAX_QUERY_RANGE_MS, MAX_MAX_QUERY_RANGE_MS: u64

// Config (Copy)
OpsDashboardConfig::new() -> Self
cfg.with_max_widgets(u32) -> Self
cfg.with_refresh_interval_ms(u64) -> Self
cfg.with_query_timeout_ms(u64) -> Self
cfg.with_max_query_range_ms(u64) -> Self
cfg.validate() -> Result<(), OpsDashboardError>

// Presets
Preset::Minimal.to_config() -> OpsDashboardConfig
Preset::Standard.to_config() -> OpsDashboardConfig
Preset::Full.to_config() -> OpsDashboardConfig

// Builder
OpsDashboardBuilder::new() -> Self
OpsDashboardBuilder::from_preset(Preset) -> Self
builder.max_widgets(u32) -> Self
builder.refresh_interval_ms(u64) -> Self
builder.query_timeout_ms(u64) -> Self
builder.max_query_range_ms(u64) -> Self
builder.build() -> Result<OpsDashboardConfig, OpsDashboardError>
```

### 20.3 Widget API

```rust
// DashboardWidgetId (Copy)
DashboardWidgetId::NIL                    // const
DashboardWidgetId::new(u32) -> Self       // const
DashboardWidgetId::nil() -> Self          // const
id.is_nil() -> bool                       // const
id.raw() -> u32                           // const

// WidgetType (Copy, #[repr(u8)])
WidgetType::Chart  = 0
WidgetType::Table  = 1
WidgetType::Gauge  = 2
WidgetType::Log    = 3
WidgetType::Alert  = 4

widget_type.is_streaming() -> bool        // const
widget_type.as_str() -> &'static str      // const
```

### 20.4 Query API

```rust
// QueryRange (Copy, Default)
QueryRange::new(u64, u64) -> Self          // const
range.duration_ms() -> u64                 // const
range.is_valid() -> bool                   // const

// QueryAggregation (Copy, Default, #[repr(u8)])
QueryAggregation::None = 0
QueryAggregation::Avg  = 1
QueryAggregation::Sum  = 2
QueryAggregation::Max  = 3
QueryAggregation::Min  = 4
QueryAggregation::P95  = 5
QueryAggregation::P99  = 6

aggregation.as_str() -> &'static str       // const
```

### 20.5 Stats API

```rust
// OpsDashboardStats (not Copy, atomic)
OpsDashboardStats::new() -> Self           // const
stats.record_query()
stats.record_query_success()
stats.record_query_failure()
stats.record_query_timeout()
stats.record_widget_render()
stats.record_data_points(u64)
stats.snapshot() -> StatsSnapshot

// StatsSnapshot (Copy, Default)
snapshot.query_success_rate_bps() -> u16   // const
snapshot.query_failure_rate_bps() -> u16   // const
snapshot.query_timeout_rate_bps() -> u16   // const
```

### 20.6 Health API

```rust
// Constants
SUCCESS_HEALTHY_BPS: u16   = 9_900
SUCCESS_DEGRADED_BPS: u16  = 9_500
SUCCESS_CRITICAL_BPS: u16  = 9_000

// HealthState (Copy, Default)
HealthState::Healthy
HealthState::Degraded
HealthState::Critical

state.is_healthy() -> bool                 // const
state.needs_attention() -> bool            // const
state.is_critical() -> bool                // const
state.as_str() -> &'static str             // const
HealthState::worst_of(a, b) -> Self        // const

// HealthReport (Copy, Default)
HealthReport::derive(&StatsSnapshot) -> Self
report.is_healthy() -> bool                // const
report.needs_attention() -> bool           // const
```

### 20.7 Service API (Phase 1 Skeleton)

```rust
OpsDashboardService::new(cfg, stats) -> Self
svc.render_widget(DashboardWidgetId) -> Result<(), OpsDashboardError>
svc.refresh_widget(DashboardWidgetId) -> Result<(), OpsDashboardError>
svc.health() -> HealthReport
svc.health_state() -> HealthState
```

### 20.8 Collector API (Phase 1 Skeleton)

```rust
MetricsCollector::new(cfg, stats) -> Self
collector.collect() -> Result<u64, OpsDashboardError>
collector.query_range(QueryRange, QueryAggregation) -> Result<u64, OpsDashboardError>
collector.health() -> HealthReport
collector.health_state() -> HealthState
```

---

## Appendix A: Basis Points

Basis points (bps) are a unit of measure for rates:

- 1 bps = 0.01%
- 100 bps = 1%
- 10,000 bps = 100%

Using basis points instead of floating-point percentages has several
advantages:

1. **No floating-point**: Integer arithmetic is faster and deterministic.
2. **Const fn**: Integer division works in `const fn`; floating-point does not.
3. **Precision**: Basis points have 2 decimal digits of precision (0.01%),
   which is sufficient for health classification.
4. **Interoperability**: Basis points are a standard unit in finance and
   monitoring.

### Conversion Table

| Percentage | Basis Points |
|------------|-------------|
| 0% | 0 |
| 1% | 100 |
| 5% | 500 |
| 10% | 1,000 |
| 50% | 5,000 |
| 90% | 9,000 |
| 95% | 9,500 |
| 99% | 9,900 |
| 99.9% | 9,990 |
| 100% | 10,000 |

---

## Appendix B: Module Dependency Graph

```text
lib.rs
 ├── error.rs        (no deps)
 ├── config.rs       (depends on error)
 ├── widget.rs       (no deps)
 ├── query.rs        (no deps)
 ├── stats.rs        (no deps)
 ├── health.rs       (depends on stats)
 ├── builder.rs      (depends on config, error)
 ├── service.rs      (depends on config, error, health, stats, widget)
 └── collector.rs    (depends on config, error, health, query, stats)
```

The dependency graph is acyclic and shallow. No module depends on more than
three other modules within the crate.

---

## Appendix C: Re-export Map

```text
ops_dashboard
 ├── OpsDashboardBuilder         (from builder)
 ├── OpsDashboardConfig          (from config)
 ├── Preset                      (from config)
 ├── OpsDashboardError           (from error)
 ├── HealthReport                (from health)
 ├── HealthState                 (from health)
 ├── QueryAggregation            (from query)
 ├── QueryRange                  (from query)
 ├── OpsDashboardService         (from service)
 ├── OpsDashboardStats           (from stats)
 ├── StatsSnapshot               (from stats)
 ├── DashboardWidgetId           (from widget)
 ├── WidgetType                  (from widget)
 ├── MetricsCollector            (from collector)
 │
 ├── ConfigKey                   (from configuration-system)
 ├── ConfigSource                (from configuration-system)
 ├── ConfigValue                 (from configuration-system)
 ├── MetricKey                   (from observability-sdk)
 ├── MetricType                  (from observability-sdk)
 ├── MetricValue                 (from observability-sdk)
 ├── TraceId                     (from observability-sdk)
 ├── SpanId                      (from observability-sdk)
 ├── SpanKind                    (from observability-sdk)
 ├── LogLevel                    (from observability-sdk)
 ├── LogRecord                   (from observability-sdk)
 ├── ObservabilityStatsInner     (from observability-sdk)
 ├── ObservabilityHealthReport   (from observability-sdk)
 ├── ObservabilityHealthState    (from observability-sdk)
 ├── ServiceId                   (from service-registry)
 ├── ServiceInfo                 (from service-registry)
 ├── ServiceState                (from service-registry)
 ├── ServiceEndpoint             (from service-registry)
 ├── NodeId                      (from service-registry)
 ├── NodeInfo                    (from service-registry)
 ├── NodeState                   (from service-registry)
 ├── Region                      (from service-registry)
 └── Zone                        (from service-registry)
```

---

## Appendix D: Verification Gates

Phase 0 must pass all five verification gates:

```bash
cargo check                    # Compiles without errors
cargo clippy -- -D warnings    # No clippy warnings
cargo fmt --check              # Formatting is correct
cargo test                     # All tests pass
cargo bench --no-run           # Benchmarks compile
```

### Gate Details

| Gate | What it checks | Failure action |
|------|---------------|----------------|
| `cargo check` | Type correctness | Fix type errors |
| `cargo clippy` | Lint compliance | Fix lint warnings |
| `cargo fmt --check` | Code formatting | Run `cargo fmt` |
| `cargo test` | Test correctness | Fix failing tests |
| `cargo bench --no-run` | Bench compilation | Fix bench code |

### Common Issues

1. **`debug_assert!` in const fn**: Not allowed in Rust 2024. Use `if` checks
   instead.

2. **`as u8` on payload enums**: Only works on unit-only enums. If an enum
   has payload variants, use a match-based `as_u8()` method.

3. **Integer division truncation**: `2 * 10000 / 3 = 6666`, not 6667. This
   is correct for advisory stats.

4. **`saturating_mul` in const fn**: Available since Rust 1.47. Use it for
   rate calculations to prevent overflow.

5. **`#[must_use]` on `Result`-returning fns**: Clippy's `double_must_use`
   lint denies this. `Result` is already `#[must_use]`.

6. **Missing `#[allow(dead_code)]` on skeleton fields**: Skeleton structs
   have fields that are not yet used. Mark them with `#[allow(dead_code)]`.

---

## Appendix E: Glossary

| Term | Definition |
|------|-----------|
| Basis points (bps) | Unit of measure for rates. 1 bps = 0.01%. 10,000 bps = 100%. |
| Dashboard widget | A visual component (chart, table, gauge, log, alert) displayed on the dashboard. |
| Health state | Tri-state classification: Healthy, Degraded, Critical. |
| Metrics collector | Component that fetches metrics from the observability SDK. |
| Query range | A half-open time interval `[start_ms, end_ms)` for metric queries. |
| Query aggregation | A function applied to raw data points (avg, sum, max, min, p95, p99). |
| Refresh interval | The period at which widgets poll for new data. |
| Skeleton | A typed facade that returns placeholder errors. Ships in Phase 0. |
| Streaming widget | A widget that receives data via push (subscription) rather than polling. |
| Worst-signal-wins | Health aggregation strategy that picks the worst state across all signals. |

---

*End of DESIGN.md*
