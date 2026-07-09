# ops-dashboard

The **Operations Dashboard** for the Vireon QUIC runtime. Provides
widget rendering, metrics collection, and health monitoring for
real-time operational visibility.

## Overview

This crate delivers three core capabilities:

1. **Widgets**: Typed widget identification (`DashboardWidgetId`) and
   classification (`WidgetType`: Chart, Table, Gauge, Log, Alert).
2. **Queries**: Time-range queries (`QueryRange`) with aggregation
   functions (`QueryAggregation`: None, Avg, Sum, Max, Min, P95, P99).
3. **Health**: Tri-state health derivation (Healthy / Degraded / Critical)
   from query success rates, with worst-signal-wins aggregation.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
ops-dashboard = { path = "../ops-dashboard" }
```

## Quick Start

### Configuration

```rust
use ops_dashboard::{OpsDashboardBuilder, Preset};

// Use a preset
let cfg = Preset::Standard.to_config();

// Or build with overrides
let cfg = OpsDashboardBuilder::new()
    .max_widgets(64)
    .refresh_interval_ms(1_000)
    .query_timeout_ms(3_000)
    .max_query_range_ms(7_200_000)
    .build()?;
```

### Widgets

```rust
use ops_dashboard::{DashboardWidgetId, WidgetType};

let id = DashboardWidgetId::new(42);
assert!(!id.is_nil());
assert_eq!(id.raw(), 42);

let widget_type = WidgetType::Chart;
assert!(!widget_type.is_streaming());
assert_eq!(widget_type.as_str(), "chart");

let log_type = WidgetType::Log;
assert!(log_type.is_streaming());
```

### Queries

```rust
use ops_dashboard::{QueryRange, QueryAggregation};

let range = QueryRange::new(1_000, 5_000);
assert!(range.is_valid());
assert_eq!(range.duration_ms(), 4_000);

let agg = QueryAggregation::P95;
assert_eq!(agg.as_str(), "p95");
```

### Statistics and Health

```rust
use ops_dashboard::{OpsDashboardStats, HealthReport};

let stats = OpsDashboardStats::new();
stats.record_query();
stats.record_query_success();

let snapshot = stats.snapshot();
assert_eq!(snapshot.query_success_rate_bps(), 10_000);

let health = HealthReport::derive(&snapshot);
assert!(health.is_healthy());
```

### Error Handling

```rust
use ops_dashboard::OpsDashboardError;

match result {
    Err(OpsDashboardError::WidgetNotFound) => {
        // Fatal: programming error
    }
    Err(OpsDashboardError::QueryFailed(msg)) => {
        // Retryable: back off and retry
    }
    Err(OpsDashboardError::CollectorUnavailable) => {
        // Retryable: collector not started
    }
    Err(OpsDashboardError::ShuttingDown) => {
        // Fatal: stop accepting new operations
    }
    Ok(_) => { /* success */ }
}
```

## Configuration

### Presets

Three deployment archetypes are available:

| Preset | Widgets | Refresh | Timeout | Use case |
|--------|---------|---------|---------|----------|
| `Minimal` | 8 | 5s | 2s | Edge nodes |
| `Standard` | 32 | 2s | 5s | Production |
| `Full` | 128 | 1s | 10s | NOC / SOC |

## Health Monitoring

Health is tri-state, derived from query success rate:

| State | Success Rate | Action |
|-------|-------------|--------|
| Healthy | >= 99% (9900 bps) | None |
| Degraded | >= 90% (9000 bps) | Investigate |
| Critical | < 90% | Page on-call |

## Phase 0 Status

This crate provides **Phase 0** of the ops dashboard:

### Implemented (Real)

- Error types with predicates
- Configuration builder with presets
- Widget types (Id, WidgetType)
- Query types (Range, Aggregation)
- Statistics (atomic counters, snapshots)
- Health derivation with thresholds
- Fluent configuration builder

### Skeletons (Phase 1)

- OpsDashboardService (returns ShuttingDown)
- MetricsCollector (returns errors)

## Benchmarks

```bash
cargo bench --bench render_widget
cargo bench --bench query_range
cargo bench --bench collect_metrics
cargo bench --bench refresh
```

## Documentation

- **DESIGN.md**: Complete architecture and design rationale (20 parts + appendices)

## License

Licensed under the Vireon project license. See `LICENSE` for details.

---

**Vireon QUIC Runtime** - High-performance, type-safe networking infrastructure.
