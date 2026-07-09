# observability-sdk

The **Observability SDK** for the Vireon QUIC runtime. Provides
metrics (counter/gauge/histogram), distributed tracing (trace/span IDs),
and structured logging for application-layer observability.

## Overview

This crate delivers three core observability capabilities:

1. **Metrics**: Typed metrics (Counter, Gauge, Histogram) with pluggable backends
2. **Distributed Tracing**: TraceId and SpanId management with span lifecycle
3. **Structured Logging**: Severity-based logging with trace context propagation

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
observability-sdk = { path = "../observability-sdk" }
```

## Quick Start

### Metrics

```rust
use observability_sdk::{MetricKey, MetricType, MetricValue};

// Create a metric key
let key = MetricKey::new("request_count")?;

// Create a counter value
let value = MetricValue::Counter(1);
```

### Distributed Tracing

```rust
use observability_sdk::{TraceId, SpanId, SpanKind};

// Create a trace context
let trace_id = TraceId::new(0x1234_5678_9abc_def0_1234_5678_9abc_def0);
let span_id = SpanId::new(0x1234_5678_9abc_def0);

// Classify span type
let kind = SpanKind::Server; // Received request
assert!(kind.is_remote());
```

### Structured Logging

```rust
use observability_sdk::{LogLevel, LogRecord, TraceId, SpanId};

// Create a log record
let record = LogRecord::new(
    LogLevel::Warn,
    0x1234, // message hash
    TraceId::new(100),
    SpanId::new(50),
    1_600_000_000_000_000_000,
);

// Check if log should be emitted
assert!(LogLevel::Error.should_log(LogLevel::Info)); // Yes
assert!(!LogLevel::Info.should_log(LogLevel::Error)); // No
```

### Statistics and Health

```rust
use observability_sdk::{ObservabilityStats, HealthReport, StatsSnapshot};

// Track operations
let stats = ObservabilityStats::new();
stats.record_metric();
stats.record_span_open();
stats.record_span_close();

// Get snapshot
let snapshot = stats.snapshot();

// Derive health
let health = HealthReport::derive(&snapshot);
if health.needs_attention() {
    // Alert operator
}
```

## Configuration

### Presets

Three deployment archetypes are available:

```rust
use observability_sdk::{Preset, ObservabilityConfig};

// Minimal: metrics only, no tracing (production-safe)
let cfg = Preset::Minimal.to_config();

// Standard: metrics + tracing with 1% sampling (balanced)
let cfg = Preset::Standard.to_config();

// Verbose: all features, 10% sampling (development)
let cfg = Preset::Verbose.to_config();
```

### Builder Pattern

Fine-tune configuration with the builder:

```rust
use observability_sdk::ObservabilityBuilder;

let cfg = ObservabilityBuilder::new()
    .max_metrics(50_000)
    .max_spans(200_000)
    .export_interval_ms(10_000)
    .tracing(true)
    .sampling_rate_bps(500) // 5%
    .build()?;
```

## Metric Types

### Counter

Monotonically increasing values for event counts:

```rust
let value = MetricValue::Counter(123);
assert_eq!(value.as_counter(), Some(123));
```

Use for: request counts, error counts, operation completions.

### Gauge

Point-in-time values that can increase or decrease:

```rust
let value = MetricValue::Gauge(-42);
assert_eq!(value.as_gauge(), Some(-42));
```

Use for: active connections, memory usage, queue depth.

### Histogram

Distribution of values with count, sum, min, max:

```rust
let value = MetricValue::Histogram {
    count: 1000,
    sum: 50_000,
    min: 10,
    max: 200,
};
```

Use for: request latencies, response sizes, processing times.

## Trace Context

### TraceId and SpanId

```rust
use observability_sdk::{TraceId, SpanId};

// Create trace context
let trace_id = TraceId::new(0x1234_5678_9abc_def0_1234_5678_9abc_def0);
let span_id = SpanId::new(0x1234_5678_9abc_def0);

// Check validity
assert!(!trace_id.is_nil());
assert!(trace_id.is_valid());
```

### Span Kinds

Classify spans by their role:

```rust
use observability_sdk::SpanKind;

let server = SpanKind::Server;    // Received request
let client = SpanKind::Client;    // Sending request
let internal = SpanKind::Internal; // Local computation

assert!(server.is_remote());
assert!(!internal.is_remote());
```

## Log Levels

Severity levels from most to least severe:

```rust
use observability_sdk::LogLevel;

let levels = [
    LogLevel::Error, // Most severe
    LogLevel::Warn,
    LogLevel::Info,  // Default
    LogLevel::Debug,
    LogLevel::Trace, // Least severe
];

// Check severity
assert!(LogLevel::Error.should_log(LogLevel::Info)); // Yes
assert!(!LogLevel::Info.should_log(LogLevel::Error)); // No
```

## Health Monitoring

Health is tri-state:

| State | Export Failure Rate | Action |
|-------|---------------------|--------|
| Healthy | ≤ 5% (500 bps) | None |
| Degraded | > 5% and ≤ 20% | Investigate |
| Critical | > 20% | Page on-call |

```rust
use observability_sdk::{HealthReport, HealthState};

let report = HealthReport::derive(&snapshot);

match report.state {
    HealthState::Healthy => {
        // All systems nominal
    }
    HealthState::Degraded => {
        // Investigate during business hours
    }
    HealthState::Critical => {
        // Page on-call immediately
    }
}
```

## Error Handling

Typed errors enable precise handling:

```rust
use observability_sdk::ObservabilityError;

match result {
    Err(ObservabilityError::ShuttingDown) => {
        // Fatal: abort
    }
    Err(ObservabilityError::ExportFailed(msg)) => {
        // Retryable: back off and retry
    }
    Err(ObservabilityError::InvalidTraceId) => {
        // Fatal: programming error
    }
    _ => Ok(()),
}
```

Check error categories:

```rust
if let Err(e) = result {
    if e.is_fatal() {
        // Don't retry
    } else if e.is_retryable() {
        // Back off and retry
    }
}
```

## Performance

The SDK is designed for high throughput:

- **Atomic counters**: Use `Relaxed` ordering for maximum throughput
- **Stack allocation**: Key types are `Copy` to avoid heap allocation
- **Const evaluation**: Key operations work in const contexts
- **Zero-cost when disabled**: All instrumentation can be compiled out

## Phase 0 Status

This crate provides **Phase 0** of the observability SDK:

### Implemented (Real)

- ✅ Error types with predicates
- ✅ Configuration builder with presets
- ✅ Metric types (Key, Type, Value)
- ✅ Tracing IDs (TraceId, SpanId, SpanKind)
- ✅ Logging (LogLevel, LogRecord)
- ✅ Statistics (atomic counters, snapshots)
- ✅ Health derivation
- ✅ Fluent configuration builder

### Skeletons (Phase 1)

- ⏳ MetricsExporter (returns ShuttingDown)
- ⏳ MetricsRegistry (returns ShuttingDown)
- ⏳ Tracer (returns ShuttingDown)

Functional implementations land in Phase 1.

## Benchmarks

Run benchmarks:

```bash
cargo bench --bench record_metric
cargo bench --bench span_enter
cargo bench --bench export
cargo bench --bench snapshot
```

## Examples

See the `/examples` directory for complete examples:

- `metrics.rs`: Metric recording and types
- `tracing.rs`: Trace context and span lifecycle
- `logging.rs`: Log levels and log records
- `health.rs`: Health monitoring and alerts

## Documentation

- **DESIGN.md**: Complete architecture and design rationale
- **Module docs**: Inline documentation for all public APIs
- **Examples**: Working code examples for common patterns

## License

Licensed under the Vireon project license. See `LICENSE` for details.

## Contributing

Contributions welcome! Please see `CONTRIBUTING.md` for guidelines.

---

**Vireon QUIC Runtime** - High-performance, type-safe networking infrastructure.
