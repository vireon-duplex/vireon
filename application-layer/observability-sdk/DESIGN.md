# Observability SDK - Design Document

## Part 1: Overview

### 1.1 Purpose

The Observability SDK provides the foundational instrumentation capabilities for the Vireon QUIC runtime. It delivers:

- **Metrics**: Counter, Gauge, and Histogram metric types with pluggable backends
- **Distributed Tracing**: TraceId and SpanId management with span lifecycle
- **Structured Logging**: Severity-based logging with trace context propagation
- **Health Monitoring**: Operational health derivation from telemetry signals

### 1.2 Design Philosophy

The SDK follows these principles:

1. **Zero-cost when disabled**: All instrumentation can be compiled out when not needed
2. **Async-first**: Non-blocking operations suitable for high-throughput systems
3. **Typed errors**: Exhaustive error variants enable precise error handling
4. **Const-friendly**: Core operations work in const contexts for compile-time validation
5. **Testability**: Every module is unit-tested with >90% coverage

### 1.3 Architecture

```text
┌─────────────────────────────────────────────────────────────┐
│                     Application Layer                        │
├─────────────────────────────────────────────────────────────┤
│  Observability SDK                                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ MetricsRegistry│  │   Tracer     │  │  LogEmitter  │      │
│  │  (Phase 1)   │  │  (Phase 1)   │  │  (Phase 1)   │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│         │                   │                   │              │
│         └───────────────────┼───────────────────┘          │
│                             ▼                                 │
│                    ┌──────────────────┐                       │
│                    │ MetricsExporter  │                       │
│                    │   (Phase 1)      │                       │
│                    └──────────────────┘                       │
└─────────────────────────────────────────────────────────────┘
```

## Part 2: Metrics System

### 2.1 Metric Types

The SDK supports three metric types:

#### Counter
Monotonically increasing value. Use for:
- Request counts
- Error counts
- Operation completions

```rust
let value = MetricValue::Counter(123);
assert_eq!(value.type_of(), MetricType::Counter);
```

#### Gauge
Point-in-time value that can increase or decrease. Use for:
- Active connections
- Memory usage
- Queue depth

```rust
let value = MetricValue::Gauge(-42);
assert_eq!(value.type_of(), MetricType::Gauge);
```

#### Histogram
Distribution of values with count, sum, min, max. Use for:
- Request latencies
- Response sizes
- Processing times

```rust
let value = MetricValue::Histogram {
    count: 1000,
    sum: 50_000,
    min: 10,
    max: 200,
};
assert_eq!(value.type_of(), MetricType::Histogram);
```

### 2.2 Metric Keys

Metric keys are string identifiers up to 63 bytes:

```rust
let key = MetricKey::new("http_request_duration")?;
assert_eq!(key.as_str(), "http_request_duration");
```

Key validation rules:
- Must not be empty
- Must not exceed 63 bytes
- Must not contain null bytes
- Must be valid UTF-8

### 2.3 Metric Registry (Phase 1)

In Phase 1, the registry will support:

```rust
let mut registry = MetricsRegistry::new(config, stats);

// Register a metric
registry.register(
    MetricKey::new("request_count")?,
    MetricType::Counter,
)?;

// Record a value
registry.record(
    MetricKey::new("request_count")?,
    1,
)?;
```

## Part 3: Distributed Tracing

### 3.1 Trace Context

Every distributed operation has a trace context:

- **TraceId**: 128-bit identifier for the entire trace
- **SpanId**: 64-bit identifier for a single span within the trace

```rust
let trace_id = TraceId::new(0x1234_5678_9abc_def0_1234_5678_9abc_def0);
let span_id = SpanId::new(0x1234_5678_9abc_def0);
```

### 3.2 Span Kinds

Spans are classified by their role in the trace:

```rust
pub enum SpanKind {
    Server,   // Received request
    Client,   // Sending request
    Internal, // Local computation
    Producer, // Publishing to queue
    Consumer, // Consuming from queue
}
```

Remote spans (Server, Client, Producer, Consumer) represent network operations.
Internal spans represent local computation.

### 3.3 Tracer (Phase 1)

In Phase 1, the tracer will support:

```rust
let mut tracer = Tracer::new(config, stats);

// Open a span
let span_id = tracer.span_open(
    0x1234, // name hash
    SpanKind::Server,
)?;

// Close the span
tracer.span_close(span_id)?;
```

## Part 4: Structured Logging

### 4.1 Log Levels

Severity levels from most to least severe:

```rust
pub enum LogLevel {
    Error, // Critical failures
    Warn,  // Potential issues
    Info,  // Normal operations (default)
    Debug, // Detailed debugging
    Trace, // Very detailed tracing
}
```

Lower numeric values = more severe.

### 4.2 Log Records

A log record contains:

```rust
pub struct LogRecord {
    pub level: LogLevel,
    pub message_hash: u64,     // For deduplication
    pub trace_id: TraceId,     // Nil if no trace context
    pub span_id: SpanId,       // Nil if no span context
    pub timestamp_ns: u64,     // Unix timestamp nanoseconds
}
```

### 4.3 Log Filtering

Logs can be filtered by severity threshold:

```rust
assert!(LogLevel::Error.should_log(LogLevel::Info));     // Yes
assert!(!LogLevel::Info.should_log(LogLevel::Error));    // No
```

## Part 5: Statistics and Health

### 5.1 ObservabilityStats

Atomic counters track all operations:

```rust
pub struct ObservabilityStats {
    pub metrics_recorded: AtomicU64,
    pub spans_opened: AtomicU64,
    pub spans_closed: AtomicU64,
    pub logs_emitted: AtomicU64,
    pub exports_total: AtomicU64,
    pub exports_failed: AtomicU64,
}
```

All counters use `Relaxed` ordering for maximum throughput.

### 5.2 StatsSnapshot

A point-in-time snapshot with precomputed rates:

```rust
pub struct StatsSnapshot {
    // Raw counters...
}

impl StatsSnapshot {
    pub fn export_failure_rate_bps(&self) -> u16 {
        // Precomputed as basis points (0..=10_000)
    }

    pub fn active_spans(&self) -> u64 {
        // spans_opened - spans_closed
    }
}
```

### 5.3 Health Classification

Health is tri-state:

| State | Condition | Action |
|-------|-----------|--------|
| Healthy | Export failure ≤ 5% | None |
| Degraded | Export failure > 5% and ≤ 20% | Investigate |
| Critical | Export failure > 20% | Page on-call |

```rust
let report = HealthReport::derive(&stats_snapshot);
if report.needs_attention() {
    // Alert operator
}
```

## Part 6: Configuration

### 6.1 ObservabilityConfig

```rust
pub struct ObservabilityConfig {
    pub max_metrics: u32,
    pub max_spans: u32,
    pub export_interval_ms: u64,
    pub enable_tracing: bool,
    pub enable_metrics: bool,
    pub sampling_rate_bps: u16, // 0..=10_000
}
```

### 6.2 Presets

Three deployment archetypes:

| Preset | Metrics | Tracing | Sampling | Use Case |
|--------|---------|---------|----------|----------|
| Minimal | Enabled | Disabled | 0% | Production, minimal overhead |
| Standard | Enabled | Enabled | 1% | Production, balanced |
| Verbose | Enabled | Enabled | 10% | Development, debugging |

```rust
let cfg = Preset::Standard.to_config();
let cfg = ObservabilityBuilder::from_preset(Preset::Verbose)
    .sampling_rate_bps(500) // Override to 5%
    .build()?;
```

## Part 7: Error Handling

### 7.1 ObservabilityError

```rust
pub enum ObservabilityError {
    ExportFailed(&'static str),
    MetricNotFound(String),
    InvalidLabel(&'static str),
    SpanNotFound(u64),
    InvalidTraceId,
    ShuttingDown,
}
```

### 7.2 Error Classification

Errors are categorized by predicates:

- **Fatal** (`is_fatal`): InvalidTraceId, ShuttingDown
- **Retryable** (`is_retryable`): ExportFailed
- **Capacity** (none yet, reserved for future)

## Part 8: Builder Pattern

### 8.1 Fluent Configuration

```rust
let cfg = ObservabilityBuilder::new()
    .max_metrics(50_000)
    .tracing(true)
    .sampling_rate_bps(200)
    .build()?;
```

The builder validates on `build()`, ensuring invalid configurations
fail early.

## Part 9: Testing Strategy

### 9.1 Unit Tests

Every module has comprehensive unit tests:

- Constructor validation
- Predicate logic
- Error classification
- Const fn behavior

### 9.2 Property Tests

Critical invariants are tested:

- Metric key length limits
- Trace ID validity checks
- Health threshold boundaries
- Sampling rate bounds

### 9.3 Benchmarks

Performance-critical operations are benchmarked:

- Metric key creation
- Span ID operations
- Stats snapshot
- Health derivation

## Part 10: Phase 0 Scope

### 10.1 Implemented (Real)

Phase 0 provides real implementations for:

1. **Error types** (`error.rs`): Complete error enum with predicates
2. **Configuration** (`config.rs`): Full config builder with presets
3. **Metric types** (`metric.rs`): MetricKey, MetricType, MetricValue
4. **Tracing IDs** (`trace.rs`): TraceId, SpanId, SpanKind
5. **Logging** (`log.rs`): LogLevel, LogRecord
6. **Statistics** (`stats.rs`): Atomic counters and snapshots
7. **Health** (`health.rs`): Health derivation from stats
8. **Builder** (`builder.rs`): Fluent configuration builder

### 10.2 Skeletons (Type Stubs)

Phase 1 modules ship as typed skeletons:

1. **MetricsExporter** (`exporter.rs`): Returns ShuttingDown
2. **MetricsRegistry** (`registry.rs`): Returns ShuttingDown
3. **Tracer** (`tracer.rs`): Returns ShuttingDown

These provide complete type signatures for downstream consumers
while deferring implementation to Phase 1.

## Part 11: Failure Modes

### 11.1 Export Failures

Export operations can fail for various reasons:

- Backend unavailable (network partition)
- Backend overloaded (rate limit)
- Invalid data (serialization error)

All export failures are retryable with exponential backoff.

### 11.2 Resource Exhaustion

The SDK limits resource usage:

- `max_metrics`: Prevents unbounded metric registration
- `max_spans`: Prevents unbounded span allocation
- `sampling_rate_bps`: Controls trace volume

When limits are reached, operations return `ShuttingDown`.

### 11.3 Invalid Data

Invalid inputs are rejected early:

- Empty metric keys
- Metric keys exceeding 63 bytes
- Null bytes in metric keys
- Invalid sampling rates

## Part 12: Observability

### 12.1 Metrics Exported

The SDK exports these metrics in Phase 2:

| Metric Name | Type | Description |
|-------------|------|-------------|
| `observability_metrics_recorded_total` | counter | Total metrics recorded |
| `observability_spans_opened_total` | counter | Total spans opened |
| `observability_spans_closed_total` | counter | Total spans closed |
| `observability_logs_emitted_total` | counter | Total logs emitted |
| `observability_exports_total` | counter | Total export attempts |
| `observability_exports_failed_total` | counter | Total export failures |
| `observability_active_spans` | gauge | Currently active spans |
| `observability_export_failure_rate` | gauge | Export failure rate (bps) |

### 12.2 Health Signals

Health is derived from:

1. Export failure rate (primary signal)
2. Active span count (secondary signal)
3. Metric registration rate (secondary signal)

## Part 13: Configuration Management

### 13.1 Default Configuration

The default config is production-safe:

```rust
pub const fn new() -> ObservabilityConfig {
    ObservabilityConfig {
        max_metrics: 10_000,
        max_spans: 100_000,
        export_interval_ms: 5_000,
        enable_tracing: false,
        enable_metrics: true,
        sampling_rate_bps: 0,
    }
}
```

### 13.2 Configuration Validation

All configs are validated before use:

```rust
pub fn validate(&self) -> Result<(), ObservabilityError> {
    // Check ranges
    // Check logical consistency
    // Return InvalidLabel for violations
}
```

### 13.3 Preset Tuning

Presets provide starting points:

```rust
let cfg = Preset::Standard.to_config();
let cfg = ObservabilityBuilder::from_preset(Preset::Standard)
    .max_metrics(20_000)  // Double capacity
    .sampling_rate_bps(50) // Reduce sampling to 0.5%
    .build()?;
```

## Part 14: Performance Considerations

### 14.1 Atomic Ordering

All stats use `Relaxed` ordering:

- Stats are advisory, not load-bearing
- Relaxed provides maximum throughput
- Momentary inconsistency is acceptable

### 14.2 Allocation Patterns

Key types are Copy to avoid heap allocation:

```rust
#[derive(Clone, Copy)]
pub struct TraceId(u128);
#[derive(Clone, Copy)]
pub struct SpanId(u64);
```

MetricKey stores inline (64 bytes) to avoid heap allocation.

### 14.3 Const Evaluation

Key operations work in const contexts:

```rust
const ID: TraceId = TraceId::new(123);
assert!(ID.is_valid()); // Evaluated at compile time
```

## Part 15: Security Considerations

### 15.1 Message Hashing

Log messages are hashed (not stored) to:

- Reduce memory usage
- Prevent PII leakage
- Enable aggregation

### 15.2 Sampling

Trace sampling prevents:

- Excessive bandwidth usage
- Backend overload
- PII exposure in traces

### 15.3 Rate Limiting

Export intervals prevent:

- Backend overload
- Exponential retry storms
- Resource exhaustion

## Part 16: Future Enhancements

### 16.1 Phase 1 Features

Phase 1 will add:

- Functional MetricsRegistry with storage
- Functional Tracer with span lifecycle
- Functional MetricsExporter with batching
- Dynamic metric registration
- Span context propagation

### 16.2 Phase 2 Features

Phase 2 will add:

- Prometheus export format
- OpenTelemetry protocol support
- Dynamic configuration reloading
- Metric aggregation and rollups
- Trace sampling strategies

### 16.3 Phase 3+ Features

Future phases may add:

- Custom metric backends
- Distributed trace context injection
- Log aggregation and filtering
- Alert rule evaluation
- Grafana dashboard generation

## Part 17: Integration Points

### 17.1 Session Manager Integration

```rust
use session_manager::SessionId;

// Trace ID can be correlated with Session ID
pub struct LogRecord {
    pub trace_id: TraceId,  // Correlates operations
    // SessionId is separate, provided by caller
}
```

### 17.2 Connection Shard Integration

```rust
// Connection-level metrics can be recorded
registry.record(
    MetricKey::new("connection_active")?,
    1,
)?;
```

### 17.3 Stream Manager Integration

```rust
// Stream-level metrics
registry.record(
    MetricKey::new("stream_bytes_sent")?,
    bytes_sent,
)?;
```

## Part 18: Operational Guidance

### 18.1 Deployment Recommendations

For production:

1. Use `Preset::Standard` for balanced observability
2. Set `sampling_rate_bps` based on traffic volume
3. Monitor `export_failure_rate` for backend health
4. Set alerts on `HealthState::Degraded`

### 18.2 Debugging Production Issues

When investigating issues:

1. Increase sampling rate temporarily
2. Check `export_failure_rate` for backend issues
3. Review `active_spans` for span leaks
4. Correlate TraceId across services

### 18.3 Capacity Planning

Plan capacity based on:

- Metrics per second: ~10K default
- Active spans: ~100K default
- Export bandwidth: ~1 MB/s default
- Backend storage: ~10 GB/day default

## Part 19: Migration Path

### 19.1 From No Observability

Migrate incrementally:

1. Enable metrics only (`Preset::Minimal`)
2. Add critical counters (errors, requests)
3. Enable tracing at 0.1% sampling
4. Increase sampling based on capacity

### 19.2 From Legacy Systems

Migrate by:

1. Run legacy and new SDK in parallel
2. Compare metrics for validation
3. Gradually shift traffic
4. Deprecate legacy endpoints

### 19.3 Configuration Migration

Convert legacy configs:

```rust
// Legacy: enable_all = true
// New:
let cfg = ObservabilityBuilder::new()
    .metrics(true)
    .tracing(true)
    .sampling_rate_bps(100)
    .build()?;
```

## Part 20: Appendix

### 20.1 Glossary

- **Basis Points (bps)**: 1/100 of a percent (0..=10_000)
- **Trace**: A distributed operation spanning multiple services
- **Span**: A single unit of work within a trace
- **Sampling**: Dropping traces probabilistically to reduce volume
- **Export**: Sending metrics/traces to a backend

### 20.2 References

- OpenTelemetry Specification: https://opentelemetry.io/docs/reference/specification/
- Prometheus Data Model: https://prometheus.io/docs/concepts/data_model/
- W3C Trace Context: https://www.w3.org/TR/trace-context/

### 20.3 Version History

- v0.1.0 (Phase 0): Core types, configuration, stats, health
- v0.2.0 (Phase 1): Functional registry, tracer, exporter
- v0.3.0 (Phase 2): Prometheus export, OpenTelemetry protocol

---

*This design document is part of the Vireon QUIC runtime project. For the latest version, see the project repository.*
