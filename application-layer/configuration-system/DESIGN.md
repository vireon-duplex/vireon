# Configuration System Design Document

## 1. Overview

The Configuration System is a hierarchical, typed configuration management subsystem for the Vireon application layer. It provides:

- **Typed key-value access** with compile-time safety
- **Layered sources** supporting file, environment, CLI flags, and runtime overrides
- **Hot reload** capabilities for production deployments
- **Freeze semantics** for immutable configuration
- **Observability** via health reports and stats

### 1.1 Design Goals

1. **Safety**: Typed access prevents configuration errors at compile time
2. **Performance**: O(1) lookup in the hot path with minimal overhead
3. **Flexibility**: Support multiple sources with clear precedence rules
4. **Observability**: Health metrics and stats for operational visibility
5. **Simplicity**: No heap allocation in Phase 0; Copy types everywhere

### 1.2 Non-Goals

- Dynamic schema generation (schema is static at compile time)
- Distributed configuration propagation (handled by upper layers)
- Configuration validation beyond type checking (handled by application)

## 2. Architecture

### 2.1 Component Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                   ConfigurationManager                       │
│  ┌──────────────────────────────────────────────────────┐  │
│  │              Layer Stack (priority-ordered)           │  │
│  │  ┌───────────┐  ┌───────────┐  ┌───────────┐        │  │
│  │  │  Runtime  │  │ CLI Flags │  │  Env Vars │  ...    │  │
│  │  └───────────┘  └───────────┘  └───────────┘        │  │
│  └──────────────────────────────────────────────────────┘  │
│  ┌──────────────┐  ┌──────────────────────────────────┐   │
│  │ ConfigStats  │  │      HealthReport                  │   │
│  └──────────────┘  └──────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
         │                        │
         ▼                        ▼
┌─────────────────┐      ┌─────────────────────┐
│   ConfigKey     │      │   ConfigValue       │
│  (fixed-size)   │      │   (typed enum)      │
└─────────────────┘      └─────────────────────┘
```

### 2.2 Layer Model

Configuration is organized into layers, each with a priority:

1. **Runtime** (priority 100): API-based overrides, hot reload
2. **CLI Flags** (priority 90): Command-line arguments
3. **Environment** (priority 80): Environment variables
4. **File** (priority 70): Configuration files (TOML, JSON, etc.)
5. **Default** (priority 10): Hardcoded fallbacks

Higher-priority layers override lower-priority layers. Layers are merged on lookup, not on load, enabling hot reload without restart.

### 2.3 Data Flow

```
Request (ConfigKey)
       │
       ▼
Layer Stack Lookup (priority order)
       │
       ▼
First Match → ConfigValue
       │
       ▼
Type Check + Return
```

## 3. Core Types

### 3.1 ConfigKey

Fixed-size key (64 bytes) stored inline:

```rust
pub struct ConfigKey {
    bytes: [u8; 64],  // [len: u8 | key_bytes: 63]
}
```

**Design Rationale**:
- **No heap allocation**: Keys are Copy and can be passed by value
- **Fixed size**: Enables cache-friendly lookup tables
- **Length prefix**: O(1) length check without heap indirection
- **Character restrictions**: Only alphanumeric, underscore, hyphen, dot

**Validation**:
- Max length: 63 bytes (1 byte reserved for length prefix)
- Empty keys rejected
- Invalid characters rejected

### 3.2 ConfigValue

Typed value stored inline:

```rust
pub enum ConfigValue {
    Bool(bool),
    I64(i64),
    U64(u64),
    Str { bytes: [u8; 256], len: u8 },
    Duration(u64),  // nanoseconds
    Bytes { bytes: [u8; 64], len: u8 },
}
```

**Design Rationale**:
- **Enum dispatch**: Type safety with minimal overhead
- **Fixed-size storage**: No heap allocation for strings/bytes
- **Copy semantics**: Values can be copied freely

**Type Accessors**:
- `as_bool()`, `as_i64()`, `as_u64()`: Return `Option<T>` for type-safe access
- `as_str()`, `as_duration_ns()`: Specialized accessors
- `type_name()`: String representation for diagnostics

### 3.3 ConfigSource

Source enumeration with inherent priority:

```rust
pub enum ConfigSource {
    File,      // priority 70
    Env,       // priority 80
    CliFlag,   // priority 90
    Runtime,   // priority 100
    Default,   // priority 10
}
```

**Priority Ordering**:
- Runtime overrides take precedence
- CLI flags override environment variables
- Environment variables override file config
- File config overrides defaults

### 3.4 ConfigLayer

Layer metadata:

```rust
pub struct ConfigLayer {
    source: ConfigSource,
    priority: u8,
    path_hash: u64,
    loaded_ns: u64,
}
```

**Purpose**:
- Tracks when a layer was loaded (for hot reload detection)
- Stores source hash for change detection
- Enables efficient layer invalidation

## 4. Configuration System Config

### 4.1 ConfigurationSystemConfig

```rust
pub struct ConfigurationSystemConfig {
    pub max_layers: u8,
    pub watch_interval_ms: u64,
    pub enable_hot_reload: bool,
    pub freeze_on_load: bool,
    pub max_key_length: u32,
}
```

**Field Semantics**:
- `max_layers`: Maximum number of layers to stack (1-16)
- `watch_interval_ms`: Poll interval for hot reload (100ms - 1hr)
- `enable_hot_reload`: Enable/disable hot reload
- `freeze_on_load`: Freeze keys on first access (immutable after)
- `max_key_length`: Maximum key length in bytes (1-1024)

### 4.2 Presets

Three presets encode common deployment patterns:

**Development**:
- 4 layers (fast iteration)
- 1s watch interval
- No freeze on load
- Hot reload enabled

**Staging**:
- 8 layers (moderate complexity)
- 5s watch interval
- Freeze on load enabled
- Hot reload enabled

**Production**:
- 16 layers (complex configs)
- 30s watch interval
- Freeze on load enabled
- Hot reload enabled

## 5. Health & Observability

### 5.1 ConfigStats

Atomic counters for tracking operations:

```rust
pub struct ConfigStats {
    pub reads_total: AtomicU64,
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub parse_errors: AtomicU64,
    pub reloads: AtomicU64,
    pub frozen_keys: AtomicU64,
    pub watch_events: AtomicU64,
}
```

**Recording Operations**:
- `record_read(hit: bool)`: Increments total + hit/miss
- `record_parse_error()`: Parse failure
- `record_reload()`: Configuration reload
- `record_freeze()`: Key freeze operation
- `record_watch_event()`: Watcher event

### 5.2 HealthReport

Derived health state:

```rust
pub struct HealthReport {
    pub state: HealthState,
    pub reads_total: u64,
    pub hit_rate_bps: u16,
    pub parse_errors: u64,
    pub reloads: u64,
}
```

**Health Classification**:
- **Healthy**: Hit rate ≥ 99%, parse errors ≤ 10
- **Degraded**: Hit rate 90-99%, parse errors 10-100
- **Critical**: Hit rate < 90%, parse errors > 100

**Threshold Constants**:
- `HIT_HEALTHY_BPS = 9900`: 99% hit rate
- `HIT_DEGRADED_BPS = 9000`: 90% hit rate
- `HIT_CRITICAL_BPS = 8000`: 80% hit rate
- `PARSE_ERRORS_DEGRADED = 10`: Degraded threshold
- `PARSE_ERRORS_CRITICAL = 100`: Critical threshold

## 6. Error Handling

### 6.1 ConfigError Variants

```rust
pub enum ConfigError {
    InvalidKey(String),      // Fatal
    ParseError(String),      // Input error
    TypeMismatch(String),    // Input error
    SourceUnavailable(&'static str),  // Retryable
    Frozen(String),          // Fatal
    ShuttingDown,            // Fatal
}
```

**Error Classification**:
- `is_fatal()`: InvalidKey, Frozen, ShuttingDown
- `is_retryable()`: SourceUnavailable

### 6.2 Error Handling Strategy

1. **InvalidKey**: Programming error, fix input
2. **ParseError**: Input error, fix configuration
3. **TypeMismatch**: Input error, fix accessor
4. **SourceUnavailable**: Transient, retry with backoff
5. **Frozen**: Lifecycle error, key is immutable
6. **ShuttingDown**: Lifecycle error, abort operation

## 7. Freeze Semantics

### 7.1 Purpose

Freeze semantics prevent runtime modification of critical configuration:

- Security: Prevent privilege escalation via config override
- Stability: Prevent accidental changes to critical parameters
- Auditing: Log all freeze operations for compliance

### 7.2 Freeze Behavior

When `freeze_on_load` is enabled:
1. First access to a key freezes it
2. Subsequent sets return `ConfigError::Frozen`
3. Freeze operations are recorded in stats

### 7.3 Freeze API

```rust
pub fn freeze(&mut self, key: ConfigKey) -> Result<(), ConfigError>
```

**Semantics**:
- Idempotent: Freezing an already-frozen key is a no-op
- Persistent: Freezes survive reload (unless explicitly cleared)
- Observable: Recorded in `frozen_keys` counter

## 8. Hot Reload

### 8.1 Design

Hot reload monitors configuration sources for changes:

1. **File watcher**: Monitors file modification time
2. **Environment poll**: Periodic environment variable check
3. **Runtime override**: API-based changes

### 8.2 Reload Process

```
Source Change Detected
       │
       ▼
Parse New Value
       │
       ▼
Validate Type
       │
       ▼
Update Layer
       │
       ▼
Record Reload Event
```

### 8.3 Watcher Behavior

The `ConfigWatcher` polls sources at `watch_interval_ms`:

- Efficient: No inotify, simple timestamp comparison
- Configurable: Balance between responsiveness and overhead
- Safe: Reload failures are logged, don't crash process

## 9. Performance Characteristics

### 9.1 Lookup Cost

- **Best case**: O(1) direct hash lookup
- **Worst case**: O(n) layer scan (n = max_layers, max 16)
- **Typical**: O(1) cache hit

### 9.2 Memory Footprint

- **ConfigKey**: 64 bytes
- **ConfigValue**: 32 bytes (Bool/I64/U64) to 256 bytes (Str)
- **ConfigLayer**: 32 bytes
- **ConfigStats**: 56 bytes (atomic counters)

### 9.3 Allocation Profile

**Zero heap allocations in Phase 0**:
- All types are Copy
- Strings/bytes stored inline in fixed-size arrays
- No Vec, HashMap, or String in hot path

**Phase 1+ may introduce**:
- HashMap for key-value storage (O(1) lookup)
- Vec for layer stack (ordered collection)
- Optional heap allocation for large values

## 10. Testing Strategy

### 10.1 Unit Tests

Each module has comprehensive unit tests:

- **Constructor validation**: Reject invalid inputs
- **Accessor correctness**: Return correct values
- **Edge cases**: Boundary values, empty inputs
- **Error paths**: All error variants exercised

### 10.2 Integration Tests

Phase 1 will add:

- **End-to-end lookup**: Full layer stack
- **Hot reload**: File change detection
- **Freeze semantics**: Immutable keys
- **Health reporting**: Stats → health mapping

### 10.3 Benchmark Tests

Four Criterion benchmarks:

1. **get_value**: Hot path lookup performance
2. **merge**: Multi-layer merge cost
3. **validate**: Key/value validation overhead
4. **reload**: Full reload performance

## 11. Failure Modes

### 11.1 Source Unavailable

**Symptom**: `ConfigError::SourceUnavailable`

**Handling**:
- Retry with exponential backoff
- Log warning
- Return last known value (cached)

**Operator Action**:
- Check source connectivity
- Verify source permissions
- Check source format

### 11.2 Parse Error

**Symptom**: `ConfigError::ParseError`

**Handling**:
- Reject invalid value
- Log error
- Keep previous value

**Operator Action**:
- Fix configuration file
- Verify value type
- Check syntax

### 11.3 Type Mismatch

**Symptom**: `ConfigError::TypeMismatch`

**Handling**:
- Return None from accessor
- Log warning

**Operator Action**:
- Fix accessor type
- Verify configuration schema
- Check value format

### 11.4 Frozen Key

**Symptom**: `ConfigError::Frozen`

**Handling**:
- Reject set operation
- Log audit event

**Operator Action**:
- Review freeze policy
- Process restart to unfreeze
- Update configuration before freeze

## 12. Observability

### 12.1 Metrics

Stats map to Prometheus metrics:

| Counter | Prometheus Type | Name |
|---------|-----------------|------|
| `reads_total` | counter | `config_reads_total` |
| `hits` | counter | `config_hits_total` |
| `misses` | counter | `config_misses_total` |
| `parse_errors` | counter | `config_parse_errors_total` |
| `reloads` | counter | `config_reloads_total` |
| `frozen_keys` | gauge | `config_frozen_keys` |
| `watch_events` | counter | `config_watch_events_total` |

### 12.2 Health Checks

Health report provides:

- **State**: Healthy/Degraded/Critical
- **Hit rate**: Cache effectiveness
- **Parse errors**: Configuration quality
- **Reloads**: Hot reload activity

### 12.3 Logging

Key events to log:

- Configuration reloads (INFO)
- Parse errors (WARN)
- Key freeze operations (INFO)
- Source unavailable (WARN)
- Health state changes (WARN/ERROR)

## 13. Configuration

### 13.1 Builder Pattern

Fluent builder for config construction:

```rust
let cfg = ConfigBuilder::new()?
    .max_layers(12)
    .watch_interval_ms(10_000)
    .enable_hot_reload(true)
    .freeze_on_load(false)
    .build()?;
```

### 13.2 Preset Usage

Start from a preset then override:

```rust
let cfg = ConfigBuilder::from_preset(Preset::Production)?
    .watch_interval_ms(60_000)
    .build()?;
```

### 13.3 Validation

Config is validated on:

- Builder construction
- Preset materialization
- Manager construction

**Validation checks**:
- Field ranges (min/max)
- Logical constraints (e.g. max_layers ≥ 1)
- Type consistency

## 14. Thread Safety

### 14.1 Phase 0

**Single-threaded by design**:
- No interior mutability
- No synchronization primitives
- Copy types prevent races

### 14.2 Phase 1+

**Multi-threaded support**:
- `AtomicU64` for stats (lock-free reads)
- `RwLock` for layer stack (read-heavy workload)
- `Mutex` for manager state (write operations)

### 14.3 Lock Strategy

- **Read path**: Lock-free (atomic stats) or read-lock (layer stack)
- **Write path**: Write-lock (manager state)
- **Hot reload**: Coordinated with reads (snapshot isolation)

## 15. Upgrade Path

### 15.1 Phase 0 → Phase 1

**Additions**:
- Real layer stack implementation
- Hash table for O(1) lookup
- File watcher for hot reload
- Multi-threaded access

**Compatibility**:
- API unchanged (skeleton → real impl)
- Config format unchanged
- Error types unchanged

### 15.2 Phase 1 → Phase 2

**Additions**:
- Configuration schema validation
- Remote source support (HTTP, etcd)
- Configuration diff/audit
- Metrics export to Prometheus

**Compatibility**:
- Backward compatible (opt-in features)
- Config migration path provided

## 16. Operational Runbook

### 16.1 Deploying Configuration

1. **Development**: Use file-based config + hot reload
2. **Staging**: Use environment variables + freeze on load
3. **Production**: Use file-based config + freeze on load + hot reload

### 16.2 Debugging Configuration Issues

**High miss rate**:
- Check layer ordering
- Verify key spelling
- Inspect parse errors

**Parse errors**:
- Check file syntax
- Verify value types
- Review source logs

**Health degraded**:
- Check hit rate
- Review parse errors
- Verify reload success

### 16.3 Performance Tuning

**Improve hit rate**:
- Increase layer cache size
- Pre-load common keys
- Reduce layer stack depth

**Reduce reload overhead**:
- Increase watch interval
- Use selective reload (per-layer)
- Batch reloads

## 17. Security Considerations

### 17.1 Input Validation

All keys are validated:
- Length check (≤ 63 bytes)
- Character check (alphanumeric + `_-.`)
- No special characters or control characters

### 17.2 Freeze Security

Freeze prevents privilege escalation:
- Runtime overrides rejected for frozen keys
- Audit log of all freeze operations
- Process restart required to unfreeze

### 17.3 Source Isolation

Sources are isolated:
- File read errors don't crash process
- Parse errors don't affect other layers
- Runtime overrides are opt-in

## 18. Compliance

### 18.1 Audit Trail

**Freeze operations**:
- Logged with timestamp
- Include key identity
- Record caller context

**Reloads**:
- Logged with timestamp
- Include source identity
- Record change count

### 18.2 Data Protection

**Sensitive configuration**:
- Use environment variables (not files)
- Enable freeze on load
- Restrict read permissions

**Access control**:
- File permissions (owner read-only)
- Environment variable isolation
- Runtime API authentication

## 19. Future Directions

### 19.1 Phase 2+

**Planned features**:
- Configuration schema validation
- Remote configuration sources
- Configuration versioning/rollback
- Prometheus metrics export
- gRPC API for distributed config

### 19.2 Experimental Features

**Under consideration**:
- Configuration encryption at rest
- Configuration signing/verification
- Multi-region config propagation
- Config change hooks

## 20. Production Checklist

### 20.1 Pre-Deployment

- [ ] Validate configuration in staging
- [ ] Test freeze behavior
- [ ] Verify hot reload works
- [ ] Check health reporting
- [ ] Review stats/metrics

### 20.2 Deployment

- [ ] Use Production preset
- [ ] Enable freeze on load
- [ ] Set appropriate watch interval
- [ ] Configure log aggregation
- [ ] Set up metrics scraping

### 20.3 Post-Deployment

- [ ] Monitor health reports
- [ ] Check hit rates
- [ ] Review parse errors
- [ ] Verify reloads succeed
- [ ] Audit freeze operations

---

**Document Version**: 1.0
**Last Updated**: 2025-01-07
**Status**: Phase 0 Complete
