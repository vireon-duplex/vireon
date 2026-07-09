# configuration-system

**Hierarchical configuration system** for the Vireon application layer.

## Overview

The configuration system provides typed, hierarchical configuration with layered sources (file, environment, CLI flags, runtime) and hot reload capabilities.

### Key Features

- **Typed access**: `ConfigKey` → `ConfigValue` with compile-time safety
- **Layered sources**: Priority-based merging (Runtime > CLI > Env > File > Default)
- **Hot reload**: Watch sources for changes without restart
- **Freeze semantics**: Make configuration keys immutable
- **Observable**: Health reports and stats for operational visibility

## Quick Start

```rust
use configuration_system::{ConfigurationManager, ConfigKey, ConfigValue, ConfigBuilder, Preset};

// Create a manager from a preset
let cfg = ConfigBuilder::from_preset(Preset::Production)?;
let mgr = ConfigurationManager::new(cfg)?;

// Get a configuration value
let key = ConfigKey::new("server.port")?;
let value = mgr.get(key)?;
if let Some(port) = value.as_u64() {
    println!("Server port: {}", port);
}

// Set a runtime override
let key = ConfigKey::new("server.threads")?;
mgr.set(key, ConfigValue::U64(8))?;

// Freeze a key
mgr.freeze(ConfigKey::new("security.secret_key")?)?;
```

## Architecture

```
ConfigKey ──► [Layer Stack] ──► ConfigValue
                  │
                  ├─ Runtime (priority 100)
                  ├─ CLI Flags (priority 90)
                  ├─ Environment (priority 80)
                  ├─ File (priority 70)
                  └─ Default (priority 10)
```

## Configuration

### Builder Pattern

```rust
let cfg = ConfigBuilder::new()?
    .max_layers(12)
    .watch_interval_ms(10_000)
    .enable_hot_reload(true)
    .freeze_on_load(true)
    .build()?;
```

### Presets

- **Development**: Fast reload, no freeze (4 layers, 1s interval)
- **Staging**: Moderate reload, freeze enabled (8 layers, 5s interval)
- **Production**: Conservative reload, freeze enabled (16 layers, 30s interval)

## Types

### ConfigKey

Fixed-size key (64 bytes) stored inline:

```rust
let key = ConfigKey::new("database.host")?;
assert_eq!(key.as_str(), "database.host");
```

**Constraints**:
- Max length: 63 bytes
- Characters: alphanumeric, `_`, `-`, `.`
- No empty keys

### ConfigValue

Typed value stored inline:

```rust
let bool_val = ConfigValue::Bool(true);
let int_val = ConfigValue::I64(-42);
let uint_val = ConfigValue::U64(100);
let str_val = ConfigValue::string("hello");
let dur_val = ConfigValue::Duration(1_000_000_000); // 1s in ns
let bytes_val = ConfigValue::bytes(b"hello");
```

**Accessors**:
- `as_bool()`, `as_i64()`, `as_u64()`: Type-safe access
- `as_str()`, `as_duration_ns()`: Specialized access
- `type_name()`: String representation

## Observability

### Health Report

```rust
let report = mgr.health();
match report.state {
    HealthState::Healthy => println!("System healthy"),
    HealthState::Degraded => println!("Hit rate: {}%", report.hit_rate_bps / 100),
    HealthState::Critical => println!("Critical: {} parse errors", report.parse_errors),
}
```

**Thresholds**:
- Healthy: Hit rate ≥ 99%, parse errors ≤ 10
- Degraded: Hit rate 90-99%, parse errors 10-100
- Critical: Hit rate < 90%, parse errors > 100

### Stats

```rust
let stats = mgr.stats();
let snapshot = stats.snapshot();
println!("Reads: {}, Hits: {}, Misses: {}",
    snapshot.reads_total, snapshot.hits, snapshot.misses);
```

## Error Handling

```rust
use configuration_system::ConfigError;

match mgr.get(key) {
    Ok(value) => handle_value(value),
    Err(ConfigError::InvalidKey(msg)) => eprintln!("Invalid key: {}", msg),
    Err(ConfigError::ParseError(msg)) => eprintln!("Parse error: {}", msg),
    Err(ConfigError::TypeMismatch(msg)) => eprintln!("Type mismatch: {}", msg),
    Err(ConfigError::SourceUnavailable(source)) => {
        eprintln!("Source unavailable: {}, retrying...", source);
    }
    Err(ConfigError::Frozen(key)) => eprintln!("Key is frozen: {}", key),
    Err(ConfigError::ShuttingDown) => eprintln!("System shutting down"),
}
```

**Error Classification**:
- **Fatal**: `InvalidKey`, `Frozen`, `ShuttingDown` (do not retry)
- **Retryable**: `SourceUnavailable` (retry with backoff)
- **Input**: `ParseError`, `TypeMismatch` (fix input)

## Freeze Semantics

Freeze prevents runtime modification of critical configuration:

```rust
// Freeze on first access (if enabled in config)
let _ = mgr.get(ConfigKey::new("security.key")?)?;

// Subsequent sets will fail
mgr.set(ConfigKey::new("security.key")?, ConfigValue::Bool(true))
    .unwrap_err(); // ConfigError::Frozen
```

**Use cases**:
- Security keys (prevent privilege escalation)
- Critical parameters (prevent accidental changes)
- Compliance (audit trail)

## Performance

### Lookup Cost

- **Best case**: O(1) direct hash lookup
- **Worst case**: O(n) layer scan (n = max_layers, max 16)
- **Typical**: O(1) cache hit

### Memory Footprint

- `ConfigKey`: 64 bytes
- `ConfigValue`: 32-256 bytes (depending on type)
- `ConfigLayer`: 32 bytes
- Zero heap allocations in Phase 0

## Testing

Run tests:

```bash
cargo test
```

Run benchmarks:

```bash
cargo bench
```

## Configuration Examples

### Development

```rust
let cfg = ConfigBuilder::from_preset(Preset::Development)?;
// Fast reload, no freeze, 4 layers
```

### Production

```rust
let cfg = ConfigBuilder::from_preset(Preset::Production)?
    .watch_interval_ms(60_000)
    .build()?;
// Conservative reload, freeze enabled, 16 layers
```

## Design

See `DESIGN.md` for detailed architecture, failure modes, and operational guidance.

## License

MIT License — see LICENSE file for details.

## Status

Phase 0: Core types, stats, health, and skeleton facades implemented.
