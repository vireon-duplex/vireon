# feature-flags

**Feature flag system** for the Vireon QUIC runtime. Provides type-safe flag keys, typed values, rule-based evaluation with tenant/user context, evaluation caching, and comprehensive observability.

## Overview

Feature flags enable runtime control over system behavior without code deployment. This crate provides a robust, performant feature flag system designed for high-throughput, low-latency environments.

### Key Features

- **Type-Safe Keys**: 63-byte ASCII identifiers with compile-time guarantees
- **Typed Values**: Boolean, Percentage (0-100), and String (up to 127 bytes)
- **Rule-Based Evaluation**: Always/Never toggles, percentage rollouts, tenant whitelists
- **Context-Aware**: Tenant and user-scoped evaluation with consistent hashing
- **High Performance**: Sub-microsecond cached evaluations, lock-free statistics
- **Observable**: Comprehensive metrics, health monitoring, and per-flag statistics (Phase 1+)

## Quick Start

### Basic Usage

```rust
use feature_flags::{
    FeatureFlagsBuilder, FlagKey, FlagValue, RuleCondition, EvaluationContext
};

// Create a flag key
let key = FlagKey::new("new_api_enabled").unwrap();

// Create a rule: 20% rollout
let rule = feature_flags::FlagRule::new(
    key,
    RuleCondition::Percentage(20),
    FlagValue::Bool(true)
);

// Evaluate in context
let ctx = EvaluationContext::new(42, 1001, 0, 0);
let enabled = rule.matches(ctx);

// Create a manager
let cfg = FeatureFlagsBuilder::new()
    .max_flags(10_000)
    .cache_ttl_ms(60_000)
    .build()
    .unwrap();

let mut manager = feature_flags::FeatureFlagManager::new(cfg).unwrap();
```

### Configuration Presets

```rust
use feature_flags::{Preset, FeatureFlagsConfig};

// Balanced for production
let cfg = Preset::Default.to_config();

// High-performance (more flags, shorter cache TTL)
let cfg = Preset::HighPerformance.to_config();

// Minimal (fewer flags, longer TTL, rules disabled)
let cfg = Preset::Minimal.to_config();
```

## Architecture

```
Application Code
       │
       ▼
FeatureFlagManager
       │
       ├─► Flag Registry (storage) ← Phase 1+
       ├─► Evaluation Cache (TTL-based) ← Phase 1+
       ├─► Rule Engine (condition matching)
       └─► Statistics (atomic counters)
```

### Core Components

- **FlagKey**: Type-safe 63-byte ASCII identifier
- **FlagValue**: Typed values (Bool, Percentage, String)
- **FlagState**: Complete flag state (enabled, value, frozen, timestamp)
- **FlagRule**: Rule mapping keys to conditions and values
- **EvaluationContext**: Tenant/user context for evaluation
- **FeatureFlagsConfig**: Configuration with validation
- **FeatureFlagsStats**: Atomic counters and snapshots
- **HealthReport**: Operational health classification

## Rule Types

### Always/Never

```rust
let rule = FlagRule::new(
    key,
    RuleCondition::Always,
    FlagValue::Bool(true)
);
// Always returns true
```

### Percentage Rollout

```rust
let rule = FlagRule::new(
    key,
    RuleCondition::Percentage(25),  // 25% of users
    FlagValue::Bool(true)
);
// Uses consistent hashing for deterministic distribution
```

### Tenant Whitelist

```rust
let mut tenants = [0u64; 8];
tenants[0] = 42;
tenants[1] = 99;
let rule = FlagRule::new(
    key,
    RuleCondition::TenantIn(tenants, 2),
    FlagValue::Bool(true)
);
// Only matches tenants 42 and 99
```

## Configuration

### Fields

- **max_flags**: Maximum number of flags (10-1,000,000)
- **evaluation_cache_ttl_ms**: Cache TTL in milliseconds (1,000-3,600,000)
- **enable_rules**: Enable rule evaluation (default: true)
- **default_offline_value**: Default when provider unavailable (default: false)

### Example

```rust
let cfg = FeatureFlagsBuilder::new()
    .max_flags(50_000)
    .cache_ttl_ms(30_000)
    .enable_rules(true)
    .default_offline_value(false)
    .build()
    .unwrap();
```

## Statistics and Health

### Metrics

- **evaluations_total**: Total evaluations performed
- **evaluations_on/off**: Evaluations returning enabled/disabled
- **cache_hits/misses**: Cache effectiveness
- **rule_matches/misses**: Rule condition results

### Health Classification

- **Healthy**: Cache hit rate ≥ 95%
- **Degraded**: Cache hit rate < 95% but ≥ 80%
- **Critical**: Cache hit rate < 80%

### Example

```rust
let manager = FeatureFlagManager::new(cfg).unwrap();
let health = manager.health();
println!("Health: {:?}", health.state);
println!("Cache hit rate: {}%", health.cache_hit_rate_bps / 100);
```

## Error Handling

All errors are typed as `FlagError`:

```rust
pub enum FlagError {
    NotFound(String),           // Flag not found
    InvalidValue(String),      // Invalid value
    EvalError(&'static str),   // Evaluation error (retryable)
    Frozen(FlagKey),            // Flag is frozen (fatal)
    ShuttingDown,              // System shutting down (fatal)
}
```

### Error Predicates

```rust
match error {
    e if e.is_fatal() => { /* abort */ }
    e if e.is_retryable() => { /* retry with backoff */ }
    _ => { /* handle other cases */ }
}
```

## Performance

### Targets

- Flag creation: < 100ns
- Cached evaluation: < 500ns
- Uncached evaluation: < 10µs
- Statistics update: < 100ns

### Memory

- Per flag: ~200 bytes
- Cache: Unbounded (Phase 0), LRU (Phase 1+)
- Statistics: Fixed 56 bytes

## Phase Status

### Phase 0 (Current)

✅ **Implemented**:
- Type-safe keys and values
- Rule conditions and evaluation
- Configuration and presets
- Statistics and health monitoring
- Comprehensive tests

⏳ **Skeletons**:
- `FeatureFlagManager` (registry + evaluation)
- `FlagProvider` (trait for flag sources)

### Phase 1+ (Planned)

- Real flag storage (HashMap)
- Evaluation cache with TTL and LRU eviction
- File-based provider integration
- Concurrent read access (RwLock)
- Per-flag statistics

## Testing

Run tests:

```bash
cargo test --package feature-flags
```

Run benchmarks:

```bash
cargo bench --package feature-flags
```

Check formatting:

```bash
cargo fmt --package feature-flags --check
```

Run linter:

```bash
cargo clippy --package feature-flags -- -D warnings
```

## Examples

See the `examples/` directory for complete examples:
- `basic_flag.rs`: Simple boolean flags
- `percentage_rollout.rs`: Gradual rollout patterns
- `tenant_specific.rs`: Tenant whitelisting
- `monitoring.rs`: Statistics and health monitoring

## Documentation

- **API Docs**: [docs.rs](https://docs.rs/feature-flags) (when published)
- **Design Doc**: [DESIGN.md](DESIGN.md) - Comprehensive architecture
- **Contributing**: See CONTRIBUTING.md in the Vireon repository

## License

GNU AFFERO GENERAL PUBLIC LICENSE v3.0

## Related Crates

- `resource-quotas`: Per-tenant resource limits
- `observability-sdk`: Metrics and tracing
- `configuration-system`: Runtime configuration

---

**Part of the Vireon Project** - High-performance realtime runtime built on QUIC and io_uring.
