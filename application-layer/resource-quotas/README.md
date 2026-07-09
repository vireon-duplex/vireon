# resource-quotas

The **Resource Quotas** system for the Vireon QUIC runtime. Provides
per-tenant resource quotas and enforcement: session/stream/bandwidth/
memory/request limits with atomic usage tracking.

## Overview

This crate delivers three core capabilities:

1. **Tenant Management**: Multi-tenant isolation with tier-based limits
2. **Quota Enforcement**: Real-time enforcement of resource limits
3. **Usage Tracking**: Atomic counters for lock-free usage tracking

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
resource-quotas = { path = "../resource-quotas" }
```

## Quick Start

### Tenant Identification

```rust
use resource_quotas::{TenantId, TenantTier};

// Create a tenant ID
let tenant_id = TenantId::new(12345);

// Assign a tier
let tier = TenantTier::Pro;
assert_eq!(tier.default_session_limit(), 1_000);
```

### Quota Limits

```rust
use resource_quotas::QuotaLimit;

// Create a limit from tier defaults
let limit = QuotaLimit::for_tier(TenantTier::Pro);

// Or create from explicit values
let limit = QuotaLimit::new(
    1000,    // max sessions
    10000,   // max streams
    100_000_000, // max bandwidth (bps)
    1_000_000_000, // max memory (bytes)
    1000,    // max requests/sec
);

// Merge limits (takes min of each field)
let merged = limit_a.merge(limit_b);
```

### Usage Tracking

```rust
use resource_quotas::QuotaUsage;

// Track usage
let usage = QuotaUsage::new();
usage.record_session_add();
usage.record_bandwidth(1_000_000);
usage.record_request();

// Check against limits
usage.check_against(&limit)?;

// Take a snapshot
let snap = usage.snapshot();
assert_eq!(snap.sessions_used, 1);
```

### Statistics and Health

```rust
use resource_quotas::{ResourceQuotaStats, HealthReport, StatsSnapshot};

// Track operations
let stats = ResourceQuotaStats::new();
stats.record_check();
stats.record_check_passed();
stats.record_tenant_tracked();

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
use resource_quotas::{Preset, ResourceQuotasConfig};

// Permissive: high limits for development
let cfg = Preset::Permissive.to_config();

// Standard: moderate limits for production
let cfg = Preset::Standard.to_config();

// Strict: low limits for conservative production
let cfg = Preset::Strict.to_config();
```

### Builder Pattern

Fine-tune configuration:

```rust
use resource_quotas::ResourceQuotasBuilder;

let cfg = ResourceQuotasBuilder::new()
    .max_tenants(50_000)
    .enforcement(true)
    .default_max_sessions(20_000)
    .default_max_streams(200_000)
    .default_max_bandwidth_bps(2_000_000_000)
    .check_interval_ms(10_000)
    .build()?;
```

## Tenant Tiers

Four tenant tiers provide default limits:

| Tier | Sessions | Bandwidth | Use Case |
|------|----------|-----------|----------|
| Free | 100 | 10 Mbps | Hobby users |
| Pro | 1,000 | 100 Mbps | Small production |
| Enterprise | 10,000 | 1 Gbps | Large production |
| Internal | 100,000 | 10 Gbps | Internal services |

```rust
use resource_quotas::TenantTier;

let tier = TenantTier::Pro;
assert_eq!(tier.default_session_limit(), 1_000);
assert_eq!(tier.default_bandwidth_limit_bps(), 100_000_000);
```

## Quota Enforcement

### Checking Quotas

Before resource-intensive operations:

```rust
use resource_quotas::{QuotaError, QuotaUsage};

match usage.check_against(&limit) {
    Ok(_) => {
        // Within quota, proceed
    }
    Err(QuotaError::LimitExceeded(resource)) => {
        // Reject or shed load
    }
    Err(e) => {
        // Other error
    }
}
```

### Recording Usage

Track resource consumption:

```rust
// Session lifecycle
usage.record_session_add();
// ... session active ...
usage.record_session_remove();

// Bandwidth tracking
usage.record_bandwidth(bytes_sent);

// Request rate limiting
usage.record_request();
// ... call once per second ...
usage.reset_request_counter();
```

## Error Handling

Typed errors enable precise handling:

```rust
use resource_quotas::QuotaError;

match result {
    Err(QuotaError::LimitExceeded(resource)) => {
        // Retryable: back off and retry
    }
    Err(QuotaError::TenantNotFound(id)) => {
        // Fatal: programming error
    }
    Err(QuotaError::InvalidQuota(msg)) => {
        // Fatal: configuration error
    }
    Err(QuotaError::ShuttingDown) => {
        // Fatal: system shutting down
    }
    Ok(_) => {}
}
```

Check error categories:

```rust
if let Err(e) = result {
    if e.is_fatal() {
        // Don't retry
    } else if e.is_retryable() {
        // Back off and retry
    } else if e.is_capacity() {
        // Shed load
    }
}
```

## Health Monitoring

Health is tri-state:

| State | Exceed Rate | Action |
|-------|-------------|--------|
| Healthy | ≤ 1% (100 bps) | None |
| Degraded | > 1% and ≤ 10% | Investigate |
| Critical | > 10% | Page on-call |

```rust
use resource_quotas::{HealthReport, HealthState};

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

## Performance

The system is designed for high throughput:

- **Atomic counters**: Use `Relaxed` ordering for maximum throughput
- **Arc sharing**: Cheap cloning with shared state
- **Const evaluation**: Key operations work in const contexts
- **Lock-free**: No mutexes or locks in hot paths

## Phase 0 Status

This crate provides **Phase 0** of the resource quotas system:

### Implemented (Real)

- ✅ Error types with predicates
- ✅ Configuration builder with presets
- ✅ Tenant identification and tiers
- ✅ Quota limits with merge operation
- ✅ Usage tracking with atomics
- ✅ Statistics and snapshots
- ✅ Health derivation
- ✅ Fluent configuration builder

### Skeletons (Phase 1)

- ⏳ QuotaManager (returns ShuttingDown or TenantNotFound)
- ⏳ QuotaEnforcer (returns ShuttingDown)

Functional implementations land in Phase 1.

## Benchmarks

Run benchmarks:

```bash
cargo bench --bench check_quota
cargo bench --bench reserve
cargo bench --bench release
cargo bench --bench snapshot
```

## Examples

See the `/examples` directory for complete examples:

- `tenant.rs`: Tenant identification and tiers
- `limits.rs`: Quota limits and merging
- `usage.rs`: Usage tracking and checking
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
