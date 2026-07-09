# Resource Quotas - Design Document

## Part 1: Overview

### 1.1 Purpose

The Resource Quotas system provides per-tenant resource quota management and enforcement for the Vireon QUIC runtime. It delivers:

- **Tenant Management**: Multi-tenant isolation with tier-based limits
- **Quota Enforcement**: Real-time enforcement of session/stream/bandwidth/memory/request limits
- **Atomic Usage Tracking**: Lock-free usage counters with snapshot support
- **Health Monitoring**: Operational health derivation from enforcement signals

### 1.2 Design Philosophy

The system follows these principles:

1. **Tenant-first**: All resources are tracked and limited per-tenant
2. **Atomic operations**: Usage tracking uses lock-free atomics for maximum throughput
3. **Tier-based defaults**: Tenant tiers provide sensible defaults while allowing customization
4. **Typed errors**: Exhaustive error variants enable precise error handling
5. **Const-friendly**: Core operations work in const contexts for compile-time validation

### 1.3 Architecture

```text
┌─────────────────────────────────────────────────────────────┐
│                     Application Layer                        │
├─────────────────────────────────────────────────────────────┤
│  Resource Quotas                                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ QuotaManager │  │ QuotaEnforcer│  │  QuotaUsage  │      │
│  │  (Phase 1)   │  │  (Phase 1)   │  │  (Phase 0)   │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│         │                  │                   │              │
│         └───────────────────┼───────────────────┘          │
│                             ▼                                 │
│                    ┌──────────────────┐                       │
│                    │  QuotaLimit     │                       │
│                    │   (Phase 0)     │                       │
│                    └──────────────────┘                       │
└─────────────────────────────────────────────────────────────┘
```

## Part 2: Tenant Model

### 2.1 Tenant Identification

Tenants are identified by a 64-bit `TenantId`:

```rust
pub struct TenantId(u64);
```

The nil ID (0) is reserved and invalid for tenant registration.

### 2.2 Tenant Tiers

Four tenant tiers provide default limits:

| Tier | Sessions | Bandwidth | Use Case |
|------|----------|-----------|----------|
| Free | 100 | 10 Mbps | Hobby users, testing |
| Pro | 1,000 | 100 Mbps | Small production workloads |
| Enterprise | 10,000 | 1 Gbps | Large production workloads |
| Internal | 100,000 | 10 Gbps | Internal services |

### 2.3 Tier-Based Limits

Each tier has computed defaults:

```rust
impl TenantTier {
    pub const fn default_session_limit(&self) -> u32 { ... }
    pub const fn default_bandwidth_limit_bps(&self) -> u64 { ... }
}
```

Custom limits can override tier defaults:

```rust
let limit = QuotaLimit::for_tier(TenantTier::Pro)
    .merge(QuotaLimit::new(2000, 20000, 200_000_000, 2_000_000_000, 2000));
```

## Part 3: Quota Limits

### 3.1 Limit Types

`QuotaLimit` encompasses five resource dimensions:

```rust
pub struct QuotaLimit {
    pub max_sessions: u32,
    pub max_streams: u32,
    pub max_bandwidth_bps: u64,
    pub max_memory_bytes: u64,
    pub max_requests_per_sec: u32,
}
```

All limits are **inclusive** (≤).

### 3.2 Limit Construction

Limits can be constructed three ways:

1. **From tier defaults**:
   ```rust
   let limit = QuotaLimit::for_tier(TenantTier::Pro);
   ```

2. **From explicit values**:
   ```rust
   let limit = QuotaLimit::new(1000, 10000, 100_000_000, 1_000_000_000, 1000);
   ```

3. **Unlimited** (all hard caps):
   ```rust
   let limit = QuotaLimit::unlimited();
   ```

### 3.3 Limit Merging

Limits can be merged (min of each field):

```rust
let merged = limit_a.merge(limit_b);
```

This allows combining system-wide limits with tenant-specific overrides.

## Part 4: Usage Tracking

### 4.1 Atomic Counters

`QuotaUsage` tracks resources with `AtomicU64` counters:

```rust
pub struct QuotaUsage {
    pub sessions_used: Arc<AtomicU64>,
    pub streams_used: Arc<AtomicU64>,
    pub bandwidth_used_bps: Arc<AtomicU64>,
    pub memory_used_bytes: Arc<AtomicU64>,
    pub requests_this_second: Arc<AtomicU64>,
}
```

All counters use `Relaxed` ordering for maximum throughput.

### 4.2 Recording Operations

Operations are recorded with simple atomic increments:

```rust
usage.record_session_add();      // +1 session
usage.record_session_remove();   // -1 session
usage.record_bandwidth(bytes);   // +bytes bandwidth
usage.record_request();          // +1 request
```

### 4.3 Request Counter Reset

The request counter is reset periodically:

```rust
usage.reset_request_counter(); // Call once per second
```

This enables rate limiting (requests per second).

### 4.4 Usage Snapshots

A point-in-time snapshot can be taken:

```rust
let snap = usage.snapshot();
```

Snapshots are `Copy + Clone + Debug + PartialEq + Eq` for easy comparison.

### 4.5 Quota Checking

Usage is checked against limits:

```rust
usage.check_against(&limit)?;
```

Returns `Err(QuotaError::LimitExceeded)` if any dimension exceeds its limit.

## Part 5: Quota Enforcement (Phase 1)

### 5.1 QuotaManager

In Phase 1, `QuotaManager` will provide:

```rust
let mut manager = QuotaManager::new(config, stats);

// Register a tenant
manager.register_tenant(tenant_id, TenantTier::Pro)?;

// Check quota before operation
manager.check(tenant_id, &limit)?;

// Reserve resources
manager.reserve(tenant_id, &limit)?;

// Release resources
manager.release(tenant_id)?;
```

### 5.2 QuotaEnforcer

In Phase 1, `QuotaEnforcer` will provide:

```rust
let mut enforcer = QuotaEnforcer::new(config, stats);

// Enforce session quota
enforcer.enforce_session(tenant_id)?;

// Enforce bandwidth quota
enforcer.enforce_bandwidth(tenant_id, bytes)?;
```

## Part 6: Statistics and Health

### 6.1 ResourceQuotaStats

Atomic counters track all enforcement operations:

```rust
pub struct ResourceQuotaStats {
    pub checks_total: AtomicU64,
    pub checks_passed: AtomicU64,
    pub checks_exceeded: AtomicU64,
    pub tenants_tracked: AtomicU64,
    pub sessions_tracked: AtomicU64,
    pub bandwidth_total_bps: AtomicU64,
}
```

### 6.2 StatsSnapshot

A point-in-time snapshot with precomputed rates:

```rust
impl StatsSnapshot {
    pub fn exceed_rate_bps(&self) -> u16 {
        // Precomputed as basis points (0..=10_000)
    }
}
```

### 6.3 Health Classification

Health is tri-state:

| State | Exceed Rate | Action |
|-------|-------------|--------|
| Healthy | ≤ 1% (100 bps) | None |
| Degraded | > 1% and ≤ 10% | Investigate |
| Critical | > 10% | Page on-call |

```rust
let report = HealthReport::derive(&stats_snapshot);
if report.needs_attention() {
    // Alert operator
}
```

## Part 7: Configuration

### 7.1 ResourceQuotasConfig

```rust
pub struct ResourceQuotasConfig {
    pub max_tenants: u32,
    pub enforcement_enabled: bool,
    pub default_max_sessions: u32,
    pub default_max_streams: u32,
    pub default_max_bandwidth_bps: u64,
    pub check_interval_ms: u64,
}
```

### 7.2 Presets

Three deployment archetypes:

| Preset | Session Limit | Stream Limit | Bandwidth | Use Case |
|--------|---------------|---------------|-----------|----------|
| Permissive | 100K | 1M | 10 Gbps | Development, testing |
| Standard | 10K | 100K | 1 Gbps | Production, balanced |
| Strict | 1K | 10K | 100 Mbps | Production, conservative |

```rust
let cfg = Preset::Standard.to_config();
```

### 7.3 Builder Pattern

Fine-tune configuration:

```rust
let cfg = ResourceQuotasBuilder::new()
    .max_tenants(50_000)
    .enforcement(true)
    .default_max_sessions(20_000)
    .build()?;
```

## Part 8: Error Handling

### 8.1 QuotaError

```rust
pub enum QuotaError {
    LimitExceeded(&'static str),
    TenantNotFound(TenantId),
    InvalidQuota(&'static str),
    DuplicateTenant(TenantId),
    ShuttingDown,
}
```

### 8.2 Error Classification

Errors are categorized by predicates:

- **Fatal** (`is_fatal`): InvalidQuota, ShuttingDown
- **Retryable** (`is_retryable`): LimitExceeded
- **Capacity** (`is_capacity`): LimitExceeded

## Part 9: Testing Strategy

### 9.1 Unit Tests

Every module has comprehensive unit tests:

- Constructor validation
- Predicate logic
- Error classification
- Atomic counter behavior
- Health threshold boundaries

### 9.2 Property Tests

Critical invariants are tested:

- Limit merging takes minimum of each field
- Usage checking fails when limits exceeded
- Tier defaults are within valid ranges
- Health classification at boundary values

### 9.3 Benchmarks

Performance-critical operations are benchmarked:

- Quota limit construction
- Usage recording
- Quota checking
- Stats snapshot
- Health derivation

## Part 10: Phase 0 Scope

### 10.1 Implemented (Real)

Phase 0 provides real implementations for:

1. **Error types** (`error.rs`): Complete error enum with predicates
2. **Configuration** (`config.rs`): Full config builder with presets
3. **Tenant types** (`tenant.rs`): TenantId, TenantTier with defaults
4. **Quota limits** (`limit.rs`): QuotaLimit with merge operation
5. **Usage tracking** (`usage.rs`): Atomic counters, snapshots, checking
6. **Statistics** (`stats.rs`): Atomic counters and snapshots
7. **Health** (`health.rs`): Health derivation from stats
8. **Builder** (`builder.rs`): Fluent configuration builder

### 10.2 Skeletons (Type Stubs)

Phase 1 modules ship as typed skeletons:

1. **QuotaManager** (`manager.rs`): Returns ShuttingDown or TenantNotFound
2. **QuotaEnforcer** (`enforcer.rs`): Returns ShuttingDown

These provide complete type signatures for downstream consumers
while deferring implementation to Phase 1.

## Part 11: Failure Modes

### 11.1 Limit Exceeded

Limit exceeded errors are:

- **Retryable**: The tenant may retry after reducing usage
- **Capacity**: Signals load-shedding is needed

Callers should:

1. Back off temporarily
2. Reduce resource consumption
3. Retry with lower usage

### 11.2 Tenant Not Found

Tenant not found errors indicate:

- Programming error (unregistered tenant)
- Race condition (tenant deregistered concurrently)

These are **not retryable** without fixing the underlying issue.

### 11.3 Resource Exhaustion

System-wide resource exhaustion is handled via:

- `max_tenants`: Prevents unbounded tenant registration
- Tier limits: Prevents any single tenant from consuming all resources

## Part 12: Observability

### 12.1 Metrics Exported

The system exports these metrics in Phase 2:

| Metric Name | Type | Description |
|-------------|------|-------------|
| `quota_checks_total` | counter | Total quota checks performed |
| `quota_checks_passed_total` | counter | Checks that passed |
| `quota_checks_exceeded_total` | counter | Checks that exceeded limits |
| `quota_tenants_tracked` | gauge | Tenants currently tracked |
| `quota_sessions_tracked` | gauge | Sessions tracked across all tenants |
| `quota_bandwidth_total_bps` | gauge | Total bandwidth across all tenants |
| `quota_exceed_rate` | gauge | Quota exceed rate (bps) |

### 12.2 Health Signals

Health is derived from:

1. Exceed rate (primary signal)
2. Tenant count (secondary signal)
3. Total bandwidth (secondary signal)

## Part 13: Configuration Management

### 13.1 Default Configuration

The default config is production-safe:

```rust
pub const fn new() -> ResourceQuotasConfig {
    ResourceQuotasConfig {
        max_tenants: 10_000,
        enforcement_enabled: true,
        default_max_sessions: 10_000,
        default_max_streams: 100_000,
        default_max_bandwidth_bps: 1_000_000_000, // 1 Gbps
        check_interval_ms: 5_000,
    }
}
```

### 13.2 Configuration Validation

All configs are validated before use:

```rust
pub fn validate(&self) -> Result<(), QuotaError> {
    // Check ranges
    // Return InvalidQuota for violations
}
```

### 13.3 Enforcement Control

Enforcement can be disabled globally:

```rust
let cfg = ResourceQuotasConfig::new()
    .with_enforcement(false);
```

When disabled, all checks pass but usage is still tracked.

## Part 14: Performance Considerations

### 14.1 Atomic Ordering

All usage counters use `Relaxed` ordering:

- Usage tracking is advisory for rate limiting
- Relaxed provides maximum throughput
- Momentary inconsistency is acceptable

### 14.2 Arc Sharing

`QuotaUsage` uses `Arc<AtomicU64>` for:

- Cheap cloning (no deep copy)
- Shared state across threads
- Efficient snapshot operations

### 14.3 Const Evaluation

Key operations work in const contexts:

```rust
const LIMIT: QuotaLimit = QuotaLimit::for_tier(TenantTier::Pro);
assert!(LIMIT.max_sessions > 0); // Evaluated at compile time
```

## Part 15: Security Considerations

### 15.1 Tenant Isolation

Tenant isolation prevents:

- One tenant from affecting another's quotas
- Noisy neighbor problems
- Resource exhaustion attacks

### 15.2 Rate Limiting

Request rate limiting prevents:

- DoS attacks via excessive requests
- Backend overload
- Unfair resource consumption

### 15.3 Bandwidth Limits

Bandwidth limits prevent:

- Network saturation
- Cost overruns
- Fairness issues

## Part 16: Future Enhancements

### 16.1 Phase 1 Features

Phase 1 will add:

- Functional QuotaManager with tenant storage
- Functional QuotaEnforcer with real-time checks
- Dynamic quota adjustment
- Tenant migration support

### 16.2 Phase 2 Features

Phase 2 will add:

- Prometheus export for quota metrics
- Dynamic configuration reloading
- Quota usage forecasting
- Alert rule evaluation

### 16.3 Phase 3+ Features

Future phases may add:

- Custom quota types
- Quota borrowing/lending between tenants
- Time-based quota windows (peak/off-peak)
- Multi-region quota coordination

## Part 17: Integration Points

### 17.1 Session Manager Integration

```rust
use session_manager::SessionId;

// Check session quota before creating session
quota_manager.check(tenant_id, &session_limit)?;
quota_manager.reserve(tenant_id, &session_limit)?;
```

### 17.2 Connection Shard Integration

```rust
// Track bandwidth per tenant
quota_enforcer.enforce_bandwidth(tenant_id, bytes_sent)?;
```

### 17.3 Stream Manager Integration

```rust
// Check stream quota before creating stream
quota_manager.check(tenant_id, &stream_limit)?;
```

## Part 18: Operational Guidance

### 18.1 Deployment Recommendations

For production:

1. Use `Preset::Standard` for balanced quotas
2. Set `enforcement_enabled = true` for active enforcement
3. Monitor `exceed_rate` for quota pressure
4. Set alerts on `HealthState::Degraded`

### 18.2 Debugging Production Issues

When investigating issues:

1. Check `exceed_rate` for quota pressure
2. Review `tenants_tracked` for tenant count
3. Examine `bandwidth_total_bps` for bandwidth usage
4. Correlate TenantId across services

### 18.3 Capacity Planning

Plan capacity based on:

- Tenants: ~10K default
- Sessions per tenant: ~10K default
- Bandwidth per tenant: ~1 Gbps default
- Total bandwidth: ~10 Tbps for 10K tenants

## Part 19: Migration Path

### 19.1 From No Quotas

Migrate incrementally:

1. Enable quotas with `Preset::Permissive`
2. Monitor `exceed_rate` for pressure
3. Tighten limits to `Preset::Standard`
4. Enable enforcement

### 19.2 From Legacy Systems

Migrate by:

1. Run legacy and new quota systems in parallel
2. Compare enforcement for validation
3. Gradually shift traffic
4. Deprecate legacy enforcement

### 19.3 Configuration Migration

Convert legacy configs:

```rust
// Legacy: max_sessions = 5000
// New:
let cfg = ResourceQuotasBuilder::new()
    .default_max_sessions(5000)
    .build()?;
```

## Part 20: Appendix

### 20.1 Glossary

- **Basis Points (bps)**: 1/100 of a percent (0..=10_000)
- **Tenant**: An isolated quota domain
- **Tier**: A tenant class with default limits
- **Quota**: A resource limit for a tenant
- **Enforcement**: Real-time limit checking

### 20.2 References

- Rate Limiting: https://stripe.com/blog/rate-limiting-and-usage-throttling
- Multi-tenant Architecture: https://微软.com/en-us/engineering/experience-2015/building-multitenant-services/
- Resource Isolation: https://aws.amazon.com/blogs/compute/isolation-patterns-for-multi-tenant-architectures/

### 20.3 Version History

- v0.1.0 (Phase 0): Core types, configuration, usage tracking, stats, health
- v0.2.0 (Phase 1): Functional manager and enforcer
- v0.3.0 (Phase 2): Prometheus export, dynamic configuration

---

*This design document is part of the Vireon QUIC runtime project. For the latest version, see the project repository.*
