# Feature Flags System - DESIGN.md

## Table of Contents
1. Overview
2. Architecture
3. Design Goals
4. Core Components
5. Type System
6. Rule Evaluation
7. Configuration
8. Statistics and Observability
9. Health Monitoring
10. Error Handling
11. Performance Characteristics
12. Testing Strategy
13. Future Extensions
14. Appendix: Design Rationale

## 1. Overview

The Feature Flags system provides a robust, type-safe mechanism for runtime feature toggling in the Vireon QUIC runtime. It enables dynamic control over system behavior without code deployment, supporting A/B testing, gradual rollouts, and emergency kill switches.

### 1.1 Scope

The feature flags system operates at the application layer, providing:
- Typed flag keys with compile-time safety
- Multiple value types (Boolean, Percentage, String)
- Rule-based evaluation with tenant and user context
- Evaluation caching with configurable TTL
- Comprehensive observability and health monitoring
- Thread-safe, lock-free operations using atomics

### 1.2 Non-Goals

The following are explicitly out of scope for Phase 0:
- Distributed flag synchronization across nodes
- Remote flag management APIs
- Audit logging and change history
- Flag dependency resolution
- Time-based flag scheduling
- Multi-region flag consistency

These may be added in Phase 1+ based on operational requirements.

## 2. Architecture

### 2.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Application Layer                        │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐    ┌──────────────┐                      │
│  │ Application  │───►│Feature Flag  │                      │
│  │    Code      │    │  Manager     │                      │
│  └──────────────┘    └──────┬───────┘                      │
│                              │                                │
│                     ┌────────▼─────────┐                     │
│                     │  Flag Registry   │                     │
│                     │  (Flags + Rules) │                     │
│                     └────────┬─────────┘                     │
│                              │                                │
│               ┌──────────────┴──────────────┐               │
│               │                             │               │
│      ┌────────▼────────┐          ┌────────▼───────┐        │
│      │ Evaluation Cache│          │  Rule Engine   │        │
│      │   (TTL-based)   │          │ (Context-aware)│        │
│      └─────────────────┘          └────────────────┘        │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### 2.2 Data Flow

```
Request → FeatureFlagManager.evaluate(key, context)
         ↓
    Check Cache (hit?)
         │
         ├─ Yes → Return cached value
         │
         └─ No → Evaluate Rules
                     ↓
                Apply Conditions
                     ↓
                Return Result + Update Cache
                     ↓
                Record Statistics
```

### 2.3 Component Relationships

```
FeatureFlagManager
├── FeatureFlagsConfig (static configuration)
├── FeatureFlagsStats (atomic counters)
├── FlagRegistry (flag storage) ← Phase 1+
├── EvaluationCache (LRU cache) ← Phase 1+
└── RuleEngine (condition matching)

FlagProvider (trait)
├── StaticProvider (file/config-based)
├── DatabaseProvider ← Phase 1+
└── RemoteProvider ← Phase 2+
```

## 3. Design Goals

### 3.1 Type Safety

**Goal**: Prevent type errors at compile time rather than runtime.

**Implementation**:
- Flag keys are strongly-typed newtypes with validation
- Flag values use Rust's type system (enum variants)
- Context parameters use explicit types (u64 for IDs)

**Rationale**: Feature flags are often used in critical paths. Type errors (e.g., treating a percentage as a boolean) can cause subtle bugs that are hard to diagnose.

### 3.2 Performance

**Goal**: Minimize overhead of flag evaluation.

**Targets**:
- Sub-microsecond evaluation for cached flags
- Sub-10-microsecond evaluation for uncached flags
- Zero allocation on hot path (stack-only evaluation)
- Lock-free reads using atomic operations

**Implementation**:
- Evaluation cache with configurable TTL
- Stack-based evaluation (no heap allocation)
- Atomic statistics (Relaxed ordering)
- Copy-on-write flag states

### 3.3 Observability

**Goal**: Enable operators to understand flag behavior and system health.

**Metrics**:
- Total evaluations (enabled/disabled breakdown)
- Cache hit/miss rates
- Rule match/miss rates
- Per-flag statistics (Phase 1+)

**Health Signals**:
- Cache effectiveness (hit rate)
- Evaluation latency (Phase 1+)
- Rule error rates
- System load (evaluation rate)

### 3.4 Usability

**Goal**: Make feature flags easy to use correctly.

**Features**:
- Fluent configuration builder
- Sensible presets for common patterns
- Clear error messages
- Comprehensive documentation
- Example code in docs

## 4. Core Components

### 4.1 FlagKey

**Purpose**: Type-safe identifier for feature flags.

**Representation**:
```rust
pub struct FlagKey {
    bytes: [u8; 64],  // Fixed-size buffer
    len: u8,          // Actual length (max 63)
}
```

**Invariants**:
- Maximum 63 bytes length
- ASCII-only characters
- No null bytes
- Case-sensitive

**Design Decisions**:
- Fixed-size buffer enables Copy trait and stack allocation
- Length prefix enables efficient comparison
- ASCII restriction enables UTF-8 guarantees and fast hashing

**Future Considerations**:
- Unicode support (requires dynamic sizing)
- Hierarchical naming (namespaces)
- Flag key aliases

### 4.2 FlagValue

**Purpose**: Typed values for feature flags.

**Variants**:
```rust
pub enum FlagValue {
    Bool(bool),                    // On/off flags
    Percentage(u8),                // Rollout percentages (0..=100)
    Str([u8; 128]),               // String values (up to 127 bytes)
}
```

**Design Decisions**:
- Small variants enable efficient copying
- Percentage stored as u8 (sufficient granularity)
- String values use fixed-size buffer for stack allocation

**Trade-offs**:
- No integer or float variants (can be modeled as strings)
- String length limit (127 bytes) simplifies memory management
- No complex types (enums, structs) - keep it simple

### 4.3 FlagState

**Purpose**: Complete state of a feature flag.

**Fields**:
```rust
pub struct FlagState {
    pub enabled: bool,      // Current enabled status
    pub value: FlagValue,   // Flag value
    pub frozen: bool,       // Whether modifications are allowed
    pub updated_ns: u64,    // Last update timestamp
}
```

**Lifecycle**:
1. Created with initial state
2. Updated via manager (unless frozen)
3. Frozen for critical flags (prevent accidental changes)
4. Evicted from cache on TTL expiry

**Frozen Semantics**:
- Frozen flags cannot be modified via standard APIs
- Freezing is one-way (no unfreeze without restart)
- Critical production flags should be frozen

### 4.4 FlagRule

**Purpose**: Define when flags should be enabled based on context.

**Structure**:
```rust
pub struct FlagRule {
    pub key: FlagKey,
    pub condition: RuleCondition,
    pub value: FlagValue,
}
```

**Conditions**:
```rust
pub enum RuleCondition {
    Always,                        // Always matches
    Never,                         // Never matches
    Percentage(u8),                // Match percentage of users
    TenantIn([u64; 8], u8),       // Match specific tenants
}
```

**Evaluation Logic**:
- `Always`: Returns true (override value)
- `Never`: Returns false (override value)
- `Percentage`: Consistent hash of (tenant_id, user_id, key) % 100 < percentage
- `TenantIn`: Exact match on tenant_id in whitelist

**Consistent Hashing**:
```rust
hash(tenant_id, user_id, flag_key) % 100 < percentage
```

This ensures:
- Same user gets same result across requests
- Deterministic distribution
- Stable across cache invalidations

## 5. Type System

### 5.1 Type Hierarchy

```
FlagKey (Copy)
├── bytes: [u8; 64]
└── len: u8

FlagValue (Copy)
├── Bool(bool)
├── Percentage(u8)
└── Str([u8; 128])

FlagState (Copy)
├── enabled: bool
├── value: FlagValue
├── frozen: bool
└── updated_ns: u64

FlagRule (Copy)
├── key: FlagKey
├── condition: RuleCondition
└── value: FlagValue
```

### 5.2 Const Safety

All types implement `Copy` and are usable in const contexts:
- Construction is infallible (except FlagKey::new which validates)
- Comparison uses `raw()` for const fn compatibility
- No dynamic dispatch or trait objects

### 5.3 Type Conversions

**Safe Conversions**:
- `FlagValue::as_bool()` → Option<bool> (None if not bool)
- `FlagValue::as_percentage()` → Option<u8> (None if not percentage)
- `FlagValue::as_str()` → Option<&str> (None if not string)

**Unsafe Conversions**:
- Direct variant access via pattern matching
- Requires explicit handling of all variants

## 6. Rule Evaluation

### 6.1 Evaluation Context

```rust
pub struct EvaluationContext {
    pub tenant_id: u64,      // Tenant identifier
    pub user_id: u64,        // User within tenant
    pub metadata_hash: u64,  // Additional context (Phase 1+)
    pub now_ns: u64,         // Current time (for cache invalidation)
}
```

### 6.2 Evaluation Algorithm

```
1. Check cache for (key, context) → hit?
   └─ Yes: Return cached value

2. Fetch flag state from provider
   └─ Error: Return default value

3. Evaluate rules in priority order
   a. Check Always/Never conditions
   b. Check Percentage condition (consistent hash)
   c. Check TenantIn condition (exact match)

4. Cache result with TTL

5. Record statistics
```

### 6.3 Consistent Hashing

**Formula**:
```rust
let key_hash = simple_hash(flag_key.as_str());
let combined = tenant_id ^ user_id ^ key_hash;
let hash_value = combined % 100;
hash_value < percentage
```

**Properties**:
- Deterministic: Same inputs always produce same output
- Uniform distribution: XOR hash provides good mixing
- Stable: Small changes in input cause large changes in output

**Phase 1+ Improvements**:
- Replace `simple_hash` with FNV-1a or similar
- Add more context fields (session_id, region, etc.)
- Support custom hash functions per flag

## 7. Configuration

### 7.1 Configuration Fields

```rust
pub struct FeatureFlagsConfig {
    pub max_flags: u32,                 // Maximum flags in registry
    pub evaluation_cache_ttl_ms: u64,   // Cache TTL
    pub enable_rules: bool,             // Enable rule evaluation
    pub default_offline_value: bool,    // Default when provider unavailable
}
```

### 7.2 Validation Rules

**max_flags**:
- Minimum: 10 (even small deployments need multiple flags)
- Maximum: 1,000,000 (hard limit to prevent memory exhaustion)

**evaluation_cache_ttl_ms**:
- Minimum: 1,000ms (1 second - avoids excessive cache misses)
- Maximum: 3,600,000ms (1 hour - ensures eventual consistency)

**enable_rules**:
- No validation (boolean flag)
- Default: true (rules are primary feature)

**default_offline_value**:
- No validation (boolean flag)
- Default: false (conservative default)

### 7.3 Presets

**Default**: Balanced for production
```toml
max_flags = 10_000
evaluation_cache_ttl_ms = 60_000
enable_rules = true
default_offline_value = false
```

**HighPerformance**: For high-throughput systems
```toml
max_flags = 50_000
evaluation_cache_ttl_ms = 30_000
enable_rules = true
default_offline_value = false
```

**Minimal**: For small deployments
```toml
max_flags = 1_000
evaluation_cache_ttl_ms = 120_000
enable_rules = false
default_offline_value = true
```

## 8. Statistics and Observability

### 8.1 Counter Categories

**Evaluation Counters**:
- `evaluations_total`: Total evaluations performed
- `evaluations_on`: Evaluations returning enabled
- `evaluations_off`: Evaluations returning disabled

**Cache Counters**:
- `cache_hits`: Cache hits (avoided rule evaluation)
- `cache_misses`: Cache misses (required rule evaluation)

**Rule Counters**:
- `rule_matches`: Rules that matched their condition
- `rule_misses`: Rules that did not match

### 8.2 Derived Metrics

**Enabled Rate**:
```
enabled_rate_bps = (evaluations_on * 10_000) / evaluations_total
```
- Units: Basis points (1/100 of a percent)
- Range: 0..=10_000 (0%..=100%)
- Default: 0 when no evaluations

**Cache Hit Rate**:
```
cache_hit_rate_bps = (cache_hits * 10_000) / (cache_hits + cache_misses)
```
- Units: Basis points
- Range: 0..=10_000
- Default: 10_000 (100%) when empty

### 8.3 Snapshot Consistency

Snapshots use `Relaxed` atomic ordering:
- Individual counters may be momentarily inconsistent
- Acceptable for advisory statistics
- Avoids synchronization overhead

**Phase 1+ Enhancement**:
- Add seqlock for consistent snapshots
- Support per-flag statistics
- Add histogram for evaluation latency

## 9. Health Monitoring

### 9.1 Health States

**Healthy**: All systems nominal
- Cache hit rate ≥ 95% (9,500 bps)
- No active alerts

**Degraded**: Performance impact
- Cache hit rate < 95% but ≥ 80% (8,000 bps)
- Investigate during business hours

**Critical**: Severe impact
- Cache hit rate < 80% (8,000 bps)
- Page on-call immediately

### 9.2 Health Derivation

```rust
state = worst_of([
    classify_cache_hit_rate(cache_hit_rate_bps),
    classify_evaluations(evaluations_total)
])
```

**Classification Logic**:
- Cache hit rate is primary signal
- Evaluation count ensures system is active
- Worst signal wins (critical > degraded > healthy)

### 9.3 Threshold Constants

```rust
pub const CACHE_HEALTHY_BPS: u16 = 9_500;   // 95%
pub const CACHE_DEGRADED_BPS: u16 = 8_000;  // 80%
```

**Rationale**:
- 95% is reasonable for properly-sized cache
- 80% indicates cache is too small or TTL too short
- Below 80% suggests misconfiguration

## 10. Error Handling

### 10.1 Error Variants

**NotFound**: Flag not found in registry
- Action: Create flag or check spelling
- Category: Recoverable
- Fatal: No
- Retryable: No

**InvalidValue**: Value validation failed
- Action: Fix input (length, encoding, range)
- Category: Programming error
- Fatal: No
- Retryable: No

**EvalError**: Rule evaluation failed
- Action: Check context and rule configuration
- Category: Transient error
- Fatal: No
- Retryable: Yes

**Frozen**: Flag is frozen and cannot be modified
- Action: Use unfrozen API or restart to unfreeze
- Category: Lifecycle state
- Fatal: Yes
- Retryable: No

**ShuttingDown**: System is shutting down
- Action: Cease operations, graceful shutdown
- Category: Lifecycle state
- Fatal: Yes
- Retryable: No

### 10.2 Error Predicates

**is_fatal()**: Should caller abort?
- Returns true for Frozen, ShuttingDown
- These errors won't change with retries

**is_retryable()**: Should caller retry?
- Returns true for EvalError
- Transient errors may resolve with retry

### 10.3 Error Messages

All errors implement `Display` with human-readable messages:
```rust
assert!(format!("{}", FlagError::NotFound("my_flag"))
    .contains("flag not found"));
```

## 11. Performance Characteristics

### 11.1 Performance Targets

| Operation | Target | Notes |
|-----------|--------|-------|
| Flag creation | < 100ns | Stack allocation |
| Flag comparison | < 10ns | Memory compare |
| Rule evaluation (cached) | < 500ns | Cache lookup |
| Rule evaluation (uncached) | < 10µs | Hash + condition check |
| Stats update | < 100ns | Atomic fetch_add |

### 11.2 Memory Usage

**Per-flag memory**:
- FlagKey: 64 bytes (fixed buffer)
- FlagValue: 128 bytes (max for string variant)
- FlagState: ~200 bytes total
- Rule overhead: ~80 bytes

**Cache memory**:
- Unbounded in Phase 0 (leaked memory)
- Phase 1+ will add LRU eviction

**Statistics memory**:
- Fixed: 7 counters × 8 bytes = 56 bytes

### 11.3 Allocation Behavior

**Zero-allocation hot path**:
- Flag evaluation uses only stack memory
- No heap allocation during evaluation
- Cache key is stack-allocated tuple

**Allocation points**:
- Flag creation (stack allocation, no malloc)
- Rule registration (heap allocation for storage)
- Cache insert (heap allocation in Phase 1+)

## 12. Testing Strategy

### 12.1 Unit Tests

**Type Tests**:
- FlagKey creation (valid/invalid)
- FlagValue creation (valid/invalid)
- FlagState accessor methods
- Type conversion methods

**Rule Tests**:
- Always/Never conditions
- Percentage calculation (edge cases)
- TenantIn matching (hit/miss)
- Deterministic evaluation

**Config Tests**:
- Validation (boundary values)
- Preset application
- Builder pattern
- Default values

**Stats Tests**:
- Counter increments
- Snapshot consistency
- Rate calculations
- Edge cases (zero values)

**Health Tests**:
- Classification logic
- Threshold boundaries
- "Worst signal wins" policy

### 12.2 Integration Tests

**Phase 1+**:
- Manager lifecycle
- Cache behavior
- Provider integration
- Concurrency stress tests

### 12.3 Property Tests

**Phase 1+**:
- Consistent hashing properties (using proptest)
- Cache coherency
- Flag state transitions

## 13. Future Extensions

### 13.1 Phase 1 Features

**Dynamic Provider**:
- Database-backed flag storage
- Remote API integration
- File-based configuration

**Cache Improvements**:
- LRU eviction policy
- Cache size limits
- Per-flag TTL

**Rule Engine Enhancements**:
- User whitelists
- Region-based targeting
- Time-based conditions
- Priority ordering

### 13.2 Phase 2 Features

**Distributed Synchronization**:
- Cross-node flag propagation
- Conflict resolution
- Eventual consistency guarantees

**Advanced Features**:
- Flag dependencies (A requires B)
- Time-based scheduling
- Gradual rollout automation
- A/B test integration

### 13.3 Observability Enhancements

**Per-flag Statistics**:
- Evaluation count per flag
- Enable rate per flag
- Cache hit rate per flag

**Auditing**:
- Change history
- Who changed what when
- Rollback capability

## 14. Appendix: Design Rationale

### 14.1 Why Fixed-Size Buffers?

**Decision**: Use fixed-size buffers for FlagKey and FlagValue::Str

**Rationale**:
1. **Performance**: Stack allocation avoids heap overhead
2. **Simplicity**: No need for lifetime management
3. **Copy**: Enables Copy trait, reducing cognitive load
4. **Memory**: Predictable memory usage per flag

**Trade-offs**:
- Maximum length limits (63 for keys, 127 for strings)
- Memory waste for short values
- No dynamic sizing

**Alternative Considered**: `String` or `&str`
- Rejected: Requires lifetime management
- Rejected: Heap allocation on every evaluation

### 14.2 Why Consistent Hashing?

**Decision**: Use `(tenant_id ^ user_id ^ key_hash) % 100` for percentage rollouts

**Rationale**:
1. **Deterministic**: Same user always gets same result
2. **Stable**: Not affected by cache invalidation
3. **Fair**: XOR provides uniform distribution
4. **Simple**: No external dependencies

**Trade-offs**:
- Simple hash function (not cryptographic)
- Limited granularity (1% steps)
- No user cohort tracking

**Alternative Considered**: Random selection
- Rejected: Non-deterministic across requests
- Rejected: Difficult to reason about rollout progress

### 14.3 Why Atomic Relaxed Ordering?

**Decision**: Use `Ordering::Relaxed` for statistics

**Rationale**:
1. **Performance**: Relaxed is fastest
2. **Sufficient**: Statistics are advisory, not load-bearing
3. **Correctness**: Counters are monotonic (no tearing)

**Trade-offs**:
- Momentary inconsistency across counters
- Possible lost updates on extreme contention

**Alternative Considered**: `Ordering::SeqCst`
- Rejected: Too expensive for hot path
- Rejected: Unnecessary for advisory counters

### 14.4 Why Skeleton Architecture?

**Decision**: Phase 0 ships skeleton facades for manager and provider

**Rationale**:
1. **API Stability**: Define interfaces early
2. **Incremental Development**: Enable parallel work
3. **Testing**: Test dependent code against contract
4. **Documentation**: Complete API documentation

**Trade-offs**:
- Stub implementations (no real functionality)
- Need to replace in Phase 1
- May discover API flaws later

**Alternative Considered**: Defer to Phase 1
- Rejected: Blocks dependent work
- Rejected: Slower overall development

### 14.5 Why Basis Points for Rates?

**Decision**: Use basis points (1/100 of a percent) for rate metrics

**Rationale**:
1. **Precision**: Integer arithmetic, no floating point
2. **Range**: 0..=10_000 covers 0%..=100%
3. **Standard**: Financial industry standard
4. **Efficiency**: No float conversion at export time

**Trade-offs**:
- Less intuitive than percentages
- Need conversion for display

**Alternative Considered**: Float percentages
- Rejected: Floating point precision issues
- Rejected: More expensive computation

---

**Document Version**: 0.1.0  
**Last Updated**: 2026-07-07  
**Status**: Phase 0 Implementation  
**Next Review**: Phase 1 Planning

## 15. Implementation Details

### 15.1 FlagKey Implementation

**Construction Validation**:
```rust
pub fn new(s: &str) -> Result<Self, FlagError> {
    if s.len() > Self::MAX_LEN as usize {
        return Err(FlagError::InvalidValue(...));
    }
    if !s.is_ascii() {
        return Err(FlagError::InvalidValue(...));
    }
    // Copy into fixed-size buffer
    let mut bytes = [0u8; Self::LEN];
    bytes[..s.len()].copy_from_slice(s.as_bytes());
    Ok(Self { bytes, len: s.len() as u8 })
}
```

**Comparison Optimization**:
```rust
pub const fn eq(&self, other: &Self) -> bool {
    if self.len != other.len {
        return false;
    }
    // Compare only the used bytes
    let mut i = 0;
    while i < self.len as usize {
        if self.bytes[i] != other.bytes[i] {
            return false;
        }
        i += 1;
    }
    true
}
```

### 15.2 Rule Matching Implementation

**Percentage Condition**:
```rust
RuleCondition::Percentage(pct) => {
    let key_hash = simple_hash(self.key.as_str());
    let combined = ctx.tenant_id ^ ctx.user_id ^ key_hash;
    (combined % 100) < pct as u64
}
```

**TenantIn Condition**:
```rust
RuleCondition::TenantIn(tenants, len) => {
    let mut i = 0;
    while i < len as usize {
        if tenants[i] == ctx.tenant_id {
            return true;
        }
        i += 1;
    }
    false
}
```

### 15.3 Statistics Implementation

**Record Evaluation**:
```rust
pub fn record_evaluation(&self, enabled: bool) {
    self.evaluations_total.fetch_add(1, Ordering::Relaxed);
    if enabled {
        self.evaluations_on.fetch_add(1, Ordering::Relaxed);
    } else {
        self.evaluations_off.fetch_add(1, Ordering::Relaxed);
    }
}
```

**Snapshot with Relaxed Ordering**:
```rust
pub fn snapshot(&self) -> StatsSnapshot {
    StatsSnapshot {
        evaluations_total: self.evaluations_total.load(Ordering::Relaxed),
        evaluations_on: self.evaluations_on.load(Ordering::Relaxed),
        // ... other fields
    }
}
```

## 16. Concurrency Model

### 16.1 Thread Safety

**Read Operations** (evaluation):
- Lock-free: All reads use immutable references
- Atomic statistics: `fetch_add` with Relaxed ordering
- No synchronization overhead

**Write Operations** (flag updates):
- Exclusive access: Require `&mut self`
- No concurrent writes: Single writer model
- Phase 1+ will add RwLock for concurrent reads

### 16.2 Memory Ordering

**Why Relaxed?**
- Statistics are advisory, not load-bearing
- Counter updates are monotonic (no tearing on u64)
- Momentary inconsistency is acceptable

**When to Use Acquire/Release**:
- Phase 1+ cache synchronization
- Phase 1+ cross-thread flag propagation
- Never needed for counter increments

### 16.3 Deadlock Freedom

**Current Design** (Phase 0):
- No locks: All operations are single-threaded
- No deadlock possible

**Phase 1+ Considerations**:
- Use RwLock for flag registry
- Lock ordering: Registry → Cache → Stats
- Avoid lock nesting in hot path

## 17. Cache Design (Phase 1+)

### 17.1 Cache Key Structure

```rust
struct CacheKey {
    flag_key: FlagKey,
    tenant_id: u64,
    user_id: u64,
}
```

### 17.2 Cache Entry Structure

```rust
struct CacheEntry {
    value: FlagValue,
    expires_at_ns: u64,
    hit_count: u32,
}
```

### 17.3 Eviction Policy

**LRU with TTL**:
1. Check TTL first (expired entries evicted)
2. Track access order for LRU
3. Evict oldest when capacity exceeded

**Phase 1+ Implementation**:
- Use `HashMap` with `VecDeque` for access tracking
- Background cleanup task (every 60 seconds)
- Capacity limit from configuration

## 18. Provider Architecture

### 18.1 Provider Trait Design

```rust
pub trait FlagProvider: Send + Sync {
    fn get(&self, key: FlagKey) -> Result<FlagState, FlagError>;
}
```

**Design Decisions**:
- `Send + Sync`: Enables concurrent access
- `&self` (not `&mut self`): Enables shared references
- Returns `FlagState`: Complete state, not just value

### 18.2 Static Provider

**Purpose**: Load flags from static source (file, compile-time config)

**Implementation** (Phase 0 skeleton):
```rust
pub struct StaticProvider {
    cfg: FeatureFlagsConfig,
}

impl FlagProvider for StaticProvider {
    fn get(&self, _key: FlagKey) -> Result<FlagState, FlagError> {
        Err(FlagError::ShuttingDown)
    }
}
```

**Phase 1+ Implementation**:
- Load from TOML/YAML file
- In-memory `HashMap` storage
- Watch file changes (hot reload)

### 18.3 Future Provider Types

**Database Provider** (Phase 1+):
- PostgreSQL/MySQL backend
- Connection pooling
- Query optimization (batch lookups)

**Remote Provider** (Phase 2+):
- HTTP/gRPC API client
- Retry logic with exponential backoff
- Local caching fallback

## 19. Configuration Best Practices

### 19.1 Cache Sizing

**Rule of Thumb**:
```
cache_size = max_flags * 0.1  // 10% of total flags
```

**Rationale**:
- Most flags are evaluated repeatedly
- 10% cache hit rate is acceptable minimum
- Memory overhead is manageable

### 19.2 TTL Selection

**Short TTL (1-10 seconds)**:
- Fast-changing flags (kill switches)
- Low traffic (cache pressure minimal)
- Testing/debugging scenarios

**Medium TTL (30-60 seconds)**:
- Gradual rollouts
- A/B testing
- Default recommendation

**Long TTL (5-60 minutes)**:
- Stable flags (feature toggles)
- High traffic (reduce cache misses)
- Resource-constrained environments

### 19.3 Max Flags Planning

**Development**: 1,000 flags
- Ample room for experimentation
- Low memory overhead

**Staging**: 10,000 flags
- Mirrors production complexity
- Enables full testing

**Production**: 50,000+ flags
- Multiple teams (10-100 engineers)
- Per-service flags
- A/B tests and experiments

## 20. Monitoring and Alerts

### 20.1 Key Metrics to Monitor

**Cache Effectiveness**:
- Cache hit rate < 90%: Alert (degraded performance)
- Cache hit rate < 70%: Critical (misconfiguration)

**Evaluation Rate**:
- Spikes in evaluations: Potential flag abuse
- Zero evaluations: Potential outage

**Rule Performance**:
- High rule miss rate: Rules not matching
- Rule evaluation errors: Configuration issues

### 20.2 Dashboard Queries

**Prometheus Examples**:
```promql
# Cache hit rate over time
rate(feature_flags_cache_hits_total[5m]) /
(rate(feature_flags_cache_hits_total[5m]) + rate(feature_flags_cache_misses_total[5m]))

# Enabled percentage
rate(feature_flags_evaluations_on_total[5m]) /
rate(feature_flags_evaluations_total[5m])
```

### 20.3 Alert Recommendations

**Warning Alerts** (investigate within 24 hours):
- Cache hit rate drops below 90%
- Evaluation rate increases by 2x
- Rule errors > 0.1% of evaluations

**Critical Alerts** (page immediately):
- Cache hit rate drops below 70%
- All evaluations returning errors
- Provider unavailable for > 60 seconds

## 21. Security Considerations

### 21.1 Flag Key Validation

**Input Sanitization**:
- ASCII-only check prevents Unicode attacks
- Length limit prevents buffer overflow
- Null byte check prevents string truncation

### 21.2 Context Validation

**Tenant/User ID Validation**:
- Phase 1+ should validate IDs are from trusted source
- Consider rate limiting per tenant
- Audit high-volume evaluations

### 21.3 Provider Security

**Static Provider**:
- File permissions: Restrict to service user only
- No secrets in config files

**Database Provider** (Phase 1+):
- Use prepared statements (prevent SQL injection)
- Principle of least privilege for DB user
- Encrypt sensitive flag values

**Remote Provider** (Phase 2+):
- TLS for transport encryption
- Mutual TLS for service authentication
- Request signing for API calls

## 22. Testing Guidelines

### 22.1 Unit Test Coverage

**Target**: 100% coverage for Phase 0 modules

**Key Areas**:
- All error variants are tested
- All validation logic is tested
- All const fn are tested
- All predicates (is_fatal, is_retryable) are tested

### 22.2 Property Testing

**Phase 1+ Property Tests**:

**FlagKey**:
- `new(s).unwrap().as_str() == s` (round-trip)
- `new(s).unwrap() == new(s).unwrap()` (idempotent)
- `new(s).unwrap().len() == s.len()` (length preservation)

**Consistent Hashing**:
- `hash(tenant, user, key)` is deterministic
- Small input changes produce large output changes
- Distribution is uniform (Chi-square test)

**Cache Behavior**:
- Cache hit after insert
- Cache miss after expiry
- LRU eviction when full

### 22.3 Integration Testing

**Phase 1+ Integration Tests**:

**Manager Lifecycle**:
```
1. Create manager from config
2. Register flags
3. Evaluate flags (hit cache)
4. Update flags (invalidate cache)
5. Evaluate again (cache miss, then hit)
6. Shutdown manager
```

**Provider Integration**:
```
1. Load provider (file/database)
2. Query known flags
3. Query unknown flags (error)
4. Update provider
5. Reload provider
```

## 23. Documentation Standards

### 23.1 Code Documentation

**Public API**: All public items must have doc comments
- What it does
- Parameters (with types)
- Return values (with types)
- Errors (when applicable)
- Examples (when helpful)

**Internal APIs**: Document complex logic
- Why (not just what)
- Invariants maintained
- Performance characteristics

### 23.2 Example Code

**Rules for Examples**:
- Must compile (tested in doctests)
- Must be realistic (not toy examples)
- Must demonstrate error handling
- Must show common patterns

**Example Template**:
```rust
/// # Examples
///
/// ```
/// use feature_flags::{FlagKey, FlagValue};
///
/// let key = FlagKey::new("my_feature").unwrap();
/// let value = FlagValue::bool(true);
/// assert!(value.as_bool() == Some(true));
/// ```
```

## 24. Migration Guide

### 24.1 From Phase 0 to Phase 1

**Breaking Changes**: None (skeleton → real implementation)

**New Capabilities**:
- Real flag storage (in-memory HashMap)
- Real evaluation cache (with TTL)
- Real provider integration (file-based)
- Concurrent read access (RwLock)

**Migration Steps**:
1. Replace skeleton methods with real implementations
2. Add cache data structures to manager
3. Integrate provider into manager constructor
4. Update tests to use real functionality
5. Run integration tests

### 24.2 From Custom System to Feature Flags

**Step 1: Identify Flag Points**
- Look for `if config["feature_enabled"]` patterns
- Look for feature toggles in environment variables
- Look for conditional compilation (`cfg(feature = "...")`)

**Step 2: Define Flag Keys**
- Convert string keys to `FlagKey::new()`
- Validate key names (ASCII, length)
- Document flag purpose in code comments

**Step 3: Create Rules**
- Start with `Always`/`Never` for simple toggles
- Add `Percentage` for gradual rollouts
- Add `TenantIn` for tenant-specific features

**Step 4: Replace Conditionals**
- Before: `if config["feature_x"] { ... }`
- After: `if manager.evaluate(flag_x, ctx)? { ... }`

**Step 5: Testing**
- Test with flags enabled
- Test with flags disabled
- Test with percentage rollouts (deterministic)

## 25. Troubleshooting

### 25.1 Common Issues

**Issue**: Cache hit rate is low (< 70%)

**Diagnosis**:
1. Check TTL is too short (< 5 seconds)
2. Check cache size is too small
3. Check evaluation pattern (high cardinality contexts)

**Solutions**:
- Increase TTL to 30-60 seconds
- Increase cache size
- Reduce context cardinality (group similar users)

**Issue**: Rule evaluation is slow (> 10ms)

**Diagnosis**:
1. Check rule complexity (many rules per flag)
2. Check condition types (TenantIn with many tenants)
3. Check provider latency (slow source)

**Solutions**:
- Reduce rule count (consolidate rules)
- Use Percentage instead of TenantIn for many tenants
- Cache provider results locally

**Issue**: Flag returns unexpected value

**Diagnosis**:
1. Check rule priority order
2. Check context values (tenant_id, user_id)
3. Check percentage hash calculation

**Solutions**:
- Reorder rules (more specific first)
- Verify context is passed correctly
- Test with known context values

### 25.2 Debugging Tools

**Phase 1+ Debug Features**:

**Verbose Logging**:
- Log all cache hits/misses
- Log rule evaluation results
- Log provider queries

**Per-Flag Statistics**:
- Evaluation count per flag
- Enable rate per flag
- Cache hit rate per flag

**Rule Tracing**:
- Which rules matched?
- Why did a rule not match?
- What was the hash value?

## 26. Performance Tuning

### 26.1 Cache Tuning

**Small Deployments** (< 1000 flags):
```toml
max_flags = 1_000
evaluation_cache_ttl_ms = 60_000  # 1 minute
```

**Medium Deployments** (1000-10000 flags):
```toml
max_flags = 10_000
evaluation_cache_ttl_ms = 30_000  # 30 seconds
```

**Large Deployments** (> 10000 flags):
```toml
max_flags = 50_000
evaluation_cache_ttl_ms = 10_000  # 10 seconds
```

### 26.2 Rule Optimization

**Use Percentage Instead of TenantIn**:
- Before: `TenantIn([1, 2, 3, ..., 1000], 1000)` - O(n)
- After: `Percentage(50)` - O(1)

**Order Rules by Specificity**:
1. Always/Never (no context needed)
2. TenantIn (exact match)
3. Percentage (hash computation)

**Avoid Complex Expressions**:
- Phase 1+ will support AND/OR
- Use sparingly (performance impact)
- Consider pre-computing complex conditions

## 27. Compatibility Matrix

### 27.1 Rust Version Compatibility

**Minimum Rust Version**: 1.85.0 (Rust 2024 edition)

**Required Features**:
- `const_mut_refs` (for const fn with &mut)
- `strict_provenance` (for pointer casts)
- `unsafe_op_in_unsafe_fn` (for explicit unsafe)

**Tested Versions**:
- Rust 1.85.0 (minimum)
- Rust nightly (for future features)

### 27.2 Platform Compatibility

**Tier 1 Platforms** (fully supported):
- x86_64-unknown-linux-gnu (primary target)
- x86_64-unknown-linux-musl (static linking)

**Tier 2 Platforms** (best effort):
- aarch64-unknown-linux-gnu (ARM64 servers)
- x86_64-apple-darwin (macOS development)

**Unsupported**:
- Windows (different threading model)
- Embedded (no std)

## 28. API Stability Guarantees

### 28.1 Semantic Versioning

**Phase 0**: Version 0.1.0
- No API stability guarantees
- Expect breaking changes

**Phase 1**: Version 0.2.0
- Stabilize public API
- Breaking changes bump minor version

**Phase 2+**: Version 1.0.0
- Stable API (following semver)
- Breaking changes bump major version

### 28.2 Deprecated APIs

**Deprecation Policy**:
- Mark deprecated with `#[deprecated]` attribute
- Document migration path
- Remove in next minor version (0.x)

**Example**:
```rust
#[deprecated(since = "0.2.0", note = "Use `FlagKey::new` instead")]
pub fn make_flag_key(s: &str) -> FlagKey {
    FlagKey::new(s).unwrap()
}
```

## 29. Contributing Guidelines

### 29.1 Code Style

**Rustfmt**: Enforced (no exceptions)
- Use `cargo fmt` before commit
- CI checks formatting

**Clippy**: Enforced (with exceptions documented)
- Use `cargo clippy -- -D warnings`
- Add `#[allow(clippy::...)]` with comment

**Naming Conventions**:
- Types: `PascalCase`
- Functions: `snake_case`
- Constants: `SCREAMING_SNAKE_CASE`

### 29.2 Pull Request Process

**Before Submitting**:
1. Run `cargo test` (all tests pass)
2. Run `cargo clippy` (no warnings)
3. Run `cargo fmt` (formatted code)
4. Update documentation (if API changed)
5. Add tests (for new functionality)

**PR Description Template**:
```markdown
## Description
Brief description of changes.

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Performance improvement
- [ ] Documentation update

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] Manual testing completed

## Checklist
- [ ] Code follows style guidelines
- [ ] Documentation updated
- [ ] No new warnings generated
```

## 30. Release Notes

### 30.1 Version 0.1.0 (Phase 0)

**Features**:
- Typed flag keys (FlagKey)
- Typed flag values (Bool, Percentage, String)
- Rule conditions (Always, Never, Percentage, TenantIn)
- Evaluation context (tenant_id, user_id)
- Configuration and presets
- Statistics and health monitoring
- Skeleton facades for manager and provider

**Known Limitations**:
- No real flag storage (skeleton)
- No evaluation cache (skeleton)
- No provider integration (skeleton)
- Simple hash function (not cryptographic)

**Next Release** (0.2.0 - Phase 1):
- Real flag storage (HashMap)
- Evaluation cache with TTL
- File-based provider
- Concurrent read access

---

**End of DESIGN.md**

**Document Owner**: Vireon Architecture Team  
**Last Updated**: 2026-07-07  
**Version**: 0.1.0  
**Status**: Phase 0 - Active Development
