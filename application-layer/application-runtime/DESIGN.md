# Application Runtime - Design Document

## Part 1: Overview

### 1.1 Purpose

The Application Runtime is the top-level composition root for the Vireon
application layer. It ties together all 20 application-layer crates into a
single runtime facade with unified configuration, health monitoring, and
lifecycle management. It provides:

- **Unified Configuration**: A single `AppRuntimeConfig` that governs module
  limits, startup/shutdown timeouts, and health check intervals across all
  subsystems.
- **Runtime Identity**: A `RuntimeId` newtype and `RuntimeRole` enum that
  classify the deployment archetype (standalone, cluster, edge, controller).
- **Atomic Statistics**: `AppRuntimeStats` with `AtomicU64` counters tracking
  modules loaded, modules started, modules failed, startups, shutdowns, and
  health checks.
- **Health Monitoring**: Tri-state health derivation (Healthy / Degraded /
  Critical) from startup success rates, with worst-signal-wins aggregation.
- **Composition Root**: `CompositionRoot` that wires together all 20
  application-layer crates and 5 lower-layer crates into a cohesive runtime.
- **Lifecycle Management**: `AppRuntime` facade that orchestrates startup,
  shutdown, and health monitoring across all subsystems.

### 1.2 Design Philosophy

The application runtime follows these principles:

1. **Typed errors**: Every fallible API returns `AppRuntimeError`, enabling
   exhaustive pattern matching without string matching.
2. **Const-friendly**: Core type constructors and predicates work in `const fn`
   contexts for compile-time validation.
3. **Zero-allocation hot path**: Key types (`RuntimeId`, `RuntimeRole`,
   `AppRuntimeConfig`) are `Copy` and stack-allocated.
4. **Atomic stats**: Counters use `AtomicU64` with `Relaxed` ordering for
   maximum throughput; stats are advisory, never load-bearing.
5. **Worst-signal-wins**: Health aggregation picks the worst state across all
   signals, ensuring degraded components are never masked by healthy ones.
6. **Preset-driven**: Three deployment archetypes encode self-consistent
   starting points (Development, Production, Minimal); tune via builder.
7. **Composition over inheritance**: The runtime composes subsystems rather
   than inheriting from a base class, keeping each crate independent.
8. **Re-export facade**: `lib.rs` re-exports key types from all 25 dependency
   crates, providing a single import surface for downstream consumers.

### 1.3 Architecture

```text
┌─────────────────────────────────────────────────────────────────────────┐
│                      Application Layer (Top)                             │
├─────────────────────────────────────────────────────────────────────────┤
│  Application Runtime                                                     │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────────┐  │
│  │   AppRuntime     │  │ CompositionRoot  │  │  AppRuntimeBuilder   │  │
│  │   (Phase 1)      │  │   (Phase 1)      │  │   (Phase 0 real)     │  │
│  └──────────────────┘  └──────────────────┘  └──────────────────────┘  │
│         │                       │                       │                │
│         └───────────────────────┼───────────────────────┘              │
│                                 ▼                                        │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                    20 Application-Layer Crates                   │   │
│  │  ┌─────────────────┐ ┌─────────────────┐ ┌───────────────────┐  │   │
│  │  │ config-system   │ │ version-negot   │ │ feature-flags     │  │   │
│  │  │ scheduler-hooks │ │ observability   │ │ resource-quotas   │  │   │
│  │  │ module-lifecycle│ │ middleware-pipe │ │ plugin-manager    │  │   │
│  │  │ service-registry│ │ auth-framework  │ │ dependency-inject │  │   │
│  │  │ graceful-start  │ │ rpc-framework   │ │ event-bus         │  │   │
│  │  │ pubsub-engine   │ │ media-streaming │ │ ai-streaming      │  │   │
│  │  │ agent-runtime   │ │ ops-dashboard   │ │                   │  │   │
│  │  └─────────────────┘ └─────────────────┘ └───────────────────┘  │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                 ▼                                        │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                    5 Lower-Layer Crates                          │   │
│  │  ┌─────────────────┐ ┌─────────────────┐ ┌───────────────────┐  │   │
│  │  │ session-manager │ │session-registry │ │ connection-shard  │  │   │
│  │  │ stream-manager  │ │ reactor         │ │                   │  │   │
│  │  └─────────────────┘ └─────────────────┘ └───────────────────┘  │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

### 1.4 Layer Position

The application-runtime sits at the very top of the application layer stack.
It depends on:

- **All 20 application-layer crates**: For composing subsystem functionality.
- **session-manager**: For session identity and lifecycle types.
- **session-registry**: For cluster node identity and directory types.
- **connection-shard**: For connection identity and sharding.
- **stream-manager**: For stream identity and management.
- **reactor**: For the underlying reactor configuration and types.

No other crate depends on application-runtime. It is the terminal consumer
and composition root of the entire application layer.

### 1.5 Phase 0 Scope

Phase 0 delivers the foundational types and typed skeleton facades:

| Module | Status | Description |
|--------|--------|-------------|
| `error` | Real | `AppRuntimeError` with `is_fatal`/`is_retryable` |
| `config` | Real | `AppRuntimeConfig` with presets and validation |
| `runtime_id` | Real | `RuntimeId` newtype, `RuntimeRole` enum |
| `stats` | Real | `AppRuntimeStats` atomics, `StatsSnapshot` |
| `health` | Real | `HealthReport`, `HealthState` tri-state |
| `builder` | Real | `AppRuntimeBuilder` fluent builder |
| `runtime` | Skeleton | `AppRuntime` returns `ShuttingDown`/`NotRunning` |
| `composition` | Skeleton | `CompositionRoot` returns `CompositionFailed` |

---

## Part 2: Error Model

### 2.1 Error Type

```rust
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum AppRuntimeError {
    AlreadyRunning,
    NotRunning,
    ModuleFailed(&'static str),
    CompositionFailed(&'static str),
    ShuttingDown,
}
```

### 2.2 Error Classification

Errors are classified into two categories via const predicates:

| Variant | `is_fatal` | `is_retryable` | Caller action |
|---------|------------|-----------------|---------------|
| `AlreadyRunning` | Yes | No | Abort; don't retry |
| `NotRunning` | Yes | No | Abort; don't retry |
| `ModuleFailed` | No | Yes | Back off + retry |
| `CompositionFailed` | No | Yes | Back off + retry |
| `ShuttingDown` | Yes | No | Abort; don't retry |

### 2.3 Design Rationale

- **`AlreadyRunning`**: Lifecycle violation. The caller attempted to start
  an already-running runtime. No amount of retrying will fix this.
- **`NotRunning`**: Lifecycle violation. The caller attempted to stop or
  query a non-running runtime. Indicates a logic error in the caller.
- **`ModuleFailed`**: A module crashed during startup or operation. The
  module may recover on retry, so this is retryable.
- **`CompositionFailed`**: Subsystem composition failed (e.g., dependency
  cycle, missing dependency). May be transient if modules are still
  registering, so retryable.
- **`ShuttingDown`**: The runtime is in the process of shutting down.
  New operations are rejected. This is terminal, not retryable.

### 2.4 Non-Exhaustive

The `#[non_exhaustive]` attribute ensures that future error variants can
be added without breaking downstream match arms. Callers must always
include a `_` wildcard arm.

### 2.5 Error Messages

All error messages are human-readable and formatted via `#[error("...")]`:

```rust
AppRuntimeError::AlreadyRunning       // "application runtime is already running"
AppRuntimeError::NotRunning           // "application runtime is not running"
AppRuntimeError::ModuleFailed("auth") // "module failed: auth"
AppRuntimeError::CompositionFailed("cycle") // "composition failed: cycle"
AppRuntimeError::ShuttingDown         // "application runtime is shutting down"
```

---

## Part 3: Configuration Model

### 3.1 Configuration Type

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AppRuntimeConfig {
    pub max_modules: u32,
    pub startup_timeout_ms: u64,
    pub shutdown_timeout_ms: u64,
    pub health_check_interval_ms: u64,
}
```

### 3.2 Field Semantics

| Field | Type | Range | Default | Description |
|-------|------|-------|---------|-------------|
| `max_modules` | `u32` | 1..=1_024 | 256 | Maximum modules the runtime manages |
| `startup_timeout_ms` | `u64` | 100..=300_000 | 30_000 | Startup deadline (ms) |
| `shutdown_timeout_ms` | `u64` | 100..=300_000 | 15_000 | Shutdown deadline (ms) |
| `health_check_interval_ms` | `u64` | 100..=300_000 | 5_000 | Health check interval (ms) |

### 3.3 Constants

```rust
pub const MIN_MAX_MODULES: u32 = 1;
pub const MAX_MAX_MODULES: u32 = 1_024;
pub const MIN_STARTUP_TIMEOUT_MS: u64 = 100;
pub const MAX_STARTUP_TIMEOUT_MS: u64 = 300_000;
pub const MIN_SHUTDOWN_TIMEOUT_MS: u64 = 100;
pub const MAX_SHUTDOWN_TIMEOUT_MS: u64 = 300_000;
pub const MIN_HEALTH_CHECK_INTERVAL_MS: u64 = 100;
pub const MAX_HEALTH_CHECK_INTERVAL_MS: u64 = 300_000;
```

### 3.4 Validation

Validation is performed by `AppRuntimeConfig::validate()`, which checks
each field against its min/max bounds. An out-of-range field returns
`AppRuntimeError::CompositionFailed` with a descriptive message.

### 3.5 Builder Methods

The config supports const builder-style methods for chaining overrides:

```rust
let cfg = AppRuntimeConfig::new()
    .with_max_modules(128)
    .with_startup_timeout_ms(20_000)
    .with_shutdown_timeout_ms(10_000)
    .with_health_check_interval_ms(3_000);
```

### 3.6 Copy Semantics

`AppRuntimeConfig` is `Copy`, so it can be freely duplicated without
`Clone`. This is safe because all fields are primitive integers. The
config is small (24 bytes on 64-bit platforms) and passed by value.

---

## Part 4: Identity Model

### 4.1 RuntimeId

```rust
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RuntimeId(u64);
```

`RuntimeId` is a `u64` newtype with a nil sentinel (0). Each runtime
instance gets a unique id assigned at startup. The id is used in logs,
metrics, and distributed coordination.

### 4.2 RuntimeId API

| Method | Signature | Description |
|--------|-----------|-------------|
| `NIL` | `pub const NIL: Self` | Nil sentinel constant |
| `new` | `pub const fn new(id: u64) -> Self` | Construct (panics on 0) |
| `nil` | `pub const fn nil() -> Self` | Construct nil |
| `is_nil` | `pub const fn is_nil(self) -> bool` | Check if nil |
| `raw` | `pub const fn raw(self) -> u64` | Get raw u64 value |

### 4.3 RuntimeRole

```rust
#[non_exhaustive]
#[repr(u8)]
pub enum RuntimeRole {
    Standalone = 0,
    Cluster = 1,
    Edge = 2,
    Controller = 3,
}
```

### 4.4 RuntimeRole Semantics

| Variant | Value | Clustered | Description |
|---------|-------|-----------|-------------|
| `Standalone` | 0 | No | Single-node deployment, no clustering |
| `Cluster` | 1 | Yes | Member of a multi-node cluster |
| `Edge` | 2 | No | Edge node with limited resources |
| `Controller` | 3 | Yes | Cluster controller/coordinator |

### 4.5 RuntimeRole API

| Method | Signature | Description |
|--------|-----------|-------------|
| `is_clustered` | `pub const fn is_clustered(&self) -> bool` | Cluster or Controller |
| `as_str` | `pub const fn as_str(&self) -> &'static str` | Human-readable name |

### 4.6 Repr U8

`RuntimeRole` uses `#[repr(u8)]` for efficient FFI and compact storage.
This allows it to be stored in a single byte in memory layouts and
transmitted as a single byte over the wire.

### 4.7 Default

`RuntimeRole` defaults to `Standalone`, which is the safest and most
common deployment archetype for development.

---

## Part 5: Statistics Model

### 5.1 AppRuntimeStats

```rust
#[derive(Debug)]
pub struct AppRuntimeStats {
    pub modules_loaded: AtomicU64,
    pub modules_started: AtomicU64,
    pub modules_failed: AtomicU64,
    pub startups_total: AtomicU64,
    pub shutdowns_total: AtomicU64,
    pub health_checks_total: AtomicU64,
}
```

### 5.2 Counter Semantics

| Counter | Type | Prometheus Type | Description |
|---------|------|-----------------|-------------|
| `modules_loaded` | Gauge | `app_runtime_modules_loaded` | Currently loaded |
| `modules_started` | Gauge | `app_runtime_modules_started` | Currently started |
| `modules_failed` | Counter | `app_runtime_modules_failed_total` | Cumulative failures |
| `startups_total` | Counter | `app_runtime_startups_total` | Cumulative startups |
| `shutdowns_total` | Counter | `app_runtime_shutdowns_total` | Cumulative shutdowns |
| `health_checks_total` | Counter | `app_runtime_health_checks_total` | Cumulative checks |

### 5.3 Ordering

All counters use `Ordering::Relaxed` because:

1. Stats are advisory, never load-bearing.
2. No synchronization depends on the ordering of counter updates.
3. `Relaxed` provides maximum throughput on modern CPUs.
4. Cross-field consistency is not required for advisory stats.

### 5.4 StatsSnapshot

```rust
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsSnapshot {
    pub modules_loaded: u64,
    pub modules_started: u64,
    pub modules_failed: u64,
    pub startups_total: u64,
    pub shutdowns_total: u64,
    pub health_checks_total: u64,
}
```

### 5.5 Rate Calculations

```rust
impl StatsSnapshot {
    pub const fn startup_success_rate_bps(&self) -> u16 {
        if self.startups_total == 0 { return 10_000; }
        let succeeded = self.startups_total.saturating_sub(self.modules_failed);
        let rate = succeeded.saturating_mul(10_000) / self.startups_total;
        if rate > u16::MAX as u64 { u16::MAX } else { rate as u16 }
    }

    pub const fn module_failure_rate_bps(&self) -> u16 {
        if self.modules_loaded == 0 { return 0; }
        let rate = self.modules_failed.saturating_mul(10_000) / self.modules_loaded;
        if rate > u16::MAX as u64 { u16::MAX } else { rate as u16 }
    }
}
```

### 5.6 Basis Points

Rates are expressed in basis points (bps), where 10_000 bps = 100%.
This avoids floating-point arithmetic while providing 0.01% precision.
Integer division truncates: 2*10000/3 = 6666, not 6667.

### 5.7 Saturating Arithmetic

All rate calculations use `saturating_mul` and `saturating_sub` to
prevent overflow and underflow:

- `saturating_mul(10_000)`: Prevents overflow when multiplying large
  counts by the basis-points scaling factor.
- `saturating_sub`: Prevents underflow when `modules_failed` exceeds
  `startups_total` (which can happen due to relaxed ordering).

### 5.8 Idle System Convention

When there are no startups (`startups_total == 0`), the success rate
is defined as 10_000 bps (100%). An idle system is considered healthy
because there are no failures to report.

When there are no modules loaded (`modules_loaded == 0`), the failure
rate is defined as 0 bps (0%). An empty system has no failures.

---

## Part 6: Health Model

### 6.1 HealthState

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

### 6.2 Tri-State Classification

| State | Meaning | Operator Action |
|-------|---------|-----------------|
| `Healthy` | All systems nominal | None |
| `Degraded` | Performance or availability impact | Investigate during business hours |
| `Critical` | Severe impact; data loss risk possible | Page on-call immediately |

### 6.3 Thresholds

```rust
pub const SUCCESS_HEALTHY_BPS: u16 = 9_900;   // >= 99%
pub const SUCCESS_DEGRADED_BPS: u16 = 9_000;   // >= 90%
pub const SUCCESS_CRITICAL_BPS: u16 = 9_000;   // < 90%
```

### 6.4 Derivation Logic

```rust
let state = if startup_success_rate_bps >= SUCCESS_HEALTHY_BPS {
    HealthState::Healthy       // >= 99%
} else if startup_success_rate_bps >= SUCCESS_CRITICAL_BPS {
    HealthState::Degraded      // >= 90%, < 99%
} else {
    HealthState::Critical      // < 90%
};
```

### 6.5 Worst-Signal-Wins

`HealthState::worst_of(a, b)` returns the worse of two health states:

```rust
Critical > Degraded > Healthy
```

This ensures that degraded components are never masked by healthy ones
when aggregating health across multiple subsystems.

### 6.6 HealthReport

```rust
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HealthReport {
    pub state: HealthState,
    pub modules_loaded: u64,
    pub modules_started: u64,
    pub modules_failed: u64,
    pub startups_total: u64,
    pub shutdowns_total: u64,
    pub health_checks_total: u64,
    pub startup_success_rate_bps: u16,
}
```

### 6.7 Health Predicates

| Method | Description |
|--------|-------------|
| `is_healthy()` | Whether the state is `Healthy` |
| `needs_attention()` | Whether the state is `Degraded` or `Critical` |
| `is_critical()` | Whether the state is `Critical` |

---

## Part 7: Builder Model

### 7.1 AppRuntimeBuilder

```rust
#[derive(Debug, Clone)]
pub struct AppRuntimeBuilder {
    cfg: AppRuntimeConfig,
}
```

### 7.2 Builder API

| Method | Description |
|--------|-------------|
| `new()` | Start with production defaults |
| `from_preset(Preset)` | Start from a deployment archetype |
| `max_modules(u32)` | Override max modules |
| `startup_timeout_ms(u64)` | Override startup timeout |
| `shutdown_timeout_ms(u64)` | Override shutdown timeout |
| `health_check_interval_ms(u64)` | Override health check interval |
| `build()` | Validate and produce final config |

### 7.3 Preset System

Three presets encode deployment archetypes:

| Preset | Modules | Startup | Shutdown | Health | Use Case |
|--------|---------|---------|----------|--------|----------|
| `Development` | 64 | 10s | 5s | 1s | Local dev / testing |
| `Production` | 256 | 30s | 15s | 5s | Production deployment |
| `Minimal` | 8 | 5s | 2s | 10s | Edge / embedded |

### 7.4 Validation on Build

The builder validates the config on `build()`. If any field is out of
range, it returns `AppRuntimeError::CompositionFailed`. This ensures
that invalid configs are caught at construction time, not at runtime.

### 7.5 Fluent Chaining

All setter methods are `const fn` and return `Self`, enabling fluent
chaining:

```rust
let cfg = AppRuntimeBuilder::from_preset(Preset::Production)
    .max_modules(512)              // override
    .health_check_interval_ms(2_000) // override
    .build()
    .unwrap();
```

---

## Part 8: Runtime Facade (Phase 1 Skeleton)

### 8.1 AppRuntime

```rust
#[derive(Debug)]
pub struct AppRuntime {
    cfg: AppRuntimeConfig,
    id: RuntimeId,
    stats: AppRuntimeStats,
}
```

### 8.2 Runtime API (Phase 1)

| Method | Phase 0 Behavior | Phase 1 Target |
|--------|------------------|-----------------|
| `new(cfg, id, stats)` | Real: stores cfg + stats | Same |
| `start()` | Skeleton: returns `ShuttingDown` | Orchestrate module startup |
| `stop()` | Skeleton: returns `NotRunning` | Orchestrate module shutdown |
| `health()` | Real: derives from stats | Same + aggregate subsystem health |
| `health_state()` | Real: derives from stats | Same |

### 8.3 Phase 1 Startup Sequence

In Phase 1, `start()` will:

1. Check if already running (return `AlreadyRunning`).
2. Record startup attempt in stats.
3. Initialize the composition root.
4. Start modules in dependency order (via `module-lifecycle`).
5. Start the health check loop.
6. Record success/failure in stats.

### 8.4 Phase 1 Shutdown Sequence

In Phase 1, `stop()` will:

1. Check if running (return `NotRunning` if not).
2. Set shutting-down flag.
3. Drain connections (via `graceful-startup-shutdown`).
4. Stop modules in reverse dependency order.
5. Wait for shutdown timeout.
6. Record shutdown in stats.

### 8.5 Phase 1 Health Aggregation

In Phase 1, `health()` will:

1. Derive local health from `AppRuntimeStats`.
2. Query each subsystem's health.
3. Aggregate using `HealthState::worst_of`.
4. Return the worst health state.

---

## Part 9: Composition Root (Phase 1 Skeleton)

### 9.1 CompositionRoot

```rust
#[derive(Debug)]
pub struct CompositionRoot {
    cfg: AppRuntimeConfig,
    stats: AppRuntimeStats,
}
```

### 9.2 Composition API (Phase 1)

| Method | Phase 0 Behavior | Phase 1 Target |
|--------|------------------|-----------------|
| `new(cfg, stats)` | Real: stores cfg + stats | Same |
| `compose()` | Skeleton: returns `CompositionFailed` | Wire all 20 subsystems |
| `resolve(name)` | Skeleton: returns `CompositionFailed` | Resolve by name |
| `health()` | Real: derives from stats | Same + subsystem health |
| `health_state()` | Real: derives from stats | Same |

### 9.3 Phase 1 Composition

In Phase 1, `compose()` will:

1. Initialize the dependency injection container.
2. Register all 20 application-layer subsystems.
3. Resolve the dependency graph (via `module-lifecycle`).
4. Detect cycles and missing dependencies.
5. Return `CompositionFailed` on any error.

### 9.4 Phase 1 Resolution

In Phase 1, `resolve(name)` will:

1. Look up the subsystem by name in the DI container.
2. Return the resolved instance or `CompositionFailed`.

### 9.5 Subsystem Registration

The 20 application-layer subsystems to be registered:

1. **configuration-system**: Hierarchical configuration management.
2. **version-negotiation**: Protocol version compatibility.
3. **feature-flags**: Feature flag evaluation.
4. **scheduler-hooks**: Reactor scheduler hooks.
5. **observability-sdk**: Metrics, tracing, and logging.
6. **resource-quotas**: Per-tenant resource limits.
7. **module-lifecycle**: Dependency-ordered module management.
8. **middleware-pipeline**: Request/response middleware chain.
9. **plugin-manager**: Versioned plugin loading.
10. **service-registry**: Service discovery and load balancing.
11. **auth-framework**: Authentication and authorization.
12. **dependency-injection**: Typed DI container.
13. **graceful-startup-shutdown**: Phase-ordered boot sequence.
14. **rpc-framework**: Unary and streaming RPC.
15. **event-bus**: Partitioned event bus.
16. **pubsub-engine**: Topic pattern matching pub/sub.
17. **media-streaming**: Audio/video track management.
18. **ai-streaming**: Token streaming for LLMs.
19. **agent-runtime**: Agent lifecycle and tool execution.
20. **ops-dashboard**: Operational visibility.

---

## Part 10: Re-Export Strategy

### 10.1 Single Import Surface

The `lib.rs` re-exports key types from all 25 dependency crates, providing
a single import surface for downstream consumers. This means consumers
only need to depend on `application-runtime` to access types from any
subsystem.

### 10.2 Application-Layer Re-Exports

Each of the 20 application-layer crates has its key types re-exported:

- Error types (e.g., `ConfigError`, `FlagError`, `AuthError`)
- Config types (e.g., `ConfigurationSystemConfig`, `FeatureFlagsConfig`)
- Builder types (e.g., `ConfigBuilder`, `FeatureFlagsBuilder`)
- Stats types (e.g., `ConfigStats`, `FeatureFlagsStats`)
- Domain types (e.g., `ConfigKey`, `FlagKey`, `PrincipalId`, `ModuleId`)

### 10.3 Lower-Layer Re-Exports

Key identity types from the 5 lower-layer crates are re-exported:

- `SessionId` from `session-manager`
- `NodeId` from `session-registry`
- `ConnectionId` from `connection-shard`
- `StreamId` from `stream-manager`
- `ReactorConfig` from `reactor`

### 10.4 Naming Conflicts

Where naming conflicts occur (e.g., multiple crates export `HealthReport`),
the re-export uses the local crate's version directly. Downstream consumers
can use fully-qualified paths if they need a specific crate's version.

### 10.5 Alias Strategy

Some re-exports use aliases to avoid conflicts:

- `EventBus` -> re-exported without alias (local type)
- `ServiceRegistry` -> only the config/stats types are re-exported (not the facade)
- `AgentRuntime` -> aliased as `AgentRuntimeFacade` to avoid conflict with local `AppRuntime`
- `PubSubEngine` -> aliased as `PubSubEngineFacade`

---

## Part 11: Failure Modes

### 11.1 Lifecycle Violations

| Scenario | Error | Recovery |
|----------|-------|----------|
| Start when already running | `AlreadyRunning` | Stop first, then start |
| Stop when not running | `NotRunning` | Check state before stopping |
| Operation during shutdown | `ShuttingDown` | Wait for shutdown to complete |

### 11.2 Module Failures

| Scenario | Error | Recovery |
|----------|-------|----------|
| Module crashes during startup | `ModuleFailed("name")` | Retry after backoff |
| Module crashes during operation | `ModuleFailed("name")` | Restart module |

### 11.3 Composition Failures

| Scenario | Error | Recovery |
|----------|-------|----------|
| Dependency cycle detected | `CompositionFailed("cycle")` | Fix dependency graph |
| Missing dependency | `CompositionFailed("missing")` | Register missing module |
| Duplicate registration | `CompositionFailed("duplicate")` | Check registration order |

### 11.4 Recovery Strategy

The runtime implements a simple recovery strategy:

1. **Fatal errors**: Log and abort. The caller must fix the underlying
   issue (lifecycle violation, programming error).
2. **Retryable errors**: Log, back off, and retry up to a configurable
   limit. If retries are exhausted, escalate to fatal.

### 11.5 Error Propagation

Errors propagate through the call stack as `Result<T, AppRuntimeError>`.
The `?` operator is used for early returns. Callers can pattern-match
on the error variant to implement custom recovery logic.

---

## Part 12: Observability

### 12.1 Metrics

Metrics are exposed via `AppRuntimeStats` and `StatsSnapshot`. In
Phase 2+, these will be exported to Prometheus via the
`observability-sdk`:

```text
# Gauge
app_runtime_modules_loaded 42
app_runtime_modules_started 40

# Counter
app_runtime_modules_failed_total 2
app_runtime_startups_total 1
app_runtime_shutdowns_total 0
app_runtime_health_checks_total 1200
```

### 12.2 Health Checks

Health checks are performed on a configurable interval
(`health_check_interval_ms`). Each check:

1. Takes a stats snapshot.
2. Derives a `HealthReport`.
3. Logs the health state.
4. Increments `health_checks_total`.

### 12.3 Tracing

In Phase 2+, tracing spans will be added to:

- `start()` — "app_runtime.startup"
- `stop()` — "app_runtime.shutdown"
- `health()` — "app_runtime.health_check"
- `compose()` — "app_runtime.compose"

### 12.4 Structured Logging

Structured log records use the `tracing` crate:

```rust
tracing::info!(
    runtime_id = %id,
    role = %role,
    modules = stats.modules_loaded.load(Ordering::Relaxed),
    "application runtime started"
);
```

### 12.5 Dashboard Integration

The `ops-dashboard` crate can consume `AppRuntimeStats` to render
runtime health widgets. The re-export of `OpsDashboardStats` and
`OpsDashboardService` from `lib.rs` enables seamless integration.

---

## Part 13: Configuration & Presets

### 13.1 Preset Archetypes

Three deployment archetypes encode self-consistent starting points:

#### Development

```rust
AppRuntimeConfig {
    max_modules: 64,
    startup_timeout_ms: 10_000,
    shutdown_timeout_ms: 5_000,
    health_check_interval_ms: 1_000,
}
```

- 64 modules is sufficient for local development.
- 10s startup timeout allows for slow module initialization in debug mode.
- 1s health check interval provides quick feedback during development.

#### Production

```rust
AppRuntimeConfig {
    max_modules: 256,
    startup_timeout_ms: 30_000,
    shutdown_timeout_ms: 15_000,
    health_check_interval_ms: 5_000,
}
```

- 256 modules supports a full production deployment.
- 30s startup timeout allows for large-scale module initialization.
- 5s health check interval balances observability with overhead.

#### Minimal

```rust
AppRuntimeConfig {
    max_modules: 8,
    startup_timeout_ms: 5_000,
    shutdown_timeout_ms: 2_000,
    health_check_interval_ms: 10_000,
}
```

- 8 modules for edge/embedded deployments with limited resources.
- 5s startup timeout for fast boot on resource-constrained devices.
- 10s health check interval minimizes overhead on edge devices.

### 13.2 Tuning Guidelines

| Field | When to increase | When to decrease |
|-------|------------------|-------------------|
| `max_modules` | Large deployment with many subsystems | Edge/embedded with limited memory |
| `startup_timeout_ms` | Slow module initialization (cold start) | Fast boot required (edge) |
| `shutdown_timeout_ms` | Long-running operations need drain time | Fast shutdown required (crash recovery) |
| `health_check_interval_ms` | Need fine-grained health visibility | Minimize overhead (edge) |

### 13.3 Validation Bounds

All fields have explicit min/max bounds to prevent misconfiguration:

- `max_modules`: 1 to 1_024 (prevents empty or excessive module counts)
- `startup_timeout_ms`: 100ms to 5min (prevents instant timeout or infinite hang)
- `shutdown_timeout_ms`: 100ms to 5min (same rationale)
- `health_check_interval_ms`: 100ms to 5min (prevents excessive checking or stale health)

---

## Part 14: Dependency Graph

### 14.1 Application-Layer Dependencies

The application-runtime depends on all 20 application-layer crates:

```text
application-runtime
├── configuration-system
├── version-negotiation
├── feature-flags
├── scheduler-hooks
├── observability-sdk
├── resource-quotas
├── module-lifecycle
├── middleware-pipeline
├── plugin-manager
├── service-registry
├── auth-framework
├── dependency-injection
├── graceful-startup-shutdown
├── rpc-framework
├── event-bus
├── pubsub-engine
├── media-streaming
├── ai-streaming
├── agent-runtime
└── ops-dashboard
```

### 14.2 Lower-Layer Dependencies

The application-runtime also depends on 5 lower-layer crates:

```text
application-runtime
├── session-manager (session-layer)
├── session-registry (session-layer)
├── connection-shard (runtime-layer)
├── stream-manager (stream-layer)
└── reactor (runtime-layer)
```

### 14.3 External Dependencies

```toml
thiserror = { workspace = true }
tracing  = { workspace = true }
```

- `thiserror`: For `#[derive(Error)]` on `AppRuntimeError`.
- `tracing`: For structured logging (used in Phase 1+).

### 14.4 Dev Dependencies

```toml
criterion = { version = "0.5", features = ["html_reports"] }
```

- `criterion`: For benchmark stubs.

### 14.5 Dependency Hygiene

- No `anyhow` (typed errors only).
- No `serde` (Phase 1+ addition for config serialization).
- No `tokio` (the runtime is reactor-based, not tokio-based).
- No `lazy_static` or `once_cell` (statics use `const fn`).

---

## Part 15: Module Structure

### 15.1 File Layout

```text
application-runtime/
├── Cargo.toml
├── DESIGN.md
├── README.md
├── benches/
│   ├── startup.rs
│   ├── shutdown.rs
│   ├── composition.rs
│   └── health_check.rs
└── src/
    ├── lib.rs           (module table + re-exports)
    ├── error.rs         (AppRuntimeError)
    ├── config.rs        (AppRuntimeConfig, Preset)
    ├── runtime_id.rs    (RuntimeId, RuntimeRole)
    ├── stats.rs         (AppRuntimeStats, StatsSnapshot)
    ├── health.rs        (HealthReport, HealthState)
    ├── builder.rs       (AppRuntimeBuilder)
    ├── runtime.rs       (AppRuntime skeleton)
    └── composition.rs   (CompositionRoot skeleton)
```

### 15.2 Module Dependency Order

```text
error ──────────────────────────────────────────────┐
                                                    │
config ──── (uses error) ──────────────────────────┤
                                                    │
runtime_id ── (standalone) ────────────────────────┤
                                                    │
stats ────── (standalone) ─────────────────────────┤
                                                    │
health ───── (uses stats) ─────────────────────────┤
                                                    │
builder ──── (uses config, error) ─────────────────┤
                                                    │
runtime ──── (uses config, error, health, ─────────┤
              runtime_id, stats)                    │
                                                    │
composition ─ (uses config, error, health, stats) ─┤
                                                    │
lib.rs ───── (re-exports all modules + deps) ──────┘
```

### 15.3 Module Phase Status

| Module | Phase | Lines (approx) | Tests |
|--------|-------|-----------------|-------|
| `error.rs` | 0 (real) | 100 | 4 |
| `config.rs` | 0 (real) | 280 | 10 |
| `runtime_id.rs` | 0 (real) | 200 | 16 |
| `stats.rs` | 0 (real) | 300 | 14 |
| `health.rs` | 0 (real) | 290 | 12 |
| `builder.rs` | 0 (real) | 190 | 9 |
| `runtime.rs` | 1 (skeleton) | 100 | 4 |
| `composition.rs` | 1 (skeleton) | 100 | 4 |
| `lib.rs` | 0 (re-exports) | 250 | 0 |
| **Total** | | ~1_810 | 72 |

---

## Part 16: Benchmark Strategy

### 16.1 Bench Stubs

Four Criterion bench files are included:

| Bench File | Focus | Functions |
|------------|-------|-----------|
| `startup.rs` | Config, builder, runtime creation, startup | 8 |
| `shutdown.rs` | Stop, health derivation, stats recording | 4 |
| `composition.rs` | Composition root, resolve, ID operations | 7 |
| `health_check.rs` | Stats snapshot, health derivation, state ops | 5 |

### 16.2 Benchmark Design

Each bench function measures a specific operation:

- **Config operations**: `new()`, `validate()` — measures const construction
  and validation overhead.
- **Builder operations**: `build()`, `from_preset()` — measures builder
  chaining and validation.
- **Runtime operations**: `new()`, `start()`, `stop()` — measures runtime
  creation and lifecycle operations (skeleton).
- **Stats operations**: `record_*()`, `snapshot()` — measures atomic counter
  operations.
- **Health operations**: `derive()`, `worst_of()` — measures health derivation
  from snapshots.
- **Identity operations**: `RuntimeId::new()`, `is_nil()` — measures ID
  construction and comparison.

### 16.3 Phase 1 Benchmarks

In Phase 1, benchmarks will be expanded to measure:

- Full startup sequence with real module initialization.
- Full shutdown sequence with connection draining.
- Composition root wiring with all 20 subsystems.
- Aggregated health checks across subsystems.

---

## Part 17: Integration Points

### 17.1 Configuration System Integration

The `configuration-system` crate provides hierarchical config management.
In Phase 1, `AppRuntimeConfig` will be loadable from config sources:

```rust
// Phase 1 vision:
let cfg = ConfigurationManager::new()
    .load_file("app.toml")?
    .load_env()?
    .extract::<AppRuntimeConfig>()?;
```

### 17.2 Module Lifecycle Integration

The `module-lifecycle` crate provides dependency-ordered initialization.
In Phase 1, `AppRuntime::start()` will use the lifecycle coordinator:

```rust
// Phase 1 vision:
let coordinator = LifecycleCoordinator::new(cfg);
coordinator.register_all(modules)?;
coordinator.start_all().await?;
```

### 17.3 Health Aggregation Integration

In Phase 1, `AppRuntime::health()` will aggregate health from all
subsystems:

```rust
// Phase 1 vision:
let local_health = HealthReport::derive(&self.stats.snapshot());
let module_health = self.lifecycle.health();
let service_health = self.registry.health();
let worst = HealthState::worst_of(local_health.state, module_health.state);
let worst = HealthState::worst_of(worst, service_health.state);
```

### 17.4 Graceful Shutdown Integration

The `graceful-startup-shutdown` crate provides phase-ordered shutdown.
In Phase 1, `AppRuntime::stop()` will use the shutdown coordinator:

```rust
// Phase 1 vision:
let coordinator = StartupShutdownCoordinator::new(cfg);
coordinator.drain_connections().await?;
coordinator.stop_modules().await?;
coordinator.finalize().await?;
```

### 17.5 Observability Integration

The `observability-sdk` provides metrics, tracing, and logging.
In Phase 2+, `AppRuntimeStats` will be exported via the observability SDK:

```rust
// Phase 2+ vision:
let registry = MetricsRegistry::new();
registry.register_counter("app_runtime_startups_total");
registry.register_gauge("app_runtime_modules_loaded");
```

---

## Part 18: Security Considerations

### 18.1 Authentication

The runtime itself does not perform authentication. Authentication is
delegated to the `auth-framework` crate, which provides:

- Pluggable auth methods (TLS, JWT, OAuth).
- RBAC/ABAC authorization.
- Token management.

### 18.2 Authorization

Module-level authorization is handled by the `auth-framework`. The
runtime does not enforce authorization directly but ensures that
the auth framework is initialized before other modules.

### 18.3 Resource Limits

Resource quotas are enforced by the `resource-quotas` crate. The
runtime ensures that quota enforcement is active before accepting
client connections.

### 18.4 Secrets Management

The runtime does not handle secrets directly. Secret management is
delegated to the `configuration-system`, which supports layered
config sources (file, env, CLI, runtime).

### 18.5 Audit Logging

In Phase 2+, audit events will be logged via the `observability-sdk`:

```rust
tracing::info!(
    target: "audit",
    runtime_id = %id,
    action = "startup",
    "application runtime started"
);
```

---

## Part 19: Testing Strategy

### 19.1 Unit Tests

Each module includes comprehensive unit tests:

| Module | Test Count | Coverage |
|--------|------------|----------|
| `error.rs` | 4 | All variants, predicates, messages |
| `config.rs` | 10 | Defaults, builder, validation, presets |
| `runtime_id.rs` | 16 | Construction, comparison, display, roles |
| `stats.rs` | 14 | Counters, rates, snapshots, edge cases |
| `health.rs` | 12 | Derivation, thresholds, worst-of, predicates |
| `builder.rs` | 9 | Defaults, chaining, validation, presets |
| `runtime.rs` | 4 | Creation, skeleton errors, health |
| `composition.rs` | 4 | Creation, skeleton errors, health |
| **Total** | 72 | |

### 19.2 Test Conventions

- Tests are inline (`#[cfg(test)] mod tests`).
- No external test crates (only `super::*`).
- Edge cases: boundaries, empty inputs, overflow.
- Rate tests verify truncation (not rounding).
- Health tests verify threshold boundaries.

### 19.3 Property Tests

Property tests are deferred to Phase 1+ when the runtime has real
behavior to property-test. Phase 0 tests are exhaustive unit tests
covering all code paths.

### 19.4 Integration Tests

Integration tests (multi-crate) are deferred to Phase 1+ when the
runtime actually composes subsystems. Phase 0 tests verify the
types and skeleton facades in isolation.

### 19.5 Benchmark Verification

Bench stubs compile and run but measure skeleton operations. In
Phase 1, benchmarks will measure real operations with meaningful
performance characteristics.

---

## Part 20: Roadmap

### 20.1 Phase 0 (Current)

- [x] `AppRuntimeError` with `is_fatal`/`is_retryable`
- [x] `AppRuntimeConfig` with presets and validation
- [x] `RuntimeId` newtype and `RuntimeRole` enum
- [x] `AppRuntimeStats` atomics and `StatsSnapshot`
- [x] `HealthReport` and `HealthState` tri-state
- [x] `AppRuntimeBuilder` fluent builder
- [x] `AppRuntime` skeleton (returns `ShuttingDown`/`NotRunning`)
- [x] `CompositionRoot` skeleton (returns `CompositionFailed`)
- [x] Re-exports from all 25 dependency crates
- [x] 4 Criterion bench stubs
- [x] DESIGN.md (1000+ lines)
- [x] README.md (~150 lines)

### 20.2 Phase 1

- [ ] Implement `AppRuntime::start()` with module orchestration
- [ ] Implement `AppRuntime::stop()` with graceful shutdown
- [ ] Implement `CompositionRoot::compose()` with DI wiring
- [ ] Implement `CompositionRoot::resolve()` with name lookup
- [ ] Add health aggregation across subsystems
- [ ] Add structured tracing spans
- [ ] Add integration tests
- [ ] Expand benchmarks with real operations

### 20.3 Phase 2

- [ ] Prometheus metrics export via `observability-sdk`
- [ ] Config loading from file/env via `configuration-system`
- [ ] Hot config reload via `configuration-system` watcher
- [ ] Audit logging for lifecycle events
- [ ] Multi-runtime coordination via `session-registry`

### 20.4 Phase 3

- [ ] Cluster-aware startup with `session-registry`
- [ ] Cross-runtime health aggregation
- [ ] Distributed composition root
- [ ] Failover and migration support

---

## Appendix A: Type Reference

### A.1 Error Types

```rust
AppRuntimeError::AlreadyRunning
AppRuntimeError::NotRunning
AppRuntimeError::ModuleFailed(&'static str)
AppRuntimeError::CompositionFailed(&'static str)
AppRuntimeError::ShuttingDown
```

### A.2 Config Types

```rust
AppRuntimeConfig { max_modules, startup_timeout_ms, shutdown_timeout_ms, health_check_interval_ms }
Preset::Development
Preset::Production
Preset::Minimal
```

### A.3 Identity Types

```rust
RuntimeId(u64)  // NIL = 0
RuntimeRole::Standalone  // = 0
RuntimeRole::Cluster     // = 1
RuntimeRole::Edge        // = 2
RuntimeRole::Controller  // = 3
```

### A.4 Stats Types

```rust
AppRuntimeStats { modules_loaded, modules_started, modules_failed, startups_total, shutdowns_total, health_checks_total }
StatsSnapshot { modules_loaded, modules_started, modules_failed, startups_total, shutdowns_total, health_checks_total }
```

### A.5 Health Types

```rust
HealthState::Healthy
HealthState::Degraded
HealthState::Critical
HealthReport { state, modules_loaded, modules_started, modules_failed, startups_total, shutdowns_total, health_checks_total, startup_success_rate_bps }
```

### A.6 Builder Types

```rust
AppRuntimeBuilder { cfg }
```

### A.7 Skeleton Types

```rust
AppRuntime { cfg, id, stats }
CompositionRoot { cfg, stats }
```

---

## Appendix B: Constant Reference

### B.1 Config Constants

```rust
MIN_MAX_MODULES: u32 = 1
MAX_MAX_MODULES: u32 = 1_024
MIN_STARTUP_TIMEOUT_MS: u64 = 100
MAX_STARTUP_TIMEOUT_MS: u64 = 300_000
MIN_SHUTDOWN_TIMEOUT_MS: u64 = 100
MAX_SHUTDOWN_TIMEOUT_MS: u64 = 300_000
MIN_HEALTH_CHECK_INTERVAL_MS: u64 = 100
MAX_HEALTH_CHECK_INTERVAL_MS: u64 = 300_000
```

### B.2 Health Thresholds

```rust
SUCCESS_HEALTHY_BPS: u16 = 9_900
SUCCESS_DEGRADED_BPS: u16 = 9_000
SUCCESS_CRITICAL_BPS: u16 = 9_000
```

### B.3 Identity Constants

```rust
RuntimeId::NIL: RuntimeId = RuntimeId(0)
RuntimeRole::Standalone: u8 = 0
RuntimeRole::Cluster: u8 = 1
RuntimeRole::Edge: u8 = 2
RuntimeRole::Controller: u8 = 3
```

---

## Appendix C: Re-Export Catalog

### C.1 Application-Layer Crate Re-Exports

| Crate | Re-Exported Types |
|-------|-------------------|
| configuration-system | ConfigBuilder, ConfigError, ConfigKey, ConfigLayer, ConfigSource, ConfigStats, ConfigValue, ConfigurationSystemConfig |
| version-negotiation | CompatEntry, CompatibilityMatrix, ProtocolVersion, VersionNegotiationBuilder, VersionNegotiationConfig, VersionNegotiationError, VersionNegotiationStats, VersionRange |
| feature-flags | EvaluationContext, FeatureFlagManager, FeatureFlagsBuilder, FeatureFlagsConfig, FeatureFlagsStats, FlagError, FlagKey, FlagRule, FlagState, FlagValue, RuleCondition |
| scheduler-hooks | HookPriority, HookTrigger, SchedulerHookError, SchedulerHookId, SchedulerHooksBuilder, SchedulerHooksConfig, SchedulerHooksStats |
| observability-sdk | LogLevel, LogRecord, MetricKey, MetricType, MetricValue, ObservabilityBuilder, ObservabilityConfig, ObservabilityError, ObservabilityStats, SpanId, SpanKind, TraceId |
| resource-quotas | QuotaError, QuotaLimit, QuotaUsage, ResourceQuotaStats, ResourceQuotasBuilder, ResourceQuotasConfig, TenantId, TenantTier, UsageSnapshot |
| module-lifecycle | LifecycleBuilder, LifecycleConfig, ModuleDescriptor, ModuleId, ModuleLifecycleError, ModuleLifecycleStats, ModulePhase |
| middleware-pipeline | MiddlewareConfig, MiddlewareContext, MiddlewareError, MiddlewareId, MiddlewarePipelineBuilder, MiddlewareStage, MiddlewareStats |
| plugin-manager | ApiVersion, PluginDescriptor, PluginError, PluginId, PluginLoader, PluginManager, PluginManagerBuilder, PluginManagerConfig, PluginManagerStats, PluginVersion |
| service-registry | ServiceEndpoint, ServiceId, ServiceInfo, ServiceInstance, ServiceRegistryBuilder, ServiceRegistryConfig, ServiceRegistryError, ServiceRegistryStats, ServiceState, WatchHandle |
| auth-framework | AuthConfig, AuthError, AuthFrameworkBuilder, AuthStats, AuthenticationPipeline, AuthorizationEngine, Credential, CredentialType, Permission, PermissionSet, PrincipalId, PrincipalType, Role, RoleAssignment, TokenManager |
| dependency-injection | BindingDescriptor, Container, ContainerBuilder, ContainerConfig, ContainerStats, DependencyError, DependencyResolver, ServiceLifetime, ServiceToken, TokenId |
| graceful-startup-shutdown | ConnectionDrainer, GracefulBuilder, GracefulConfig, GracefulStats, PhaseBarrier, ShutdownError, ShutdownPhase, StartupShutdownCoordinator, StartupPhase |
| rpc-framework | MethodDispatcher, RpcClient, RpcConfig, RpcError, RpcFrameworkBuilder, RpcMethodId, RpcMethodType, RpcServer, RpcStats, RpcStatus, StreamHandle |
| event-bus | ConsumerGroupId, ConsumerId, EventBusBuilder, EventBusConfig, EventBusStats, EventError, Offset, OffsetStore, PartitionId, PartitionRebalancer, PartitionState, SubscriptionMode, TopicConfig, TopicId |
| pubsub-engine | PubSubBuilder, PubSubConfig, PubSubEngine, PubSubError, PubSubMessage, PubSubStats, SubscriptionId, TopicPattern, TopicTrie |
| media-streaming | AdaptiveBitrateController, BitrateLadder, CodecConfig, CodecType, MediaError, MediaSession, MediaStreamingBuilder, MediaStreamingConfig, MediaStreamingStats, MediaTrackId, TrackDirection, TrackKind |
| ai-streaming | AiStreamBuilder, AiStreamConfig, AiStreamError, AiStreamId, AiStreamPipeline, AiStreamStats, StreamMode, TokenAggregator, TokenChunk, TokenType, ToolCall, ToolCallId, ToolStatus |
| agent-runtime | AgentContext, AgentExecutor, AgentId, AgentRuntime, AgentRuntimeBuilder, AgentRuntimeConfig, AgentRuntimeError, AgentRuntimeStats, AgentScheduler, AgentState, ToolDescriptor, ToolId, ToolResult |
| ops-dashboard | DashboardWidgetId, MetricsCollector, OpsDashboardBuilder, OpsDashboardConfig, OpsDashboardError, OpsDashboardService, OpsDashboardStats, QueryAggregation, QueryRange, WidgetType |

### C.2 Lower-Layer Crate Re-Exports

| Crate | Re-Exported Types |
|-------|-------------------|
| session-manager | QuotaSnapshot, ResourceQuota, SessionClass, SessionId, SessionManagerConfig, SessionManagerError, SessionState, SessionStats |
| session-registry | Generation, LeaseInfo, LeaseState, NodeAddress, NodeCapacity, NodeId, NodeInfo, NodeState, OwnershipEntry, OwnershipTable, Region, RegistryConfig, RegistryError, RegistryStats, SessionLocator, Zone |
| connection-shard | BackpressureLevel, ConnectionId, ConnectionPriority, ConnectionState, ConnectionShard, ConnectionShardBuilder, ConnectionSpec, ShardConfig, ShardError |
| stream-manager | StreamId, StreamPriority, StreamType, StreamManagerConfig, StreamManagerError, StreamState |
| reactor | CpuPinning, IdleStrategy, Reactor, ReactorBuilder, ReactorConfig, ReactorError, ReactorPreset, ReactorStats, TickBudget, TickResult |

---

## Appendix D: Verification Gates

### D.1 Cargo Check

```bash
cargo check -p application-runtime
```

Verifies that the crate compiles without errors.

### D.2 Cargo Clippy

```bash
cargo clippy -p application-runtime -- -D warnings
```

Verifies that there are no clippy warnings. Workspace lints deny:
`unwrap_used`, `expect_used`, `panic`, `todo`, `unimplemented`,
`unreachable`, `dbg_macro`, `print_stdout`, `print_stderr`.

### D.3 Cargo Fmt

```bash
cargo fmt -p application-runtime -- --check
```

Verifies that all code is properly formatted.

### D.4 Cargo Test

```bash
cargo test -p application-runtime
```

Runs all 72 unit tests. All must pass.

### D.5 Cargo Bench (No-Run)

```bash
cargo bench -p application-runtime --no-run
```

Verifies that all 4 bench files compile without errors or warnings.

---

## Appendix E: Workspace Lints

The workspace enforces strict clippy lints:

```toml
[workspace.lints]
clippy = {
    "unwrap_used" = "deny",
    "expect_used" = "deny",
    "panic" = "deny",
    "todo" = "deny",
    "unimplemented" = "deny",
    "unreachable" = "deny",
    "dbg_macro" = "deny",
    "print_stdout" = "deny",
    "print_stderr" = "deny"
}
```

These lints ensure production-quality code with no panicking,
no debug output, and no unimplemented stubs in non-skeleton code.

---

## Appendix F: Crate Metadata

```toml
[package]
name = "application-runtime"
version.workspace = true
edition.workspace = true
repository.workspace = true
authors.workspace = true
license.workspace = true
description = "Application runtime: top-level composition root for the Vireon application layer."
autobenches = false
```

- `autobenches = false`: Bench targets are explicitly declared.
- `version.workspace = true`: Inherits `0.1.0` from workspace.
- `edition.workspace = true`: Inherits `2024` from workspace.

---

*End of DESIGN.md*
