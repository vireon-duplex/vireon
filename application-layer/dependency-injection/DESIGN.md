# Dependency Injection Container - Design Specification

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Service Tokens](#3-service-tokens)
4. [Lifetime Management](#4-lifetime-management)
5. [Service Bindings](#5-service-bindings)
6. [Dependency Resolution](#6-dependency-resolution)
7. [Container Core](#7-container-core)
8. [Configuration](#8-configuration)
9. [Error Handling](#9-error-handling)
10. [Observability](#10-observability)
11. [Performance](#11-performance)
12. [Security](#12-security)
13. [Testing](#13-testing)
14. [Future Work](#14-future-work)

---

## 1. Overview

### 1.1 Purpose

The Dependency Injection (DI) Container provides runtime service composition, lifetime management, and dependency graph resolution for the Vireon application layer. It enables loose coupling between components while maintaining type safety through Rust's type system.

### 1.2 Key Features

- **Type-safe service tokens** through `ServiceToken<T>` generic wrappers
- **Lifetime management** supporting singleton, scoped, and transient services
- **Dependency graph resolution** with cycle detection
- **Configuration presets** for common deployment patterns
- **Observability** through atomic statistics and health reporting
- **Zero-allocation** hot path where possible

### 1.3 Design Philosophy

The container follows these principles:

1. **Type Safety First**: Leverage Rust's type system to prevent class casting errors at compile time
2. **Explicit Lifetimes**: Service lifetimes are declared explicitly, not inferred
3. **Fail Fast**: Configuration errors are caught at startup, not runtime
4. **Observable**: Every operation is instrumented for metrics and debugging
5. **Zero-Cost Abstractions**: The container adds minimal overhead compared to manual DI

---

## 2. Architecture

### 2.1 Layer Structure

```
┌─────────────────────────────────────────────────────────────┐
│                     Application Layer                         │
├─────────────────────────────────────────────────────────────┤
│  ┌────────────────────────────────────────────────────────┐ │
│  │            Dependency Injection Container                 │ │
│  │                                                           │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │ │
│  │  │   Service    │  │   Binding    │  │  Lifetime     │ │ │
│  │  │   Tokens     │  │  Descriptor  │  │  Management   │ │ │
│  │  └──────────────┘  └──────────────┘  └──────────────┘ │ │
│  │                                                           │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │ │
│  │  │   Resolver   │  │   Container   │  │   Stats/      │ │ │
│  │  │              │  │              │  │   Health      │ │ │
│  │  └──────────────┘  └──────────────┘  └──────────────┘ │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 Module Responsibilities

| Module | Responsibility | Phase |
|--------|---------------|-------|
| `error` | Typed error enumeration | 0 ✅ |
| `config` | Configuration and presets | 0 ✅ |
| `token` | Service token identifiers | 0 ✅ |
| `lifetime` | Lifetime policy enumeration | 0 ✅ |
| `binding` | Service binding metadata | 0 ✅ |
| `stats` | Atomic statistics collection | 0 ✅ |
| `health` | Health reporting from stats | 0 ✅ |
| `builder` | Fluent configuration builder | 0 ✅ |
| `container` | Registration and resolution facade | 0 (skeleton) |
| `resolver` | Dependency graph resolution | 0 (skeleton) |

### 2.3 Data Flow

```
Registration Flow:
┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│ Application  │ ───► │ Container    │ ───► │ Binding       │
│ Code         │      │ register()   │      │ Descriptor    │
└──────────────┘      └──────────────┘      └──────────────┘
                                                    │
                                                    ▼
                                            ┌──────────────┐
                                            │ Dependency   │
                                            │ Graph        │
                                            └──────────────┘

Resolution Flow:
┌──────────────┐      ┌──────────────┐      ┌──────────────┐
│ Application  │ ───► │ Container    │ ───► │ Resolver     │
│ Code         │      │ resolve()    │      │              │
└──────────────┘      └──────────────┘      └──────────────┘
                                                    │
                                                    ▼
                                            ┌──────────────┐
                                            │ Instance     │
                                            │ (maybe new)  │
                                            └──────────────┘
```

---

## 3. Service Tokens

### 3.1 TokenId

`TokenId` is the internal representation of service identifiers:

```rust
pub struct TokenId(u64);

impl TokenId {
    pub const NIL: Self = Self(0);  // Represents absence
    pub const fn new(id: u64) -> Self;
    pub const fn is_nil(self) -> bool;
    pub const fn raw(self) -> u64;
}
```

**Design Decisions:**

- `u64` provides sufficient address space (2^64 possible services)
- `NIL` at 0 allows efficient nil-checks
- `const fn` enables compile-time token creation
- `Copy` semantics for efficient passing

### 3.2 ServiceToken<T>

`ServiceToken<T>` provides type-safe service identifiers:

```rust
pub struct ServiceToken<T> {
    id: TokenId,
    _phantom: PhantomData<*const ()>,
}

impl<T> ServiceToken<T> {
    pub const fn new(id: TokenId) -> Self;
    pub const fn id(self) -> TokenId;
    pub const fn is_nil(self) -> bool;
}
```

**Design Decisions:**

- `PhantomData<*const ()>` makes the token:
  - **Invariant** over `T` (prevents problematic subtyping)
  - **Send + Sync** for all `T` (enables cross-thread usage)
- `const fn` allows compile-time token construction
- `Copy` semantics avoid clones

### 3.3 Token Allocation

Token IDs are allocated sequentially during registration:

```
Registration #1 → TokenId(1)
Registration #2 → TokenId(2)
Registration #3 → TokenId(3)
...
```

The container maintains a monotonically increasing counter, ensuring unique IDs without collision detection overhead.

---

## 4. Lifetime Management

### 4.1 ServiceLifetime Enum

```rust
#[repr(u8)]
pub enum ServiceLifetime {
    Singleton = 0,   // One instance for container lifetime
    Scoped    = 1,   // One instance per scope
    Transient = 2,   // New instance per resolution
}
```

### 4.2 Lifetime Behavior

| Lifetime | Instance Creation | Instance Disposal | Thread Safety |
|----------|------------------|-------------------|---------------|
| Singleton | First resolution only | Container shutdown | Sync required |
| Scoped | First resolution in scope | Scope disposal | Sync required |
| Transient | Every resolution | Immediately after use | Case-dependent |

### 4.3 Singleton Semantics

Singleton services are stored in the container and reused:

```
Container Startup
    │
    ├─ Service A registered (singleton)
    │
    ▼
First resolve(A) → Create instance → Store in container
Subsequent resolve(A) → Return stored instance
Container shutdown → Drop instance
```

**Use Cases:**
- Configuration objects
- Shared connection pools
- Global state caches

### 4.4 Scoped Semantics

Scoped services are stored per-scope:

```
Scope #1 created
    │
    ├─ resolve(A) → Create instance for Scope #1
    │
Scope #2 created
    │
    ├─ resolve(A) → Create NEW instance for Scope #2
    │
Scope #1 disposed → Drop A instance for Scope #1
Scope #2 disposed → Drop A instance for Scope #2
```

**Use Cases:**
- Request-scoped transaction handlers
- Per-session user context
- Temporary resource allocations

### 4.5 Transient Semantics

Transient services are created anew each time:

```
resolve(A) → Create instance #1 → Use → Drop
resolve(A) → Create instance #2 → Use → Drop
resolve(A) → Create instance #3 → Use → Drop
```

**Use Cases:**
- Lightweight stateless objects
- Disposable resources
- Temporary computations

---

## 5. Service Bindings

### 5.1 BindingDescriptor Structure

```rust
pub struct BindingDescriptor {
    pub token: TokenId,                    // Service identifier
    pub lifetime: ServiceLifetime,         // Lifetime policy
    pub factory_hash: u64,                 // Factory function ID
    pub dependencies: [TokenId; 8],        // Dependency list
    pub dep_count: u8,                     // Valid dependency count
    pub is_initialized: bool,              // Whether factory has run
}
```

### 5.2 Dependency Storage

Dependencies are stored in a fixed-size array:

- **Capacity**: 8 dependencies per service
- **Rationale**: Covers 99% of real-world cases without heap allocation
- **Overflow**: Returns error if exceeded (compile-time check possible)

```rust
pub const MAX_DEPS: usize = 8;

pub fn add_dependency(mut self, dep: TokenId) -> Result<Self, &'static str> {
    if self.dep_count >= Self::MAX_DEPS as u8 {
        return Err("dependency array full");
    }
    self.dependencies[self.dep_count as usize] = dep;
    self.dep_count += 1;
    Ok(self)
}
```

### 5.3 Factory Hash

The `factory_hash` uniquely identifies the factory function:

```rust
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

fn hash_factory<T>(f: fn() -> T) -> u64 {
    let mut hasher = DefaultHasher::new();
    f.hash(&mut hasher);  // Hash function pointer
    hasher.finish()
}
```

**Purpose:**
- Detect duplicate registrations
- Enable factory-based caching
- Support dependency graph edge identification

---

## 6. Dependency Resolution

### 6.1 Resolution Algorithm

The resolver performs topological sort:

```
1. Build adjacency list from bindings
2. Perform DFS with cycle detection
3. Return nodes in post-order (dependencies before dependents)
```

### 6.2 Cycle Detection

Cycle detection uses the classic DFS three-color algorithm:

```rust
enum VisitState {
    Unvisited,
    Visiting,  // In current recursion stack
    Visited,
}
```

If we encounter a `Visiting` node, we've found a cycle.

### 6.3 Resolution Phases

| Phase | Operation | Error Condition |
|-------|----------|-----------------|
| 1 | Validate all dependencies exist | `ServiceNotRegistered` |
| 2 | Detect cycles | `CircularDependency` |
| 3 | Topological sort | `ResolutionFailed` |
| 4 | Instantiate in order | Factory errors |

---

## 7. Container Core

### 7.1 Container Structure

```rust
pub struct Container {
    cfg: ContainerConfig,
    stats: ContainerStats,
    binding_count: u32,
    // Phase 1+: actual storage, scope map, resolver
}
```

### 7.2 Registration API

```rust
impl Container {
    pub fn register(&mut self, binding: BindingDescriptor) -> Result<(), DependencyError>;
}
```

**Registration Steps (Phase 1+):**
1. Check if token already registered
2. Validate configuration (if enabled)
3. Store binding
4. Update statistics

### 7.3 Resolution API

```rust
impl Container {
    pub fn resolve(&mut self, token: TokenId) -> Result<(), DependencyError>;
}
```

**Resolution Steps (Phase 1+):**
1. Look up binding
2. Check lifecycle (create/reuse)
3. Resolve dependencies recursively
4. Invoke factory
5. Cache if needed

### 7.4 Scope API

```rust
impl Container {
    pub fn create_scope(&mut self) -> Result<u64, DependencyError>;
    pub fn dispose_scope(&mut self, scope_id: u64) -> Result<(), DependencyError>;
}
```

**Scope Lifecycle (Phase 1+):**
```
create_scope()
    │
    ├─ Allocate scope ID
    ├─ Create scope-local storage
    │
    ▼
[use scope for resolutions]
    │
    ▼
dispose_scope(id)
    │
    ├─ Drop all scoped instances
    ├─ Remove scope-local storage
    │
    ▼
Scope resources cleaned up
```

---

## 8. Configuration

### 8.1 ContainerConfig

```rust
pub struct ContainerConfig {
    pub max_services: u32,              // Capacity limit
    pub max_scope_depth: u8,            // Nesting limit
    pub enable_lazy_resolution: bool,    // Resolution strategy
    pub default_lifetime: ServiceLifetime,  // Default policy
    pub validate_on_register: bool,      // Validation timing
}
```

### 8.2 Configuration Presets

#### Strict Preset

```rust
Preset::Strict → ContainerConfig {
    enable_lazy_resolution: false,
    validate_on_register: true,
    default_lifetime: Singleton,
    // ...
}
```

**Use Case:** High-reliability systems where errors should surface immediately.

#### Lazy Preset

```rust
Preset::Lazy → ContainerConfig {
    enable_lazy_resolution: true,
    validate_on_register: false,
    default_lifetime: Transient,
    // ...
}
```

**Use Case:** High-throughput systems where startup time is critical.

### 8.3 Builder Pattern

```rust
let cfg = ContainerBuilder::new()
    .max_services(100)
    .default_lifetime(ServiceLifetime::Scoped)
    .validate_on_register(true)
    .build()?;
```

---

## 9. Error Handling

### 9.1 Error Taxonomy

```rust
pub enum DependencyError {
    // Fatal errors (do not retry)
    CircularDependency(TokenId),
    TypeMismatch(&'static str),
    ShuttingDown,

    // Retryable errors
    ResolutionFailed(&'static str),

    // Configuration errors
    ServiceNotRegistered(TokenId),
    AlreadyRegistered(TokenId),
    ScopeClosed,
}
```

### 9.2 Error Classification

| Method | Returns True For |
|--------|-----------------|
| `is_fatal()` | CircularDependency, TypeMismatch, ShuttingDown |
| `is_retryable()` | ResolutionFailed |
| `is_configuration_error()` | ServiceNotRegistered, AlreadyRegistered |

### 9.3 Error Propagation

```rust
pub fn resolve(&mut self, token: TokenId) -> Result<T, DependencyError> {
    self.stats.record_resolution_attempt();

    let binding = self.bindings.get(&token)
        .ok_or(DependencyError::ServiceNotRegistered(token))?;

    if let Some(cycle) = self.detect_cycle(token) {
        self.stats.record_circular_error();
        return Err(DependencyError::CircularDependency(cycle));
    }

    // ... resolution logic
}
```

---

## 10. Observability

### 10.1 Statistics Collection

```rust
pub struct ContainerStats {
    // Counters
    pub registrations_total: AtomicU64,
    pub resolutions_total: AtomicU64,
    pub resolutions_succeeded: AtomicU64,
    pub resolutions_failed: AtomicU64,
    pub circular_errors: AtomicU64,

    // Gauges
    pub singletons_active: AtomicU64,
    pub scopes_active: AtomicU64,
}
```

### 10.2 Snapshot API

```rust
impl ContainerStats {
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            resolutions_total: self.resolutions_total.load(Relaxed),
            resolutions_succeeded: self.resolutions_succeeded.load(Relaxed),
            // ...
        }
    }
}
```

**Why `Relaxed` ordering?**
- Stats are advisory, never load-bearing
- Trade consistency for throughput
- Momentary inconsistencies (e.g., total > succeeded + failed) are acceptable

### 10.3 Health Reporting

```rust
pub struct HealthReport {
    pub state: HealthState,
    pub registrations_total: u64,
    pub resolution_failure_rate_bps: u16,
    pub circular_errors: u64,
}
```

**Health Classification:**

| State | Condition | Action |
|-------|-----------|--------|
| Healthy | failure_rate ≤ 5%, circular_errors = 0 | None |
| Degraded | 5% < failure_rate ≤ 20%, circular_errors = 1 | Monitor |
| Critical | failure_rate > 20%, circular_errors ≥ 5 | Page |

### 10.4 Metric Mapping

Each counter maps to a Prometheus metric:

| Counter | Type | Name |
|---------|------|------|
| `registrations_total` | counter | `di_registrations_total` |
| `resolutions_total` | counter | `di_resolutions_total` |
| `resolutions_succeeded` | counter | `di_resolutions_succeeded_total` |
| `resolutions_failed` | counter | `di_resolutions_failed_total` |
| `singletons_active` | gauge | `di_singletons_active` |
| `scopes_active` | gauge | `di_scopes_active` |
| `circular_errors` | counter | `di_circular_errors_total` |

---

## 11. Performance

### 11.1 Allocation Strategy

**Zero-allocation hot path:**
- `TokenId` and `ServiceToken<T>` are `Copy`
- `BindingDescriptor` stores dependencies inline (array of 8)
- Resolution uses pre-allocated buffers where possible

**Heap allocations:**
- Binding storage (hash map)
- Scope instance storage (hash map per scope)
- Resolution result vectors (can reuse with capacity)

### 11.2 Lock Contention

**Phase 1+ strategy:**
- Read-write lock for binding storage (read-heavy)
- Per-scope mutexes for scoped instances
- Lock-free statistics with `AtomicU64`

### 11.3 Resolution Caching

**Lazy resolution (Phase 1+):**
- Build dependency graph on first resolution
- Cache topological order
- Reuse order for subsequent resolutions

**Trade-off:** First resolution slower, subsequent resolutions faster.

### 11.4 Benchmark Targets

| Operation | Target | Notes |
|-----------|--------|-------|
| Token creation | < 1 ns | Inline, trivial |
| Binding creation | < 10 ns | Stack allocation |
| Registration | < 100 ns | Hash map insert |
| Singleton resolution | < 50 ns | Cached lookup |
| Transient resolution | < 200 ns | Factory call + drop |

---

## 12. Security

### 12.1 Input Validation

**Token validation:**
- Reject NIL token for registration
- Validate token range if bounded

**Configuration validation:**
- Check max_services ∈ [1, 512]
- Check max_scope_depth ∈ [1, 32]
- Validate presets on load

### 12.2 Resource Limits

**Capacity controls:**
```rust
if self.binding_count >= self.cfg.max_services {
    return Err(DependencyError::TypeMismatch("capacity exceeded"));
}
```

**Depth limits:**
```rust
if self.current_scope_depth >= self.cfg.max_scope_depth {
    return Err(DependencyError::TypeMismatch("max depth exceeded"));
}
```

### 12.3 Panic Safety

The container is panic-safe:
- No `unwrap()` or `expect()` in production code
- All fallible operations return `Result`
- Statistics use saturating arithmetic

---

## 13. Testing

### 13.1 Unit Test Strategy

**Per-module tests:**
- Each module has comprehensive `#[cfg(test)]` blocks
- Tests cover public API surface and edge cases
- Assertions use `assert_eq!`, not regex on error messages

**Test coverage goals:**
- All public functions have tests
- All error variants are exercised
- All predicates (`is_*` methods) are verified

### 13.2 Integration Tests (Phase 1+)

**Scenario tests:**
- Full registration → resolution cycle
- Multi-level dependency chains
- Scope lifecycle
- Error recovery paths

### 13.3 Benchmark Tests

**Benchmark coverage:**
- Token creation and manipulation
- Binding creation with dependency counts
- Resolution with various lifetimes
- Scope creation and disposal

---

## 14. Future Work

### 14.1 Phase 1+ Features

**Container completion:**
- Actual binding storage and lookup
- Full resolution with dependency graph traversal
- Scope lifecycle management
- Instance caching by lifetime

**Resolver completion:**
- Topological sort implementation
- Cycle detection with detailed reporting
- Resolution result caching

### 14.2 Advanced Features (Phase 2+)

**Async support:**
- `async fn` factory registration
- Async resolution with dependency tracking
- Per-scope async runtime integration

**Decorator pattern:**
- Service decorators/proxies
- Aspect-oriented programming hooks
- Interceptor chains

**Conditional registration:**
- `if_*` methods for conditional registration
- Feature flag integration
- Environment-specific bindings

**Distributed DI:**
- Cross-process service resolution
- Cluster-wide singleton coordination
- Distributed scoped contexts

### 14.3 Performance Optimizations

**Memory pool:**
- Pre-allocated token ID pool
- Binding descriptor arena allocation
- Scope instance reuse

**Concurrent resolution:**
- Parallel dependency resolution
- Lock-free scope instances
- Optimistic concurrency control

---

## Appendix A: Type Erasure

The container uses type erasure to store heterogeneous services:

```rust
// Type-erased service storage
struct ServiceInstance {
    instance: Box<dyn Any>,  // Stores any type
    token: TokenId,
    lifetime: ServiceLifetime,
}
```

**Downcasting:**
```rust
impl Container {
    pub fn resolve<T: 'static>(&self, token: ServiceToken<T>) -> Result<&T, DependencyError> {
        let instance = self.instances.get(&token.id())
            .ok_or(DependencyError::ServiceNotRegistered(token.id()))?;

        instance.downcast_ref::<T>()
            .ok_or(DependencyError::TypeMismatch("wrong type"))
    }
}
```

---

## Appendix B: Thread Safety

**Sync requirements:**
- Singleton instances: `T: Sync + Send`
- Scoped instances: `T: Sync` (within scope)
- Transient instances: No requirement (immediate use)

**Container synchronization:**
- Binding storage: `RwLock<HashMap<TokenId, BindingDescriptor>>`
- Instance storage: `RwLock<HashMap<TokenId, ServiceInstance>>`
- Statistics: `AtomicU64` per counter

---

## Appendix C: Examples

### Example 1: Basic Registration and Resolution

```rust
use dependency_injection::*;

let cfg = ContainerBuilder::new()
    .default_lifetime(ServiceLifetime::Singleton)
    .build()?;

let mut container = Container::new(cfg);

// Register a service
let token = ServiceToken::<MyService>::new(TokenId::new(1));
let binding = BindingDescriptor::new(token.id(), ServiceLifetime::Singleton);
container.register(binding)?;

// Resolve the service
let instance: &MyService = container.resolve(token)?;
```

### Example 2: Scoped Services

```rust
let mut container = Container::new(cfg);

// Create a scope
let scope_id = container.create_scope()?;

// Resolve within scope (creates scoped instance)
let instance: &MyScopedService = container.resolve(token)?;

// Dispose scope (drops scoped instance)
container.dispose_scope(scope_id)?;
```

### Example 3: Dependency Chain

```rust
// Service depends on Service B, which depends on Service C
let binding_a = BindingDescriptor::new(token_a, ServiceLifetime::Transient)
    .add_dependency(token_b)
    .unwrap();

let binding_b = BindingDescriptor::new(token_b, ServiceLifetime::Transient)
    .add_dependency(token_c)
    .unwrap();

let binding_c = BindingDescriptor::new(token_c, ServiceLifetime::Transient);

container.register(binding_a)?;
container.register(binding_b)?;
container.register(binding_c)?;

// Resolution order: C → B → A
let instance_a = container.resolve(token_a)?;
```

---

*Document Version: 1.0*
*Last Updated: 2026-07-07*
