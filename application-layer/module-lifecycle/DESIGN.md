# Module Lifecycle System — Design Document

## Part 1: Overview

### 1.1 Purpose

The Module Lifecycle System provides dependency-ordered initialization, startup, and shutdown for application modules within the Vireon runtime. It ensures that modules start in the correct order based on their dependencies, detects circular dependencies, and supports graceful shutdown with failure rollback.

### 1.2 Scope

This system handles:
- Module registration and metadata management
- Dependency graph construction and validation
- Topological sorting for dependency-ordered execution
- Phase state machine (Registered → Initialized → Starting → Active → Stopping → Stopped)
- Failure detection and rollback
- Parallel and sequential initialization modes
- Health monitoring and observability

### 1.3 Architecture

```text
   ModuleId ──► [Dependency Graph] ──► Topo Sort ──► Phase Transitions
                     │
                     ├─ DependencyCycle Detection
                     ├─ Phase State Machine
                     ├─ Failure Rollback
                     └─ LifecycleCoordinator
```

### 1.4 Key Properties

1. **Dependency Safety**: Detects cycles before any initialization begins
2. **Ordering Guarantees**: Modules always initialize/start/stop in dependency order
3. **Failure Isolation**: One module's failure doesn't crash the coordinator
4. **Observability**: Every phase transition is recorded and exported
5. **Flexibility**: Supports both eager (parallel) and lazy (sequential) modes

## Part 2: Components

### 2.1 ModuleId

Unique identifier for application modules. Newtype around `u32` with reserved nil value (0).

**Operations**:
- `new(u32)` — Construct from non-zero value
- `nil()` — Construct nil identifier
- `is_nil()` — Check if nil
- `raw()` — Get underlying u32

**Invariants**:
- Real modules have ids in range `1..=u32::MAX`
- Nil (0) represents "no module"

### 2.2 ModulePhase

State machine tracking module lifecycle progression.

**States**:
1. `Registered` — Module known but not initialized
2. `Initialized` — Resources allocated, ready to start
3. `Starting` — Transitioning to active
4. `Active` — Serving requests
5. `Stopping` — Graceful shutdown in progress
6. `Stopped` — Resources released
7. `Failed` — Failed to initialize/start/stop

**Transitions**:
- Forward progress: Registered → Initialized → Starting → Active → Stopping → Stopped
- Any non-terminal → Failed
- No reverse transitions (except via re-registration)

### 2.3 ModuleDescriptor

Metadata describing an application module.

**Fields**:
- `id: ModuleId` — Unique identifier
- `name: [u8; 64]` — Fixed-size name array
- `version: u32` — Module version
- `dependencies: [ModuleId; 16]` — Up to 16 dependencies
- `priority: u8` — Initialization priority (0-255)
- `is_critical: bool` — Whether failure triggers shutdown

**Builder Pattern**:
```rust
let desc = ModuleDescriptor::new(ModuleId::new(1))
    .with_name("http-server")
    .with_version(2)
    .add_dependency(ModuleId::new(2))
    .with_priority(100)
    .with_critical(true);
```

### 2.4 LifecycleConfig

Declarative configuration for the lifecycle system.

**Fields**:
- `max_modules: u32` — Maximum modules (1-1024)
- `init_timeout_ms: u64` — Per-module init timeout (100-300,000ms)
- `start_timeout_ms: u64` — Per-module start timeout
- `stop_timeout_ms: u64` — Per-module stop timeout
- `parallel_init: bool` — Enable parallel initialization
- `fail_fast: bool` — Stop on first error

### 2.5 Preset

Pre-configured settings for common deployment scenarios.

**Variants**:
- `Eager` — Fast parallel init, short timeouts (dev/testing)
- `Lazy` — Sequential init, long timeouts (production)
- `Critical` — Fail-fast enabled, short timeouts (critical systems)

### 2.6 DependencyGraph

Maintains module dependency structure and provides topological ordering.

**Operations**:
- `add_edge(from, to)` — Add dependency edge
- `topo_sort()` — Compute topological ordering
- `has_cycle()` — Check for cycles

**Algorithm**:
- Uses Kahn's algorithm for topological sort
- DFS-based cycle detection
- O(V + E) complexity

### 2.7 LifecycleCoordinator

Main facade coordinating all lifecycle operations.

**Operations**:
- `register(desc)` — Register a module
- `init_all()` — Initialize all modules
- `start_all()` — Start all initialized modules
- `stop_all()` — Stop all active modules
- `health()` — Get health report

### 2.8 ModuleLifecycleStats

Atomic counters tracking lifecycle events.

**Counters**:
- `modules_registered` — Current registered count
- `modules_initialized` — Total initialized
- `modules_started` — Total started
- `modules_stopped` — Total stopped
- `modules_failed` — Total failed
- `init_time_ns_total` — Accumulated init time
- `shutdown_time_ns_total` — Accumulated shutdown time

### 2.9 HealthReport

Operational health derived from stats snapshot.

**Metrics**:
- `state` — Healthy/Degraded/Critical
- `modules_active` — Currently active modules
- `modules_failed` — Failed modules
- `failure_rate_bps` — Failure rate in basis points

**Thresholds**:
- Failure rate > 5% (500 bps) → Degraded
- Failure rate > 20% (2000 bps) → Critical

### 2.10 ModuleLifecycleError

Typed failure surface for lifecycle operations.

**Fatal Errors** (no retry):
- `DependencyCycle` — Circular dependency detected
- `InvalidTransition` — Illegal phase transition
- `ShuttingDown` — System shutting down

**Retryable Errors** (back off + retry):
- `InitFailed` — Initialization failed temporarily
- `StartFailed` — Startup failed temporarily
- `StopFailed` — Shutdown failed temporarily

**Capacity Errors** (shed load):
- `DuplicateModule` — Module already registered
- `AlreadyRunning` — Module already running
- `NotRunning` — Module not running
- `DependencyFailed` — Dependency failed

## Part 3: Dependency Resolution

### 3.1 Graph Construction

Dependencies form a directed acyclic graph (DAG):
- Nodes are modules (identified by ModuleId)
- Edges represent "depends on" relationships
- Edge A → B means "A depends on B"

### 3.2 Cycle Detection

Uses DFS-based detection:
1. Mark nodes as unvisited, visiting, visited
2. When exploring node, mark as visiting
3. If we encounter a visiting node, cycle exists
4. After exploring all neighbors, mark as visited

**Complexity**: O(V + E)

### 3.3 Topological Sorting

Uses Kahn's algorithm:
1. Compute in-degree for each node
2. Add all zero in-degree nodes to queue
3. While queue not empty:
   - Remove node from queue
   - Add to sorted result
   - Decrement in-degree of neighbors
   - Add zero in-degree neighbors to queue
4. If result length < node count, cycle exists

**Complexity**: O(V + E)

### 3.4 Execution Order

**Initialization**:
1. Topological sort yields dependency order
2. Initialize leaves first (no dependencies)
3. Move up to dependents

**Startup**:
1. Same order as initialization
2. Dependencies must be active before dependents start

**Shutdown**:
1. Reverse topological order
2. Shut down dependents first
3. Move down to dependencies

## Part 4: Phase State Machine

### 4.1 State Definitions

Each module progresses through phases:

| Phase | Description | Can Transition To |
|-------|-------------|-------------------|
| Registered | Module registered, not initialized | Initialized, Failed |
| Initialized | Resources allocated | Starting, Failed |
| Starting | Transitioning to active | Active, Failed |
| Active | Serving requests | Stopping, Failed |
| Stopping | Graceful shutdown | Stopped, Failed |
| Stopped | Shutdown complete | — |
| Failed | Failed in any phase | — |

### 4.2 Transition Validation

`can_transition_to(current, target)` returns true if legal:
- Same phase always allowed (no-op)
- Forward progress allowed (Registered → Initialized → ...)
- Any non-terminal can transition to Failed
- No reverse transitions
- No skipping phases

### 4.3 Transition Logging

Every transition is logged with:
- ModuleId
- From phase
- To phase
- Timestamp
- Result (success/failure)

## Part 5: Initialization Strategies

### 5.1 Parallel Initialization

**When enabled** (`parallel_init: true`):
- Multiple modules initialize concurrently
- Respects dependency ordering (must wait for dependencies)
- Uses thread pool or async runtime

**Benefits**:
- Faster startup for independent modules
- Better resource utilization

**Drawbacks**:
- Harder to debug
- Potential for race conditions
- More complex error handling

### 5.2 Sequential Initialization

**When disabled** (`parallel_init: false`):
- Modules initialize one at a time
- Strict dependency order
- Simpler debugging

**Benefits**:
- Predictable execution order
- Easier debugging
- Clearer error messages

**Drawbacks**:
- Slower startup
- Underutilized resources

### 5.3 Fail-Fast Mode

**When enabled** (`fail_fast: true`):
- Stop on first initialization error
- Don't attempt to initialize remaining modules
- Fast failure for critical systems

**When disabled**:
- Continue initializing remaining modules
- Report all errors at end
- Better for debugging multiple issues

## Part 6: Failure Handling

### 6.1 Failure Classification

**InitFailed**:
- Module initialization failed
- Typically resource allocation or config error
- Retryable if transient

**StartFailed**:
- Module startup failed
- Typically dependency or network error
- Retryable if transient

**StopFailed**:
- Module shutdown failed
- Typically resource cleanup error
- May require manual intervention

**DependencyFailed**:
- Dependency module failed
- This module cannot proceed
- Wait for dependency recovery

### 6.2 Rollback Strategy

On failure during initialization:
1. Stop all successfully initialized modules
2. Release all allocated resources
3. Return to Registered state
4. Report error

On failure during startup:
1. Stop all active modules
2. Return to Initialized state
3. Report error

On failure during shutdown:
1. Force terminate remaining modules
2. Log incomplete cleanup
3. Report error

### 6.3 Critical Modules

When `is_critical: true`:
- Failure triggers system shutdown
- All modules stopped immediately
- System enters degraded state

## Part 7: Timeouts

### 7.1 Init Timeout

Maximum time for single module initialization:
- Default: 5 seconds
- Range: 100ms - 300 seconds
- Per-module, not total

**Exceeded** → `InitFailed`

### 7.2 Start Timeout

Maximum time for single module startup:
- Default: 10 seconds
- Range: 100ms - 300 seconds
- Per-module, not total

**Exceeded** → `StartFailed`

### 7.3 Stop Timeout

Maximum time for single module shutdown:
- Default: 30 seconds
- Range: 100ms - 300 seconds
- Per-module, not total

**Exceeded** → Force terminate, log warning

## Part 8: Priority System

### 8.1 Priority Field

`priority: u8` (0 = lowest, 255 = highest)

**Usage**:
- When multiple modules are ready to initialize
- Higher priority modules go first
- Does not override dependency ordering

### 8.2 Priority vs Dependencies

Dependencies always override priority:
- If A depends on B, B initializes first regardless of priority
- Priority only breaks ties among independent modules

## Part 9: Observability

### 9.1 Counters

All operations increment atomic counters:
- `modules_registered` — On registration
- `modules_initialized` — On successful init
- `modules_started` — On successful start
- `modules_stopped` — On successful stop
- `modules_failed` — On any failure
- `init_time_ns_total` — Accumulate init time
- `shutdown_time_ns_total` — Accumulate shutdown time

### 9.2 Snapshot

Point-in-time snapshot via `snapshot()`:
- Reads all counters with Relaxed ordering
- Computes rates (failure_rate_bps)
- Computes averages (avg_init_time_ns, avg_shutdown_time_ns)

### 9.3 Health State

Derived from snapshot:
- `Healthy` — Failure rate ≤ 5%
- `Degraded` — Failure rate 5-20%
- `Critical` — Failure rate > 20%

## Part 10: Thread Safety

### 10.1 Stats

`ModuleLifecycleStats` uses `AtomicU64`:
- All operations lock-free
- Relaxed ordering (advisory stats)
- Safe for concurrent access

### 10.2 Coordinator

Phase 1 coordinator will use:
- Mutex for module registry
- RwLock for dependency graph
- Channels for coordination

## Part 11: Failure Modes

### 11.1 Dependency Cycle

**Detection**: During graph construction or topo_sort

**Impact**: Fatal — no modules can initialize

**Recovery**:
1. Remove circular dependency
2. Re-register modules
3. Retry initialization

### 11.2 Init Timeout

**Detection**: Module init exceeds timeout

**Impact**: Retryable — module fails to initialize

**Recovery**:
1. Fix slow initialization
2. Increase timeout
3. Retry initialization

### 11.3 Dependency Failed

**Detection**: Dependency module failed

**Impact**: Retryable — this module waits

**Recovery**:
1. Fix dependency
2. Re-initialize dependency
3. Resume initialization

### 11.4 Shutting Down

**Detection**: System shutting down

**Impact**: Fatal — reject new operations

**Recovery**:
1. Wait for shutdown complete
2. Restart system
3. Re-initialize

## Part 12: Configuration

### 12.1 Minimal Config

```rust
let cfg = LifecycleConfig::new();
```

Defaults:
- 256 max modules
- 5s init timeout
- 10s start timeout
- 30s stop timeout
- Parallel init enabled
- Fail-fast disabled

### 12.2 Preset Config

```rust
let cfg = Preset::Eager.to_config();
```

### 12.3 Custom Config

```rust
let cfg = LifecycleBuilder::new()
    .max_modules(512)
    .init_timeout_ms(10_000)
    .parallel_init(false)
    .fail_fast(true)
    .build()?;
```

## Part 13: Performance

### 13.1 Complexity

| Operation | Complexity | Notes |
|-----------|------------|-------|
| Register | O(1) | Add to registry |
| Add edge | O(1) | Add to adjacency list |
| Cycle detect | O(V + E) | DFS traversal |
| Topo sort | O(V + E) | Kahn's algorithm |
| Init all | O(V * T) | V modules, avg init time T |
| Start all | O(V * T) | V modules, avg start time T |
| Stop all | O(V * T) | V modules, avg stop time T |

### 13.2 Memory

Per-module overhead:
- ModuleDescriptor: ~320 bytes
- Graph node: ~64 bytes
- Graph edges: ~16 bytes per edge

Total for 256 modules with 4 deps each:
- Descriptors: 82 KB
- Graph: 20 KB
- Total: ~100 KB

## Part 14: Testing

### 14.1 Unit Tests

Each module has comprehensive tests:
- Newtype constructors and predicates
- State machine transitions
- Config validation
- Error classification
- Stats accumulation

### 14.2 Integration Tests

Phase 1 will add:
- Full initialization sequence
- Dependency ordering
- Cycle detection
- Failure rollback
- Health state transitions

### 14.3 Benchmarks

Phase 1 will measure:
- Topological sort performance
- Phase transition cost
- Init sequence throughput
- Shutdown sequence throughput

## Part 15: Future Work

### 15.1 Phase 1+

- Real dependency graph implementation
- Parallel initialization with thread pool
- Proper failure rollback
- Health-based auto-restart
- Dynamic module registration/deregistration

### 15.2 Phase 2+

- Hot module reloading
- Dependency versioning
- Multi-version compatibility
- Distributed lifecycle coordination
- Lifecycle event streaming

### 15.3 Phase 3+

- AI-driven failure prediction
- Automatic timeout tuning
- Dependency optimization
- Lifecycle analytics dashboard
- Cross-node lifecycle orchestration
