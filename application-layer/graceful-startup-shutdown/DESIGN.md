# Graceful Startup and Shutdown System — Design Document

## Overview

The Graceful Startup and Shutdown system provides orchestrated, phase-ordered boot sequence and crash-safe shutdown with connection draining for the Vireon runtime. It ensures that all components start in the correct order, dependencies are satisfied before services accept traffic, and shutdown completes cleanly with proper resource cleanup.

### Key Features

- **Phase-ordered startup**: Components start in a defined sequence with dependency validation
- **Crash-safe shutdown**: Graceful termination with rollback on failure
- **Connection draining**: Active connections are given time to complete before forced closure
- **Timeout handling**: Configurable timeouts with optional forced termination
- **Health monitoring**: Real-time health reporting based on startup/shutdown metrics
- **Observable**: Comprehensive statistics and metrics for operational visibility

## Architecture

### One-Line Architecture

```
StartupRequest ──► [Phase Machine] ──► GracefulShutdown
                      │
                      ├─ StartupShutdownCoordinator
                      ├─ ConnectionDrainer
                      ├─ PhaseBarrier
                      └─ Health & Stats
```

## Startup Sequence

### Phase Machine

The startup sequence is defined as a state machine with the following phases:

1. **Init**: Initial state, no initialization performed
2. **LoadConfig**: Load and validate configuration
3. **BindPorts**: Bind network addresses and ports
4. **AcceptConnections**: Begin accepting incoming connections
5. **Ready**: System fully operational

### Phase Transitions

```rust
Init → LoadConfig → BindPorts → AcceptConnections → Ready
```

Each phase must complete successfully before advancing to the next. The system validates that transitions respect the phase order.

### Startup Behavior

- Startup begins with a call to `StartupShutdownCoordinator::startup()`
- Each phase must complete before `advance_startup()` succeeds
- Configuration specifies timeout for each phase
- Health checks can be enabled to validate component readiness
- Connections are accepted only at or after the configured phase

### Error Handling

- **PhaseTimeout**: A phase failed to complete within its timeout
- **DependencyFailed**: A required dependency failed to start
- **StartupFailed**: Fatal error during startup, abort sequence
- **InvalidPhase**: Attempted to transition to an invalid phase

## Shutdown Sequence

### Phase Machine

The shutdown sequence is defined as a state machine with the following phases:

1. **Running**: System operational, not shutting down
2. **StopAccepting**: Stop accepting new connections
3. **DrainConnections**: Wait for active connections to complete
4. **StopModules**: Stop application modules in reverse order
5. **FlushState**: Flush state to persistent storage
6. **ReleaseResources**: Release system resources (memory, file handles)
7. **Complete**: Shutdown complete

### Phase Transitions

```rust
Running → StopAccepting → DrainConnections → StopModules → FlushState → ReleaseResources → Complete
```

### Shutdown Behavior

- Shutdown begins with a call to `begin_shutdown()`
- Each phase completes before advancing to the next
- Connection draining gives active connections time to complete gracefully
- After drain timeout, remaining connections are force-closed if configured
- State is flushed before resources are released
- Shutdown completes with all resources released

### Error Handling

- **PhaseTimeout**: A phase failed to complete within its timeout
- **DependencyFailed**: A component failed to stop cleanly
- **ForcedShutdown**: Emergency termination, resources may not be released cleanly
- **AlreadyShuttingDown**: Shutdown already in progress

## Phase Machine

### StartupPhase Enum

```rust
pub enum StartupPhase {
    Init = 0,
    LoadConfig = 1,
    BindPorts = 2,
    AcceptConnections = 3,
    Ready = 4,
}
```

**Methods:**
- `is_ready()`: Check if phase is Ready
- `is_active()`: Check if phase is not Init
- `can_transition_to(target)`: Validate transition to target phase
- `as_str()`: Human-readable phase name
- `order()`: Numeric order for comparison

### ShutdownPhase Enum

```rust
pub enum ShutdownPhase {
    Running = 0,
    StopAccepting = 1,
    DrainConnections = 2,
    StopModules = 3,
    FlushState = 4,
    ReleaseResources = 5,
    Complete = 6,
}
```

**Methods:**
- `is_complete()`: Check if phase is Complete
- `is_draining()`: Check if in draining phase
- `can_transition_to(target)`: Validate transition to target phase
- `as_str()`: Human-readable phase name
- `order()`: Numeric order for comparison

## Connection Draining

### Drain Process

1. **Stop Accepting**: New connections are rejected
2. **Drain Active**: Active connections are monitored for completion
3. **Timeout**: After drain timeout, remaining connections are force-closed
4. **Complete**: All connections closed or drained

### ConnectionDrainer API

```rust
impl ConnectionDrainer {
    pub fn new(cfg: GracefulConfig) -> Self;
    pub fn begin_drain(&mut self) -> Result<u64, ShutdownError>;
    pub fn drain_step(&mut self) -> Result<u64, ShutdownError>;
    pub fn is_drained(&self) -> bool;
    pub fn force_close(&mut self) -> Result<u64, ShutdownError>;
    pub fn active_connections(&self) -> u64;
}
```

### Drain Behavior

- `begin_drain()` starts the drain process and returns active connection count
- `drain_step()` performs one drain iteration and returns remaining count
- `is_drained()` checks if all connections are complete
- `force_close()` forcibly closes remaining connections
- Active connections are tracked in real-time

## Barrier Synchronization

### PhaseBarrier

The `PhaseBarrier` provides synchronization for phase completion:

```rust
pub struct PhaseBarrier {
    pub phase_id: u8,
    pub completion_count: u32,
    pub is_complete: bool,
    pub started_ns: u64,
    pub completed_ns: u64,
}
```

**Methods:**
- `new(phase_id, started_ns)`: Create a new barrier
- `mark_progress()`: Record progress within the phase
- `mark_complete(now_ns)`: Mark the phase as complete
- `is_done()`: Check if phase is complete
- `duration_ns(now_ns)`: Get phase duration in nanoseconds

### Barrier Usage

Barriers are created when a phase begins and used to track completion:

1. Create barrier on phase entry
2. Call `mark_progress()` for each completion step
3. Call `mark_complete()` when phase finishes
4. Use `duration_ns()` to measure phase time

## Timeout & Force

### Configuration

Timeouts are configured per-phase:

```rust
pub struct GracefulConfig {
    pub startup_timeout_ms: u64,      // Total startup timeout
    pub shutdown_timeout_ms: u64,     // Total shutdown timeout
    pub drain_timeout_ms: u64,        // Connection drain timeout
    pub force_on_timeout: bool,       // Force on timeout?
    pub enable_health_checks: bool,   // Enable health checks
    pub accept_connections_at_phase: StartupPhase,
}
```

### Timeout Behavior

- **Soft timeout**: Log warning, continue waiting
- **Hard timeout**: Force next phase or terminate
- **Force on timeout**: If enabled, forcibly terminate after timeout
- **No force**: If disabled, wait indefinitely or until manual intervention

### Force Shutdown

When `force_on_timeout` is true:

1. Startup timeout → Force startup to complete, may start incomplete
2. Shutdown timeout → Force termination, may skip cleanup
3. Drain timeout → Force close all connections

## Crash Recovery

### Startup Failure Recovery

1. Detect failure phase and cause
2. Rollback to previous stable phase
3. Clean up partial state
4. Return error with details
5. Allow retry if error is retryable

### Shutdown Failure Recovery

1. Detect failure phase and cause
2. Force shutdown if not already forced
3. Release remaining resources
4. Log error for post-mortem
5. Exit with error code

### State Persistence

- Startup progress is tracked in memory
- On crash, system restarts from Init phase
- No persistent state is maintained in Phase 0
- Future phases will implement state persistence

## Failure Modes

### Error Classification

Errors are classified into three categories:

1. **Fatal**: Must not retry
   - `ForcedShutdown`
   - `StartupFailed`
   - `ShuttingDown`

2. **Retryable**: May retry after backoff
   - `PhaseTimeout`
   - `DependencyFailed`

3. **Lifecycle**: State-based, check before retry
   - `AlreadyShuttingDown`
   - `NotStarted`
   - `InvalidPhase`

### Error Handling

```rust
pub enum ShutdownError {
    PhaseTimeout(&'static str),
    DependencyFailed(&'static str),
    ForcedShutdown,
    AlreadyShuttingDown,
    StartupFailed(&'static str),
    NotStarted,
    InvalidPhase(&'static str),
    ShuttingDown,
}
```

### Predicates

- `is_fatal()`: Check if error is fatal
- `is_retryable()`: Check if error is retryable
- `is_lifecycle()`: Check if error is lifecycle-related

## Observability

### Statistics

All statistics are tracked using atomic counters:

```rust
pub struct GracefulStats {
    pub startups_total: AtomicU64,
    pub startups_completed: AtomicU64,
    pub startups_failed: AtomicU64,
    pub shutdowns_total: AtomicU64,
    pub shutdowns_completed: AtomicU64,
    pub shutdowns_forced: AtomicU64,
    pub connections_drained: AtomicU64,
    pub total_startup_time_ns: AtomicU64,
    pub total_shutdown_time_ns: AtomicU64,
}
```

### StatsSnapshot

Point-in-time snapshot with precomputed rates:

```rust
pub struct StatsSnapshot {
    pub startups_total: u64,
    pub startups_completed: u64,
    pub startups_failed: u64,
    pub shutdowns_total: u64,
    pub shutdowns_completed: u64,
    pub shutdowns_forced: u64,
    pub connections_drained: u64,
    pub total_startup_time_ns: u64,
    pub total_shutdown_time_ns: u64,
}
```

**Methods:**
- `startup_failure_rate_bps()`: Startup failure rate in basis points
- `forced_shutdown_rate_bps()`: Forced shutdown rate in basis points

### Health Report

Health is classified into three states:

```rust
pub enum HealthState {
    Healthy,      // All systems nominal
    Degraded,     // Performance or availability impact
    Critical,     // Severe impact; data loss risk
}
```

### Health Thresholds

- **Startup Failure Degraded**: ≥ 500 bps (5%)
- **Startup Failure Critical**: ≥ 2000 bps (20%)
- **Forced Shutdown Degraded**: > 0 bps (any forced shutdown)
- **Forced Shutdown Critical**: ≥ 500 bps (5%)

## Configuration

### GracefulConfig

```rust
pub struct GracefulConfig {
    pub startup_timeout_ms: u64,
    pub shutdown_timeout_ms: u64,
    pub drain_timeout_ms: u64,
    pub accept_connections_at_phase: StartupPhase,
    pub enable_health_checks: bool,
    pub force_on_timeout: bool,
}
```

### Presets

Three deployment archetypes are provided:

#### Fast Preset

- **Use Case**: Development / Testing
- **Startup Timeout**: 10 seconds
- **Shutdown Timeout**: 10 seconds
- **Drain Timeout**: 5 seconds
- **Force on Timeout**: Yes
- **Health Checks**: No

#### Standard Preset

- **Use Case**: Production default
- **Startup Timeout**: 30 seconds
- **Shutdown Timeout**: 30 seconds
- **Drain Timeout**: 10 seconds
- **Force on Timeout**: No
- **Health Checks**: No

#### Safe Preset

- **Use Case**: Critical production
- **Startup Timeout**: 60 seconds
- **Shutdown Timeout**: 60 seconds
- **Drain Timeout**: 30 seconds
- **Force on Timeout**: No
- **Health Checks**: Yes

### Validation

All configurations are validated before use:

- Minimum timeout: 1,000 ms (1 second)
- Maximum timeout: 300,000 ms (5 minutes)
- Accept connections phase must be valid startup phase

## Performance

### Benchmarks

Four benchmark suites are provided:

1. **phase_transition**: Phase transition performance
2. **drain_check**: Connection draining check performance
3. **barrier_wait**: Barrier synchronization performance
4. **full_startup**: Full startup sequence performance

### Performance Considerations

- All stats use `AtomicU64` with `Relaxed` ordering for minimal overhead
- Phase checks are `const` functions where possible
- Barriers are `Copy` types for cheap cloning
- No heap allocation in hot paths

## API Reference

### StartupShutdownCoordinator

```rust
pub struct StartupShutdownCoordinator {
    cfg: GracefulConfig,
    stats: GracefulStats,
    current_startup: StartupPhase,
    current_shutdown: ShutdownPhase,
}

impl StartupShutdownCoordinator {
    pub fn new(cfg: GracefulConfig) -> Self;
    pub fn startup(&mut self) -> Result<StartupPhase, ShutdownError>;
    pub fn advance_startup(&mut self) -> Result<StartupPhase, ShutdownError>;
    pub fn begin_shutdown(&mut self) -> Result<ShutdownPhase, ShutdownError>;
    pub fn advance_shutdown(&mut self) -> Result<ShutdownPhase, ShutdownError>;
    pub fn current_startup_phase(&self) -> StartupPhase;
    pub fn current_shutdown_phase(&self) -> ShutdownPhase;
    pub fn health(&self) -> HealthReport;
    pub fn stats(&self) -> &GracefulStats;
    pub fn config(&self) -> &GracefulConfig;
}
```

### ConnectionDrainer

```rust
pub struct ConnectionDrainer {
    cfg: GracefulConfig,
    stats: GracefulStats,
    active_connections: u64,
}

impl ConnectionDrainer {
    pub fn new(cfg: GracefulConfig) -> Self;
    pub fn begin_drain(&mut self) -> Result<u64, ShutdownError>;
    pub fn drain_step(&mut self) -> Result<u64, ShutdownError>;
    pub fn is_drained(&self) -> bool;
    pub fn force_close(&mut self) -> Result<u64, ShutdownError>;
    pub fn active_connections(&self) -> u64;
    pub fn stats(&self) -> &GracefulStats;
    pub fn config(&self) -> &GracefulConfig;
}
```

### GracefulBuilder

```rust
pub struct GracefulBuilder {
    cfg: GracefulConfig,
}

impl GracefulBuilder {
    pub fn new() -> Self;
    pub fn from_preset(preset: Preset) -> Self;
    pub fn startup_timeout_ms(self, ms: u64) -> Self;
    pub fn shutdown_timeout_ms(self, ms: u64) -> Self;
    pub fn drain_timeout_ms(self, ms: u64) -> Self;
    pub fn force_on_timeout(self, force: bool) -> Self;
    pub fn enable_health_checks(self, enable: bool) -> Self;
    pub fn accept_connections_at_phase(self, phase: StartupPhase) -> Self;
    pub fn build(self) -> Result<GracefulConfig, ShutdownError>;
}
```

## Testing

### Unit Tests

Each module has comprehensive unit tests covering:

- Normal operation
- Error conditions
- Boundary cases
- Predicate correctness

### Integration Tests

Integration tests cover:

- Full startup sequence
- Full shutdown sequence
- Startup → Shutdown transition
- Error recovery
- Force shutdown behavior

### Benchmarks

Performance benchmarks ensure:

- Phase transitions are fast (< 100 ns)
- Barrier operations are cheap (< 50 ns)
- Full startup is efficient (< 1 µs)
- Health checks are lightweight (< 200 ns)

## Ops Runbook

### Normal Startup

1. Load configuration (from file or preset)
2. Create `StartupShutdownCoordinator` with config
3. Call `startup()` to begin sequence
4. Call `advance_startup()` until reaching `Ready` phase
5. Monitor health reports during startup
6. On timeout, investigate logs and retry if retryable

### Normal Shutdown

1. Call `begin_shutdown()` to initiate shutdown
2. Call `advance_shutdown()` to progress through phases
3. Monitor drain progress during `DrainConnections` phase
4. Verify state is flushed before `ReleaseResources`
5. Confirm shutdown reaches `Complete` phase
6. Check health report for any forced actions

### Emergency Shutdown

1. Set `force_on_timeout: true` in config
2. Call `begin_shutdown()`
3. If timeout occurs, forced shutdown triggers automatically
4. Resources may not be released cleanly
5. Review logs for partial cleanup
6. Plan graceful restart when safe

### Troubleshooting

**Startup Fails:**
1. Check error variant (`StartupFailed`, `PhaseTimeout`, `DependencyFailed`)
2. Review logs for failed component
3. If retryable, increase timeout or fix dependency
4. If fatal, fix configuration or code before retry

**Shutdown Hangs:**
1. Check current shutdown phase
2. If `DrainConnections`, check active connections
3. If `StopModules`, check module stop timeout
4. Consider force shutdown if critical
5. Review logs for stuck component

**Health Degraded:**
1. Check health report for specific signal
2. High startup failure rate → check configuration
3. Any forced shutdown → check drain timeout
4. Review metrics trends over time

## Integration

### With Module Lifecycle

The graceful startup system integrates with the module lifecycle system:

- `ModuleId` is re-exported for dependency tracking
- Module phases align with startup/shutdown phases
- Module failures trigger dependency failures

### With Configuration System

The graceful startup system integrates with the configuration system:

- `ConfigKey` is re-exported for configuration access
- Configuration is loaded during `LoadConfig` phase
- Invalid configuration triggers startup failure

### With Service Registry

The graceful startup system integrates with the service registry:

- `ServiceId` is re-exported for service tracking
- Services are registered during `AcceptConnections` phase
- Service health checks during startup if enabled

### With Scheduler Hooks

The graceful startup system integrates with the scheduler hooks:

- `SchedulerHookId` is re-exported for hook registration
- Hooks can be registered during startup phases
- Hooks are unregistered during shutdown phases

## Future Work (Phase 1+)

### Enhanced Coordination

- Implement real phase transition logic with dependency validation
- Add rollback on startup failure
- Implement parallel phase execution where safe
- Add phase-specific health checks

### Enhanced Draining

- Implement real connection tracking
- Add per-connection drain timeout
- Implement graceful connection closure
- Add drain progress callbacks

### State Persistence

- Persist startup progress to disk
- Enable crash recovery to last successful phase
- Implement state validation on restart
- Add state rollback capability

### Advanced Observability

- Add per-phase metrics
- Implement histogram for phase durations
- Add distributed tracing integration
- Export metrics to Prometheus

### Enhanced Configuration

- Add per-phase timeouts
- Implement configuration hot-reload
- Add configuration validation hooks
- Support for multiple config sources

## Conclusion

The Graceful Startup and Shutdown system provides a robust framework for managing application lifecycle in the Vireon runtime. Phase 0 delivers the core data structures, error handling, statistics, health monitoring, and configuration system with skeleton implementations for the coordinator and drainer. Future phases will implement full coordination logic, connection draining, and state persistence.
