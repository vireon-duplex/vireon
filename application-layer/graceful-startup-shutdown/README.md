# graceful-startup-shutdown

Orchestrated startup and shutdown for the Vireon runtime. Provides phase-ordered boot sequence, connection draining, and crash-safe shutdown with rollback.

## Overview

This crate provides a robust framework for managing application lifecycle with:

- **Phase-ordered startup**: Components start in a defined sequence with dependency validation
- **Crash-safe shutdown**: Graceful termination with rollback on failure
- **Connection draining**: Active connections are given time to complete before forced closure
- **Timeout handling**: Configurable timeouts with optional forced termination
- **Health monitoring**: Real-time health reporting based on startup/shutdown metrics
- **Observable**: Comprehensive statistics and metrics for operational visibility

## Startup Sequence

The startup sequence progresses through these phases:

1. **Init** → Initial state
2. **LoadConfig** → Load and validate configuration
3. **BindPorts** → Bind network addresses and ports
4. **AcceptConnections** → Begin accepting incoming connections
5. **Ready** → System fully operational

## Shutdown Sequence

The shutdown sequence progresses through these phases:

1. **Running** → System operational
2. **StopAccepting** → Stop accepting new connections
3. **DrainConnections** → Wait for active connections to complete
4. **StopModules** → Stop application modules in reverse order
5. **FlushState** → Flush state to persistent storage
6. **ReleaseResources** → Release system resources
7. **Complete** → Shutdown complete

## Usage

### Basic Usage

```rust
use graceful_startup_shutdown::{GracefulConfig, StartupShutdownCoordinator};

// Create coordinator with default config
let cfg = GracefulConfig::new();
let mut coordinator = StartupShutdownCoordinator::new(cfg);

// Begin startup
let phase = coordinator.startup()?;

// Advance through phases
while !coordinator.current_startup_phase().is_ready() {
    coordinator.advance_startup()?;
}

// Check health
let health = coordinator.health();
assert!(health.is_healthy());
```

### Using Presets

```rust
use graceful_startup_shutdown::{GracefulBuilder, Preset};

// Use Fast preset for development
let cfg = GracefulBuilder::from_preset(Preset::Fast)
    .build()?;

// Use Safe preset for production
let cfg = GracefulBuilder::from_preset(Preset::Safe)
    .build()?;
```

### Custom Configuration

```rust
use graceful_startup_shutdown::{GracefulBuilder, StartupPhase};

let cfg = GracefulBuilder::new()
    .startup_timeout_ms(45_000)
    .shutdown_timeout_ms(60_000)
    .drain_timeout_ms(15_000)
    .accept_connections_at_phase(StartupPhase::BindPorts)
    .enable_health_checks(true)
    .force_on_timeout(false)
    .build()?;
```

### Connection Draining

```rust
use graceful_startup_shutdown::{ConnectionDrainer, GracefulConfig};

let cfg = GracefulConfig::new();
let mut drainer = ConnectionDrainer::new(cfg);

// Begin draining
let active = drainer.begin_drain()?;

// Drain in steps
while !drainer.is_drained() {
    drainer.drain_step()?;
}

// Or force close if timeout
let closed = drainer.force_close()?;
```

### Health Monitoring

```rust
use graceful_startup_shutdown::{GracefulConfig, StartupShutdownCoordinator};

let coordinator = StartupShutdownCoordinator::new(GracefulConfig::new());
let health = coordinator.health();

match health.state {
    graceful_startup_shutdown::HealthState::Healthy => {
        println!("System healthy")
    }
    graceful_startup_shutdown::HealthState::Degraded => {
        eprintln!("System degraded: investigate soon")
    }
    graceful_startup_shutdown::HealthState::Critical => {
        eprintln!("System critical: page on-call")
    }
}
```

## Configuration

### Presets

| Preset | Startup | Shutdown | Drain | Force | Health | Use Case |
|--------|---------|----------|-------|-------|--------|----------|
| Fast | 10s | 10s | 5s | Yes | No | Development/Testing |
| Standard | 30s | 30s | 10s | No | No | Production default |
| Safe | 60s | 60s | 30s | No | Yes | Critical production |

### Configuration Options

- `startup_timeout_ms`: Total startup timeout (1,000 - 300,000 ms)
- `shutdown_timeout_ms`: Total shutdown timeout (1,000 - 300,000 ms)
- `drain_timeout_ms`: Connection drain timeout (1,000 - 300,000 ms)
- `accept_connections_at_phase`: Phase at which to accept connections
- `enable_health_checks`: Enable health checks during startup
- `force_on_timeout`: Force shutdown on timeout

## Error Handling

Errors are classified into three categories:

### Fatal Errors
These must not be retried:
- `ForcedShutdown`: Forced termination was initiated
- `StartupFailed`: Fatal error during startup
- `ShuttingDown`: System is shutting down

### Retryable Errors
These may be retried after backoff:
- `PhaseTimeout`: A phase timed out
- `DependencyFailed`: A dependency failed to start/stop

### Lifecycle Errors
These indicate state issues:
- `AlreadyShuttingDown`: Shutdown already in progress
- `NotStarted`: Operation attempted before startup
- `InvalidPhase`: Invalid phase transition

## Statistics

The system tracks comprehensive statistics:

- `startups_total`: Total startup attempts
- `startups_completed`: Successful startups
- `startups_failed`: Failed startups
- `shutdowns_total`: Total shutdown attempts
- `shutdowns_completed`: Successful shutdowns
- `shutdowns_forced`: Forced shutdowns
- `connections_drained`: Connections drained during shutdown
- `total_startup_time_ns`: Total startup time
- `total_shutdown_time_ns`: Total shutdown time

## Health Monitoring

Health is classified into three states:

- **Healthy**: All systems nominal
- **Degraded**: Performance or availability impact
- **Critical**: Severe impact; data loss risk

### Health Thresholds

- Startup failure rate ≥ 5% → Degraded
- Startup failure rate ≥ 20% → Critical
- Any forced shutdown → Degraded
- Forced shutdown rate ≥ 5% → Critical

## Integration

This crate integrates with other Vireon application layer crates:

- **module-lifecycle**: Provides `ModuleId` for dependency tracking
- **configuration-system**: Provides `ConfigKey` for configuration access
- **service-registry**: Provides `ServiceId` for service tracking
- **scheduler-hooks**: Provides `SchedulerHookId` for hook registration

## Phase 0 Status

In Phase 0, the following are fully implemented:

- ✅ Error types and predicates
- ✅ Configuration and presets
- ✅ Phase state machines
- ✅ Barrier synchronization
- ✅ Statistics and health monitoring
- ✅ Fluent builder API

The following ship as typed skeletons:

- 🔲 `StartupShutdownCoordinator` (core coordination logic)
- 🔲 `ConnectionDrainer` (draining implementation)

Full coordination and draining implementations will land in Phase 1+.

## Documentation

For detailed design documentation, see [DESIGN.md](DESIGN.md).

## License

This crate is part of the Vireon project and is licensed under the same terms.
