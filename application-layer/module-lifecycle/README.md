# module-lifecycle

**Module lifecycle management** for the Vireon application layer.

## Overview

Provides dependency-ordered initialization, startup, and shutdown for application modules with cycle detection and failure rollback.

## Features

- **Dependency Resolution**: Topological sorting ensures modules start in correct order
- **Cycle Detection**: Detects circular dependencies before initialization
- **Phase State Machine**: Tracks modules through Registered → Initialized → Starting → Active → Stopping → Stopped
- **Failure Handling**: Graceful rollback on initialization/startup failures
- **Flexible Execution**: Support for both parallel and sequential initialization
- **Observability**: Atomic counters and health reporting
- **Configuration**: Presets for common deployment patterns

## Installation

Add to `Cargo.toml`:

```toml
[dependencies]
module-lifecycle = { path = "../application-layer/module-lifecycle" }
```

## Quick Start

```rust
use module_lifecycle::{LifecycleCoordinator, ModuleDescriptor, ModuleId, Preset};

// Create coordinator with preset
let cfg = Preset::Eager.to_config();
let mut coord = LifecycleCoordinator::new(cfg).unwrap();

// Register modules
let db = ModuleDescriptor::new(ModuleId::new(1))
    .with_name("database")
    .with_critical(true);

let server = ModuleDescriptor::new(ModuleId::new(2))
    .with_name("http-server")
    .add_dependency(ModuleId::new(1)); // Depends on database

coord.register(db).unwrap();
coord.register(server).unwrap();

// Initialize all modules (dependency order)
let count = coord.init_all().unwrap();

// Start all modules
let count = coord.start_all().unwrap();

// Check health
let health = coord.health();
println!("Active modules: {}", health.modules_active);
```

## Configuration

### Presets

```rust
// Fast parallel init, short timeouts (dev/testing)
let cfg = Preset::Eager.to_config();

// Sequential init, long timeouts (production)
let cfg = Preset::Lazy.to_config();

// Fail-fast enabled, short timeouts (critical systems)
let cfg = Preset::Critical.to_config();
```

### Custom Configuration

```rust
use module_lifecycle::LifecycleBuilder;

let cfg = LifecycleBuilder::new()
    .max_modules(512)
    .init_timeout_ms(10_000)
    .start_timeout_ms(15_000)
    .stop_timeout_ms(30_000)
    .parallel_init(false)
    .fail_fast(true)
    .build()?;
```

## Module Phases

Modules progress through these phases:

```
Registered → Initialized → Starting → Active → Stopping → Stopped
     │              │            │          │          │
     └──────────────┴────────────┴──────────┴──────────┘
                                    │
                                    ▼
                                 Failed
```

## Dependency Management

### Adding Dependencies

```rust
let desc = ModuleDescriptor::new(ModuleId::new(3))
    .with_name("api-server")
    .add_dependency(ModuleId::new(1))  // database
    .add_dependency(ModuleId::new(2)); // http-server
```

### Cycle Detection

The system detects circular dependencies automatically:

```rust
// This will be rejected:
// module1 depends on module2
// module2 depends on module1
coord.register(module1).unwrap(); // OK
coord.register(module2).unwrap(); // Error: DependencyCycle
```

## Error Handling

```rust
use module_lifecycle::ModuleLifecycleError;

match coord.init_all() {
    Ok(count) => println!("Initialized {} modules", count),
    Err(ModuleLifecycleError::DependencyCycle(id)) => {
        eprintln!("Cycle involving module {:?}", id);
    }
    Err(ModuleLifecycleError::InitFailed(reason)) => {
        eprintln!("Init failed: {}", reason);
    }
    Err(e) => eprintln!("Error: {}", e),
}
```

## Health Monitoring

```rust
let health = coord.health();

match health.state {
    HealthState::Healthy => println!("System healthy"),
    HealthState::Degraded => println!("System degraded: {}% failure rate",
        health.failure_rate_bps / 100),
    HealthState::Critical => println!("System critical! Failure rate: {}%",
        health.failure_rate_bps / 100),
}
```

## Statistics

```rust
let stats = coord.stats().snapshot();

println!("Registered: {}", stats.modules_registered);
println!("Initialized: {}", stats.modules_initialized);
println!("Failed: {}", stats.modules_failed);
println!("Failure rate: {}%", stats.failure_rate_bps as f64 / 100.0);
println!("Avg init time: {} μs", stats.avg_init_time_ns() / 1000);
```

## Module Descriptors

### Fields

- `id`: Unique identifier
- `name`: Module name (max 64 bytes)
- `version`: Module version
- `dependencies`: Up to 16 dependency ModuleIds
- `priority`: Initialization priority (0-255)
- `is_critical`: Whether failure triggers shutdown

### Builder Pattern

```rust
let desc = ModuleDescriptor::new(ModuleId::new(10))
    .with_name("cache-service")
    .with_version(2)
    .with_priority(100)
    .with_critical(false)
    .add_dependency(ModuleId::new(1))
    .add_dependency(ModuleId::new(2));
```

## Phase Transitions

Query whether a transition is valid:

```rust
use module_lifecycle::ModulePhase;

assert!(ModulePhase::Registered.can_transition_to(ModulePhase::Initialized));
assert!(ModulePhase::Active.can_transition_to(ModulePhase::Stopping));
assert!(!ModulePhase::Active.can_transition_to(ModulePhase::Registered)); // Invalid
```

## Design

See [`DESIGN.md`](DESIGN.md) for complete architecture documentation including:
- Component details
- Dependency resolution algorithms
- Phase state machine
- Failure handling strategies
- Performance characteristics
- Testing approach

## License

MIT

## Contributing

See Vireon project contribution guidelines.
