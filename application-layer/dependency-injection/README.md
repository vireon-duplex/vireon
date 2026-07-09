# dependency-injection

**Dependency Injection Container** for the Vireon application layer. Provides typed service tokens, lifetime management (singleton/scoped/transient), and dependency graph resolution.

## Overview

The dependency injection container enables runtime service composition while maintaining compile-time type safety. It supports three service lifetimes, detects circular dependencies, and provides comprehensive observability.

## Features

- **Type-safe service tokens** through `ServiceToken<T>` generic wrappers
- **Three lifetime policies**: singleton, scoped, and transient
- **Dependency graph resolution** with cycle detection
- **Configuration presets** for common deployment patterns
- **Observability** through atomic statistics and health reporting
- **Zero-allocation** hot path for token operations

## Quick Start

```rust
use dependency_injection::*;

// Create a container with configuration
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

## Service Lifetimes

### Singleton
One instance for the entire container lifetime. Created on first resolution, reused thereafter, disposed on container shutdown.

```rust
let binding = BindingDescriptor::new(token, ServiceLifetime::Singleton);
```

### Scoped
One instance per scope. Created when scope enters, disposed when scope exits.

```rust
let scope_id = container.create_scope()?;
let instance: &MyService = container.resolve(token)?;
container.dispose_scope(scope_id)?;  // Disposes scoped instance
```

### Transient
New instance on each resolution. No caching.

```rust
let binding = BindingDescriptor::new(token, ServiceLifetime::Transient);
let instance1 = container.resolve(token)?;
let instance2 = container.resolve(token)?;  // Different instance
```

## Configuration

### Builder Pattern

```rust
let cfg = ContainerBuilder::new()
    .max_services(100)
    .max_scope_depth(16)
    .enable_lazy(false)
    .validate_on_register(true)
    .default_lifetime(ServiceLifetime::Scoped)
    .build()?;
```

### Presets

**Strict** - High-reliability systems with early validation:
```rust
let cfg = Preset::Strict.to_config();
```

**Lazy** - High-throughput systems with deferred validation:
```rust
let cfg = Preset::Lazy.to_config();
```

## Error Handling

The container provides typed errors for all failure modes:

```rust
match container.resolve(token) {
    Ok(instance) => /* use instance */,
    Err(DependencyError::ServiceNotRegistered(id)) => {
        eprintln!("Service {id} not registered");
    }
    Err(DependencyError::CircularDependency(id)) => {
        eprintln!("Circular dependency detected at {id}");
    }
    Err(e) if e.is_fatal() => {
        eprintln!("Fatal error: {e}");
    }
    Err(e) if e.is_retryable() => {
        // Retry after backoff
    }
    Err(e) => {
        eprintln!("Error: {e}");
    }
}
```

## Observability

### Statistics

```rust
let stats = container.stats();
let snap = stats.snapshot();

println!("Resolutions: {}", snap.resolutions_total);
println!("Success rate: {} bps", snap.resolution_success_rate_bps());
println!("Active singletons: {}", snap.singletons_active);
```

### Health

```rust
let report = container.health();

match report.state {
    HealthState::Healthy => println!("System healthy"),
    HealthState::Degraded => println!("System degraded, investigate"),
    HealthState::Critical => println!("System critical, page on-call"),
}
```

## Dependencies

The container depends on:

- `configuration-system` - For configuration key types
- `service-registry` - For service ID types
- `thiserror` - For error derivation
- `tracing` - For instrumentation

## Testing

Run tests:
```bash
cargo test
```

Run benchmarks:
```bash
cargo bench
```

## Phase Status

**Phase 0** (Current):
- ✅ Error types and predicates
- ✅ Configuration and presets
- ✅ Service tokens (TokenId, ServiceToken<T>)
- ✅ Lifetime enumeration
- ✅ Binding descriptor
- ✅ Statistics and health reporting
- ✅ Container builder
- 🔄 Container and resolver skeletons

**Phase 1+** (Future):
- Full registration and resolution
- Dependency graph traversal
- Scope lifecycle management
- Instance caching by lifetime

## Documentation

See [DESIGN.md](DESIGN.md) for the complete design specification.

## License

Licensed under the Vireon project license.

## Authors

Vireon Project Contributors
