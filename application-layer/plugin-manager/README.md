# plugin-manager

**Plugin manager** for the Vireon application layer. Provides versioned plugin loading with API compatibility checking, capability declarations, and dependency resolution.

## Overview

The plugin manager is responsible for loading, managing, and orchestrating third-party code (plugins) within the Vireon runtime. It ensures that all plugins are validated for compatibility and dependencies before execution, maintaining strict safety guarantees while enabling runtime extensibility.

## Key Features

- **Versioned Loading**: Plugins declare semantic versions and API requirements
- **Compatibility Checking**: API version mismatches are detected before loading
- **Dependency Resolution**: Plugins declare dependencies; manager validates before loading
- **Capability Declaration**: Plugins declare feature flags (routing, auth, metrics, etc.)
- **Hot Reload**: Update plugins without restarting the runtime (development mode)
- **Observability**: Track plugin health, failures, and compatibility issues

## Architecture

```
PluginId → [PluginManager] → LoadedPlugin
               │
               ├─ PluginLoader (discover + load)
               ├─ ApiVersion (compatibility check)
               ├─ PluginDescriptor (metadata)
               └─ DependencyResolution
```

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
plugin-manager = { path = "../application-layer/plugin-manager" }
```

Basic usage:

```rust
use plugin_manager::{PluginManager, PluginDescriptor, PluginId, PluginVersion, Preset};

// Create manager with strict configuration
let cfg = Preset::Strict.to_config();
let mut manager = PluginManager::new(cfg)?;

// Create plugin descriptor
let desc = PluginDescriptor::new(
    PluginId::from_hash(42),
    PluginVersion::new(1, 0, 0),
)
.with_name("my_plugin")
.add_capability(plugin_manager::CAP_ROUTING);

// Load plugin
if let Err(e) = manager.load(desc) {
    eprintln!("Failed to load plugin: {}", e);
}
```

## Configuration

Three presets encode common deployment patterns:

### Strict (Production)

```rust
let cfg = Preset::Strict.to_config();
// max_plugins: 64
// allow_dev_plugins: false
// strict_api_check: true
// enable_hot_reload: false
```

### Lenient (Testing)

```rust
let cfg = Preset::Lenient.to_config();
// max_plugins: 64
// allow_dev_plugins: true
// strict_api_check: false
// enable_hot_reload: false
```

### Development

```rust
let cfg = Preset::Development.to_config();
// max_plugins: 128
// allow_dev_plugins: true
// strict_api_check: false
// enable_hot_reload: true
// scan_interval_ms: 5000
```

Custom configuration:

```rust
use plugin_manager::PluginManagerBuilder;

let cfg = PluginManagerBuilder::new()
    .max_plugins(100)
    .allow_dev(true)
    .strict_api(false)
    .enable_hot_reload(true)
    .scan_interval_ms(10_000)
    .build()?;
```

## Plugin Metadata

Plugins declare their metadata via `PluginDescriptor`:

```rust
pub struct PluginDescriptor {
    pub id: PluginId,              // Unique identifier (hash)
    pub version: PluginVersion,     // Semver (major.minor.patch)
    pub name: [u8; 64],            // Human-readable name
    pub api_version: ApiVersion,   // Required API version
    pub dependencies: [PluginId; 8], // Dependency IDs
    pub capabilities: u32,        // Capability bitmask
}
```

### Capability Flags

| Flag | Value | Meaning |
|------|-------|---------|
| `CAP_ROUTING` | 0x01 | Packet routing / forwarding |
| `CAP_AUTH` | 0x02 | Authentication / authorization |
| `CAP_METRICS` | 0x04 | Metrics collection / export |
| `CAP_LOGGING` | 0x08 | Structured logging |
| `CAP_SCHEDULING` | 0x10 | Task scheduling / timers |

## API Compatibility

The runtime declares its supported API version:

```rust
pub const CURRENT_API: ApiVersion = ApiVersion::new(1, 0);
```

Plugins must be compatible with this version:

- Same major version required (incompatible if different)
- Runtime minor version must be >= plugin's required minor

**Examples**:
- Plugin requires `1.0` → Compatible (exact match)
- Plugin requires `1.5` → Incompatible (runtime is `1.0`, behind)
- Plugin requires `2.0` → Incompatible (different major)

## Health & Observability

Check system health:

```rust
let health = manager.health();
match health.state {
    HealthState::Healthy => println!("All systems nominal"),
    HealthState::Degraded => println!("Degraded: {}% failures", health.load_failure_rate_bps / 100),
    HealthState::Critical => eprintln!("Critical: {}% failures", health.load_failure_rate_bps / 100),
}
```

View stats:

```rust
let stats = manager.stats().snapshot();
println!("Loaded: {}", stats.plugins_loaded);
println!("Failures: {}", stats.load_failures);
println!("API mismatches: {}", stats.api_mismatches);
```

## Error Handling

All operations return typed `PluginError` values:

| Error | Meaning | Retryable? |
|-------|---------|------------|
| `NotFound` | Plugin not loaded | No |
| `VersionConflict` | Version conflict with loaded plugin | No |
| `LoadFailed` | I/O error during load | Yes |
| `ApiMismatch` | Plugin API incompatible with runtime | No |
| `AlreadyLoaded` | Plugin already loaded | No |
| `DependencyMissing` | Required dependency not loaded | No |
| `CapabilityMissing` | Required capability not available | No |
| `ShuttingDown` | Manager is shutting down | No |

Use error predicates for handling:

```rust
match manager.load(desc) {
    Err(e) if e.is_retryable() => {
        // Retry after backoff
    }
    Err(e) if e.is_fatal() => {
        // Log and abort
    }
    Err(e) if e.is_validation() => {
        // Fix input and retry
    }
    _ => Ok(()),
}
```

## Phase 0 Status

✅ **Real implementations**:
- Error types and predicates
- ID and version types
- API version checking
- Plugin descriptor
- Configuration and presets
- Stats and health reporting
- Builder pattern

🚧 **Skeleton implementations** (Phase 1+):
- Plugin loading lifecycle
- Dependency resolution
- Hot reload and file monitoring
- Plugin lifecycle hooks

## Documentation

See [`DESIGN.md`](DESIGN.md) for the complete design specification covering:
- Plugin model and versioning
- API compatibility rules
- Dependency resolution
- Loading lifecycle
- Hot reload mechanics
- Configuration and presets
- Failure modes and error taxonomy
- Observability and health reporting
- Testing strategy
- Ops runbook

## License

Licensed under the same terms as the Vireon project.

## Status

Phase 0: Core modules implemented, facade skeletons for Phase 1+.
