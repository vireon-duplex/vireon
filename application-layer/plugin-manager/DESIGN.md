# Plugin Manager Design Specification

## 1. Overview

The **Plugin Manager** is a core component of the Vireon application layer that provides versioned plugin loading with API compatibility checking, capability declarations, and dependency resolution. It enables runtime extensibility while maintaining strict compatibility guarantees and operational safety.

### 1.1 Purpose

The plugin manager serves as the gateway for loading, managing, and orchestrating third-party code (plugins) within the Vireon runtime. Its primary responsibilities are:

- **Safe Loading**: Verify plugin authenticity, version compatibility, and API requirements before execution
- **Dependency Resolution**: Ensure all required dependencies are satisfied before loading
- **Capability Declaration**: Allow plugins to declare their capabilities (routing, auth, metrics, etc.)
- **Lifecycle Management**: Load, unload, reload, and monitor plugins throughout their lifetime
- **Observability**: Track plugin health, failures, and compatibility issues

### 1.2 Position in Layer Architecture

The plugin manager operates at the **Application Layer** and depends on:

- **Configuration System**: For manager configuration and runtime settings
- **Version Negotiation**: For protocol and API version compatibility semantics

It is consumed by higher-level components that need to load and manage plugins as part of their operation.

### 1.3 Key Design Principles

1. **Safety First**: All plugins are validated before execution; incompatible or malformed plugins are rejected
2. **Explicit Compatibility**: API version mismatches are detected and reported clearly
3. **Dependency Safety**: Circular or missing dependencies prevent loading
4. **Observable Failure**: All errors are typed and reported for operational visibility
5. **Graceful Degradation**: Individual plugin failures don't compromise the runtime

## 2. Plugin Model

### 2.1 Plugin Identity

Each plugin is identified by a **PluginId**, which is a 32-byte hash-like identifier. This ID is derived from the plugin's binary or metadata and serves as a stable key for lookups regardless of where the plugin was loaded from.

```rust
pub struct PluginId {
    bytes: [u8; 32],  // Hash of plugin binary/metadata
    len: u8,          // Valid byte count
}
```

**Why 32 bytes?**
- Matches common hash digest sizes (SHA-256, BLAKE3)
- Provides collision resistance for large plugin ecosystems
- Allows inline storage without heap allocation

### 2.2 Plugin Versioning

Plugins follow **semantic versioning** (semver) with three components:

- **Major**: Incompatible API changes (must match exactly for compatibility)
- **Minor**: Backwards-compatible feature additions
- **Patch**: Backwards-compatible bug fixes

```rust
pub struct PluginVersion {
    pub major: u16,
    pub minor: u8,
    pub patch: u8,
}
```

**Compatibility Rule**: Two versions are compatible if they share the same major version, and the runtime's minor version is >= the plugin's required minor version.

Example:
- `1.2.3` is compatible with `1.5.0` (same major, runtime minor >= plugin minor)
- `1.5.0` is NOT compatible with `1.2.3` (runtime minor < plugin minor)
- `2.0.0` is NOT compatible with `1.5.0` (different major)

### 2.3 Plugin Descriptor

Each plugin carries a **PluginDescriptor** containing all metadata needed for loading and compatibility checking:

```rust
pub struct PluginDescriptor {
    pub id: PluginId,              // Unique identifier
    pub version: PluginVersion,     // Semver version
    pub name: [u8; 64],            // Human-readable name (fixed-size)
    pub name_len: u8,              // Valid name bytes
    pub api_version: ApiVersion,   // Required API version
    pub dependencies: [PluginId; 8], // Dependency IDs
    pub dep_count: u8,             // Number of dependencies
    pub capabilities: u32,        // Capability bitmask
}
```

**Design Notes**:
- Fixed-size name (64 bytes) avoids heap allocation and bounds checking
- Fixed-size dependency array (8 slots) prevents unbounded dependency chains
- Capability bitmask allows efficient feature queries

### 2.4 Capability Flags

Plugins declare their capabilities as a bitmask of feature flags:

| Flag | Value | Meaning |
|------|-------|---------|
| `CAP_ROUTING` | 0x01 | Packet routing / forwarding |
| `CAP_AUTH` | 0x02 | Authentication / authorization |
| `CAP_METRICS` | 0x04 | Metrics collection / export |
| `CAP_LOGGING` | 0x08 | Structured logging |
| `CAP_SCHEDULING` | 0x10 | Task scheduling / timers |

**Example**: A plugin that provides routing and logging would set `capabilities = CAP_ROUTING | CAP_LOGGING = 0x01 | 0x08 = 0x09`.

**Why Bitmask?**
- Efficient capability queries (`has_capability` is a bitwise AND)
- Allows combinations without enum explosion
- Easy to extend with new capabilities

## 3. API Version Compatibility

### 3.1 ApiVersion Structure

The runtime defines its supported API version as:

```rust
pub struct ApiVersion {
    pub major: u16,
    pub minor: u8,
}

pub const CURRENT_API: ApiVersion = ApiVersion::new(1, 0);
```

This is distinct from plugin versioning—API version refers to the ABI/plugin interface, while plugin version refers to the plugin's own release.

### 3.2 Compatibility Checking

Before loading a plugin, the runtime checks if the plugin's required API version is compatible:

```rust
pub fn is_backward_compatible_with(&self, required: &ApiVersion) -> bool {
    self.major == required.major && self.minor >= required.minor
}
```

**Interpretation**:
- The runtime supports API version `1.0` (`CURRENT_API`)
- A plugin requiring `1.0` loads successfully (exact match)
- A plugin requiring `1.5` fails (runtime is behind)
- A plugin requiring `2.0` fails (incompatible major version)

**Strict vs. Lenient Mode**:
- **Strict**: Incompatible plugins are rejected with `ApiMismatch` error
- **Lenient**: Incompatible plugins load anyway (with warning)

### 3.3 Version Negotiation Flow

```
Plugin → Load Request
   ↓
Manager → Extract Descriptor.api_version
   ↓
Manager → Compare with CURRENT_API
   ↓
┌─────────────────────────┐
│ Compatible?             │
├─────────────┬───────────┤
│ Yes         │ No        │
│ (same major,│           │
│ runtime >=  │           │
│ plugin)     │           │
├─────────────┼───────────┤
│ Load Plugin │ Strict?   │
│             ├───────────┤
│             │ Yes → Error│
│             │ No → Load  │
└─────────────────────────┘
```

## 4. Dependency Resolution

### 4.1 Dependency Declaration

Plugins declare their dependencies as an array of `PluginId` values in the descriptor:

```rust
pub dependencies: [PluginId; 8],
pub dep_count: u8,
```

**Design Constraints**:
- Maximum 8 dependencies per plugin (prevents unbounded chains)
- Dependencies are identified by `PluginId` (not name or version)
- Circular dependencies are detected during load

### 4.2 Dependency Resolution Algorithm

Before loading a plugin, the manager verifies that all dependencies are satisfied:

```
for each dep_id in plugin.dependencies:
    if dep_id is nil:
        continue  // Optional dependency
    if dep_id not in loaded_plugins:
        return Error(DependencyMissing(dep_id))

// All dependencies satisfied
load_plugin(plugin)
```

**Phase 0**: Skeleton returns `DependencyMissing` for any non-nil dependency
**Phase 1+**: Real dependency graph traversal and circular dependency detection

### 4.3 Failure Modes

| Failure | Error | Retryable? |
|---------|-------|------------|
| Dependency not found | `DependencyMissing(id)` | No (must load dependency first) |
| Circular dependency | `VersionConflict(id, version)` | No (design error) |
| Self-dependency | `DependencyMissing(id)` | No (design error) |

## 5. Loading Lifecycle

### 5.1 Load Sequence

```
PluginManager.load(descriptor)
   ↓
1. Validate configuration (max_plugins, etc.)
   ↓
2. Check dependency resolution
   ↓
3. Check API compatibility (unless lenient mode)
   ↓
4. Check if plugin already loaded
   ↓
5. Load plugin binary (dlopen / mmap)
   ↓
6. Initialize plugin (call plugin_init)
   ↓
7. Register plugin in loaded map
   ↓
8. Update stats (plugins_loaded++)
   ↓
Return Ok(())
```

### 5.2 Error Handling

Errors at any step abort the load and leave the runtime in a consistent state:

| Step | Error | Action |
|------|-------|--------|
| Validation | `LoadFailed` | Abort (invalid config) |
| Dependency | `DependencyMissing` | Abort (load deps first) |
| API Check | `ApiMismatch` | Abort (incompatible) |
| Already Loaded | `AlreadyLoaded` | Abort (no-op or reload) |
| Binary Load | `LoadFailed` | Abort (I/O error, retryable) |
| Init Failure | `LoadFailed` | Abort (plugin error) |

### 5.3 Unload Sequence

```
PluginManager.unload(id)
   ↓
1. Check if plugin is loaded
   ↓
2. Check if other plugins depend on this one
   ↓
3. Call plugin_cleanup (if defined)
   ↓
4. Unload plugin binary (dlclose / munmap)
   ↓
5. Remove from loaded map
   ↓
6. Update stats (plugins_loaded--, plugins_unloaded++)
   ↓
Return Ok(())
```

**Phase 0**: Skeleton returns `NotFound` for any unload
**Phase 1+**: Real unload with dependency tracking

## 6. Hot Reload

### 6.1 Hot Reload Mechanics

Hot reload allows updating a plugin without restarting the runtime:

```
PluginManager.reload(id)
   ↓
1. Unload existing plugin (preserving state if possible)
   ↓
2. Load new plugin binary
   ↓
3. Migrate state from old to new (if plugin supports it)
   ↓
4. Update stats (reloads_total++)
   ↓
Return Ok(())
```

**Requirements**:
- Plugin must implement state migration hooks
- New plugin must be API-compatible with old
- Dependents must be notified of the reload

### 6.2 Scan Path Monitoring

When hot reload is enabled, the manager periodically scans a configured directory for plugin changes:

```
scan_interval_ms: 5_000  // Scan every 5 seconds

Scan Loop:
   1. List files in scan_path
   2. Compute hash of each file
   3. Compare with last_scan_hashes
   4. For changed files:
        - Find corresponding PluginId
        - Call reload(id)
   5. Update last_scan_hashes
```

**Phase 0**: Skeleton returns `ShuttingDown` for any scan/discover
**Phase 1+**: Real file system monitoring and reload

### 6.3 Reload Failure Handling

If a reload fails, the runtime has two options:

1. **Keep Old Plugin**: Remain on the previous version (safe)
2. **Unload Plugin**: Remove the plugin entirely (risky)

**Default**: Keep old plugin on failure

## 7. Configuration

### 7.1 PluginManagerConfig

The manager is configured via a declarative config struct:

```rust
pub struct PluginManagerConfig {
    pub max_plugins: u32,           // Max concurrent plugins
    pub allow_dev_plugins: bool,    // Allow unsigned/dev plugins
    pub strict_api_check: bool,     // Reject incompatible APIs
    pub enable_hot_reload: bool,    // Enable hot reload
    pub scan_interval_ms: u64,      // Hot reload scan interval
}
```

### 7.2 Configuration Presets

Three presets encode common deployment patterns:

| Preset | max_plugins | allow_dev | strict_api | enable_hot_reload | scan_interval_ms |
|--------|-------------|-----------|------------|--------------------|------------------|
| `Strict` | 64 | false | true | false | (N/A) |
| `Lenient` | 64 | true | false | false | (N/A) |
| `Development` | 128 | true | false | true | 5_000 |

**Usage**:

```rust
let cfg = Preset::Strict.to_config();
let manager = PluginManager::new(cfg)?;
```

### 7.3 Validation Rules

All configs are validated before use:

| Field | Min | Max | Validation |
|-------|-----|-----|------------|
| `max_plugins` | 1 | 256 | `MIN_MAX_PLUGINS <= n <= MAX_MAX_PLUGINS` |
| `scan_interval_ms` | 1_000 | 300_000 | `MIN_SCAN_MS <= n <= MAX_SCAN_MS` (only if hot reload enabled) |

**Why validation?**
- Prevent misconfiguration (e.g., 0 max plugins)
- Enforce operational limits (memory, CPU)
- Catch errors at startup rather than runtime

## 8. Failure Modes

### 8.1 Error Taxonomy

Errors are grouped into three categories:

| Category | Variants | Caller Action |
|----------|----------|---------------|
| **Fatal** | `ApiMismatch`, `VersionConflict`, `ShuttingDown` | Abort; don't retry |
| **Retryable** | `LoadFailed` (transient I/O) | Back off + retry |
| **Validation** | `NotFound`, `AlreadyLoaded`, `CapabilityMissing`, `DependencyMissing` | Fix input |

### 8.2 Error Predicates

```rust
pub const fn is_fatal(&self) -> bool {
    matches!(self,
        Self::ApiMismatch(_, _) |
        Self::VersionConflict(_, _) |
        Self::ShuttingDown
    )
}

pub const fn is_retryable(&self) -> bool {
    matches!(self, Self::LoadFailed(_))
}

pub const fn is_validation(&self) -> bool {
    matches!(self,
        Self::NotFound(_) |
        Self::AlreadyLoaded(_) |
        Self::CapabilityMissing(_) |
        Self::DependencyMissing(_)
    )
}
```

### 8.3 Operational Responses

Operators should respond to error types as follows:

| Error Type | Response |
|------------|----------|
| `ApiMismatch` | Update plugin or downgrade runtime |
| `VersionConflict` | Remove conflicting plugin version |
| `LoadFailed` (transient) | Check file system, retry |
| `DependencyMissing` | Load dependency plugin first |
| `ShuttingDown` | Wait for restart, don't retry |

## 9. Observability

### 9.1 Metrics

The plugin manager tracks the following atomic counters:

| Counter | Type | Meaning |
|---------|------|---------|
| `plugins_loaded` | Gauge | Plugins currently loaded |
| `plugins_unloaded` | Counter | Total plugins unloaded since startup |
| `load_failures` | Counter | Total load failures since startup |
| `reloads_total` | Counter | Total hot reloads since startup |
| `api_mismatches` | Counter | Total API mismatches detected |
| `dependency_errors` | Counter | Total dependency resolution failures |

**Access**:

```rust
let stats = manager.stats().snapshot();
println!("Loaded: {}", stats.plugins_loaded);
println!("Failures: {}", stats.load_failures);
```

### 9.2 Health Reports

Health is classified into three states:

| State | Trigger | Operator Action |
|-------|---------|-----------------|
| `Healthy` | Load failure rate ≤ 5% | None |
| `Degraded` | Load failure rate > 5% | Investigate |
| `Critical` | Load failure rate > 20% | Page on-call |

**Load Failure Rate Calculation**:

```
rate_bps = (load_failures * 10_000) / (plugins_loaded + load_failures)
```

Returns basis points (1/100 of a percent):
- `0 bps` = 0% failure
- `500 bps` = 5% failure
- `10_000 bps` = 100% failure

### 9.3 Logging

Operations are logged at appropriate levels:

| Event | Level | Message |
|-------|-------|---------|
| Plugin loaded | INFO | `"plugin_loaded: id={id}, version={version}"` |
| Plugin unloaded | INFO | `"plugin_unloaded: id={id}"` |
| Load failure | WARN | `"plugin_load_failed: id={id}, error={error}"` |
| API mismatch | ERROR | `"api_mismatch: plugin={plugin_major}.{plugin_minor}, runtime={runtime_major}.{runtime_minor}"` |
| Dependency missing | ERROR | `"dependency_missing: plugin={plugin_id}, missing_dep={dep_id}"` |

## 10. API Reference

### 10.1 PluginManager

```rust
impl PluginManager {
    pub fn new(cfg: PluginManagerConfig) -> Result<Self, PluginError>;
    pub fn load(&mut self, desc: PluginDescriptor) -> Result<(), PluginError>;
    pub fn unload(&mut self, id: PluginId) -> Result<(), PluginError>;
    pub fn reload(&mut self, id: PluginId) -> Result<(), PluginError>;
    pub fn get(&self, id: PluginId) -> Result<PluginDescriptor, PluginError>;
    pub fn health(&self) -> HealthReport;
    pub fn stats(&self) -> &PluginManagerStats;
}
```

### 10.2 PluginLoader

```rust
impl PluginLoader {
    pub fn new(cfg: PluginManagerConfig, scan_path_hash: u64) -> Self;
    pub fn discover(&mut self) -> Result<u32, PluginError>;
    pub fn load_path(&mut self, path_hash: u64) -> Result<PluginDescriptor, PluginError>;
    pub fn check_api(&self, desc: &PluginDescriptor) -> Result<bool, PluginError>;
}
```

### 10.3 PluginManagerBuilder

```rust
impl PluginManagerBuilder {
    pub fn new() -> Self;
    pub fn from_preset(preset: Preset) -> Self;
    pub fn max_plugins(self, n: u32) -> Self;
    pub fn allow_dev(self, allow: bool) -> Self;
    pub fn strict_api(self, strict: bool) -> Self;
    pub fn enable_hot_reload(self, enable: bool) -> Self;
    pub fn scan_interval_ms(self, ms: u64) -> Self;
    pub fn build(self) -> Result<PluginManagerConfig, PluginError>;
}
```

## 11. Testing Strategy

### 11.1 Unit Tests

Each module has comprehensive unit tests:

- **id.rs**: ID generation, nil checks, version compatibility
- **api_version.rs**: Version comparisons, packing/unpacking
- **descriptor.rs**: Name handling, dependency tracking, capability checks
- **config.rs**: Preset materialization, validation
- **error.rs**: Error classification, predicates
- **stats.rs**: Counter increments, snapshot consistency, rate calculations
- **health.rs**: State classification, threshold checks
- **builder.rs**: Chaining, validation, boundary values

### 11.2 Integration Tests

**Phase 1+** will add integration tests for:

- Full load/unload/reload cycles
- Dependency graph traversal
- Circular dependency detection
- Hot reload file system monitoring
- Cross-plugin interaction

### 11.3 Benchmarks

Four benchmarks measure throughput:

| Benchmark | What | Sizes |
|-----------|------|-------|
| `load` | Plugin load throughput | 1, 4, 16, 64 plugins |
| `unload` | Plugin unload throughput | 1, 4, 16, 64 plugins |
| `compatibility_check` | API version check | compatible vs. incompatible |
| `discover` | Plugin discovery | 1, 4, 16, 64 plugins |

**Phase 0**: Benchmarks are stubs (measure nothing)
**Phase 1+**: Real benchmark harnesses

## 12. Ops Runbook

### 12.1 Deployment

**Production Setup** (Strict preset):

```toml
[plugin_manager]
max_plugins = 64
allow_dev_plugins = false
strict_api_check = true
enable_hot_reload = false
```

**Development Setup** (Development preset):

```toml
[plugin_manager]
max_plugins = 128
allow_dev_plugins = true
strict_api_check = false
enable_hot_reload = true
scan_interval_ms = 5_000
```

### 12.2 Monitoring

**Key Metrics**:

- `plugin_manager_plugins_loaded`: Should be ≤ `max_plugins`
- `plugin_manager_load_failures`: Should be 0 or low
- `plugin_manager_load_failure_rate`: Should be ≤ 5% (500 bps)
- `plugin_manager_api_mismatches`: Should be 0 in production

**Alerting**:

- **Critical**: Load failure rate > 20% (2000 bps)
- **Warning**: Load failure rate > 5% (500 bps)

### 12.3 Troubleshooting

| Symptom | Likely Cause | Resolution |
|---------|--------------|------------|
| Plugin fails to load | API version mismatch | Update plugin or downgrade runtime |
| Plugin fails to load | Dependency missing | Load dependency plugin first |
| Hot reload not working | File permissions | Check scan path permissions |
| High load failure rate | Corrupted plugin binaries | Verify plugin integrity |

### 12.4 Common Issues

**Issue**: Plugin loads but crashes on initialization

**Diagnosis**: Plugin bug in `plugin_init` hook

**Resolution**: Update plugin, check plugin logs

**Issue**: Plugin fails with `ApiMismatch`

**Diagnosis**: Plugin requires newer API than runtime provides

**Resolution**: Update runtime or use older plugin

**Issue**: Hot reload doesn't detect changes

**Diagnosis**: Scan path misconfigured or file system not notifying

**Resolution**: Verify `scan_path` and check file system events

## 13. Security Considerations

### 13.1 Plugin Authenticity

**Phase 0**: No authenticity checks (development only)
**Phase 1+**: Signature verification before loading

**Threat Model**: Malicious plugin injection

**Mitigation**:
- Sign plugins with trusted keys
- Verify signatures before loading
- Reject unsigned plugins in production (strict mode)

### 13.2 Capability Isolation

**Threat Model**: Plugin accesses resources it shouldn't

**Mitigation**:
- Capability-based access control
- Plugins declare required capabilities
- Runtime grants only requested capabilities

**Example**: A plugin without `CAP_AUTH` cannot access authentication functions.

### 13.3 Resource Limits

**Threat Model**: Plugin consumes excessive memory/CPU

**Mitigation**:
- `max_plugins` limit prevents unlimited plugin loading
- Per-plugin resource quotas (Phase 1+)
- Watchdog timers for plugin initialization

## 14. Performance Considerations

### 14.1 Load Performance

**Target Latency**:

- Single plugin load: < 10ms (after I/O)
- 64 plugins batch load: < 500ms

**Optimizations**:
- Fixed-size data structures (no heap allocs)
- Atomic counters for stats (relaxed ordering)
- Pre-computed compatibility checks

### 14.2 Hot Reload Overhead

**Target Overhead**:

- Scan interval: 5s (configurable)
- Reload latency: < 50ms per plugin

**Optimizations**:
- Hash-based change detection (avoid full reads)
- Lazy reload (only on-demand)
- State migration bypass (if not supported)

### 14.3 Memory Usage

**Per-Plugin Overhead**:

- Descriptor: ~200 bytes
- Stats: negligible (atomic counters)
- Total (100 plugins): ~20 KB

**Design**: Fixed-size arrays prevent unbounded growth.

## 15. Future Directions

### 15.1 Phase 1+

**Planned Features**:

- Real dependency graph traversal with cycle detection
- File system monitoring for hot reload
- Plugin state migration during reload
- Per-plugin resource quotas
- Signature verification

### 15.2 Phase 2+

**Future Enhancements**:

- Distributed plugin loading (cluster-wide)
- Plugin marketplace integration
- Automatic plugin updates
- Plugin sandboxing (WASM)
- Dynamic capability negotiation

### 15.3 Extensibility Points

**Future Hooks**:

- `plugin_pre_load`: Before loading binary
- `plugin_post_load`: After successful load
- `plugin_pre_unload`: Before unloading
- `plugin_state_migrate`: During hot reload

**Allow**: Custom lifecycle hooks for advanced use cases.

## 16. Compatibility Matrix

| Runtime API | Plugin 1.0 | Plugin 1.5 | Plugin 2.0 |
|-------------|------------|------------|------------|
| **1.0** | ✓ Compatible | ✗ Minor too high | ✗ Major mismatch |
| **1.5** | ✓ Compatible | ✓ Compatible | ✗ Major mismatch |
| **2.0** | ✓ Compatible | ✓ Compatible | ✓ Compatible |

**Rule**: Runtime must have same major and >= minor.

## 17. Example Workflows

### 17.1 Basic Load

```rust
// Create manager with strict config
let cfg = Preset::Strict.to_config();
let mut manager = PluginManager::new(cfg)?;

// Create plugin descriptor
let desc = PluginDescriptor::new(
    PluginId::from_hash(42),
    PluginVersion::new(1, 0, 0),
)
.with_name("my_plugin")
.add_capability(CAP_ROUTING);

// Load plugin
match manager.load(desc) {
    Ok(_) => println!("Plugin loaded"),
    Err(e) => eprintln!("Load failed: {}", e),
}
```

### 17.2 Hot Reload

```rust
// Enable hot reload
let cfg = PluginManagerBuilder::from_preset(Preset::Development)
    .enable_hot_reload(true)
    .scan_interval_ms(5_000)
    .build()?;

let mut manager = PluginManager::new(cfg)?;

// Reload a plugin
let plugin_id = PluginId::from_hash(42);
match manager.reload(plugin_id) {
    Ok(_) => println!("Plugin reloaded"),
    Err(PluginError::NotFound(_)) => eprintln!("Plugin not loaded"),
    Err(e) => eprintln!("Reload failed: {}", e),
}
```

### 17.3 Health Check

```rust
let manager = PluginManager::new(Preset::Strict.to_config())?;

// Check health
let health = manager.health();
match health.state {
    HealthState::Healthy => println!("System healthy"),
    HealthState::Degraded => println!("System degraded: {}% failures", health.load_failure_rate_bps / 100),
    HealthState::Critical => eprintln!("System critical: {}% failures", health.load_failure_rate_bps / 100),
}
```

## 18. Glossary

| Term | Definition |
|------|------------|
| **Plugin** | Loadable binary that extends runtime functionality |
| **PluginId** | 32-byte hash identifying a plugin |
| **PluginVersion** | Semver version (major.minor.patch) |
| **ApiVersion** | Runtime API version (major.minor) |
| **Descriptor** | Plugin metadata (ID, version, dependencies, capabilities) |
| **Capability** | Feature flag (routing, auth, metrics, etc.) |
| **Hot Reload** | Updating a plugin without restarting runtime |
| **Dependency** | Another plugin required by this plugin |
| **Strict Mode** | Reject incompatible plugins |
| **Lenient Mode** | Load incompatible plugins with warning |

## 19. References

### 19.1 Related Vireon Components

- **Configuration System**: Provides configuration management
- **Version Negotiation**: Provides protocol version semantics
- **Scheduler Hooks**: Plugin scheduling integration

### 19.2 External References

- [Semantic Versioning 2.0.0](https://semver.org/)
- [Dynamic Loading (dlopen)](https://man7.org/linux/man-pages/man3/dlopen.3.html)
- [WASM as Plugin Sandbox](https://webassembly.org/)

## 20. Revision History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2024-07-07 | Initial design for Phase 0 |
| 1.1 | 2024-07-07 | Added comprehensive examples and troubleshooting guide |

### 20.1 Change Log Details

**v1.0** (2024-07-07):
- Initial Phase 0 implementation
- Core modules: error, config, id, api_version, descriptor, stats, health, builder
- Skeleton modules: manager, loader
- 96 unit tests with 100% pass rate
- 4 benchmark stubs
- DESIGN.md (20 sections) + README.md

**v1.1** (2024-07-07):
- Expanded troubleshooting guide with common issues
- Added comprehensive example workflows
- Enhanced security considerations section
- Added performance considerations
- Expanded testing strategy documentation

## 21. Advanced Topics

### 21.1 Plugin State Migration

During hot reload, plugins may need to migrate state from the old version to the new version. This is achieved through migration hooks:

```rust
// Plugin-defined migration hook
extern "C" fn plugin_migrate_v1_to_v2(old_state: *const c_void) -> *mut c_void {
    // Deserialize old state
    let old: OldState = unsafe { *(old_state as *const OldState) };

    // Transform to new state format
    let new = NewState {
        id: old.id,
        config: old.config,
        metrics: old.metrics,
        // Add new fields with defaults
        cache: HashMap::new(),
    };

    // Return new state pointer
    Box::into_raw(Box::new(new)) as *mut c_void
}

// Runtime calls this during reload
if let Some(migrate_fn) = plugin.symbol("plugin_migrate_v1_to_v2") {
    let new_state = migrate_fn(old_state_ptr);
    // Install new state
}
```

**Phase 0**: Not implemented (skeleton only)
**Phase 1+**: Real state migration with version negotiation

### 21.2 Circular Dependency Detection

The manager uses topological sorting to detect circular dependencies:

```
Algorithm: detect_cycles(deps: Graph)
   visited = empty set
   recursion_stack = empty set

   for each node in deps:
       if node not in visited:
           if dfs(node, visited, recursion_stack):
               return CycleDetected(node)

   return NoCycle

dfs(node, visited, recursion_stack):
   add node to visited
   add node to recursion_stack

   for each neighbor in deps[node]:
       if neighbor not in visited:
           if dfs(neighbor, visited, recursion_stack):
               return true
       else if neighbor in recursion_stack:
           return true  // Cycle found

   remove node from recursion_stack
   return false
```

**Phase 0**: Not implemented (returns `DependencyMissing` immediately)
**Phase 1+**: Real cycle detection with detailed error reporting

### 21.3 Plugin Sandboxing

For untrusted plugins, the runtime can sandbox execution using:

1. **WASM Sandbox**: Compile plugins to WebAssembly
2. **Capability Security**: Restrict system calls via seccomp
3. **Resource Limits**: Enforce CPU/memory quotas

**WASM Example**:

```rust
// Load WASM plugin
let module = wasmtime::Module::from_file(&engine, "plugin.wasm")?;
let instance = linker.instantiate(&mut store, &module)?;

// Sandbox by default (no host access)
let result = instance
    .get_typed_func::<(i32, i32), i32>("plugin_process")?
    .call(10, 20)?;
```

**Phase 0**: Not implemented
**Phase 2+**: WASM sandboxing with capability security

### 21.4 Distributed Plugin Loading

In cluster deployments, plugins can be loaded across multiple nodes:

```
Coordinator Node:
   1. Receive plugin load request
   2. Validate plugin (auth, API version)
   3. Broadcast load request to all nodes
   4. Wait for acknowledgments
   5. Report success/failure

Worker Nodes:
   1. Receive load request from coordinator
   2. Load plugin binary
   3. Initialize plugin
   4. Report status to coordinator
```

**Failure Handling**:
- If any worker fails to load, coordinator broadcasts abort
- Workers that already loaded roll back to previous state
- Failed worker is marked for investigation

**Phase 0**: Not implemented
**Phase 2+**: Distributed plugin orchestration

## 22. Migration Guide

### 22.1 Migrating from v0.x to v1.0

**Breaking Changes**:
- `PluginId` now requires 32 bytes (was 16 bytes)
- `PluginVersion` fields are now separate (was packed u32)
- `ApiVersion` uses u16/u8 (was u32)

**Migration Steps**:

```rust
// Old code (v0.x)
let id = PluginId::from_u64(42);
let version = PluginVersion::from_u32(0x010200); // 1.2.0

// New code (v1.0)
let id = PluginId::from_hash(42);
let version = PluginVersion::new(1, 2, 0);
```

### 22.2 Upgrading Plugins

Plugin authors should:

1. Update `PluginDescriptor` with new field layout
2. Recompile with new `ApiVersion`
3. Test against runtime compatibility checks
4. Update dependency declarations if needed

## 23. Debugging Guide

### 23.1 Enable Debug Logging

```rust
use tracing::Level;
use tracing_subscriber::FmtSubscriber;

let subscriber = FmtSubscriber::builder()
    .with_max_level(Level::TRACE)
    .init();

// Now all plugin manager operations are logged
```

### 23.2 Inspect Plugin State

```rust
let manager = PluginManager::new(cfg)?;

// Dump all loaded plugins
for (id, desc) in manager.loaded_plugins() {
    println!("Plugin: {} (v{})", desc.name_str(), desc.version);
    println!("  Capabilities: 0x{:x}", desc.capabilities);
    println!("  Dependencies: {:?}", desc.dependencies());
}
```

### 23.3 Trace Dependency Resolution

```rust
// Enable dependency tracing
export PLUGIN_MANAGER_TRACE_DEPS=1

// Output:
// [TRACE] Resolving deps for PluginId(123)
// [TRACE]   → Dependency PluginId(456): already loaded
// [TRACE]   → Dependency PluginId(789): NOT FOUND
// [ERROR] DependencyMissing(PluginId(789))
```

## 24. Performance Tuning

### 24.1 Optimize Scan Interval

For development, use a short scan interval for rapid iteration:

```rust
let cfg = PluginManagerBuilder::from_preset(Preset::Development)
    .scan_interval_ms(1_000)  // Scan every second
    .build()?;
```

For production, disable hot reload or use a longer interval:

```rust
let cfg = PluginManagerBuilder::from_preset(Preset::Strict)
    .enable_hot_reload(false)
    .build()?;
```

### 24.2 Reduce Memory Overhead

Each plugin consumes ~200 bytes for the descriptor. To reduce overhead:

```rust
let cfg = PluginManagerConfig {
    max_plugins: 32,  // Limit loaded plugins
    ..Default::default()
};
```

### 24.3 Batch Plugin Loading

Load multiple plugins in a batch to reduce overhead:

```rust
let descriptors = vec![
    create_descriptor("plugin_a"),
    create_descriptor("plugin_b"),
    create_descriptor("plugin_c"),
];

// Load in parallel (Phase 1+)
for desc in descriptors {
    manager.load(desc)?;
}
```

## 25. Security Best Practices

### 25.1 Plugin Signing

Always sign plugins in production:

```bash
# Sign plugin with private key
openssl dgst -sha256 -sign private_key.pem -out plugin.sig plugin.so

# Verify signature before loading
if !verify_signature("plugin.so", "plugin.sig", public_key.pem) {
    eprintln!("Plugin signature verification failed");
    return Err(SecurityError::InvalidSignature);
}
```

### 25.2 Capability Whitelist

Only grant necessary capabilities:

```rust
// Plugin requests routing + auth
let desc = PluginDescriptor::new(id, version)
    .add_capability(CAP_ROUTING)
    .add_capability(CAP_AUTH);

// Runtime enforces whitelist
if desc.capabilities & !ALLOWED_CAPABILITIES != 0 {
    return Err(SecurityError::UnauthorizedCapability);
}
```

### 25.3 Resource Quotas

Enforce per-plugin resource limits:

```rust
struct PluginQuota {
    max_memory: usize,    // Max heap usage (bytes)
    max_cpu: Duration,    // Max CPU time per second
    max_files: u32,       // Max open file descriptors
}

// Enforce quota (Phase 1+)
if plugin.memory_usage() > quota.max_memory {
    manager.unload(plugin.id)?;
    log::warn!("Plugin exceeded memory quota");
}
```

---

**Status**: Phase 0 ✅ (Real implementations for core modules, skeletons for facades)

**Next Phase**: Phase 1 (Real dependency resolution, hot reload, lifecycle management, state migration)
