# Service Registry

Application-level service discovery for the Vireon QUIC runtime.

## Overview

The Service Registry provides:

- **Service Registration**: Dynamic service catalog with metadata
- **Service Discovery**: Lookup services by name or ID
- **Health Tracking**: Real-time health monitoring of instances
- **Load Balancing**: Multiple strategies (round-robin, least-loaded, weighted)
- **Watch System**: Subscribe to service changes

## Quick Start

```rust
use service_registry::{
    ServiceRegistry, ServiceRegistryBuilder,
    ServiceInfo, ServiceId, ServiceEndpoint, NodeId
};
use service_registry::config::Preset;

// Create a registry from a preset
let cfg = Preset::Local.to_config();
let mut registry = ServiceRegistry::new(cfg)?;

// Register a service
let info = ServiceInfo::new(ServiceId::new(1))
    .with_name("my-service");
registry.register(info)?;

// Add an endpoint
let endpoint = ServiceEndpoint::new(
    NodeId::new(1),
    [127, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    8080
);
registry.add_endpoint(ServiceId::new(1), endpoint)?;

// Discover the service
let endpoint = registry.discover(ServiceId::new(1))?;
```

## Architecture

```
Service Request → [Service Registry] → Endpoint Selection
                      ↓
                 ┌────────────────────────────────────┐
                 │                                    │
                 ├─ Service Catalog                  │
                 ├─ Endpoint Directory                │
                 ├─ Health Tracker                   │
                 ├─ Load Balancer                     │
                 └─ Watch System                      │
                 └────────────────────────────────────┘
```

## Key Concepts

### Services

Services are logical entities identified by a `ServiceId`:

- `ServiceId`: 64-bit unique identifier (0 reserved)
- `ServiceState`: Registered → Active → Draining → Unregistered
- `ServiceInfo`: Metadata including name, state, endpoint count

### Endpoints

Endpoints are concrete network locations:

- `ServiceEndpoint`: Node ID, IP address (IPv6-ready), port, weight
- Uniqueness: `(service_id, node_id, address, port)`
- Health: Boolean flag for immediate filtering

### Instances

Instances have runtime state for load balancing:

- `ServiceInstance`: Endpoint + active requests + health score
- Load Factor: 0-10000 basis points (higher = more loaded)
- Health Score: 0-100 (higher = healthier)

### Load Balancing

Multiple strategies for endpoint selection:

- **Round-Robin**: Fair distribution (default)
- **Least-Loaded**: Minimize active requests
- **Weighted**: Respect endpoint weights

## Configuration

### Presets

Pre-configured settings for common deployments:

```rust
let cfg = Preset::Local.to_config();      // 100 services, 10 instances
let cfg = Preset::Cluster.to_config();    // 10K services, 50 instances
let cfg = Preset::Federation.to_config(); // 100K services, 100 instances
let cfg = Preset::Edge.to_config();        // 1K services, 20 instances
```

### Custom Configuration

```rust
let cfg = ServiceRegistryBuilder::new()
    .max_services(5000)
    .max_instances_per_service(100)
    .health_check_interval_ms(15_000)
    .load_balancing(true)
    .drain_timeout_ms(45_000)
    .build()?;
```

## Health Tracking

### Health Scoring

Health scores (0-100) are computed from:

- Success rate (0-40 point deduction)
- Latency percentiles (0-20 point deduction)
- Error rates (0-30 point deduction)
- Circuit breaker state (0-50 point deduction)

**Score Interpretation**:
- 0-50: Unhealthy (excluded from load balancing)
- 51-80: Degraded (low priority)
- 81-100: Healthy (normal priority)

### Health Checks

Supported health check types:

- **HTTP**: Request a health endpoint (default `/health`)
- **TCP**: Verify port connectivity
- **Custom**: Implement `CustomHealthCheck` trait

## Observability

### Statistics

```rust
let stats = registry.stats().snapshot();
println!("Services: {}", stats.services_active);
println!("Endpoints: {} / {}",
    stats.endpoints_healthy,
    stats.endpoints_total
);
println!("Hit Rate: {}%", stats.lookup_hit_rate_bps() as f64 / 100.0);
```

### Health Reporting

```rust
let report = registry.health();
match report.state {
    HealthState::Healthy => println!("All systems nominal"),
    HealthState::Degraded => println!("Investigate during business hours"),
    HealthState::Critical => println!("Page on-call immediately"),
}
```

## Error Handling

All operations return `Result<T, ServiceRegistryError>`:

```rust
pub enum ServiceRegistryError {
    ServiceNotFound(ServiceId),
    ServiceExists(ServiceId),
    NoHealthyInstance(ServiceId),
    InvalidEndpoint(&'static str),
    EndpointExists(ServiceEndpoint),
    ServiceDraining(ServiceId),
    CapacityExceeded(&'static str),
    ShuttingDown,
}
```

Error classification predicates:

```rust
error.is_fatal()      // Abort, don't retry
error.is_retryable()  // Back off and retry
error.is_capacity()   // Shed load
```

## Performance

### Targets

- Service Discovery: < 1ms p95 latency
- Registration: < 10ms p95 latency
- Health Check: < 100ms per check
- Throughput: 100K lookups/second

### Optimization Techniques

- Lock-free reads for statistics
- Cache-friendly fixed-size structures
- Lazy health evaluation
- Batch health updates

## Limitations

- Max 255 endpoints per service (u8 limitation)
- Service names max 63 bytes
- Single health check type per service (Phase 0)
- In-memory watch subscriptions only

## Dependencies

- **configuration-system**: Configuration management
- **session-registry**: Node and cluster information
- **session-manager**: Session identification
- **connection-shard**: Connection tracking

## License

Licensed under the same terms as the Vireon project.

## See Also

- [DESIGN.md](DESIGN.md) - Detailed design documentation
- [Session Registry](../../session-layer/session-registry/) - Cluster node tracking
- [Session Manager](../../session-layer/session-manager/) - Session orchestration
