# Service Registry Design Document

## 1. Overview

The Service Registry is a critical component of the Vireon Application Layer that provides application-level service discovery, health tracking, endpoint management, and load balancing for microservices architectures. It serves as the central authority for mapping service names to concrete network endpoints, enabling dynamic service-to-service communication without hardcoded addresses.

## 2. Architecture

### 2.1 System Model

The Service Registry operates as a distributed directory service that maintains:

- **Service Catalog**: Logical service definitions with metadata
- **Endpoint Directory**: Network-accessible service instances
- **Health State**: Real-time health status for all instances
- **Load Balancer**: Multiple strategies for request distribution

```
Service Request → [Service Registry] → Endpoint Selection
                      ↓
                 ┌───────────────────────────────────────┐
                 │                                       │
                 ├─ Service Catalog (name → metadata)     │
                 ├─ Endpoint Directory (service → IPs)   │
                 ├─ Health Tracker (instance health)      │
                 ├─ Load Balancer (selection strategies)  │
                 └─ Watch System (change notifications)   │
                 └───────────────────────────────────────┘
```

### 2.2 Key Properties

- **High Availability**: No single point of failure; registry remains available during partitions
- **Consistency**: Strong consistency for service registration; eventual consistency for health updates
- **Scalability**: Supports 100K+ services, 1M+ endpoints across distributed clusters
- **Performance**: Service discovery in sub-millisecond latency with high throughput
- **Observability**: Comprehensive metrics for latency, hit rates, and health status

## 3. Service Model

### 3.1 Service Identification

Services are identified by a 64-bit `ServiceId`:

- **0**: Reserved as nil sentinel
- **1-2^56-1**: Valid service identifiers
- **2^56-2^64**: Reserved for future use

```rust
pub struct ServiceId(u64);

impl ServiceId {
    pub const NIL: Self = Self(0);
    pub const fn new(id: u64) -> Self;
    pub const fn is_nil(self) -> bool;
    pub const fn raw(self) -> u64;
}
```

### 3.2 Service Lifecycle

Services progress through four states:

```
Registered → Active → Draining → Unregistered
     ↓
     └────→ Unregistered (immediate)
```

**State Transitions**:

- `Registered → Active`: Service activated, accepting requests
- `Active → Draining`: Graceful shutdown in progress
- `Draining → Unregistered`: All connections drained, service removed
- `Registered → Unregistered`: Removed before activation
- `Active → Unregistered`: Force removal (emergency shutdown)

**State Properties**:

```rust
pub enum ServiceState {
    Registered,  // Initial state, not yet active
    Active,      // Accepting requests
    Draining,    // Graceful shutdown
    Unregistered, // Terminal state
}

impl ServiceState {
    pub const fn is_active(self) -> bool;      // Can serve traffic
    pub const fn is_draining(self) -> bool;    // Graceful shutdown
    pub const fn can_transition_to(self, target: Self) -> bool;
}
```

### 3.3 Service Metadata

Each service carries:

```rust
pub struct ServiceInfo {
    pub id: ServiceId,           // Unique identifier
    pub name: [u8; 64],         // UTF-8 name (truncated)
    pub name_len: u8,           // Name byte length
    pub state: ServiceState,    // Current lifecycle state
    pub endpoint_count: u8,    // Number of registered endpoints
    pub registered_ns: u64,     // Registration timestamp
}
```

**Constraints**:

- `name`: UTF-8 string, max 63 bytes (64th byte reserved for null)
- `endpoint_count`: 0-255 endpoints per service
- `state`: Must follow state transition rules

## 4. Endpoint Model

### 4.1 Endpoint Definition

Endpoints represent concrete network locations where service instances are accessible:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ServiceEndpoint {
    pub node_id: NodeId,         // Hosting node
    pub address: [u8; 16],      // IPv6-ready address
    pub port: u16,              // TCP/UDP/QUIC port
    pub weight: u32,            // Load balancer weight
    pub is_healthy: bool,       // Current health status
}
```

**Address Format**:

- IPv4 addresses mapped to IPv6: `::ffff:a.b.c.d`
- IPv6 addresses: Native 128-bit values
- Loopback: `::1` (127.0.0.1)

**Port Conventions**:

- `443`: HTTPS/QUIC (default)
- `8443`: Alt-HTTPS/QUIC
- `8080-8999`: HTTP services

**Weight Semantics**:

- Default: `100`
- Range: `1-10000` (higher = more traffic)
- Normalized by: `weight / sum(weights)`

### 4.2 Endpoint Uniqueness

Endpoints are unique per `(service_id, node_id, address, port)` tuple:

- Adding duplicate endpoint returns `Err(EndpointExists)`
- Removing non-existent endpoint returns `Err(ShuttingDown)` in Phase 0
- Same address on different ports → different endpoints

### 4.3 Health Indicators

Endpoint health is determined by:

- **Active Health Checks**: Periodic probes (HTTP/TCP/custom)
- **Passive Monitoring**: Request success/failure rates
- **Circuit Breaking**: Temporary blacklisting after failures

Health updates:

```rust
pub fn record_endpoint_health_change(&self, now_healthy: bool);
```

## 5. Instance Model

### 5.1 Instance State

Each endpoint has runtime state for load balancing:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServiceInstance {
    pub endpoint: ServiceEndpoint,       // Network location
    pub active_requests: u32,           // Current request count
    pub last_health_ns: u64,            // Last health check timestamp
    pub health_score: u8,               // 0-100 health metric
}
```

### 5.2 Health Scoring

Health scores are computed from:

- **Success Rate**: `(success / total) * 100`
- **Latency Percentiles**: p95 < threshold = bonus
- **Error Rates**: `errors / total` deduction
- **Circuit State**: Open circuit = score 0

**Score Interpretation**:

- `0-50`: Unhealthy (excluded from load balancing)
- `51-80`: Degraded (low priority)
- `81-100`: Healthy (normal priority)

### 5.3 Load Factor

Load factor estimates instance utilization:

```rust
pub const fn load_factor(&self) -> u16 {
    // Returns 0-10000 basis points
    // 0 = idle, 10000 = fully loaded
    let factor = (self.active_requests * 10000) / MAX_REQUESTS;
    factor.min(10000) as u16
}
```

**Load Balancing Uses**:

- **Least-loaded**: Select instance with minimum load factor
- **Thresholds**: `factor > 8000` → stop sending new requests
- **Autoscaling**: Average `factor > 7000` → scale up

## 6. Load Balancing

### 6.1 Strategies

The registry supports multiple load balancing strategies:

#### 6.1.1 Round-Robin

Distributes requests sequentially across endpoints:

```rust
pub fn round_robin(&self, endpoints: &[ServiceEndpoint])
    -> Result<ServiceEndpoint, ServiceRegistryError>;
```

**Properties**:

- Fair distribution across healthy endpoints
- O(1) selection with atomic counter
- Respects endpoint weights (weighted round-robin)

#### 6.1.2 Least-Loaded

Selects instance with fewest active requests:

```rust
pub fn least_loaded(&self, instances: &[ServiceInstance])
    -> Result<ServiceInstance, ServiceRegistryError>;
```

**Properties**:

- Minimizes request queue depth
- O(n) scan (acceptable for n < 1000)
- Accounts for health score: `adjusted_load = active_requests / (health_score / 100)`

#### 6.1.3 Weighted Selection

Probabilistic selection based on endpoint weights:

```rust
pub fn weighted_select(&self, endpoints: &[ServiceEndpoint])
    -> Result<ServiceEndpoint, ServiceRegistryError>;
```

**Algorithm**:

1. Compute cumulative weights: `cumsum[i] = sum(endpoints[0..i].weight)`
2. Generate random `r ∈ [0, total_weight)`
3. Find first `i` where `cumsum[i] > r`
4. Return `endpoints[i]`

### 6.2 Strategy Selection

Applications choose strategy via configuration:

```rust
pub enum LoadBalanceStrategy {
    RoundRobin,      // Default, fair distribution
    LeastLoaded,     // Minimize queue depth
    Weighted,        // Respect endpoint weights
    Random,          // Pure random selection
}
```

**Recommendations**:

- **Stateless Services**: Round-robin or random
- **Stateful Services**: Least-loaded or consistent-hash
- **Heterogeneous Capacity**: Weighted selection

## 7. Health Tracking

### 7.1 Health Check Types

The registry supports multiple health check mechanisms:

#### 7.1.1 HTTP Health Checks

```rust
pub struct HttpHealthCheck {
    pub path: String,           // Default: "/health"
    pub interval_ms: u64,       // Check interval
    pub timeout_ms: u64,        // Request timeout
    pub expected_status: u16,    // Default: 200
}
```

**Semantics**:

- `2xx`: Healthy
- `5xx`: Unhealthy
- Timeout: Unhealthy
- Network error: Unhealthy

#### 7.1.2 TCP Health Checks

```rust
pub struct TcpHealthCheck {
    pub interval_ms: u64,
    pub timeout_ms: u64,
}
```

**Semantics**:

- Connection succeeds: Healthy
- Connection fails: Unhealthy

#### 7.1.3 Custom Health Checks

```rust
pub trait CustomHealthCheck: Send + Sync {
    fn check(&self) -> Result<bool, Box<dyn Error>>;
}
```

**Use Cases**:

- Database connectivity checks
- External API availability
- Resource exhaustion detection

### 7.2 Health State Machine

Endpoints transition between health states:

```
Healthy ←→ Unhealthy ←→ Critical
    ↓           ↓           ↓
    └──────────┴───────────┴──→ Blacklisted (temporary)
```

**Transitions**:

- `Healthy → Unhealthy`: 3 consecutive failures
- `Unhealthy → Healthy`: 5 consecutive successes
- `Unhealthy → Critical`: 10 consecutive failures
- `Critical → Healthy`: 10 consecutive successes
- `Any → Blacklisted`: Circuit breaker threshold

**Thresholds** (configurable):

```rust
pub const HEALTH_FAILURE_THRESHOLD: u32 = 3;
pub const HEALTH_RECOVERY_THRESHOLD: u32 = 5;
pub const CRITICAL_FAILURE_THRESHOLD: u32 = 10;
pub const CIRCUIT_BREAK_THRESHOLD: u32 = 20;
```

### 7.3 Health Scoring Algorithm

Health scores combine multiple signals:

```rust
fn compute_health_score(
    success_rate: f64,        // 0.0-1.0
    latency_ms: u64,          // Average latency
    error_rate: f64,          // 0.0-1.0
    circuit_state: CircuitState,
) -> u8 {
    let mut score = 100u8;

    // Success rate deduction: up to 40 points
    score -= ((1.0 - success_rate) * 40.0) as u8;

    // Latency deduction: up to 20 points
    if latency_ms > 1000 {
        score -= 20;
    } else if latency_ms > 500 {
        score -= 10;
    }

    // Error rate deduction: up to 30 points
    score -= (error_rate * 30.0) as u8;

    // Circuit state deduction
    if matches!(circuit_state, CircuitState::Open) {
        score = score.saturating_sub(50);
    }

    score.max(0)
}
```

## 8. Watch System

### 8.1 Subscription Model

The watch system allows clients to subscribe to service changes:

```rust
pub struct WatchHandle {
    pub id: u64,
}

impl WatchHandle {
    pub fn cancel(&mut self) -> Result<(), ServiceRegistryError>;
    pub fn is_active(&self) -> bool;
}
```

### 8.2 Watch Events

Subscribers receive notifications for:

```rust
pub enum WatchEvent {
    ServiceRegistered(ServiceId),
    ServiceUnregistered(ServiceId),
    EndpointAdded { service: ServiceId, endpoint: ServiceEndpoint },
    EndpointRemoved { service: ServiceId, endpoint: ServiceEndpoint },
    HealthChanged { endpoint: ServiceEndpoint, was_healthy: bool, now_healthy: bool },
}
```

### 8.3 Delivery Guarantees

- **At-least-once**: Events may be duplicated
- **Ordering**: Per-service event ordering is guaranteed
- **Filtering**: Clients can filter by service or event type

## 9. Statistics and Observability

### 9.1 Key Metrics

The registry exposes:

```rust
pub struct ServiceRegistryStats {
    // Service lifecycle
    pub services_registered: AtomicU64,
    pub services_active: AtomicU64,
    pub services_draining: AtomicU64,

    // Endpoint lifecycle
    pub endpoints_total: AtomicU64,
    pub endpoints_healthy: AtomicU64,

    // Discovery hot path
    pub lookups_total: AtomicU64,
    pub lookups_hit: AtomicU64,

    // Load balancing
    pub load_balance_decisions: AtomicU64,
}
```

### 9.2 Derived Metrics

From the raw counters, we compute:

```rust
pub struct StatsSnapshot {
    // Raw counters
    pub services_registered: u64,
    pub endpoints_healthy: u64,
    pub lookups_total: u64,
    pub lookups_hit: u64,

    // Computed rates (in basis points)
    pub lookup_hit_rate_bps: u16,         // 0-10000
    pub endpoint_health_ratio_bps: u16,   // 0-10000
}
```

**Rate Calculations**:

- `lookup_hit_rate = (lookups_hit / lookups_total) * 10000`
- `endpoint_health_ratio = (endpoints_healthy / endpoints_total) * 10000`
- Basis points enable lossless integer arithmetic

### 9.3 Health Reporting

```rust
pub struct HealthReport {
    pub state: HealthState,               // Healthy/Degraded/Critical
    pub services_active: u64,
    pub endpoints_healthy: u64,
    pub lookup_hit_rate_bps: u16,
    pub endpoints_unhealthy_ratio_bps: u16,
}
```

**Health Thresholds**:

- `Healthy`: Unhealthy ratio < 10% (1000 bps)
- `Degraded`: Unhealthy ratio 10-30% (1000-3000 bps)
- `Critical`: Unhealthy ratio > 30% (3000 bps)

## 10. Configuration

### 10.1 Configuration Structure

```rust
pub struct ServiceRegistryConfig {
    pub max_services: u32,                // Capacity limit
    pub max_instances_per_service: u8,    // Per-service limit
    pub health_check_interval_ms: u64,    // Check frequency
    pub enable_load_balancing: bool,       // Feature flag
    pub drain_timeout_ms: u64,             // Graceful shutdown
}
```

### 10.2 Presets

Pre-configured settings for common deployments:

```rust
pub enum Preset {
    Local,       // Single-node development
    Cluster,     // Multi-node production
    Federation,  // Cross-cluster deployment
    Edge,        // Edge / limited connectivity
}
```

**Preset Defaults**:

| Preset | Max Services | Instances | Health Check | Drain Timeout |
|--------|--------------|-----------|--------------|---------------|
| Local  | 100          | 10        | 5s           | 10s           |
| Cluster| 10,000       | 50        | 10s          | 30s           |
| Federation | 100,000    | 100       | 30s          | 60s           |
| Edge   | 1,000        | 20        | 60s          | 120s          |

### 10.3 Builder Pattern

Fluent configuration API:

```rust
let cfg = ServiceRegistryBuilder::from_preset(Preset::Cluster)
    .max_services(5000)
    .health_check_interval_ms(15_000)
    .load_balancing(true)
    .build()?;
```

## 11. Error Handling

### 11.1 Error Taxonomy

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

### 11.2 Error Classification

Errors implement predicate methods:

```rust
impl ServiceRegistryError {
    pub const fn is_fatal(&self) -> bool;
    pub const fn is_retryable(&self) -> bool;
    pub const fn is_capacity(&self) -> bool;
}
```

**Classification**:

- **Fatal**: `InvalidEndpoint`, `ShuttingDown` → Abort, don't retry
- **Retryable**: `NoHealthyInstance`, `ServiceDraining` → Back off, retry
- **Capacity**: `CapacityExceeded` → Shed load

## 12. Security Considerations

### 12.1 Service Isolation

- Services run with principle of least privilege
- No cross-service communication without explicit registration
- Endpoint addresses validated before registration

### 12.2 Authentication

- Service registration requires authenticated principals
- Health checks are authenticated (mTLS)
- Watch subscriptions require authorization

### 12.3 Rate Limiting

- Service registration: 100 requests/second per principal
- Endpoint updates: 1000 requests/second per service
- Lookups: 10,000 requests/second per client

## 13. Performance

### 13.1 Performance Targets

- **Service Discovery**: < 1ms p95 latency
- **Registration**: < 10ms p95 latency
- **Health Check**: < 100ms per check
- **Throughput**: 100K lookups/second per instance

### 13.2 Optimization Techniques

- **Lock-Free Reads**: Atomic counters for stats
- **Cache-Friendly Structures**: Fixed-size arrays for endpoints
- **Lazy Evaluation**: Health checks only when needed
- **Batching**: Bulk health updates

### 13.3 Scaling Limits

| Resource | Maximum | Notes |
|----------|---------|-------|
| Services | 100K | Limited by config.max_services |
| Endpoints per Service | 255 | Limited by u8 endpoint_count |
| Total Endpoints | 10M | Limited by cluster memory |
| Health Checks | 1M/second | Limited by health_check_interval |

## 14. Deployment

### 14.1 Single-Node Deployment

```
┌─────────────────────────────────────┐
│          Application                │
│         (vireon-app)                │
└──────────┬──────────────────────────┘
           │
           ↓
┌─────────────────────────────────────┐
│      Service Registry              │
│   (Local Preset, 100 services)     │
└─────────────────────────────────────┘
```

### 14.2 Cluster Deployment

```
┌───────────────┐     ┌───────────────┐     ┌───────────────┐
│   App Node 1  │     │   App Node 2  │     │   App Node 3  │
└───────┬───────┘     └───────┬───────┘     └───────┬───────┘
        │                     │                     │
        └─────────────────────┼─────────────────────┘
                              ↓
                    ┌───────────────────────┐
                    │ Service Registry      │
                    │ (Cluster Preset)      │
                    └───────────────────────┘
```

### 14.3 Federation Deployment

```
┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│   Region A      │       │   Region B      │       │   Region C      │
│                 │       │                 │       │                 │
│ ┌─────────────┐ │       │ ┌─────────────┐ │       │ ┌─────────────┐ │
│ │   Registry  │ │       │ │   Registry  │ │       │ │   Registry  │ │
│ └─────────────┘ │       │ └─────────────┘ │       │ └─────────────┘ │
└────────┬────────┘       └────────┬────────┘       └────────┬────────┘
         │                           │                           │
         └───────────────────────────┼───────────────────────────┘
                                     ↓
                         ┌───────────────────────┐
                         │ Federation Gateway    │
                         │ (Aggregates registries)│
                         └───────────────────────┘
```

## 15. Testing

### 15.1 Unit Tests

Each module has comprehensive unit tests:

- **Error Classification**: Verify error predicates
- **State Transitions**: Validate state machine rules
- **Configuration**: Test presets and validation
- **Statistics**: Counter accuracy and snapshot consistency

### 15.2 Integration Tests

Integration tests verify:

- End-to-end service registration and discovery
- Health check lifecycle
- Load balancer selection accuracy
- Watch system event delivery

### 15.3 Performance Tests

Benchmarks measure:

- Service registration throughput
- Discovery latency under load
- Health check batch processing
- Load balancer selection speed

## 16. Phase 1+ Roadmap

### 16.1 Phase 1 (Next Sprint)

**Service Registry Implementation**:

- Real service store (HashMap-based)
- Endpoint management
- Basic health tracking
- Statistics integration

**Load Balancer Implementation**:

- Round-robin strategy
- Least-loaded strategy
- Weighted selection

### 16.2 Phase 2 (Future)

**Advanced Features**:

- Distributed registry (Raft-based)
- Health check plugins
- Event streaming (watch system)
- Prometheus metrics export

### 16.3 Phase 3+ (Future)

**Advanced Features**:

- Service mesh integration
- DNS-based discovery
- Geo-aware routing
- Advanced load balancing (consistent hashing)

## 17. Dependencies

### 17.1 Internal Dependencies

- **configuration-system**: Configuration management
- **session-registry**: Node and cluster information
- **session-manager**: Session identification
- **connection-shard**: Connection tracking

### 17.2 External Dependencies

- **thiserror**: Error derivation
- **tracing**: Structured logging
- **criterion**: Benchmarking (dev-only)

## 18. Compatibility

### 18.1 Rust Version

Minimum: Rust 2024 edition
Target: Stable Rust 1.85+

### 18.2 Platform Support

- **Linux**: Primary target (x86_64, aarch64)
- **macOS**: Development target
- **Windows**: Community support

### 18.3 Network Protocols

- IPv4: Mapped to IPv6 (`::ffff:a.b.c.d`)
- IPv6: Native support
- QUIC: Default port 443
- TCP: Alternative transport

## 19. Limitations

### 19.1 Known Limitations

- **Endpoint Count**: Max 255 per service (u8 limitation)
- **Service Name**: Max 63 bytes (fixed-size array)
- **Health Check**: Single check type per service
- **Watch System**: In-memory only (no persistence)

### 19.2 Future Enhancements

- Dynamic endpoint limits (u32 endpoint_count)
- Service name references (String-based)
- Multiple health check types per service
- Persistent watch subscriptions

## 20. References

### 20.1 Design Inspiration

- **Consul**: Service discovery and health checking
- **Eureka**: Netflix service registry
- **etcd**: Distributed key-value store
- **Kubernetes**: Service abstraction

### 20.2 Academic Papers

- "A Byzantine Fault-Tolerant Raft Implementation" (Diego Ongaro)
- "The Mystery Machine: Predictive End-to-End Performance" (Peter Bodik)
- "Scaling Microservices with an Intelligent Load Balancer" (Google)

### 20.3 Standards

- RFC 6265 (HTTP State Management)
- RFC 8446 (TLS 1.3)
- RFC 9000 (QUIC Transport Protocol)
