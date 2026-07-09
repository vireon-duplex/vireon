# rpc-framework

**RPC framework for the Vireon QUIC runtime**

The RPC framework provides a complete Remote Procedure Call system built on top of Vireon's QUIC transport. It supports four streaming modes (unary, server-streaming, client-streaming, and bidirectional streaming) with a focus on low-latency, high-throughput communication.

## Features

- **Four Streaming Modes**: Unary, server-streaming, client-streaming, and bidirectional streaming
- **Type-Safe API**: Strongly typed method IDs and status codes
- **Error Handling**: Comprehensive error classification with retry logic
- **Configuration Presets**: Low-latency, balanced, and throughput configurations
- **Statistics**: Detailed call, stream, and byte-level metrics
- **Health Monitoring**: Built-in health reports with thresholds
- **Flexible Configuration**: Builder pattern with validation

## Phase 0 Status

This crate is in Phase 0, which includes:

- ✅ Core type definitions (method IDs, stream handles, status codes)
- ✅ Error types with classification predicates
- ✅ Configuration with presets
- ✅ Statistics with snapshots
- ✅ Health monitoring with thresholds
- ✅ Skeleton implementations for server, client, and dispatcher

Phase 1+ will include:
- Full server implementation with method registration
- Full client implementation with connection pooling
- Full dispatcher implementation with routing
- Request/response codecs
- Async handler support
- Streaming handler support

## Usage

### Basic Setup

```rust
use rpc_framework::{RpcConfig, RpcClient, RpcServer, RpcMethodId, RpcMethodType};

// Create a config
let config = RpcConfig::new()
    .max_concurrent_streams(128)
    .default_timeout_ms(30_000);

// Create a server
let mut server = RpcServer::new(config);

// Register methods (Phase 1+)
server.register_method(RpcMethodId::new(1), RpcMethodType::Unary)?;

// Create a client
let client = RpcClient::new(config);
```

### Configuration Presets

```rust
use rpc_framework::{Preset, RpcFrameworkBuilder};

// Low-latency configuration
let config = RpcFrameworkBuilder::from_preset(Preset::LowLatency).build()?;

// High-throughput configuration
let config = RpcFrameworkBuilder::from_preset(Preset::Throughput).build()?;
```

### Statistics

```rust
use rpc_framework::{RpcStats, StatsSnapshot};

let stats = RpcStats::new();
// ... perform RPC calls ...
let snapshot = stats.snapshot();
println!("Success rate: {}%", snapshot.success_rate_bps() / 100);
println!("Active streams: {}", snapshot.active_streams());
```

### Health Monitoring

```rust
use rpc_framework::HealthReport;

let report = HealthReport::derive(&stats.snapshot());
if report.needs_attention() {
    eprintln!("Health check failed: {:?}", report.state);
}
```

## Architecture

```
Application Layer
    ↓
RPC Client → Dispatcher → RPC Server
    ↓            ↓            ↓
Stream Manager (QUIC streams)
```

### Components

- **RPC Client**: Initiates RPC calls with retry logic
- **Dispatcher**: Routes requests to registered handlers
- **RPC Server**: Handles incoming requests
- **Stream Manager**: Manages QUIC stream lifecycle

## Error Handling

Errors are classified into three categories:

```rust
use rpc_framework::RpcError;

match result {
    Err(e) if e.is_fatal() => {
        // Don't retry - programming error or terminal state
    }
    Err(e) if e.is_retryable() => {
        // Transient error - back off and retry
    }
    Err(e) if e.is_cancelled() => {
        // Operation was cancelled
    }
    _ => Ok(response) => {
        // Success
    }
}
```

## Configuration

### Fields

- `max_concurrent_streams`: Maximum concurrent streams (1-10,000)
- `default_timeout_ms`: Default timeout in milliseconds (100ms-10min)
- `max_message_bytes`: Maximum message size (1KB-100MB)
- `enable_compression`: Enable payload compression
- `max_retries`: Maximum retries for retryable errors (0-10)

### Presets

| Preset | Streams | Timeout | Message Size | Use Case |
|--------|---------|---------|--------------|----------|
| LowLatency | 32 | 5s | 1 MB | Real-time |
| Balanced | 64 | 30s | 4 MB | General |
| Throughput | 256 | 60s | 16 MB | Batch |

## Performance

The framework is designed for:
- **Low Latency**: Sub-millisecond dispatch overhead
- **High Throughput**: Millions of calls per second
- **Zero-Copy**: Minimal allocations in hot paths

Benchmarks are available via:
```bash
cargo bench --bench dispatch_unary
cargo bench --bench open_stream
cargo bench --bench round_trip
cargo bench --bench cancel
```

## Documentation

- [DESIGN.md](DESIGN.md) - Complete design specification
- [Module Documentation](https://docs.rs/rpc-framework) - API docs

## Testing

Run tests:
```bash
cargo test
```

Run benchmarks:
```bash
cargo bench
```

Check code:
```bash
cargo clippy -- -D warnings
```

## Dependencies

- `configuration-system` - Configuration management
- `middleware-pipeline` - Request/response processing
- `version-negotiation` - Protocol version negotiation
- `session-manager` - Session management
- `stream-manager` - QUIC stream lifecycle
- `thiserror` - Error derive macros
- `tracing` - Structured logging

## License

Licensed under the same terms as the Vireon project.

## Contributing

This crate is part of the Vireon QUIC runtime. See the main project for contribution guidelines.
