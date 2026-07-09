# RPC Framework Design Specification

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Core Concepts](#3-core-concepts)
4. [Method Registry](#4-method-registry)
5. [Streaming Modes](#5-streaming-modes)
6. [Status Codes](#6-status-codes)
7. [Error Handling](#7-error-handling)
8. [Configuration](#8-configuration)
9. [Statistics](#9-statistics)
10. [Health Monitoring](#10-health-monitoring)
11. [Server Implementation](#11-server-implementation)
12. [Client Implementation](#12-client-implementation)
13. [Dispatcher Implementation](#13-dispatcher-implementation)
14. [Stream Lifecycle](#14-stream-lifecycle)
15. [Timeout and Cancellation](#15-timeout-and-cancellation)
16. [Retry Logic](#16-retry-logic)
17. [Compression](#17-compression)
18. [Performance Considerations](#18-performance-considerations)
19. [Testing Strategy](#19-testing-strategy)
20. [Future Enhancements](#20-future-enhancements)

---

## 1. Overview

The RPC Framework provides a complete Remote Procedure Call system built on top of Vireon's QUIC transport. It supports four streaming modes (unary, server-streaming, client-streaming, and bidirectional streaming) with a focus on low-latency, high-throughput communication.

### 1.1 Design Goals

- **Simplicity**: Easy-to-use API that hides QUIC complexity
- **Performance**: Zero-copy where possible, minimal allocations
- **Reliability**: Comprehensive error handling and retry logic
- **Observability**: Rich statistics and health monitoring
- **Flexibility**: Support for multiple streaming modes and compression

### 1.2 Non-Goals

- Language-agnostic RPC (this is Rust-to-Rust only)
- Schema/code generation (no .proto files)
- Load balancing (handled by higher layers)
- Service discovery (handled by session-registry)

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         Application                          │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                         RPC Client                          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│  │  Unary   │  │  Server  │  │  Client  │  │   Bidi   │  │
│  │   Call   │  │ Streaming│  │ Streaming│  │ Streaming│  │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘  │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                      Dispatcher                              │
│            (Route → Handler → Response)                     │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                         RPC Server                          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│  │  Method  │  │ Request  │  │ Response │  │  Stream  │  │
│  │ Registry│  │ Handler  │  │  Codec   │  │ Manager  │  │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘  │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│                    Stream Manager                           │
│              (QUIC Stream Lifecycle)                         │
└─────────────────────────────────────────────────────────────┘
```

### 2.1 Layer Responsibilities

| Layer | Responsibility |
|-------|---------------|
| RPC Client | Connection pooling, call initiation, retry logic |
| Dispatcher | Method routing, load balancing |
| RPC Server | Method registration, request handling |
| Stream Manager | QUIC stream lifecycle, flow control |

---

## 3. Core Concepts

### 3.1 RPC Methods

RPC methods are identified by a [`RpcMethodId`] (u32) and have a specific [`RpcMethodType`]:

- **Unary**: One request, one response
- **Server Streaming**: One request, multiple responses
- **Client Streaming**: Multiple requests, one response
- **Bidirectional Streaming**: Multiple requests, multiple responses

### 3.2 Stream Handles

Active RPC streams are tracked using [`StreamHandle`] (u64). These handles are used for:
- Cancellation
- Flow control
- Status queries

### 3.3 Status Codes

Every RPC operation returns a [`RpcStatus`]:
- **Ok**: Operation succeeded
- **Error(code)**: Operation failed with specific error code
- **Cancelled**: Operation was cancelled by caller
- **DeadlineExceeded**: Operation exceeded its timeout

---

## 4. Method Registry

### 4.1 Registration

Methods are registered at server startup:

```rust
let mut server = RpcServer::new(config);
server.register_method(
    RpcMethodId::new(1),
    RpcMethodType::Unary
)?;
```

### 4.2 Lookup

The dispatcher maintains a hash table for O(1) method lookup:

```rust
let handler = dispatcher.route(RpcMethodId::new(1))?;
```

### 4.3 Method Handlers

Phase 1+ will support:
- Synchronous handlers (fn(&[u8]) -> Vec<u8>)
- Async handlers (async fn(&[u8]) -> Vec<u8>)
- Streaming handlers (async fn(RequestStream) -> ResponseStream)

---

## 5. Streaming Modes

### 5.1 Unary

Simple request-response pattern:

```
Client ──► Request ──► Server
  ◄──── Response ────┘
```

### 5.2 Server Streaming

Client sends one request, server streams multiple responses:

```
Client ──► Request ──► Server
  ◄──── Response1 ────┘
  ◄──── Response2 ────┘
  ◄──── Response3 ────┘
```

### 5.3 Client Streaming

Client streams multiple requests, server sends one response:

```
Client ──► Request1 ──► Server
Client ──► Request2 ──► Server
Client ──► Request3 ──► Server
  ◄──── Response ────┘
```

### 5.4 Bidirectional Streaming

Both sides stream concurrently:

```
Client ──► Request1 ──► Server
  ◄──── Response1 ────┘
Client ──► Request2 ──► Server
  ◄──── Response2 ────┘
```

---

## 6. Status Codes

### 6.1 Status Values

| Status | Code | Description |
|--------|------|-------------|
| Ok | 0 | Operation succeeded |
| Error | 1+ | Operation failed (application-specific) |
| Cancelled | - | Cancelled by caller |
| DeadlineExceeded | - | Timeout exceeded |

### 6.2 Error Codes

Error codes (1-65535) are application-specific. Common conventions:
- 1-99: Protocol errors
- 100-199: Validation errors
- 200-299: Rate limiting
- 500-599: Internal server errors
- 900-999: Application-specific

---

## 7. Error Handling

### 7.1 Error Types

[`RpcError`] enum variants:

| Variant | Category | Retryable? |
|---------|----------|------------|
| MethodNotFound | Fatal | No |
| InvalidRequest | Fatal | No |
| SerializationError | Fatal | No |
| Timeout | Transient | Yes |
| StreamClosed | Transient | Yes |
| Unavailable | Transient | Yes |
| Cancelled | Cancelled | No |
| PermissionDenied | Fatal | No |
| ShuttingDown | Fatal | No |

### 7.2 Error Predicates

```rust
if let Err(e) = client.call(id, &request) {
    if e.is_fatal() {
        // Don't retry
    } else if e.is_retryable() {
        // Back off and retry
    } else if e.is_cancelled() {
        // Handle cancellation
    }
}
```

---

## 8. Configuration

### 8.1 Config Structure

[`RpcConfig`] fields:

- `max_concurrent_streams`: Maximum concurrent streams per connection
- `default_timeout_ms`: Default timeout for RPC calls
- `max_message_bytes`: Maximum message size
- `enable_compression`: Enable payload compression
- `max_retries`: Maximum retries for retryable errors

### 8.2 Presets

Three presets are provided:

| Preset | Streams | Timeout | Message Size | Retries | Use Case |
|--------|---------|---------|--------------|---------|----------|
| LowLatency | 32 | 5s | 1 MB | 2 | Real-time apps |
| Balanced | 64 | 30s | 4 MB | 3 | General purpose |
| Throughput | 256 | 60s | 16 MB | 5 | Batch processing |

### 8.3 Builder Pattern

```rust
let config = RpcFrameworkBuilder::new()
    .max_concurrent_streams(128)
    .default_timeout_ms(10_000)
    .max_message_bytes(8_000_000)
    .compression(true)
    .max_retries(5)
    .build()?;
```

---

## 9. Statistics

### 9.1 Counter Types

[`RpcStats`] tracks:

- **Calls**: Total, succeeded, failed
- **Streams**: Opened, closed, active
- **Timeouts**: Total timeout count
- **Cancellations**: Total cancellation count
- **Bytes**: Sent, received

### 9.2 Snapshot API

```rust
let snapshot = stats.snapshot();
let success_rate = snapshot.success_rate_bps(); // basis points
let failure_rate = snapshot.failure_rate_bps();
let active_streams = snapshot.active_streams();
```

### 9.3 Rates

Rates are precomputed in basis points (1/100 of a percent):
- 10_000 bps = 100%
- 9_900 bps = 99%
- 5_000 bps = 50%
- 0 bps = 0%

---

## 10. Health Monitoring

### 10.1 Health States

[`HealthState`]:
- **Healthy**: All systems nominal
- **Degraded**: Performance impact, investigate
- **Critical**: Severe impact, page on-call

### 10.2 Thresholds

| Signal | Healthy | Degraded | Critical |
|--------|---------|---------|----------|
| Success Rate | ≥ 99% | 90-99% | < 90% |
| Timeout Rate | ≤ 5% | 5-20% | > 20% |

### 10.3 Health Report

```rust
let report = HealthReport::derive(&stats.snapshot());
if report.needs_attention() {
    // Alert operator
}
```

---

## 11. Server Implementation

### 11.1 Server Lifecycle

```
Startup → Register Methods → Serve Requests → Shutdown
```

### 11.2 Method Registration

Phase 1+ will support:
```rust
server.register_method(RpcMethodId::new(1), RpcMethodType::Unary, handler)?;
```

### 11.3 Request Handling

Request handling flow:
1. Receive request on QUIC stream
2. Dispatch to registered handler
3. Serialize response
4. Send response on stream
5. Update statistics

### 11.4 Graceful Shutdown

On shutdown:
1. Stop accepting new requests
2. Drain active requests
3. Close all streams
4. Return ShuttingDown errors

---

## 12. Client Implementation

### 12.1 Connection Pooling

Phase 1+ will maintain a pool of connections to each server.

### 12.2 Call API

Unary call:
```rust
let response = client.call(method_id, &request)?;
```

Streaming call:
```rust
let stream = client.open_stream(method_id, RpcMethodType::ServerStreaming)?;
```

### 12.3 Retry Logic

Retry strategy:
1. Check if error is retryable
2. Increment retry counter
3. Back off (exponential)
4. Retry if under max_retries

### 12.4 Cancellation

Cancel an active stream:
```rust
client.cancel(stream_handle)?;
```

---

## 13. Dispatcher Implementation

### 13.1 Routing Table

The dispatcher maintains a hash table:
```rust
HashMap<RpcMethodId, Handler>
```

### 13.2 Load Balancing

Phase 1+ will support:
- Round-robin
- Least-connections
- Weighted

### 13.3 Dispatch Flow

1. Lookup method in registry
2. Select handler instance
3. Invoke handler with request
4. Return response

---

## 14. Stream Lifecycle

### 14.1 Stream States

```
Init → Opening → Open → Closing → Closed
                      ↓
                  Cancelled
```

### 14.2 Stream Handle

Each stream gets a unique `StreamHandle`:
- Allocated on `open_stream()`
- Valid until stream is closed
- Used for cancellation

### 14.3 Flow Control

Flow control is managed by Stream Manager:
- Per-stream send/recv windows
- Backpressure propagation
- Credit-based flow control

---

## 15. Timeout and Cancellation

### 15.1 Timeouts

Timeouts are enforced per-call:
```rust
let config = RpcConfig::new()
    .default_timeout_ms(5_000); // 5 seconds
```

### 15.2 Cancellation

Cancellation flow:
1. Client calls `cancel(handle)`
2. Stream is marked for cancellation
3. Handler is notified
4. Resources are cleaned up

### 15.3 Deadline Propagation

Phase 1+ will propagate deadlines:
- From client to server
- From server to handler
- Across async boundaries

---

## 16. Retry Logic

### 16.1 Retryable Errors

Retryable errors:
- Timeout
- Unavailable
- StreamClosed

### 16.2 Backoff Strategy

Exponential backoff:
```rust
let delay = base_delay * 2 ^ retry_attempt;
```

### 16.3 Max Retries

Configure max retries:
```rust
let config = RpcConfig::new()
    .max_retries(5);
```

---

## 17. Compression

### 17.1 Compression Algorithm

Phase 1+ will support:
- Zstandard (zstd)
- LZ4
- Snappy

### 17.2 Compression Threshold

Only compress messages above a threshold:
```rust
if message.len() > COMPRESSION_THRESHOLD {
    compress(&mut message);
}
```

### 17.3 Negotiation

Compression is negotiated per-connection.

---

## 18. Performance Considerations

### 18.1 Zero-Copy

Where possible, use zero-copy techniques:
- `bytes::Bytes` for payloads
- Slice references for read-only
- Avoid intermediate allocations

### 18.2 Batching

Batch multiple RPCs:
- Client batching (send multiple requests)
- Server batching (process multiple requests)

### 18.3 Connection Reuse

Reuse QUIC connections:
- Multiplex multiple streams
- Connection pooling
- Keep-alive

---

## 19. Testing Strategy

### 19.1 Unit Tests

Each module has comprehensive unit tests:
- Newtype types (5+ tests)
- Enums (5+ tests)
- Configs (6+ tests)
- Stats (6+ tests)
- Health (5+ tests)

### 19.2 Integration Tests

Phase 1+ will add:
- Round-trip tests
- Streaming tests
- Error injection tests
- Timeout tests

### 19.3 Benchmarks

Four benchmarks:
- `dispatch_unary`: Dispatch throughput
- `open_stream`: Stream opening latency
- `round_trip`: End-to-end latency
- `cancel`: Cancellation overhead

---

## 20. Future Enhancements

### 20.1 Phase 1

- Full server implementation
- Full client implementation
- Full dispatcher implementation
- Request/response codecs

### 20.2 Phase 2

- Async handler support
- Streaming handler support
- Advanced retry strategies
- Circuit breaker pattern

### 20.3 Phase 3+

- Schema evolution
- Backward compatibility
- Load balancing strategies
- Metrics integration (Prometheus)

---

## Appendix A: Type Reference

### A.1 Core Types

```rust
pub struct RpcMethodId(u32);
pub struct StreamHandle(u64);
pub enum RpcMethodType { Unary, ServerStreaming, ClientStreaming, BidiStreaming }
pub enum RpcStatus { Ok, Error(u16), Cancelled, DeadlineExceeded }
```

### A.2 Error Types

```rust
pub enum RpcError {
    MethodNotFound(RpcMethodId),
    InvalidRequest(&'static str),
    SerializationError(&'static str),
    Timeout,
    StreamClosed(StreamHandle),
    Unavailable(&'static str),
    Cancelled(StreamHandle),
    PermissionDenied,
    ShuttingDown,
}
```

### A.3 Config Types

```rust
pub struct RpcConfig {
    pub max_concurrent_streams: u32,
    pub default_timeout_ms: u64,
    pub max_message_bytes: u64,
    pub enable_compression: bool,
    pub max_retries: u8,
}

pub enum Preset {
    LowLatency,
    Balanced,
    Throughput,
}
```

---

## Appendix B: Constants Reference

### B.1 Stream Limits

```rust
pub const MIN_CONCURRENT_STREAMS: u32 = 1;
pub const MAX_CONCURRENT_STREAMS: u32 = 10_000;
```

### B.2 Timeout Limits

```rust
pub const MIN_TIMEOUT_MS: u64 = 100;
pub const MAX_TIMEOUT_MS: u64 = 600_000;
```

### B.3 Message Size Limits

```rust
pub const MIN_MESSAGE_BYTES: u64 = 1_024;
pub const MAX_MESSAGE_BYTES: u64 = 100_000_000;
```

### B.4 Retry Limits

```rust
pub const MIN_RETRIES: u8 = 0;
pub const MAX_RETRIES: u8 = 10;
```

---

## Appendix C: Migration Guide

### C.1 From Phase 0 to Phase 1

When upgrading from Phase 0 to Phase 1:

1. Replace skeleton implementations with real ones
2. Register actual method handlers
3. Implement request/response codecs
4. Add connection pooling

### C.2 Configuration Changes

Configuration is backward compatible:
- Existing configs continue to work
- New fields have sensible defaults
- Presets remain unchanged

---

## Appendix D: Troubleshooting

### D.1 Common Issues

**Issue**: `MethodNotFound` error
- **Cause**: Method not registered
- **Fix**: Register method on server startup

**Issue**: `Timeout` error
- **Cause**: Request exceeded deadline
- **Fix**: Increase timeout or optimize handler

**Issue**: `StreamClosed` error
- **Cause**: Stream closed by peer
- **Fix**: Retry the operation

### D.2 Debugging

Enable debug logging:
```rust
RUST_LOG=rpc_framework=debug cargo run
```

---

## Appendix E: Performance Tuning

### E.1 Low Latency

Use `LowLatency` preset:
- Fewer concurrent streams
- Shorter timeouts
- Smaller message sizes

### E.2 High Throughput

Use `Throughput` preset:
- More concurrent streams
- Longer timeouts
- Larger message sizes
- More retries

---

## Appendix F: Security Considerations

### F.1 Authentication

Authentication is handled by middleware-pipeline:
- Pre-auth stage
- AuthN/AuthZ stage
- Permission checks

### F.2 Authorization

Authorization errors return `PermissionDenied`:
- Not retryable
- Fatal error
- Log for audit

### F.3 Message Size Limits

Enforce `max_message_bytes`:
- Prevent memory exhaustion
- Reject oversized messages
- Return `InvalidRequest`

---

## Appendix G: Glossary

- **Unary**: One request, one response
- **Streaming**: Multiple messages in one direction or both
- **Handle**: Identifier for an active stream
- **Dispatcher**: Routes requests to handlers
- **Method**: RPC procedure identified by ID
- **Status**: Result of an RPC operation
- **Basis Points**: 1/100 of a percent (10_000 = 100%)

---

## Appendix H: References

### H.1 Related Crates

- `stream-manager`: QUIC stream lifecycle
- `session-manager`: Session management
- `middleware-pipeline`: Request/response processing
- `configuration-system`: Configuration management

### H.2 External Standards

- gRPC: https://grpc.io/
- HTTP/2: https://http2.github.io/
- QUIC: https://quicwg.org/

---

*This design document is part of the Vireon QUIC runtime project.*
