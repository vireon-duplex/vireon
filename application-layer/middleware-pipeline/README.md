# middleware-pipeline

**Request/response middleware pipeline** for the Vireon application layer.

## Overview

Provides ordered chain execution with stage-based dispatch (pre-auth, auth, pre-dispatch, post-dispatch, response, error) for composable request processing.

## Features

- **Stage-Based Dispatch**: Seven execution stages (PreAuth, AuthN, AuthZ, PreDispatch, PostDispatch, Response, Error)
- **Ordered Execution**: Middlewares execute in registration order within each stage
- **Short-Circuit**: Early exit on rejection (auth failures, rate limits)
- **Context Propagation**: Request context flows through entire chain
- **Error Handling**: Dedicated error stage for error transformation
- **Observability**: Atomic counters and health reporting
- **Configuration**: Presets for common deployment patterns

## Installation

Add to `Cargo.toml`:

```toml
[dependencies]
middleware-pipeline = { path = "../application-layer/middleware-pipeline" }
```

## Quick Start

```rust
use middleware_pipeline::{Pipeline, MiddlewareContext, MiddlewareId, Preset, MiddlewareStage};

// Create pipeline with preset
let cfg = Preset::Standard.to_config();
let mut pipeline = Pipeline::new(cfg).unwrap();

// Execute middleware chain
let ctx = MiddlewareContext::new(MiddlewareStage::PreAuth)
    .with_method(hash_method("GET"))
    .with_metadata(hash_headers(headers));

let result = pipeline.execute(ctx)?;
```

## Configuration

### Presets

```rust
// Minimal: auth only, few middlewares
let cfg = Preset::Minimal.to_config();

// Standard: auth + dispatch, moderate middlewares
let cfg = Preset::Standard.to_config();

// Full: all stages, many middlewares
let cfg = Preset::Full.to_config();
```

### Custom Configuration

```rust
use middleware_pipeline::MiddlewarePipelineBuilder;

let cfg = MiddlewarePipelineBuilder::new()
    .max_middlewares(128)
    .max_per_stage(32)
    .short_circuit(true)
    .default_timeout_ms(10_000)
    .build()?;
```

## Middleware Stages

### Execution Flow

```text
Request → PreAuth → AuthN → AuthZ → PreDispatch → [Handler]
  │                                                       │
  ▼                                                       ▼
Response ←──── Response ←──── PostDispatch ←─────────────────┘

Error ←──── Error (from any stage)
```

### Stage Descriptions

| Stage | Order | Purpose | Can Abort |
|-------|-------|---------|-----------|
| PreAuth | 0 | Logging, metrics | Yes |
| AuthN | 1 | Authentication | Yes |
| AuthZ | 2 | Authorization | Yes |
| PreDispatch | 3 | Routing, headers | Yes |
| PostDispatch | 4 | Response transformation | Yes |
| Response | 5 | Compression, caching | No |
| Error | 6 | Error handling | No |

## Context Management

### Creating Context

```rust
let ctx = MiddlewareContext::new(MiddlewareStage::PreAuth)
    .with_session_id(12345)
    .with_method(hash_method("POST"))
    .with_metadata(hash_headers(&headers))
    .with_timestamp(now_ns());
```

### Mutating Context

```rust
// Set error and abort
ctx.set_error(401);
ctx.abort();

// Or use builder style
let ctx = ctx.with_error_code(403).with_abort();
```

### Checking Context

```rust
if ctx.is_aborted() {
    return Err(Error::Rejected(ctx.error_code));
}

if ctx.has_error() {
    // Handle error
}
```

## Chain Operations

### Register Middleware

```rust
use middleware_pipeline::MiddlewareChain;

let mut chain = MiddlewareChain::new(cfg);

chain.register(
    MiddlewareId::new(1),
    MiddlewareStage::AuthN
)?;

chain.register(
    MiddlewareId::new(2),
    MiddlewareStage::AuthZ
)?;
```

### Remove Middleware

```rust
chain.remove(MiddlewareId::new(1))?;
```

### Count Middleware

```rust
let count = chain.count();
println!("Registered {} middlewares", count);
```

## Error Handling

```rust
use middleware_pipeline::MiddlewareError;

match pipeline.execute(ctx) {
    Ok(ctx) => println!("Request processed"),
    Err(MiddlewareError::Rejected(reason)) => {
        eprintln!("Request rejected: {}", reason);
    }
    Err(MiddlewareError::ChainAborted(reason)) => {
        eprintln!("Chain aborted: {}", reason);
    }
    Err(e) => eprintln!("Error: {}", e),
}
```

## Health Monitoring

```rust
let health = pipeline.health();

match health.state {
    HealthState::Healthy => println!("System healthy"),
    HealthState::Degraded => println!("System degraded: {}% rejection rate",
        health.rejection_rate_bps / 100),
    HealthState::Critical => println!("System critical! Rejection rate: {}%",
        health.rejection_rate_bps / 100),
}
```

## Statistics

```rust
let stats = pipeline.stats().snapshot();

println!("Executions: {}", stats.executions_total);
println!("Succeeded: {}", stats.executions_succeeded);
println!("Rejected: {}", stats.executions_rejected);
println!("Rejection rate: {}%", stats.rejection_rate_bps as f64 / 100.0);
println!("Avg time: {} μs", stats.avg_time_ns() / 1000);
println!("Success rate: {}%", stats.success_rate_bps as f64 / 100.0);
```

## Use Cases

### Authentication Middleware

```rust
fn auth_middleware(mut ctx: MiddlewareContext) -> MiddlewareContext {
    if !ctx.has_session() {
        ctx.set_error(401);
        return ctx.with_abort();
    }
    ctx
}

// Register at AuthN stage
chain.register(MiddlewareId::new(1), MiddlewareStage::AuthN)?;
```

### Rate Limiting Middleware

```rust
fn rate_limit_middleware(mut ctx: MiddlewareContext) -> MiddlewareContext {
    if !check_rate_limit(ctx.session_id_hash) {
        ctx.set_error(429);
        return ctx.with_abort();
    }
    ctx
}

// Register at AuthN stage (after auth)
chain.register(MiddlewareId::new(2), MiddlewareStage::AuthN)?;
```

### Logging Middleware

```rust
fn logging_middleware(ctx: MiddlewareContext) -> MiddlewareContext {
    log_request(
        ctx.method_hash,
        ctx.metadata_hash,
        ctx.timestamp_ns
    );
    ctx // Never abort
}

// Register at PreAuth stage (first)
chain.register(MiddlewareId::new(3), MiddlewareStage::PreAuth)?;
```

## Stage Classification

```rust
use middleware_pipeline::MiddlewareStage;

// Check stage type
assert!(MiddlewareStage::AuthN.is_auth_stage());
assert!(MiddlewareStage::AuthZ.is_auth_stage());
assert!(MiddlewareStage::PreDispatch.is_dispatch_stage());
assert!(MiddlewareStage::PostDispatch.is_dispatch_stage());
assert!(MiddlewareStage::Response.is_response_stage());

// Get stage order
assert_eq!(MiddlewareStage::PreAuth.order(), 0);
assert_eq!(MiddlewareStage::AuthN.order(), 1);
assert_eq!(MiddlewareStage::Error.order(), 6);
```

## Short-Circuit Behavior

### Enabled (default)

```rust
let cfg = MiddlewarePipelineBuilder::new()
    .short_circuit(true)
    .build()?;
```

When middleware aborts:
- Remaining middlewares in stage skipped
- Jump to appropriate response
- Faster rejection

### Disabled

```rust
let cfg = MiddlewarePipelineBuilder::new()
    .short_circuit(false)
    .build()?;
```

When middleware aborts:
- All middlewares still execute
- Useful for logging, metrics
- More observable but slower

## Design

See [`DESIGN.md`](DESIGN.md) for complete architecture documentation including:
- Component details
- Stage dispatch algorithm
- Chain execution flow
- Context propagation
- Performance characteristics
- Testing approach

## License

MIT

## Contributing

See Vireon project contribution guidelines.
