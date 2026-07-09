# Middleware Pipeline System — Design Document

## Part 1: Overview

### 1.1 Purpose

The Middleware Pipeline System provides ordered chain execution for request/response processing with stage-based dispatch. It enables composable request processing through middleware that execute at specific stages (pre-auth, auth, dispatch, response, error).

### 1.2 Scope

This system handles:
- Middleware registration and ordering
- Stage-based dispatch (PreAuth, AuthN, AuthZ, PreDispatch, PostDispatch, Response, Error)
- Chain execution with short-circuit support
- Context propagation through the chain
- Error handling and transformation
- Health monitoring and observability

### 1.3 Architecture

```text
   Request ──► [Middleware Chain] ──► Staged Dispatch ──► Response
                     │
                     ├─ PreAuth Stage
                     ├─ AuthN/AuthZ Stage
                     ├─ PreDispatch Stage
                     ├─ PostDispatch Stage
                     ├─ Response Stage
                     └─ Error Stage
```

### 1.4 Key Properties

1. **Ordered Execution**: Middlewares execute in registered order within each stage
2. **Stage Isolation**: Each stage has a specific purpose and permission set
3. **Short-Circuit**: Middleware can abort the chain early (e.g., auth rejection)
4. **Context Propagation**: Request context flows through entire chain
5. **Observability**: Every execution is recorded and exported

## Part 2: Components

### 2.1 MiddlewareId

Unique identifier for middleware components. Newtype around `u64` with reserved nil value (0).

**Operations**:
- `new(u64)` — Construct from non-zero value
- `nil()` — Construct nil identifier
- `is_nil()` — Check if nil
- `raw()` — Get underlying u64

**Invariants**:
- Real middlewares have ids in range `1..=u64::MAX`
- Nil (0) represents "no middleware"

### 2.2 MiddlewareStage

Execution stage for middleware components.

**Stages**:
1. `PreAuth` — Before authentication (logging, metrics)
2. `AuthN` — Authentication (verify credentials)
3. `AuthZ` — Authorization (check permissions)
4. `PreDispatch` — Before request dispatch (routing, headers)
5. `PostDispatch` — After request dispatch (response transformation)
6. `Response` — Response processing (compression, caching)
7. `Error` — Error handling (error transformation, logging)

**Stage Flow**:
```text
   Request ──► PreAuth ──► AuthN ──► AuthZ ──► PreDispatch ──► [Handler]
      │                                                                │
      ▼                                                                ▼
   Response ◄─── Response ◄─── PostDispatch ◄─────────────────────────┘

   Error ◄─── Error (from any stage)
```

### 2.3 MiddlewareContext

Request context passed through the middleware chain.

**Fields**:
- `stage: MiddlewareStage` — Current execution stage
- `session_id_hash: u64` — Hash of session identifier (if authenticated)
- `method_hash: u64` — Hash of request method (GET, POST, etc.)
- `metadata_hash: u64` — Hash of request metadata (headers, params)
- `continue_chain: bool` — Whether to continue executing
- `error_code: u16` — Error code (if error occurred)
- `timestamp_ns: u64` — Execution timestamp

**Builder Pattern**:
```rust
let ctx = MiddlewareContext::new(MiddlewareStage::AuthN)
    .with_session_id(12345)
    .with_method(67890)
    .with_timestamp(now_ns);
```

### 2.4 MiddlewareConfig

Declarative configuration for the middleware pipeline system.

**Fields**:
- `max_middlewares: u32` — Maximum middlewares (1-1024)
- `max_per_stage: u8` — Maximum middlewares per stage (1-128)
- `enable_short_circuit: bool` — Enable early exit on rejection
- `default_timeout_ms: u64` — Default timeout per middleware (100-300,000ms)

### 2.5 Preset

Pre-configured settings for common deployment scenarios.

**Variants**:
- `Minimal` — Authentication only, few middlewares (16 total, 4 per stage)
- `Standard` — Auth + dispatch stages, moderate middlewares (64 total, 16 per stage)
- `Full` — All stages, many middlewares (128 total, 32 per stage)

### 2.6 MiddlewareChain

Maintains middleware registry and stage assignments.

**Operations**:
- `register(id, stage)` — Register middleware at stage
- `remove(id)` — Remove middleware from chain
- `count()` — Get total middleware count

**Phase 1 Implementation**:
- Per-stage middleware lists
- Order-preserving insertion
- Stage capacity validation

### 2.7 Pipeline

Main facade coordinating middleware execution.

**Operations**:
- `execute(ctx)` — Execute middleware chain
- `health()` — Get health report

**Phase 1 Implementation**:
- Stage-by-stage execution
- Short-circuit on rejection
- Error stage dispatch on errors

### 2.8 MiddlewareStats

Atomic counters tracking pipeline events.

**Counters**:
- `executions_total` — Total pipeline executions
- `executions_succeeded` — Successful executions
- `executions_rejected` — Rejected (short-circuit)
- `chain_aborts` — Mid-chain aborts
- `total_time_ns` — Accumulated execution time

### 2.9 HealthReport

Operational health derived from stats snapshot.

**Metrics**:
- `state` — Healthy/Degraded/Critical
- `executions_total` — Total executions
- `rejection_rate_bps` — Rejection rate in basis points
- `avg_time_ns` — Average execution time

**Thresholds**:
- Rejection rate > 10% (1000 bps) → Degraded
- Rejection rate > 50% (5000 bps) → Critical

### 2.10 MiddlewareError

Typed failure surface for pipeline operations.

**Fatal Errors** (no retry):
- `DuplicateMiddleware` — Middleware already registered
- `InvalidOrder` — Invalid order value
- `StageMismatch` — Stage mismatch in registration
- `ShuttingDown` — System shutting down

**Retryable Errors** (back off + retry):
- `Rejected` — Middleware rejected request
- `ChainAborted` — Chain execution aborted

**Capacity Errors** (check registration):
- `NotFound` — Middleware not found

## Part 3: Stage Dispatch

### 3.1 Stage Ordering

Stages execute in fixed order:
1. PreAuth (0)
2. AuthN (1)
3. AuthZ (2)
4. PreDispatch (3)
5. [Handler executes]
6. PostDispatch (4)
7. Response (5)

Error stage (6) executes on any error.

### 3.2 Within-Stage Ordering

Within each stage, middleware executes in registration order:
- First registered executes first
- Order is preserved across executions
- No reordering after registration

### 3.3 Stage Permissions

Each stage has specific permissions:

| Stage | Can Read | Can Write | Can Abort | Purpose |
|-------|----------|-----------|-----------|---------|
| PreAuth | Request | — | Yes | Logging, metrics |
| AuthN | Request | Session | Yes | Authentication |
| AuthZ | Request, Session | — | Yes | Authorization |
| PreDispatch | Request, Session | Request | Yes | Routing, headers |
| PostDispatch | Response | Response | Yes | Response transform |
| Response | Response | — | No | Compression, caching |
| Error | Error | Response | No | Error handling |

## Part 4: Chain Execution

### 4.1 Normal Flow

```text
Request → PreAuth → AuthN → AuthZ → PreDispatch → Handler → PostDispatch → Response → Response
```

Each middleware:
1. Receives context
2. Processes request/response
3. Returns modified context
4. Chain continues unless aborted

### 4.2 Short-Circuit Flow

```text
Request → PreAuth → AuthN (reject) → Response
```

When middleware aborts:
- Remaining middlewares in stage skipped
- Jump to appropriate response stage
- Error stage if error occurred

### 4.3 Error Flow

```text
Request → PreAuth → AuthN → AuthZ (error) → Error → Response
```

On any error:
- Normal execution stops
- Error stage executes
- Error response returned

## Part 5: Context Propagation

### 5.1 Context Creation

Context created at pipeline start:
```rust
let ctx = MiddlewareContext::new(MiddlewareStage::PreAuth)
    .with_method(hash_method(req.method))
    .with_metadata(hash_headers(req.headers))
    .with_timestamp(now_ns());
```

### 5.2 Context Mutation

Each middleware can mutate context:
- Set session_id_hash after authentication
- Modify request metadata
- Abort chain by setting continue_chain = false
- Set error_code on errors

### 5.3 Context Consumption

Later middlewares read context:
- AuthZ checks session_id_hash
- PreDispatch reads method and metadata
- Response stage reads all accumulated data

## Part 6: Short-Circuit

### 6.1 Enable Short-Circuit

When `enable_short_circuit: true`:
- Middleware can abort chain early
- Remaining middlewares skipped
- Faster rejection (auth failures, rate limits)

### 6.2 Disable Short-Circuit

When `enable_short_circuit: false`:
- All middlewares execute even after abort
- Useful for logging, metrics collection
- Slower but more observable

### 6.3 Abort vs Error

**Abort** (`continue_chain = false`):
- Normal rejection (auth, rate limit)
- Error code set
- Response returned

**Error** (`error_code != 0`):
- Exceptional condition
- Error stage triggered
- Error response returned

## Part 7: Configuration

### 7.1 Minimal Config

```rust
let cfg = MiddlewareConfig::new();
```

Defaults:
- 64 max middlewares
- 16 per stage
- Short-circuit enabled
- 5s default timeout

### 7.2 Preset Config

```rust
let cfg = Preset::Standard.to_config();
```

### 7.3 Custom Config

```rust
let cfg = MiddlewarePipelineBuilder::new()
    .max_middlewares(128)
    .max_per_stage(32)
    .short_circuit(true)
    .default_timeout_ms(10_000)
    .build()?;
```

## Part 8: Registration

### 8.1 Register Middleware

```rust
chain.register(
    MiddlewareId::new(1),
    MiddlewareStage::AuthN
)?;
```

### 8.2 Per-Stage Limits

Each stage has capacity limit (`max_per_stage`):
- Registration fails if stage full
- Prevents resource exhaustion
- Configurable per deployment

### 8.3 Duplicate Detection

Registering same ID twice fails:
- Returns `DuplicateMiddleware` error
- Must remove before re-registering
- Ensures ID uniqueness

## Part 9: Execution

### 9.1 Execute Chain

```rust
let result = pipeline.execute(ctx)?;
```

**Process**:
1. Record execution start
2. Execute PreAuth middlewares
3. Execute AuthN middlewares
4. Execute AuthZ middlewares
5. Execute PreDispatch middlewares
6. (Handler executes separately)
7. Execute PostDispatch middlewares
8. Execute Response middlewares
9. Record execution complete

### 9.2 Stage Execution

For each stage:
1. Set context stage
2. For each middleware in order:
   - Call middleware with context
   - Check if aborted
   - Break if aborted and short-circuit enabled
3. Continue to next stage

### 9.3 Error Handling

On any middleware error:
1. Set error_code in context
2. Jump to Error stage
3. Execute Error middlewares
4. Return error response

## Part 10: Observability

### 10.1 Counters

All operations increment atomic counters:
- `executions_total` — On pipeline start
- `executions_succeeded` — On successful completion
- `executions_rejected` — On short-circuit reject
- `chain_aborts` — On mid-chain abort
- `total_time_ns` — Accumulate execution time

### 10.2 Snapshot

Point-in-time snapshot via `snapshot()`:
- Reads all counters with Relaxed ordering
- Computes rates (rejection_rate_bps, success_rate_bps)
- Computes averages (avg_time_ns)

### 10.3 Health State

Derived from snapshot:
- `Healthy` — Rejection rate ≤ 10%
- `Degraded` — Rejection rate 10-50%
- `Critical` — Rejection rate > 50%

## Part 11: Performance

### 11.1 Complexity

| Operation | Complexity | Notes |
|-----------|------------|-------|
| Register | O(1) | Add to stage list |
| Remove | O(N) | Scan stage list |
| Execute | O(M) | M middlewares total |
| Stage execute | O(S) | S middlewares in stage |

### 11.2 Memory

Per-middleware overhead:
- Registration: ~32 bytes
- Context per execution: ~64 bytes

Total for 64 middlewares:
- Registration: ~2 KB
- Context per request: ~64 bytes

## Part 12: Error Modes

### 12.1 Rejected

**Detection**: Middleware aborts with error code

**Impact**: Retryable — request rejected

**Recovery**:
1. Fix rejection condition
2. Retry request

### 12.2 Chain Aborted

**Detection**: Mid-chain abort (not rejection)

**Impact**: Retryable — chain incomplete

**Recovery**:
1. Identify aborting middleware
2. Fix condition
3. Retry request

### 12.3 Duplicate Middleware

**Detection**: Register same ID twice

**Impact**: Fatal — registration failed

**Recovery**:
1. Remove existing middleware
2. Re-register

### 12.4 Not Found

**Detection**: Remove non-existent middleware

**Impact**: Capacity — middleware not registered

**Recovery**:
1. Verify middleware ID
2. Check registration status

## Part 13: Use Cases

### 13.1 Authentication

```rust
// Register auth middleware
chain.register(
    MiddlewareId::new(1),
    MiddlewareStage::AuthN
)?;

// In middleware:
if !ctx.has_session() {
    ctx.set_error(401);
    return ctx.with_abort();
}
```

### 13.2 Authorization

```rust
// Register authz middleware
chain.register(
    MiddlewareId::new(2),
    MiddlewareStage::AuthZ
)?;

// In middleware:
if !check_permissions(ctx.session_id_hash, resource) {
    ctx.set_error(403);
    return ctx.with_abort();
}
```

### 13.3 Logging

```rust
// Register logging middleware
chain.register(
    MiddlewareId::new(3),
    MiddlewareStage::PreAuth
)?;

// In middleware:
log_request(ctx.method_hash, ctx.metadata_hash);
return ctx; // Never abort
```

### 13.4 Rate Limiting

```rust
// Register rate limit middleware
chain.register(
    MiddlewareId::new(4),
    MiddlewareStage::AuthN
)?;

// In middleware:
if !check_rate_limit(ctx.session_id_hash) {
    ctx.set_error(429);
    return ctx.with_abort();
}
```

## Part 14: Testing

### 14.1 Unit Tests

Each module has comprehensive tests:
- Newtype constructors and predicates
- Stage ordering and classification
- Context mutation and builders
- Config validation
- Error classification
- Stats accumulation

### 14.2 Integration Tests

Phase 1 will add:
- Full chain execution
- Stage dispatch
- Short-circuit behavior
- Error handling
- Health state transitions

### 14.3 Benchmarks

Phase 1 will measure:
- Chain execution throughput
- Stage dispatch cost
- Insert/remove latency
- Short-circuit performance

## Part 15: Future Work

### 15.1 Phase 1+

- Real chain execution with stage dispatch
- Proper short-circuit implementation
- Error stage handling
- Dynamic middleware registration/deregistration
- Per-middleware timeouts

### 15.2 Phase 2+

- Async middleware support
- Middleware composition (chaining middleware)
- Conditional execution ( predicates)
- Middleware dependencies
- Hot reloading

### 15.3 Phase 3+

- Distributed tracing integration
- Middleware analytics dashboard
- AI-powered middleware ordering
- Automatic timeout tuning
- Cross-pipeline middleware sharing
