# ai-streaming — Design Document

> **AI Streaming Framework** for the Vireon application layer.
> Token streaming, tool call aggregation, and pipeline management for
> LLM-based AI workloads.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Goals & Non-Goals](#2-goals--non-goals)
3. [Architecture](#3-architecture)
4. [Stream Model](#4-stream-model)
5. [Token Model](#5-token-model)
6. [Tool Call Model](#6-tool-call-model)
7. [Pipeline Design](#7-pipeline-design)
8. [Token Aggregation](#8-token-aggregation)
9. [Backpressure & Flow Control](#9-backpressure--flow-control)
10. [Concurrency Model](#10-concurrency-model)
11. [Failure Modes](#11-failure-modes)
12. [Observability](#12-observability)
13. [Configuration & Presets](#13-configuration--presets)
14. [Health & Alerting](#14-health--alerting)
15. [Performance Characteristics](#15-performance-characteristics)
16. [Security Considerations](#16-security-considerations)
17. [Integration Points](#17-integration-points)
18. [Testing Strategy](#18-testing-strategy)
19. [Benchmarking](#19-benchmarking)
20. [Roadmap](#20-roadmap)

---

## 1. Overview

The `ai-streaming` crate provides the foundational types and runtime
facade for streaming AI model output (tokens and tool calls) through
the Vireon realtime framework. It sits in the application layer,
consuming the RPC framework for transport and the session manager for
session identity.

### Why a dedicated crate?

LLM streaming has unique characteristics that distinguish it from
general-purpose RPC streaming:

1. **Token-level granularity**: Output arrives in small chunks
   (1-256 tokens), each needing immediate forwarding to the client.
2. **Mixed content types**: A single stream may interleave text
   tokens with structured tool-call invocations.
3. **Tool call lifecycle**: Tool calls are issued mid-stream and
   require execution, status tracking, and result injection.
4. **Aggregation**: Clients may want either raw token streaming or
   reassembled complete responses.
5. **Backpressure**: Token generation rates can exceed client
   consumption rates, requiring bounded buffering.

### Position in the Vireon stack

```
┌─────────────────────────────────────────────────────────┐
│                    Application Layer                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │ ai-streaming │  │  event-bus   │  │ pubsub-engine│  │
│  └──────┬───────┘  └──────────────┘  └──────────────┘  │
│         │                                                │
│  ┌──────┴──────────────────────────────────────────┐    │
│  │           rpc-framework                         │    │
│  └──────┬──────────────────────────────────────────┘    │
│         │                                                │
│  ┌──────┴──────────────────────────────────────────┐    │
│  │    session-manager / middleware-pipeline         │    │
│  └─────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
```

The `ai-streaming` crate depends on:
- `rpc-framework` — for `RpcMethodId` and `StreamHandle` types
- `session-manager` — for session identity
- `middleware-pipeline` — for request/response middleware
- `configuration-system` — for configuration infrastructure

### Design principles

1. **Type safety first**: All IDs, statuses, and modes are strongly
   typed newtypes or enums. No string-typed identifiers.
2. **Zero-cost abstractions**: Configs are `Copy`, newtypes are
   transparent wrappers, and hot-path types avoid heap allocation.
3. **Observability built-in**: Every operation updates atomic
   counters; health reports are derived from snapshots.
4. **Explicit lifecycle**: Streams have clear open/active/closed
   states; tool calls have a Pending -> Executing -> Completed/Failed
   lifecycle.
5. **Phased delivery**: Phase 0 ships foundational types and typed
   skeletons. Phase 1+ adds functional implementations.

---

## 2. Goals & Non-Goals

### Goals

- Define a complete, typed surface for AI token streaming
- Support three stream modes: text-only, tool-calls-only, mixed
- Provide atomic counters for all observability signals
- Derive health from stats using a transparent, rule-based classifier
- Support three deployment presets: Chat, CodeAssistant, Agent
- Integrate cleanly with the RPC framework's stream handle model
- Enable token aggregation for clients that want complete responses

### Non-Goals

- Implementing actual LLM inference (that is the model runtime's job)
- Transport-level streaming (delegated to rpc-framework)
- Authentication and authorization (delegated to auth-framework)
- Message serialization format (delegated to the frame layer)
- Multi-model orchestration (future crate: agent-runtime)
- Prompt template management (future crate)
- Embedding/streaming for non-text modalities (future crate)

### Future scope (Phase 1+)

- Functional stream lifecycle management
- Backpressure with bounded channels
- Tool call dispatch and result injection
- Token reassembly with partial flush
- Per-stream statistics tracking
- Graceful shutdown with stream draining

---

## 3. Architecture

### Component diagram

```
                    ┌────────────────────────────────────┐
                    │         AiStreamPipeline           │
                    │                                    │
  Client Request ──►│  open_stream()                     │
                    │  send_token_chunk()                │──► Token Chunks
                    │  close_stream()                    │    to Client
                    │  health()                          │
                    └──────────┬─────────────────────────┘
                               │
                    ┌──────────┴─────────────────────────┐
                    │         TokenAggregator             │
                    │                                    │
                    │  aggregate()                       │
                    │  flush()                            │
                    │  health()                           │
                    └──────────┬─────────────────────────┘
                               │
           ┌───────────────────┼───────────────────┐
           │                   │                   │
  ┌────────┴───────┐  ┌───────┴────────┐  ┌───────┴────────┐
  │  AiStreamStats  │  │  AiStreamConfig │  │  HealthReport  │
  │  (atomics)      │  │  (Copy + validate)│  │  (derived)    │
  └────────────────┘  └────────────────┘  └────────────────┘
```

### Module map

| Module         | Phase | Key Types                              |
|----------------|-------|----------------------------------------|
| `error`        | 0     | `AiStreamError`                        |
| `config`       | 0     | `AiStreamConfig`, `Preset`             |
| `stream`       | 0     | `AiStreamId`, `StreamMode`             |
| `token`        | 0     | `TokenChunk`, `TokenType`              |
| `tool`         | 0     | `ToolCallId`, `ToolCall`, `ToolStatus` |
| `stats`        | 0     | `AiStreamStats`, `StatsSnapshot`       |
| `health`       | 0     | `HealthReport`, `HealthState`          |
| `builder`      | 0     | `AiStreamBuilder`                      |
| `pipeline`     | 1     | `AiStreamPipeline`                     |
| `aggregator`   | 1     | `TokenAggregator`                      |

### Data flow

1. A client initiates an AI stream via `AiStreamPipeline::open_stream()`
2. The pipeline allocates an `AiStreamId` and tracks the stream
3. Token chunks arrive from the model runtime and are sent via
   `send_token_chunk()`
4. If tool calls are present, they are dispatched and tracked via
   `ToolStatus`
5. The client receives `TokenChunk` items via the RPC stream handle
6. Optionally, `TokenAggregator` reassembles chunks into complete
   responses
7. The stream is closed via `close_stream()`
8. All operations update `AiStreamStats` atomics
9. Health is derived from stats on demand via `HealthReport::derive()`

---

## 4. Stream Model

### AiStreamId

`AiStreamId` is a `u64` newtype that uniquely identifies an AI
stream within the Vireon runtime. It follows the same pattern as
other Vireon IDs:

```rust
pub struct AiStreamId(u64);

impl AiStreamId {
    pub const NIL: Self = Self(0);
    pub const fn new(id: u64) -> Self { Self(id) }
    pub const fn nil() -> Self { Self::NIL }
    pub const fn is_nil(&self) -> bool { self.0 == 0 }
    pub const fn raw(self) -> u64 { self.0 }
}
```

**Design decisions:**

- **u64 width**: Supports 2^64 unique streams, sufficient for any
  deployment scale. Using u32 would limit to ~4 billion streams,
  which could be exhausted in long-running high-throughput
  deployments.
- **NIL = 0**: The zero value is reserved as a sentinel for "no
  stream." This allows `Option<AiStreamId>` to be represented
  efficiently (niche optimization).
- **Copy + Ord**: Streams IDs are totally ordered, enabling sorted
  iteration and BTreeMap usage.
- **Display**: Formats as `ai-stream#<id>` for human-readable logs.

### StreamMode

`StreamMode` is a `#[repr(u8)]` enum with three variants:

| Variant    | Value | Description                          |
|------------|-------|--------------------------------------|
| `Text`     | 0     | Text-only token streaming            |
| `ToolCalls`| 1     | Tool-call-only streaming              |
| `Mixed`    | 2     | Interleaved text and tool-call tokens |

**Design decisions:**

- **#[repr(u8)]**: Enables FFI compatibility and compact storage
  (1 byte). The discriminant can be used for serialization without
  a separate encoding.
- **Default = Text**: The most common streaming mode. Chat
  applications that don't use tools get the default for free.
- **is_streaming()**: Always returns `true`. This method exists for
  API consistency with `RpcMethodType::is_streaming()` in the
  rpc-framework crate, where unary (non-streaming) is an option.
- **as_str()**: Returns a human-readable name for metrics labels
  and logging.

### Stream lifecycle

```
  [open_stream()]     [send_token_chunk()]      [close_stream()]
       │                      │                        │
       ▼                      ▼                        ▼
  ┌─────────┐          ┌──────────┐            ┌─────────┐
  │ Opening │ ──────►  │ Active   │ ─────────► │ Closing │
  └─────────┘          └──────────┘            └─────────┘
                              │                      │
                              │ can send tokens      │ drain buffers
                              │ can receive tools    │ finalize stats
                              ▼                      ▼
                         ┌──────────┐          ┌─────────┐
                         │ Streaming│          │ Closed  │
                         └──────────┘          └─────────┘
```

In Phase 0, all lifecycle transitions return `AiStreamError::ShuttingDown`.
Phase 1 will implement the full lifecycle with proper state tracking.

---

## 5. Token Model

### TokenType

`TokenType` is a `#[repr(u8)]` enum classifying the semantic type
of a token:

| Variant       | Value | Description                        |
|---------------|-------|------------------------------------|
| `Text`        | 0     | Generated text content             |
| `ToolCall`    | 1     | Tool invocation request            |
| `EndOfStream` | 2     | End-of-stream marker               |
| `Error`       | 3     | Error indicator                    |

**Design decisions:**

- **Four variants**: Covers the complete set of token types needed
  for LLM streaming. Text for content, ToolCall for structured
  invocations, EndOfStream for completion, Error for failures.
- **Default = Text**: The most common token type.
- **as_str()**: Provides stable string names for metrics labels.
  These names are part of the public API and should not change
  between versions.

### TokenChunk

`TokenChunk` is the atomic unit of streaming:

```rust
pub struct TokenChunk {
    pub token_id: u64,
    pub text: String,
    pub is_final: bool,
}
```

**Design decisions:**

- **token_id: u64**: Unique within a stream. Used for ordering,
  deduplication, and gap detection. The ID is assigned by the
  pipeline when the chunk is created.
- **text: String**: The actual content. For text tokens, this is
  the generated text. For tool-call tokens, this is the serialized
  arguments (typically JSON). For EndOfStream, this may be empty.
- **is_final: bool**: Indicates whether this is the last chunk in
  the stream. Allows clients to detect stream completion without
  a separate close event.
- **Owned String**: Chunks are self-contained and don't borrow from
  the stream. This simplifies lifetime management at the cost of
  one allocation per chunk. For high-throughput scenarios, Phase 1+
  may introduce a `Bytes`-backed variant.
- **Eq + Clone**: Chunks can be compared for testing and cloned
  for fan-out delivery.

### Chunk creation patterns

```rust
// Regular text chunk
let chunk = TokenChunk::new(1, String::from("Hello, "));

// Final text chunk
let chunk = TokenChunk::new_final(2, String::from("world!"));

// Builder-style
let chunk = TokenChunk::new(1, String::from("text"))
    .with_final(true);
```

### Token ID allocation

Token IDs are monotonically increasing within a stream. The pipeline
is responsible for ID allocation. In Phase 0, this is not implemented;
Phase 1 will use an `AtomicU64` per stream for lock-free ID
generation.

---

## 6. Tool Call Model

### ToolCallId

`ToolCallId` is a `u64` newtype following the same pattern as
`AiStreamId`:

```rust
pub struct ToolCallId(u64);

impl ToolCallId {
    pub const NIL: Self = Self(0);
    pub const fn new(id: u64) -> Self { Self(id) }
    pub const fn nil() -> Self { Self::NIL }
    pub const fn is_nil(&self) -> bool { self.0 == 0 }
    pub const fn raw(self) -> u64 { self.0 }
}
```

Tool call IDs are scoped to a single stream. Two streams may use
the same tool call IDs without conflict. This scoping matches the
LLM function-calling pattern where tool calls are identified within
a conversation turn.

### ToolStatus

`ToolStatus` is a `#[repr(u8)]` enum representing the tool call
execution lifecycle:

| Variant     | Value | Terminal | Description              |
|-------------|-------|----------|--------------------------|
| `Pending`   | 0     | No       | Issued, not yet started  |
| `Executing` | 1     | No       | Currently running        |
| `Completed` | 2     | Yes      | Finished successfully    |
| `Failed`    | 3     | Yes      | Finished with error      |

**Design decisions:**

- **Terminal states**: `Completed` and `Failed` are terminal. The
  `is_terminal()` method allows callers to check without matching
  all variants.
- **Default = Pending**: New tool calls start in the Pending state.
- **Linear progression**: The lifecycle is intentionally linear
  (Pending -> Executing -> Completed/Failed). There is no Cancelled
  state in Phase 0; this may be added in Phase 1 if needed.
- **#[repr(u8)]**: Compact storage and FFI compatibility.

### ToolCall

`ToolCall` carries the tool invocation details:

```rust
pub struct ToolCall {
    pub id: ToolCallId,
    pub name: String,
    pub arguments: String,
}
```

**Design decisions:**

- **name: String**: The tool name (e.g., "search", "calculate").
  This is the registered tool name, not a display name.
- **arguments: String**: Serialized arguments (typically JSON).
  Stored as a String rather than a structured type because:
  1. Different tools have different argument schemas
  2. The streaming framework should not parse tool arguments
  3. The model runtime is responsible for argument serialization
- **No status field**: The `ToolCall` struct is intentionally
  separated from `ToolStatus`. A tool call's status changes over
  time, but the call itself is immutable. This separation allows
  multiple status observations without cloning the call.
- **Eq**: Tool calls can be compared for testing and deduplication.

### Tool call lifecycle

```
  ┌─────────┐     ┌───────────┐     ┌───────────┐
  │ Pending │ ──► │ Executing │ ──► │ Completed │
  └─────────┘     └───────────┘     └───────────┘
                       │
                       ▼
                  ┌───────────┐
                  │  Failed   │
                  └───────────┘
```

In Phase 0, tool call dispatch is not implemented. Phase 1 will
add a `ToolDispatcher` that manages the execution lifecycle.

---

## 7. Pipeline Design

### AiStreamPipeline

The pipeline is the primary facade for AI streaming operations.
It manages:

1. **Stream lifecycle**: Opening, tracking, and closing streams
2. **Token routing**: Delivering token chunks to the correct stream
3. **Tool dispatch**: Routing tool calls to executors (Phase 1+)
4. **Statistics**: Recording all operations in atomic counters
5. **Health**: Deriving health reports from statistics

### Phase 0 skeleton

In Phase 0, the pipeline is a typed skeleton:

```rust
pub struct AiStreamPipeline {
    cfg: AiStreamConfig,
    stats: AiStreamStats,
}

impl AiStreamPipeline {
    pub fn new(cfg: AiStreamConfig) -> Self { ... }
    pub fn open_stream(&self, _mode: StreamMode) -> Result<AiStreamId, AiStreamError> {
        // TODO(phase-1): Implement stream opening.
        Err(AiStreamError::ShuttingDown)
    }
    pub fn send_token_chunk(&self, _stream: AiStreamId, _chunk: TokenChunk) -> Result<(), AiStreamError> {
        // TODO(phase-1): Implement token chunk delivery.
        Err(AiStreamError::ShuttingDown)
    }
    pub fn close_stream(&self, _stream: AiStreamId) -> Result<(), AiStreamError> {
        // TODO(phase-1): Implement stream closing.
        Err(AiStreamError::ShuttingDown)
    }
    pub fn health(&self) -> HealthReport { ... }
}
```

### Phase 1 design

Phase 1 will add:

- **Stream registry**: `DashMap<AiStreamId, StreamState>` for
  concurrent stream tracking
- **ID allocator**: `AtomicU64` for lock-free stream ID generation
- **Token channel**: Per-stream bounded channel for token delivery
- **Backpressure**: When a stream's channel is full, the pipeline
  applies backpressure to the model runtime
- **Shutdown coordination**: `Arc<AtomicBool>` for graceful shutdown

### Concurrency model

The pipeline is designed for concurrent access:

- `open_stream()`: Takes `&self`, uses atomic ID allocation
- `send_token_chunk()`: Takes `&self`, routes to the correct stream
- `close_stream()`: Takes `&self`, marks the stream as closing
- `health()`: Takes `&self`, reads atomic counters

All methods use `&self` (not `&mut self`) to enable shared ownership
via `Arc<AiStreamPipeline>`.

---

## 8. Token Aggregation

### TokenAggregator

The aggregator provides buffered token reassembly for clients that
want complete responses rather than raw token streams. It manages:

1. **Per-stream buffers**: Accumulating token chunks by stream
2. **Flush semantics**: Delivering reassembled content on demand
3. **Completion detection**: Knowing when a stream's tokens are
   complete (via `is_final` flag)
4. **Statistics**: Recording aggregation operations

### Phase 0 skeleton

```rust
pub struct TokenAggregator {
    cfg: AiStreamConfig,
    stats: AiStreamStats,
}

impl TokenAggregator {
    pub fn new(cfg: AiStreamConfig) -> Self { ... }
    pub fn aggregate(&self, _stream: AiStreamId, _chunk: TokenChunk) -> Result<(), AiStreamError> {
        // TODO(phase-1): Implement token aggregation.
        Err(AiStreamError::ShuttingDown)
    }
    pub fn flush(&self, _stream: AiStreamId) -> Result<(), AiStreamError> {
        // TODO(phase-1): Implement flush.
        Err(AiStreamError::ShuttingDown)
    }
    pub fn health(&self) -> HealthReport { ... }
}
```

### Phase 1 design

Phase 1 will add:

- **Per-stream buffer**: `DashMap<AiStreamId, AggregatorBuffer>`
  where `AggregatorBuffer` is a `Vec<TokenChunk>` with a completion
  flag
- **Capacity limits**: Bounded buffer size per stream, configurable
  via `AiStreamConfig`
- **Flush API**: Returns the reassembled text and clears the buffer
- **Auto-flush**: When `is_final` is received, the buffer is
  automatically marked complete

### Aggregation vs streaming

Clients have two options:

1. **Streaming mode**: Receive each `TokenChunk` as it arrives.
   Low latency, but the client must handle reassembly.
2. **Aggregation mode**: Use `TokenAggregator` to buffer and flush.
   Higher latency for the first token, but simpler client logic.

The choice depends on the use case:

| Use case              | Mode         | Rationale                          |
|-----------------------|--------------|------------------------------------|
| Chat UI               | Streaming    | Low latency, progressive display   |
| Code completion       | Streaming    | Immediate feedback                 |
| Batch processing      | Aggregation  | Complete response needed           |
| Tool execution        | Aggregation  | Need complete arguments            |
| Logging/audit         | Aggregation  | Complete record per stream         |

---

## 9. Backpressure & Flow Control

### Problem statement

Token generation rates can exceed client consumption rates. If the
pipeline pushes tokens faster than the client can receive them,
memory usage grows unboundedly.

### Design approach

Phase 1 will implement bounded backpressure:

1. **Per-stream bounded channel**: Each stream has a bounded channel
   (default capacity: 256 tokens). When the channel is full, the
   pipeline blocks the producer.
2. **Configurable capacity**: `AiStreamConfig` will gain a
   `stream_buffer_capacity` field in Phase 1.
3. **Timeout**: If a channel remains full for longer than a
   configurable timeout, the stream enters a degraded state.
4. **Drop policy**: In extreme cases, the pipeline may drop tokens
   and record a `tokens_dropped` counter (Phase 2).

### Backpressure propagation

```
  Model Runtime ──► Pipeline ──► Channel ──► Client
                      │              │
                      │   full?      │
                      │   block ─────┘
                      │
                      ▼
                Backpressure to
                Model Runtime
```

The model runtime is expected to respect backpressure by pausing
token generation when the pipeline blocks.

### Flow control metrics

Phase 1 will track:

- `tokens_buffered`: Current tokens in flight (gauge)
- `tokens_dropped`: Tokens dropped due to overflow (counter)
- `backpressure_events`: Number of times backpressure was applied
- `backpressure_duration_ns`: Total time spent in backpressure

---

## 10. Concurrency Model

### Thread safety

All types in `ai-streaming` are designed for concurrent access:

| Type              | Thread Safety                          |
|-------------------|----------------------------------------|
| `AiStreamConfig`  | `Copy` — no synchronization needed     |
| `AiStreamId`      | `Copy` — no synchronization needed     |
| `StreamMode`      | `Copy` — no synchronization needed     |
| `TokenChunk`      | `Clone` — not `Copy` (owns String)     |
| `ToolCall`        | `Clone` — not `Copy` (owns String)     |
| `ToolStatus`      | `Copy` — no synchronization needed     |
| `AiStreamStats`   | `&self` methods, internal atomics      |
| `StatsSnapshot`   | `Copy` — point-in-time snapshot        |
| `HealthReport`    | `Copy` — derived from snapshot         |
| `AiStreamPipeline`| `&self` methods, internal atomics      |
| `TokenAggregator` | `&self` methods, internal atomics      |

### Atomic operations

`AiStreamStats` uses `AtomicU64` for all counters with
`Ordering::Relaxed`:

```rust
pub fn record_stream_opened(&self) {
    self.streams_opened.fetch_add(1, Ordering::Relaxed);
}
```

**Why Relaxed?**

- Counters are monotonically increasing (fetch_add only)
- No other atomic operation depends on the counter value
- The snapshot reads are eventually consistent
- Relaxed ordering has the lowest overhead on x86/ARM

If a stronger ordering is needed in the future (e.g., for
causality tracking), the ordering can be upgraded to `AcqRel`
without API changes.

### Lock-free design

Phase 1 will use lock-free data structures:

- **Stream registry**: `DashMap` for concurrent stream tracking
- **ID allocator**: `AtomicU64::fetch_add` for unique IDs
- **Token channels**: Bounded MPSC channels (tokio or crossbeam)

No mutexes or RwLocks are used in the hot path. Locks may be used
for rare operations like shutdown coordination.

---

## 11. Failure Modes

### AiStreamError

The error enum is `#[non_exhaustive]` to allow adding variants in
future phases without breaking changes:

```rust
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum AiStreamError {
    StreamNotFound(AiStreamId),
    StreamExists(AiStreamId),
    TokenFailed(&'static str),
    ToolCallFailed,
    ShuttingDown,
}
```

### Error classification

| Variant          | Fatal | Retryable | Caller Action                     |
|------------------|-------|-----------|-----------------------------------|
| `StreamNotFound` | No    | No        | Check stream ID, create new       |
| `StreamExists`   | Yes   | No        | Programming error, fix caller     |
| `TokenFailed`    | No    | Yes       | Back off and retry                |
| `ToolCallFailed` | No    | Yes       | Back off and retry or fallback    |
| `ShuttingDown`   | Yes   | No        | Abort, drain remaining work       |

### Fatal vs retryable

- **Fatal** (`is_fatal()`): The caller must NOT retry. These
  indicate programming mistakes or terminal lifecycle states.
- **Retryable** (`is_retryable()`): The caller may retry after a
  backoff. These are typically transient failures.
- **Neither**: The error is a logical error (e.g., `StreamNotFound`)
  that requires caller logic to handle, not a retry.

### Error messages

Error messages use `&'static str` for the `TokenFailed` variant
because:

1. Error messages should not be dynamically constructed (they are
   not for end users)
2. `&'static str` avoids allocation in the error path
3. The messages are stable identifiers for log matching

### Phase 1 additions

Future phases may add:

- `StreamClosed(AiStreamId)` — attempted to operate on a closed stream
- `BackpressureExceeded` — backpressure timeout exceeded
- `TokenLimitExceeded` — stream exceeded max_tokens_per_stream
- `ToolTimeout` — tool call exceeded tool_call_timeout_ms
- `InvalidArguments(&'static str)` — invalid tool call arguments

---

## 12. Observability

### AiStreamStats

All counters use `AtomicU64` for lock-free concurrent updates:

| Counter                  | Type   | Description                        |
|--------------------------|--------|------------------------------------|
| `streams_opened`         | Counter| Total streams opened               |
| `streams_closed`         | Counter| Total streams closed               |
| `tokens_generated`       | Counter| Total tokens generated             |
| `tool_calls_initiated`   | Counter| Total tool calls started           |
| `tool_calls_completed`   | Counter| Total tool calls completed OK      |
| `tool_calls_failed`      | Counter| Total tool calls failed            |

### StatsSnapshot

`StatsSnapshot` is a `Copy` struct that captures a point-in-time
view of all counters. It provides:

- **`tool_success_rate_bps()`**: Success rate in basis points
  (0..=10_000). Returns 10_000 (100%) when no tool calls have been
  initiated.
- **`active_streams()`**: Currently active streams (opened minus
  closed). Uses `saturating_sub` to prevent underflow.

### Basis points (bps)

Rates are expressed in basis points (1/100 of a percent):

| bps    | Percentage | Example                    |
|--------|------------|----------------------------|
| 10_000 | 100%       | All tool calls succeeded   |
| 9_500  | 95%        | 19/20 succeeded            |
| 9_000  | 90%        | 9/10 succeeded             |
| 8_000  | 80%        | 4/5 succeeded              |
| 5_000  | 50%        | Half succeeded             |
| 0      | 0%         | All failed                 |

**Why bps instead of f64?**

1. No floating-point arithmetic (deterministic, no precision loss)
2. Compact (u16, 2 bytes vs f64, 8 bytes)
3. Sufficient precision (0.01% granularity)
4. Integer division truncates predictably

### Rate calculation

```rust
pub const fn tool_success_rate_bps(&self) -> u16 {
    if self.tool_calls_initiated == 0 {
        return 10_000;
    }
    let rate = (self.tool_calls_completed.saturating_mul(10_000))
        / self.tool_calls_initiated;
    if rate > u16::MAX as u64 {
        u16::MAX
    } else {
        rate as u16
    }
}
```

**Key implementation notes:**

- `saturating_mul` prevents overflow when multiplying by 10_000
- Integer division truncates: 2*10000/3 = 6666 (not 6667)
- The clamp to `u16::MAX` is defensive; in practice, the rate
  cannot exceed 10_000 because `completed <= initiated`
- The `if total == 0` guard prevents division by zero and returns
  10_000 (100%) to indicate "no failures" when there are no tool
  calls

### Recording methods

```rust
stats.record_stream_opened();        // +1 streams_opened
stats.record_stream_closed();        // +1 streams_closed
stats.record_tokens(100);            // +100 tokens_generated
stats.record_tool_call_initiated();  // +1 tool_calls_initiated
stats.record_tool_call_completed();  // +1 tool_calls_completed
stats.record_tool_call_failed();     // +1 tool_calls_failed
```

### Snapshot consistency

The `snapshot()` method reads all counters with `Relaxed` ordering.
This means the snapshot may not reflect a single instant in time
(if a counter is being updated concurrently). However, the
consistency guarantees are sufficient for observability:

- Counters are monotonically increasing (never decrease)
- The snapshot reflects a state that existed at some point
- Rate calculations are stable under concurrent updates

---

## 13. Configuration & Presets

### AiStreamConfig

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AiStreamConfig {
    pub max_concurrent_streams: u32,
    pub max_tokens_per_stream: u64,
    pub token_chunk_size: u32,
    pub tool_call_timeout_ms: u64,
}
```

### Field reference

| Field                     | Type | Min   | Max       | Default | Description                    |
|---------------------------|------|-------|-----------|---------|--------------------------------|
| `max_concurrent_streams`  | u32  | 1     | 10_000    | 128     | Max concurrent AI streams      |
| `max_tokens_per_stream`   | u64  | 128   | 1_000_000 | 4_096   | Max tokens per stream          |
| `token_chunk_size`        | u32  | 1     | 10_000    | 64      | Tokens per chunk               |
| `tool_call_timeout_ms`    | u64  | 100   | 600_000   | 5_000   | Tool call timeout (ms)         |

### Constants

```rust
pub const MIN_MAX_CONCURRENT_STREAMS: u32 = 1;
pub const MAX_MAX_CONCURRENT_STREAMS: u32 = 10_000;
pub const MIN_MAX_TOKENS_PER_STREAM: u64 = 128;
pub const MAX_MAX_TOKENS_PER_STREAM: u64 = 1_000_000;
pub const MIN_TOKEN_CHUNK_SIZE: u32 = 1;
pub const MAX_TOKEN_CHUNK_SIZE: u32 = 10_000;
pub const MIN_TOOL_CALL_TIMEOUT_MS: u64 = 100;
pub const MAX_TOOL_CALL_TIMEOUT_MS: u64 = 600_000;
```

### Validation

The `validate()` method checks each field against its min/max bounds.
If any field is out of range, it returns
`AiStreamError::TokenFailed("field_name out of range")`.

### Builder pattern

```rust
let cfg = AiStreamConfig::new()
    .with_max_concurrent_streams(256)
    .with_max_tokens_per_stream(8_192)
    .with_token_chunk_size(32)
    .with_tool_call_timeout_ms(10_000);
```

All `with_*` methods are `const fn`, enabling compile-time config
construction:

```rust
const CFG: AiStreamConfig = AiStreamConfig::new()
    .with_max_concurrent_streams(64)
    .with_tool_call_timeout_ms(15_000);
```

### Presets

Three deployment archetypes encode common workload profiles:

| Preset          | Streams | Tokens  | Chunk | Timeout  | Use Case                    |
|-----------------|---------|---------|-------|----------|-----------------------------|
| `Chat`          | 128     | 4_096   | 64    | 5_000ms  | Chat applications           |
| `CodeAssistant` | 64      | 16_384  | 128   | 15_000ms | Code completion/assistant   |
| `Agent`         | 32      | 32_768  | 256   | 60_000ms | Autonomous agents           |

**Chat**: High concurrency, short responses, fast tool calls.
Optimized for interactive chat where users expect quick responses.
Default config matches this preset.

**CodeAssistant**: Moderate concurrency, longer responses, larger
chunks. Code generation produces longer outputs that benefit from
larger chunk sizes (fewer syscalls per token).

**Agent**: Low concurrency, very long responses, generous tool
timeouts. Autonomous agents may run for minutes, making many tool
calls with long execution times.

### Preset selection guide

```
  High concurrency?  ──yes──►  Chat
        │ no
        ▼
  Long responses?    ──yes──►  CodeAssistant
        │ no
        ▼
  Autonomous?        ──yes──►  Agent
        │ no
        ▼
  Use default (Chat)
```

---

## 14. Health & Alerting

### HealthState

Tri-state health classification:

| State      | Color   | Action              |
|------------|---------|---------------------|
| `Healthy`  | Green   | No action           |
| `Degraded` | Yellow  | Investigate         |
| `Critical` | Red     | Page on-call        |

### HealthReport

Derived from `StatsSnapshot` using a "worst signal wins" policy:

```rust
pub struct HealthReport {
    pub state: HealthState,
    pub tokens_generated: u64,
    pub tool_success_rate_bps: u16,
    pub active_streams: u64,
}
```

### Thresholds

```rust
pub const TOOL_SUCCESS_DEGRADED_BPS: u16 = 9_000; // < 90% → degraded
pub const TOOL_SUCCESS_CRITICAL_BPS: u16 = 8_000; // < 80% → critical
```

**Design decisions:**

- **Tool success rate as primary signal**: Tool call failures are
  the most actionable signal for AI streaming. If tools are failing,
  the AI is likely producing incomplete or incorrect responses.
- **Conservative thresholds**: 90% success rate for degraded is
  intentionally high. AI applications are expected to have very
  high tool success rates (>95%) in normal operation.
- **Worst signal wins**: If any signal is critical, the overall
  state is critical. If any signal is degraded (but none critical),
  the overall state is degraded.
- **Boundary semantics**: The comparison uses `<` (strictly less
  than), so exactly 90% success rate (9_000 bps) is still Healthy,
  and exactly 80% (8_000 bps) is still Degraded (not Critical).

### Health derivation

```rust
impl HealthReport {
    pub fn derive(stats: &StatsSnapshot) -> Self {
        let tool_success_rate_bps = stats.tool_success_rate_bps();
        let active_streams = stats.active_streams();
        let state = Self::classify(tool_success_rate_bps);
        Self { state, tokens_generated: stats.tokens_generated, ... }
    }

    fn classify(tool_success_bps: u16) -> HealthState {
        if tool_success_bps < Self::TOOL_SUCCESS_CRITICAL_BPS {
            HealthState::Critical
        } else if tool_success_bps < Self::TOOL_SUCCESS_DEGRADED_BPS {
            HealthState::Degraded
        } else {
            HealthState::Healthy
        }
    }
}
```

### Alerting recommendations

| State     | Alert Level | Notification         |
|-----------|-------------|----------------------|
| Healthy   | Info        | None (log only)      |
| Degraded  | Warning     | Slack/email          |
| Critical  | Critical    | Page on-call         |

### Health check API

Both `AiStreamPipeline` and `TokenAggregator` expose `health()`:

```rust
let pipeline = AiStreamPipeline::new(cfg);
let report = pipeline.health();
if report.needs_attention() {
    // Alert operator
}
```

---

## 15. Performance Characteristics

### Expected performance (Phase 1 targets)

| Operation              | Target Latency | Throughput        |
|------------------------|----------------|-------------------|
| `open_stream()`        | < 1 us         | 1M streams/sec    |
| `send_token_chunk()`   | < 500 ns       | 2M chunks/sec     |
| `close_stream()`       | < 500 ns       | 2M closes/sec     |
| `health()`             | < 100 ns       | 10M reports/sec   |
| `StatsSnapshot`        | < 50 ns        | 20M snapshots/sec |

### Memory overhead

| Type              | Size    | Notes                          |
|-------------------|---------|--------------------------------|
| `AiStreamConfig`  | 32 bytes| Copy, no allocation            |
| `AiStreamId`      | 8 bytes | Copy, no allocation            |
| `StreamMode`      | 1 byte  | Copy, no allocation            |
| `TokenChunk`      | 40 bytes| Plus String heap allocation    |
| `ToolCall`        | 56 bytes| Plus String heap allocations   |
| `ToolStatus`      | 1 byte  | Copy, no allocation            |
| `StatsSnapshot`   | 48 bytes| Copy, no allocation            |
| `HealthReport`    | 24 bytes| Copy, no allocation            |
| `AiStreamStats`   | 48 bytes| AtomicU64 x 6                  |

### Optimization notes

1. **Copy types**: All configuration and snapshot types are `Copy`,
   eliminating clone overhead and enabling stack-only usage.
2. **AtomicU64**: 64-bit atomics are the optimal width for counters
   on x86_64 and ARM64. Using smaller types would require additional
   masking logic.
3. **Relaxed ordering**: The lowest atomic ordering overhead on all
   architectures.
4. **No allocations in hot path**: `open_stream`, `send_token_chunk`,
   and `close_stream` do not allocate in Phase 1 (stream entries are
   pre-allocated in a pool).
5. **String in TokenChunk**: The `String` field requires allocation,
   but this is unavoidable for variable-length text content. Phase 2
   may introduce a `Bytes`-backed variant for zero-copy scenarios.

### Benchmarking

Four Criterion benchmarks are provided:

1. `token_chunk`: Token chunk creation and type inspection
2. `open_stream`: Stream open/close operations
3. `tool_call`: Tool call creation and status inspection
4. `aggregate`: Token aggregation and flush operations

See [Benchmarking](#19-benchmarking) for details.

---

## 16. Security Considerations

### Input validation

- **Config validation**: `AiStreamConfig::validate()` checks all
  fields against min/max bounds. Invalid configs return an error.
- **Tool arguments**: Tool call arguments are stored as opaque
  strings. The streaming framework does not parse or validate them.
  The tool executor is responsible for input validation.
- **Stream IDs**: Stream IDs are `u64` values assigned by the
  pipeline. Clients cannot specify arbitrary stream IDs, preventing
  ID spoofing.

### Resource limits

- **Max concurrent streams**: Prevents resource exhaustion from too
  many simultaneous streams.
- **Max tokens per stream**: Prevents memory exhaustion from
  unbounded token generation.
- **Tool call timeout**: Prevents tool calls from blocking
  indefinitely.

### Data sensitivity

- **Token content**: Token chunks may contain sensitive information
  (PII, credentials). The streaming framework does not log token
  content; only counters are recorded.
- **Tool arguments**: Tool call arguments may contain sensitive
  data. Same handling as token content.
- **Error messages**: Error messages use `&'static str` to avoid
  leaking dynamic content.

### Phase 1 considerations

- **Authentication**: Stream operations will require a valid
  session token (via session-manager integration).
- **Rate limiting**: Per-session rate limiting to prevent abuse.
- **Audit logging**: Stream open/close events will be logged for
  audit trails (without token content).

---

## 17. Integration Points

### rpc-framework

The `ai-streaming` crate re-exports `RpcMethodId` and `StreamHandle`
from `rpc-framework`:

```rust
pub use rpc_framework::{RpcMethodId, StreamHandle};
```

**RpcMethodId**: Used to identify AI streaming RPC methods (e.g.,
"generate_tokens", "execute_tool_call"). The AI streaming framework
defines standard method IDs in Phase 1.

**StreamHandle**: Used to correlate AI streams with RPC transport
streams. Each `AiStreamId` maps to a `StreamHandle` for QUIC
stream management.

### session-manager

The session manager provides `SessionId` for correlating AI streams
with user sessions. Each stream belongs to exactly one session.

### middleware-pipeline

The middleware pipeline processes requests and responses:
- **Request middleware**: Authentication, rate limiting, logging
- **Response middleware**: Content filtering, compression, tracing

### configuration-system

The configuration system provides:
- Config loading from files/environment
- Config validation and hot-reload
- Config versioning and migration

### event-bus

The event bus may be used in Phase 2 for:
- Publishing stream lifecycle events
- Subscribing to tool call events
- Cross-service stream coordination

---

## 18. Testing Strategy

### Test coverage

Phase 0 includes comprehensive unit tests for all real-impl modules:

| Module    | Tests | Coverage                                |
|-----------|-------|-----------------------------------------|
| `error`   | 10    | All variants, predicates, messages      |
| `config`  | 15    | Defaults, builder, validation, presets  |
| `stream`  | 15    | ID lifecycle, mode variants, Display    |
| `token`   | 14    | Chunk creation, type variants, equality |
| `tool`    | 18    | ID lifecycle, status, call creation     |
| `stats`   | 16    | All counters, rate calculation, snapshot|
| `health`  | 12    | All states, thresholds, derivation      |
| `builder` | 9     | Builder chaining, validation, presets   |
| `pipeline`| 5     | Skeleton returns, health                |
| `aggregator`| 4   | Skeleton returns, health                |
| **Total** | **113**|                                        |

### Test patterns

1. **Construction tests**: Verify that `new()`, `nil()`, `default()`
   produce correct values.
2. **Equality tests**: Verify that equal values compare equal and
   unequal values compare unequal.
3. **Display tests**: Verify that `Display` implementation produces
   human-readable output.
4. **Validation tests**: Verify that `validate()` accepts valid
   values and rejects invalid values (including boundary cases).
5. **Predicate tests**: Verify that `is_fatal()`, `is_retryable()`,
   `is_terminal()`, etc. return correct values for all variants.
6. **Skeleton tests**: Verify that skeleton methods return the
   expected error (`ShuttingDown`).
7. **Const fn tests**: Verify that const functions can be evaluated
   at compile time.

### Test principles

- **No `unwrap()` in tests**: The workspace lints deny `unwrap_used`.
  Tests use `unwrap()` via a separate test helper or direct assertion.
- **Descriptive test names**: Test names describe the scenario
  being tested, not the implementation detail.
- **One assertion per test**: Each test verifies one behavior.
  Multiple related assertions are acceptable if they test the same
  concept.
- **Boundary testing**: Every min/max boundary is tested with
  values at, above, and below the boundary.

### Future testing (Phase 1+)

- **Property-based tests**: `proptest` for invariant testing
- **Concurrency tests**: `loom` for concurrent access verification
- **Integration tests**: Cross-crate integration with rpc-framework
- **Load tests**: High-throughput scenario testing
- **Chaos tests**: Failure injection and recovery

---

## 19. Benchmarking

### Benchmark stubs

Four Criterion benchmark stubs are provided:

#### token_chunk

Measures the cost of:
- `TokenChunk::new()` — chunk creation
- `TokenChunk::new_final()` — final chunk creation
- `TokenType::as_str()` — type string lookup

#### open_stream

Measures the cost of:
- `AiStreamPipeline::open_stream()` — stream opening
- `AiStreamPipeline::close_stream()` — stream closing

#### tool_call

Measures the cost of:
- `ToolCall::new()` — tool call creation
- `ToolStatus::as_str()` — status string lookup
- `ToolStatus::is_terminal()` — terminal check

#### aggregate

Measures the cost of:
- `TokenAggregator::aggregate()` — token aggregation
- `TokenAggregator::flush()` — buffer flush

### Running benchmarks

```bash
# Compile only (Phase 0 verification)
cargo bench --no-run

# Run all benchmarks
cargo bench

# Run specific benchmark
cargo bench --bench token_chunk
```

### Benchmark design

All benchmarks use `criterion::black_box` to prevent the compiler
from optimizing away the operation under test. The benchmark
functions are minimal to isolate the operation being measured.

### Phase 1 benchmarks

Phase 1 will add:
- **Throughput benchmarks**: Tokens/sec under load
- **Concurrency benchmarks**: Operations/sec under concurrent access
- **Memory benchmarks**: Allocations per operation
- **Comparison benchmarks**: Before/after optimization comparisons

---

## 20. Roadmap

### Phase 0 (current)

- [x] `AiStreamError` — typed error enum with `is_fatal()` / `is_retryable()`
- [x] `AiStreamConfig` — Copy config with validate + Preset + Builder
- [x] `AiStreamId` — u64 newtype with NIL, Display
- [x] `StreamMode` — #[repr(u8)] enum with as_str()
- [x] `TokenChunk` — struct with token_id, text, is_final
- [x] `TokenType` — #[repr(u8)] enum with as_str()
- [x] `ToolCallId` — u64 newtype with Display
- [x] `ToolCall` — struct with id, name, arguments
- [x] `ToolStatus` — #[repr(u8)] enum with is_terminal(), as_str()
- [x] `AiStreamStats` — AtomicU64 counters
- [x] `StatsSnapshot` — Copy struct with rate calculations
- [x] `HealthReport` / `HealthState` — tri-state health
- [x] `AiStreamBuilder` — fluent builder
- [x] `AiStreamPipeline` — typed skeleton
- [x] `TokenAggregator` — typed skeleton
- [x] 4 Criterion bench stubs
- [x] DESIGN.md (this document)
- [x] README.md

### Phase 1

- [ ] Functional stream lifecycle (open/active/closed)
- [ ] Token chunk routing and delivery
- [ ] Per-stream bounded channels with backpressure
- [ ] Tool call dispatch and lifecycle management
- [ ] Token aggregation with flush semantics
- [ ] Graceful shutdown with stream draining
- [ ] Per-stream statistics tracking
- [ ] Integration tests with rpc-framework

### Phase 2

- [ ] Tool result injection into streams
- [ ] Stream cancellation and abort
- [ ] Token deduplication and gap detection
- [ ] Per-session rate limiting
- [ ] Audit logging for stream events
- [ ] Metrics export (Prometheus/OpenTelemetry)
- [ ] Dynamic config reload
- [ ] Multi-model stream routing

### Phase 3

- [ ] Semantic caching for repeated requests
- [ ] Stream replay for reconnection
- [ ] Cross-service stream migration
- [ ] Adaptive chunk sizing
- [ ] Predictive backpressure
- [ ] Token-level tracing (OpenTelemetry spans)

---

## Appendix A: Type Reference

### Newtypes

| Type          | Inner | NIL | Display              |
|---------------|-------|-----|----------------------|
| `AiStreamId`  | u64   | 0   | `ai-stream#<id>`     |
| `ToolCallId`  | u64   | 0   | `tool-call#<id>`     |

### Enums

| Enum          | Repr | Variants                        | Default    |
|---------------|------|---------------------------------|------------|
| `StreamMode`  | u8   | Text(0), ToolCalls(1), Mixed(2) | Text       |
| `TokenType`   | u8   | Text(0), ToolCall(1), EOS(2), Error(3) | Text |
| `ToolStatus`  | u8   | Pending(0), Executing(1), Completed(2), Failed(3) | Pending |
| `HealthState` | -    | Healthy, Degraded, Critical     | Healthy    |

### Structs

| Struct          | Copy | Fields |
|-----------------|------|--------|
| `AiStreamConfig`| Yes  | 4      |
| `TokenChunk`    | No   | 3      |
| `ToolCall`      | No   | 3      |
| `StatsSnapshot` | Yes  | 6      |
| `HealthReport`  | Yes  | 4      |

---

## Appendix B: Error Handling Guide

### When to retry

```rust
match result {
    Ok(value) => { /* use value */ }
    Err(AiStreamError::TokenFailed(msg)) => {
        // Retryable: back off and retry
        retry_with_backoff().await;
    }
    Err(AiStreamError::ToolCallFailed) => {
        // Retryable: retry or use fallback tool
        retry_or_fallback().await;
    }
    Err(AiStreamError::StreamNotFound(id)) => {
        // Not retryable: check if stream exists or create new
        if stream_exists(id) {
            // race condition, handle gracefully
        } else {
            create_new_stream().await;
        }
    }
    Err(AiStreamError::StreamExists(id)) => {
        // Fatal: programming error
        log::error!("stream already exists: {}", id);
        return;
    }
    Err(AiStreamError::ShuttingDown) => {
        // Fatal: framework is shutting down
        log::info!("framework shutting down, aborting");
        return;
    }
}
```

### Using predicates

```rust
if let Err(e) = result {
    if e.is_fatal() {
        // Don't retry, log and abort
        log::error!("fatal error: {}", e);
        return;
    }
    if e.is_retryable() {
        // Retry with backoff
        retry_with_backoff().await;
    }
}
```

---

## Appendix C: Configuration Examples

### Chat application

```rust
let cfg = Preset::Chat.to_config();
let pipeline = AiStreamPipeline::new(cfg);
```

### Code assistant

```rust
let cfg = Preset::CodeAssistant.to_config();
let pipeline = AiStreamPipeline::new(cfg);
```

### Autonomous agent

```rust
let cfg = Preset::Agent.to_config();
let pipeline = AiStreamPipeline::new(cfg);
```

### Custom configuration

```rust
let cfg = AiStreamConfig::new()
    .with_max_concurrent_streams(200)
    .with_max_tokens_per_stream(8_192)
    .with_token_chunk_size(48)
    .with_tool_call_timeout_ms(8_000);
let pipeline = AiStreamPipeline::new(cfg);
```

### Using the builder

```rust
let cfg = AiStreamBuilder::from_preset(Preset::Chat)
    .max_concurrent_streams(64)
    .tool_call_timeout_ms(10_000)
    .build()?;
let pipeline = AiStreamPipeline::new(cfg);
```

---

## Appendix D: API Stability

### Stability guarantees

- **Non-exhaustive enums**: `AiStreamError`, `HealthState`, and
  `Preset` are `#[non_exhaustive]`. New variants may be added in
  minor versions.
- **Public fields**: Struct fields are public and stable. Renaming
  fields requires a major version bump.
- **Method signatures**: Public method signatures are stable. Adding
  new methods is fine; changing existing ones requires a major
  version bump.
- **Const fn**: Methods marked `const fn` will remain `const fn`.
  Non-const methods may become `const fn` in minor versions.

### Deprecation policy

- Deprecations are announced in minor versions with `#[deprecated]`
- Deprecated APIs are removed in the next major version
- Migration guides are provided in release notes

---

## Appendix E: Comparison with Similar Systems

### OpenAI Streaming API

| Feature           | OpenAI           | ai-streaming           |
|-------------------|------------------|------------------------|
| Stream ID         | String           | u64 newtype             |
| Token type        | String content   | Typed enum              |
| Tool calls        | JSON in stream   | Separate ToolCall type  |
| Backpressure      | None             | Bounded channels (P1)  |
| Health monitoring | None             | Built-in                |
| Config presets    | None             | Chat/CodeAssistant/Agent|

### gRPC Streaming

| Feature           | gRPC             | ai-streaming           |
|-------------------|------------------|------------------------|
| Stream modes      | 4 (unary/bidi/etc) | 3 (text/tool/mixed)  |
| Backpressure      | HTTP/2 flow ctrl | Bounded channels (P1)  |
| Tool calls        | N/A              | First-class             |
| Health checks     | grpc-health-probe | Built-in               |
| Observability     | External         | Built-in atomics        |

### Vercel AI SDK

| Feature           | Vercel AI SDK    | ai-streaming           |
|-------------------|------------------|------------------------|
| Language          | TypeScript       | Rust                   |
| Stream model      | SSE              | QUIC streams            |
| Tool calls        | Protocol-level   | First-class types       |
| Backpressure      | None             | Bounded channels (P1)  |
| Health monitoring | None             | Built-in                |

---

*This document is the authoritative design specification for the
`ai-streaming` crate. Implementation should conform to this
specification. Deviations require updating this document first.*
