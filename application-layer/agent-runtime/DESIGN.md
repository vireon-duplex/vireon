# agent-runtime Design Document

This document is the authoritative specification for the `agent-runtime` crate
within the Vireon application layer. It covers architecture, data structures,
algorithms, failure modes, observability, configuration, presets, performance,
API reference, testing strategy, ops runbook, security, and future directions.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Data Structures](#3-data-structures)
4. [Algorithms](#4-algorithms)
5. [Failure Modes](#5-failure-modes)
6. [Observability](#6-observability)
7. [Configuration](#7-configuration)
8. [Presets](#8-presets)
9. [Performance Characteristics](#9-performance-characteristics)
10. [API Reference](#10-api-reference)
11. [Testing Strategy](#11-testing-strategy)
12. [Ops Runbook](#12-ops-runbook)
13. [Security](#13-security)
14. [Future Directions](#14-future-directions)

---

## 1. Overview

### 1.1 Purpose

The `agent-runtime` crate provides an **agent runtime** for the Vireon
application layer. It is the primary execution environment for LLM-based AI
agents, providing agent lifecycle management, tool execution, context
management with token budget tracking, and priority-based scheduling. The
design is inspired by agent frameworks such as AutoGen, LangGraph, and
OpenAI's Assistants API, but adapted for the single-process, io_uring-driven
Vireon runtime.

### 1.2 Goals

- **Agent lifecycle management.** Agents are created, transition through
  well-defined states (Idle, Running, Waiting, Stopped, Failed), and are
  cleaned up on termination.
- **Tool execution with timeouts.** Tools registered with an agent are
  executed with configurable timeouts, and results are returned in a typed
  structure.
- **Context management.** Each agent operates within a token budget that
  tracks usage and prevents runaway context growth.
- **Priority-based scheduling.** Agents are scheduled for execution based
  on priority, with fair scheduling across agents of equal priority.
- **Typed errors.** Every fallible API returns a small, exhaustive
  `AgentRuntimeError` enum so callers can branch precisely on failure modes.
- **Observability.** Atomic counters and a derived health report are
  available at all times with zero allocation on the hot path.
- **Configuration-driven.** All operational knobs live in
  `AgentRuntimeConfig` and can be materialized from presets or built fluently.

### 1.3 Non-Goals

- **Cross-process agent orchestration.** The agent runtime is
  intra-process. For cross-process agent communication, use the
  `rpc-framework` crate.
- **Persistent agent state.** Agent state is held in memory. Durability is
  the responsibility of higher layers that checkpoint to disk or replicated
  stores.
- **Model inference.** The agent runtime does not perform LLM inference
  directly. It delegates to the `ai-streaming` crate for token generation
  and tool call streaming.
- **Tool implementation.** Tools are registered as descriptors; their
  implementation is provided by the caller via function pointers or trait
  objects in Phase 1+.

### 1.4 Phase Status

The crate is developed in phases:

| Phase | Scope | Status |
|-------|-------|--------|
| 0 | Core types, config, stats, health, builder, typed skeletons | Complete |
| 1 | Functional `AgentRuntime`, `AgentExecutor`, `AgentScheduler` | Planned |
| 2 | Persistent agent state, WAL integration | Future |
| 3 | Distributed agent orchestration via rpc-framework | Future |

This document describes Phase 0 in detail and outlines the Phase 1+ design
where skeletons already exist.

### 1.5 Crate Layout

```
agent-runtime/
  Cargo.toml
  DESIGN.md           <- this document
  README.md
  src/
    lib.rs            <- crate root, re-exports
    config.rs         <- AgentRuntimeConfig, Preset
    agent.rs          <- AgentId, AgentState
    tool.rs           <- ToolId, ToolDescriptor, ToolResult
    context.rs        <- AgentContext (token budget)
    stats.rs          <- AgentRuntimeStats, StatsSnapshot
    health.rs         <- HealthReport, HealthState
    builder.rs        <- AgentRuntimeBuilder
    runtime.rs        <- AgentRuntime facade (skeleton)
    executor.rs       <- AgentExecutor facade (skeleton)
    scheduler.rs      <- AgentScheduler facade (skeleton)
  benches/
    agent_lifecycle.rs
    tool_execute.rs
    context_switch.rs
    schedule.rs
```

### 1.6 Dependencies

| Dependency | Purpose |
|------------|---------|
| `configuration-system` | Declarative config loading, validation hooks |
| `rpc-framework` | `RpcMethodId`, `StreamHandle` re-exports |
| `ai-streaming` | `AiStreamId`, `TokenChunk` re-exports |
| `event-bus` | `TopicId` re-export for event-driven agent hooks |
| `session-manager` | `SessionId` for session-scoped agent contexts |
| `thiserror` | Derive `Error` for `AgentRuntimeError` |
| `tracing` | Structured logging spans for agent lifecycle events |
| `criterion` (dev) | Criterion benchmark harness |

---

## 2. Architecture

### 2.1 One-Line Summary

```
Client --> [Agent Runtime] --> Agent Lifecycle
              |
              +-- AgentRegistry      (AgentId -> Agent)
              +-- AgentExecutor      (tool dispatch)
              +-- AgentScheduler     (fair scheduling)
              +-- ContextManager     (token budgets)
              +-- StatsCollector     (observability)
```

### 2.2 Component Diagram

```
                    +---------------------------------+
                    |          AgentRuntime           |
                    |                                 |
  Client ---------> |  +-----------+   +-----------+  |
                    |  | AgentReg  |   | Scheduler |  |
                    |  +-----------+   +-----------+  |
                    |  +-----------+   +-----------+  |
                    |  | Executor  |   | Context   |  |
                    |  +-----------+   +-----------+  |
                    |  +-----------+                  |
                    |  | Stats     |                  |
                    |  +-----------+                  |
                    +---------------------------------+
                              |
                              v
                    +---------------------------------+
                    |       Adjacent Layers           |
                    |  ai-streaming: AiStreamId       |
                    |  rpc-framework: RpcMethodId     |
                    |  event-bus: TopicId             |
                    |  session-manager: SessionId     |
                    +---------------------------------+
```

### 2.3 Data Flow

```
1. Client calls AgentRuntime::create_agent()
2. Runtime assigns AgentId, creates AgentContext with token budget
3. Client registers tools via ToolDescriptor
4. Client calls AgentScheduler::schedule(agent_id)
5. Scheduler picks next agent via next_agent()
6. For each tool call:
   a. AgentExecutor::execute_tool(agent_id, tool) is called
   b. Tool runs with timeout enforcement
   c. ToolResult is returned (success or failure)
   d. Stats are updated atomically
7. Context tokens are tracked via AgentContext::use_tokens()
8. When done, Client calls AgentRuntime::stop_agent(agent_id)
9. Agent transitions to Stopped state
```

### 2.4 Layer Interaction

The agent runtime sits at the top of the application layer stack,
orchestrating interactions between:

- **ai-streaming**: Provides token streaming for LLM inference. The agent
  runtime creates `AiStreamId`s for each inference request and processes
  `TokenChunk`s as they arrive.
- **rpc-framework**: Provides RPC method dispatch for tool execution. Tools
  that require remote calls use `RpcMethodId` to identify the RPC endpoint.
- **event-bus**: Provides pub/sub for agent lifecycle events. Agent state
  transitions are published as events on `TopicId`s for other modules to
  react to.
- **session-manager**: Provides `SessionId` for session-scoped agent
  contexts. Each agent is associated with a session for authentication and
  quota enforcement.

### 2.5 Threading Model

The agent runtime is designed for the single-process, io_uring-driven Vireon
runtime. In Phase 0, all operations are synchronous and lock-free (using
atomic counters for stats). In Phase 1+:

- The runtime will use async/await with the Vireon reactor for non-blocking
  tool execution.
- The scheduler will run as a task on the reactor, picking agents from the
  ready queue.
- Stats collection remains lock-free via `AtomicU64`.

---

## 3. Data Structures

### 3.1 AgentId

A `u64` newtype representing a unique agent identifier.

```rust
pub struct AgentId(u64);
```

- `NIL = 0` is reserved and never assigned to a real agent.
- `new(id: u64) -> Self` constructs a new ID.
- `nil() -> Self` returns the nil sentinel.
- `is_nil(&self) -> bool` checks for nil.
- `raw(self) -> u64` returns the underlying value.
- Implements `Display` as `agent#{id}` for error messages.
- Derives `Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord`.

### 3.2 AgentState

A `#[repr(u8)]` enum representing the lifecycle state of an agent.

```rust
#[repr(u8)]
pub enum AgentState {
    Idle = 0,
    Running = 1,
    Waiting = 2,
    Stopped = 3,
    Failed = 4,
}
```

- `is_active() -> bool`: true for `Running` and `Waiting`.
- `is_terminal() -> bool`: true for `Stopped` and `Failed`.
- `as_str() -> &'static str`: human-readable name for metrics labels.

State transitions:

```
Idle -> Running -> Waiting -> Running -> ... -> Stopped
                  |
                  +--------> Failed
```

Once terminal (`Stopped` or `Failed`), an agent does not transition further.

### 3.3 ToolId

A `u64` newtype representing a unique tool identifier.

```rust
pub struct ToolId(u64);
```

- `NIL = 0` is reserved.
- Same trait implementations as `AgentId`.
- Display format: `tool#{id}`.

### 3.4 ToolDescriptor

A static descriptor for a tool available to agents.

```rust
pub struct ToolDescriptor {
    pub id: ToolId,
    pub name: &'static str,
    pub timeout_ms: u32,
}
```

- `new(id: ToolId, name: &'static str) -> Self` with default timeout of 30s.
- `with_timeout_ms(self, ms: u32) -> Self` builder method.
- `DEFAULT_TIMEOUT_MS = 30_000`.

### 3.5 ToolResult

The result of a tool execution.

```rust
pub struct ToolResult {
    pub tool_id: ToolId,
    pub success: bool,
    pub output: String,
}
```

- `success(tool_id: ToolId, output: String) -> Self` constructor.
- `failure(tool_id: ToolId, output: String) -> Self` constructor.

### 3.6 AgentContext

Per-agent execution context with token budget tracking.

```rust
pub struct AgentContext {
    pub agent_id: AgentId,
    pub session_id: SessionId,
    pub tokens_used: u64,
    pub tokens_limit: u64,
}
```

- `new(agent_id: AgentId, session_id: SessionId) -> Self` with default limit of 8192.
- `with_token_limit(self, limit: u64) -> Self` builder method.
- `remaining_tokens() -> u64`: saturating subtraction.
- `use_tokens(n: u64)`: saturates at the limit.
- `is_exhausted() -> bool`: true when `tokens_used >= tokens_limit`.

### 3.7 AgentRuntimeStats

Atomic counters for observability.

```rust
pub struct AgentRuntimeStats {
    pub agents_created: AtomicU64,
    pub agents_stopped: AtomicU64,
    pub tool_calls_initiated: AtomicU64,
    pub tool_calls_completed: AtomicU64,
    pub tool_calls_failed: AtomicU64,
    pub context_tokens_used: AtomicU64,
}
```

- All counters use `Ordering::Relaxed` for minimal overhead.
- `snapshot() -> StatsSnapshot` for a consistent point-in-time view.

### 3.8 StatsSnapshot

A `Copy` struct that captures all counters at a point in time.

```rust
pub struct StatsSnapshot {
    pub agents_created: u64,
    pub agents_stopped: u64,
    pub tool_calls_initiated: u64,
    pub tool_calls_completed: u64,
    pub tool_calls_failed: u64,
    pub context_tokens_used: u64,
}
```

- `tool_success_rate_bps() -> u16`: basis points (0..=10_000), uses
  `saturating_mul` for overflow safety. Returns 10_000 when no tool calls.
- `active_agents() -> u64`: saturating subtraction of stopped from created.

### 3.9 HealthReport

Operational health derived from stats.

```rust
pub struct HealthReport {
    pub state: HealthState,
    pub tool_success_rate_bps: u16,
    pub active_agents: u64,
    pub context_tokens_used: u64,
}
```

- `derive(&StatsSnapshot) -> Self`: worst-signal-wins classification.
- Thresholds:
  - `TOOL_SUCCESS_DEGRADED_BPS = 9_000` (90%)
  - `TOOL_SUCCESS_CRITICAL_BPS = 8_000` (80%)

### 3.10 HealthState

Tri-state health classification.

```rust
pub enum HealthState {
    Healthy,
    Degraded,
    Critical,
}
```

- `is_healthy()`, `needs_attention()`, `is_critical()` predicates.
- `as_str() -> &'static str` for metrics labels.

### 3.11 AgentRuntimeConfig

Declarative configuration for the agent runtime.

```rust
pub struct AgentRuntimeConfig {
    pub max_agents: u32,
    pub max_tools_per_agent: u32,
    pub tool_timeout_ms: u32,
    pub context_window_tokens: u64,
}
```

- All fields are public for direct construction.
- `validate()` checks all fields against MIN/MAX constants.
- Builder-style `with_*` methods for fluent construction.

### 3.12 AgentRuntimeBuilder

Fluent builder for `AgentRuntimeConfig`.

```rust
pub struct AgentRuntimeBuilder {
    cfg: AgentRuntimeConfig,
}
```

- `new() -> Self`: starts with defaults.
- `from_preset(Preset) -> Self`: starts from a preset.
- Fluent setters for each config field.
- `build() -> Result<AgentRuntimeConfig, AgentRuntimeError>`: validates on build.

### 3.13 AgentRuntime (Skeleton)

Agent runtime facade (Phase 0 skeleton).

```rust
pub struct AgentRuntime {
    cfg: AgentRuntimeConfig,
    stats: AgentRuntimeStats,
}
```

- `new(cfg: AgentRuntimeConfig) -> Self`.
- `create_agent() -> Result<AgentId, AgentRuntimeError>`: returns `ShuttingDown`.
- `stop_agent(AgentId) -> Result<(), AgentRuntimeError>`: returns `ShuttingDown`.
- `health() -> HealthReport`: returns derived health from stats.

### 3.14 AgentExecutor (Skeleton)

Tool execution facade (Phase 0 skeleton).

```rust
pub struct AgentExecutor {
    cfg: AgentRuntimeConfig,
    stats: AgentRuntimeStats,
}
```

- `new(cfg: AgentRuntimeConfig) -> Self`.
- `execute_tool(AgentId, &ToolDescriptor) -> Result<ToolResult, AgentRuntimeError>`: returns `ShuttingDown`.
- `health() -> HealthReport`: returns derived health from stats.

### 3.15 AgentScheduler (Skeleton)

Agent scheduling facade (Phase 0 skeleton).

```rust
pub struct AgentScheduler {
    cfg: AgentRuntimeConfig,
    stats: AgentRuntimeStats,
}
```

- `new(cfg: AgentRuntimeConfig) -> Self`.
- `schedule(AgentId) -> Result<(), AgentRuntimeError>`: returns `ShuttingDown`.
- `next_agent() -> Result<AgentId, AgentRuntimeError>`: returns `ShuttingDown`.
- `health() -> HealthReport`: returns derived health from stats.

---

## 4. Algorithms

### 4.1 Agent ID Generation (Phase 1+)

Agent IDs will be generated using a global atomic counter, starting at 1
(zero is reserved for NIL). The counter ensures uniqueness within a process.
For distributed deployments (Phase 3), the high bits of the ID will encode
a node identifier.

```rust
static AGENT_COUNTER: AtomicU64 = AtomicU64::new(1);

fn next_agent_id() -> AgentId {
    AgentId::new(AGENT_COUNTER.fetch_add(1, Ordering::Relaxed))
}
```

### 4.2 Tool Execution with Timeout (Phase 1+)

Tool execution will use the Vireon timer-wheel for timeout enforcement.
When a tool is executed:

1. A timer is set for `tool.timeout_ms` milliseconds.
2. The tool runs asynchronously.
3. If the tool completes before the timer fires, the timer is cancelled.
4. If the timer fires first, the tool is cancelled and `ToolFailed` is returned.

### 4.3 Token Budget Enforcement

Token budgets are enforced via `AgentContext::use_tokens(n)`:

1. `tokens_used` is incremented by `n` using `saturating_add`.
2. If `tokens_used` exceeds `tokens_limit`, it is clamped to `tokens_limit`.
3. `remaining_tokens()` returns `tokens_limit.saturating_sub(tokens_used)`.
4. `is_exhausted()` returns true when `tokens_used >= tokens_limit`.

This ensures that:
- Token counts never overflow.
- Remaining tokens never underflow.
- The budget is strictly enforced.

### 4.4 Health Classification

Health is derived from stats using a "worst signal wins" policy:

1. Compute tool success rate in basis points.
2. If rate < `TOOL_SUCCESS_CRITICAL_BPS` (80%): state = Critical.
3. Else if rate < `TOOL_SUCCESS_DEGRADED_BPS` (90%): state = Degraded.
4. Else: state = Healthy.

In Phase 1+, additional signals will be added:
- Agent creation failure rate
- Scheduler queue depth
- Tool execution latency percentiles

### 4.5 Scheduler Fairness (Phase 1+)

The scheduler will use a weighted round-robin algorithm:

1. Agents are assigned priorities (low, normal, high).
2. Within each priority level, agents are scheduled in round-robin order.
3. Higher priority levels are drained before lower levels.
4. To prevent starvation, a "priority boost" mechanism will periodically
   promote starved low-priority agents.

### 4.6 Rate Calculation

Tool success rate is calculated as basis points (bps):

```
rate_bps = (tool_calls_completed * 10_000) / tool_calls_initiated
```

- Uses `saturating_mul(10_000)` to prevent overflow.
- Integer division truncates: 2 * 10_000 / 3 = 6666 (not 6667).
- Returns 10_000 (100%) when no tool calls have been initiated.
- Capped at `u16::MAX` (65535) as a safety measure.

---

## 5. Failure Modes

### 5.1 Error Variants

```rust
#[non_exhaustive]
pub enum AgentRuntimeError {
    AgentNotFound(AgentId),
    AgentExists(AgentId),
    ToolFailed(&'static str),
    SchedulerFull,
    ShuttingDown,
}
```

### 5.2 Error Classification

| Variant | Fatal | Retryable | Caller Action |
|---------|-------|-----------|---------------|
| `AgentNotFound` | No | No | Check agent ID |
| `AgentExists` | Yes | No | Programming error; fix caller |
| `ToolFailed` | No | Yes | Back off and retry |
| `SchedulerFull` | No | Yes | Back off and retry |
| `ShuttingDown` | Yes | No | Abort; runtime is terminating |

### 5.3 Fatal Errors

Fatal errors indicate programming mistakes (`AgentExists`) or terminal
lifecycle states (`ShuttingDown`). The caller must NOT retry. These are
surfaced as hard errors in logs and should trigger alerts.

### 5.4 Retryable Errors

Retryable errors are transient. The caller should implement exponential
backoff:

```rust
match runtime.create_agent() {
    Ok(id) => { /* proceed */ }
    Err(e) if e.is_retryable() => {
        // back off and retry
    }
    Err(e) if e.is_fatal() => {
        // abort
    }
    Err(e) => {
        // handle other errors
    }
}
```

### 5.5 Error Propagation

Errors propagate via `Result<T, AgentRuntimeError>`. The `?` operator can
be used in functions that return `Result`. Errors are logged via `tracing`
at the point of occurrence.

### 5.6 Non-Exhaustive

The `#[non_exhaustive]` attribute on `AgentRuntimeError` ensures that
callers cannot exhaustively match on all variants, allowing new variants
to be added in future versions without breaking existing code.

---

## 6. Observability

### 6.1 Atomic Counters

All counters in `AgentRuntimeStats` use `AtomicU64` with `Ordering::Relaxed`:
- `agents_created`: incremented on agent creation.
- `agents_stopped`: incremented on agent stop.
- `tool_calls_initiated`: incremented when a tool execution starts.
- `tool_calls_completed`: incremented on successful tool completion.
- `tool_calls_failed`: incremented on tool failure.
- `context_tokens_used`: incremented by `n` when `use_tokens(n)` is called.

### 6.2 StatsSnapshot

`StatsSnapshot` is a `Copy` struct that captures all counters at a point
in time. It provides:
- `tool_success_rate_bps()`: success rate as basis points.
- `active_agents()`: currently active agent count.

### 6.3 HealthReport

`HealthReport::derive(&StatsSnapshot)` computes a tri-state health
classification:
- `Healthy`: all signals nominal.
- `Degraded`: at least one signal needs attention.
- `Critical`: at least one signal is critical.

### 6.4 HealthState

The `HealthState` enum provides:
- `is_healthy()`: true for `Healthy`.
- `needs_attention()`: true for `Degraded` and `Critical`.
- `is_critical()`: true for `Critical`.
- `as_str()`: human-readable name for metrics labels.

### 6.5 Export Format

Stats and health are designed for direct export to Prometheus:

```
agent_runtime_agents_created_total
agent_runtime_agents_stopped_total
agent_runtime_tool_calls_initiated_total
agent_runtime_tool_calls_completed_total
agent_runtime_tool_calls_failed_total
agent_runtime_context_tokens_used_total
agent_runtime_active_agents
agent_runtime_tool_success_rate_bps
agent_runtime_health_state{state="healthy|degraded|critical"}
```

### 6.6 Tracing

The `tracing` crate is used for structured logging. Key spans:
- `agent_runtime::create_agent`: covers agent creation.
- `agent_runtime::stop_agent`: covers agent stop.
- `agent_runtime::execute_tool`: covers tool execution.
- `agent_runtime::schedule`: covers agent scheduling.

---

## 7. Configuration

### 7.1 AgentRuntimeConfig

```rust
pub struct AgentRuntimeConfig {
    pub max_agents: u32,
    pub max_tools_per_agent: u32,
    pub tool_timeout_ms: u32,
    pub context_window_tokens: u64,
}
```

### 7.2 Constants

| Constant | Value |
|----------|-------|
| `MIN_MAX_AGENTS` | 1 |
| `MAX_MAX_AGENTS` | 10_000 |
| `MIN_MAX_TOOLS_PER_AGENT` | 1 |
| `MAX_MAX_TOOLS_PER_AGENT` | 1_000 |
| `MIN_TOOL_TIMEOUT_MS` | 100 |
| `MAX_TOOL_TIMEOUT_MS` | 600_000 (10 min) |
| `MIN_CONTEXT_WINDOW_TOKENS` | 128 |
| `MAX_CONTEXT_WINDOW_TOKENS` | 1_000_000 |

### 7.3 Defaults

| Field | Default Value |
|-------|---------------|
| `max_agents` | 128 |
| `max_tools_per_agent` | 32 |
| `tool_timeout_ms` | 30_000 (30s) |
| `context_window_tokens` | 16_384 |

### 7.4 Validation

`AgentRuntimeConfig::validate()` checks each field against its MIN/MAX
range. If any field is out of range, it returns
`AgentRuntimeError::ToolFailed("field_name out of range")`.

### 7.5 Builder Methods

Each field has a `with_*` const method for fluent construction:

```rust
let cfg = AgentRuntimeConfig::new()
    .with_max_agents(256)
    .with_max_tools_per_agent(64)
    .with_tool_timeout_ms(10_000)
    .with_context_window_tokens(32_768);
```

### 7.6 Integration with configuration-system

The `configuration-system` crate provides declarative config loading from
files, environment variables, and command-line flags. In Phase 1+,
`AgentRuntimeConfig` will implement the `ConfigLayer` trait to integrate
with the configuration system.

---

## 8. Presets

### 8.1 Preset Enum

```rust
#[non_exhaustive]
pub enum Preset {
    Minimal,
    Standard,
    Full,
}
```

### 8.2 Preset Configurations

| Preset | Max Agents | Max Tools | Tool Timeout | Context Window |
|--------|------------|-----------|--------------|----------------|
| `Minimal` | 32 | 8 | 5_000 ms | 4_096 |
| `Standard` | 128 | 32 | 30_000 ms | 16_384 |
| `Full` | 512 | 128 | 60_000 ms | 65_536 |

### 8.3 Preset Usage

```rust
use agent_runtime::{Preset, AgentRuntimeBuilder};

// From preset
let cfg = Preset::Standard.to_config();

// From preset with overrides
let cfg = AgentRuntimeBuilder::from_preset(Preset::Minimal)
    .max_agents(100)
    .build()?;
```

### 8.4 Preset Design Rationale

- **Minimal**: For lightweight agent workloads with few tools and short
  interactions. Suitable for testing and development.
- **Standard**: The default configuration for production workloads.
  Balances concurrency, toolset size, and resource usage.
- **Full**: For high-capacity deployments with many agents, large toolsets,
  and generous timeouts. Suitable for complex multi-step agent workflows.

### 8.5 Preset Validation

All presets produce validated configs. The `all_presets_produce_valid_configs`
test ensures that every preset's `to_config()` output passes `validate()`.

---

## 9. Performance Characteristics

### 9.1 Hot Path Operations

| Operation | Cost | Allocation |
|-----------|------|------------|
| `AgentId::new` | 1 ns | None |
| `AgentState::as_str` | 1 ns | None |
| `AgentContext::use_tokens` | 2 ns | None |
| `AgentContext::remaining_tokens` | 1 ns | None |
| `AgentRuntimeStats::record_*` | 5-10 ns | None |
| `AgentRuntimeStats::snapshot` | 20-30 ns | None |
| `HealthReport::derive` | 30-50 ns | None |

### 9.2 Cold Path Operations

| Operation | Cost | Allocation |
|-----------|------|------------|
| `AgentRuntimeConfig::validate` | 10-20 ns | None |
| `AgentRuntimeBuilder::build` | 20-30 ns | None |
| `ToolResult::success/failure` | 50-100 ns | 1 (String) |

### 9.3 Memory Footprint

| Type | Size | Notes |
|------|------|-------|
| `AgentId` | 8 bytes | u64 newtype |
| `AgentState` | 1 byte | repr(u8) enum |
| `ToolId` | 8 bytes | u64 newtype |
| `ToolDescriptor` | 24 bytes | id(8) + name(16) + timeout(4) + padding |
| `ToolResult` | 32 bytes | tool_id(8) + success(1) + padding + String(24) |
| `AgentContext` | 48 bytes | agent_id(8) + session_id(16) + tokens(16) + padding |
| `AgentRuntimeStats` | 48 bytes | 6 x AtomicU64 |
| `StatsSnapshot` | 48 bytes | 6 x u64 |
| `HealthReport` | 24 bytes | state(1) + bps(2) + padding + agents(8) + tokens(8) |
| `AgentRuntimeConfig` | 24 bytes | 3 x u32 + u64 + padding |

### 9.4 Benchmark Stubs

Four Criterion benchmark stubs are provided:
- `agent_lifecycle`: measures `AgentId::new`, `AgentState::as_str`, `AgentRuntime::health`.
- `tool_execute`: measures `ToolDescriptor::new`, `ToolResult::success/failure`.
- `context_switch`: measures `AgentContext::new`, `use_tokens`, `remaining_tokens`.
- `schedule`: measures `AgentScheduler::new`, `health`, `AgentId::is_nil`.

### 9.5 Scalability

The agent runtime is designed to scale to:
- 10,000 concurrent agents (configurable via `max_agents`).
- 1,000 tools per agent (configurable via `max_tools_per_agent`).
- 1,000,000 token context windows (configurable via `context_window_tokens`).

Stats collection uses lock-free atomics, ensuring O(1) overhead regardless
of agent count.

---

## 10. API Reference

### 10.1 AgentId

```rust
impl AgentId {
    pub const NIL: Self;
    pub const fn new(id: u64) -> Self;
    pub const fn nil() -> Self;
    pub const fn is_nil(&self) -> bool;
    pub const fn raw(self) -> u64;
}
```

### 10.2 AgentState

```rust
impl AgentState {
    pub const fn is_active(&self) -> bool;
    pub const fn is_terminal(&self) -> bool;
    pub const fn as_str(&self) -> &'static str;
}
```

### 10.3 ToolId

```rust
impl ToolId {
    pub const NIL: Self;
    pub const fn new(id: u64) -> Self;
    pub const fn nil() -> Self;
    pub const fn is_nil(&self) -> bool;
    pub const fn raw(self) -> u64;
}
```

### 10.4 ToolDescriptor

```rust
impl ToolDescriptor {
    pub const DEFAULT_TIMEOUT_MS: u32 = 30_000;
    pub fn new(id: ToolId, name: &'static str) -> Self;
    pub const fn with_timeout_ms(self, ms: u32) -> Self;
}
```

### 10.5 ToolResult

```rust
impl ToolResult {
    pub fn success(tool_id: ToolId, output: String) -> Self;
    pub fn failure(tool_id: ToolId, output: String) -> Self;
}
```

### 10.6 AgentContext

```rust
impl AgentContext {
    pub const DEFAULT_TOKEN_LIMIT: u64 = 8_192;
    pub fn new(agent_id: AgentId, session_id: SessionId) -> Self;
    pub const fn with_token_limit(self, limit: u64) -> Self;
    pub const fn remaining_tokens(&self) -> u64;
    pub fn use_tokens(&mut self, n: u64);
    pub const fn is_exhausted(&self) -> bool;
}
```

### 10.7 AgentRuntimeStats

```rust
impl AgentRuntimeStats {
    pub const fn new() -> Self;
    pub fn record_agent_created(&self);
    pub fn record_agent_stopped(&self);
    pub fn record_tool_call_initiated(&self);
    pub fn record_tool_call_completed(&self);
    pub fn record_tool_call_failed(&self);
    pub fn record_context_tokens(&self, count: u64);
    pub fn snapshot(&self) -> StatsSnapshot;
}
```

### 10.8 StatsSnapshot

```rust
impl StatsSnapshot {
    pub const fn tool_success_rate_bps(&self) -> u16;
    pub const fn active_agents(&self) -> u64;
}
```

### 10.9 HealthReport

```rust
impl HealthReport {
    pub const TOOL_SUCCESS_DEGRADED_BPS: u16 = 9_000;
    pub const TOOL_SUCCESS_CRITICAL_BPS: u16 = 8_000;
    pub fn derive(stats: &StatsSnapshot) -> Self;
    pub const fn is_healthy(&self) -> bool;
    pub const fn needs_attention(&self) -> bool;
}
```

### 10.10 HealthState

```rust
impl HealthState {
    pub const fn is_healthy(&self) -> bool;
    pub const fn needs_attention(&self) -> bool;
    pub const fn is_critical(&self) -> bool;
    pub const fn as_str(&self) -> &'static str;
}
```

### 10.11 AgentRuntimeConfig

```rust
impl AgentRuntimeConfig {
    pub fn new() -> Self;
    pub const fn with_max_agents(self, agents: u32) -> Self;
    pub const fn with_max_tools_per_agent(self, tools: u32) -> Self;
    pub const fn with_tool_timeout_ms(self, ms: u32) -> Self;
    pub const fn with_context_window_tokens(self, tokens: u64) -> Self;
    pub fn validate(&self) -> Result<(), AgentRuntimeError>;
}
```

### 10.12 AgentRuntimeBuilder

```rust
impl AgentRuntimeBuilder {
    pub fn new() -> Self;
    pub fn from_preset(preset: Preset) -> Self;
    pub fn max_agents(self, agents: u32) -> Self;
    pub fn max_tools_per_agent(self, tools: u32) -> Self;
    pub fn tool_timeout_ms(self, ms: u32) -> Self;
    pub fn context_window_tokens(self, tokens: u64) -> Self;
    pub fn build(self) -> Result<AgentRuntimeConfig, AgentRuntimeError>;
}
```

### 10.13 AgentRuntime (Skeleton)

```rust
impl AgentRuntime {
    pub fn new(cfg: AgentRuntimeConfig) -> Self;
    pub fn create_agent(&self) -> Result<AgentId, AgentRuntimeError>;
    pub fn stop_agent(&self, agent: AgentId) -> Result<(), AgentRuntimeError>;
    pub fn health(&self) -> HealthReport;
}
```

### 10.14 AgentExecutor (Skeleton)

```rust
impl AgentExecutor {
    pub fn new(cfg: AgentRuntimeConfig) -> Self;
    pub fn execute_tool(&self, agent: AgentId, tool: &ToolDescriptor) -> Result<ToolResult, AgentRuntimeError>;
    pub fn health(&self) -> HealthReport;
}
```

### 10.15 AgentScheduler (Skeleton)

```rust
impl AgentScheduler {
    pub fn new(cfg: AgentRuntimeConfig) -> Self;
    pub fn schedule(&self, agent: AgentId) -> Result<(), AgentRuntimeError>;
    pub fn next_agent(&self) -> Result<AgentId, AgentRuntimeError>;
    pub fn health(&self) -> HealthReport;
}
```

### 10.16 AgentRuntimeError

```rust
impl AgentRuntimeError {
    pub const fn is_fatal(&self) -> bool;
    pub const fn is_retryable(&self) -> bool;
}
```

### 10.17 Re-exports

The crate re-exports the following adjacent-layer types for convenience:
- `ai_streaming::AiStreamId`
- `ai_streaming::TokenChunk`
- `event_bus::TopicId`
- `rpc_framework::RpcMethodId`
- `rpc_framework::StreamHandle`

---

## 11. Testing Strategy

### 11.1 Test Coverage

The crate has 115 tests covering all Phase 0 modules:

| Module | Tests | Coverage |
|--------|-------|----------|
| `agent` | 17 | AgentId, AgentState |
| `config` | 13 | Config, validation, presets |
| `context` | 12 | Token budget, saturation |
| `error` | 10 | Error classification, Display |
| `executor` | 3 | Skeleton behavior |
| `health` | 12 | Health derivation, thresholds |
| `builder` | 8 | Builder chaining, validation |
| `runtime` | 4 | Skeleton behavior |
| `scheduler` | 4 | Skeleton behavior |
| `stats` | 12 | Counters, snapshot, rates |
| `tool` | 20 | ToolId, ToolDescriptor, ToolResult |
| **Total** | **115** | |

### 11.2 Test Categories

- **Unit tests**: Each module has its own test submodule.
- **Boundary tests**: MIN/MAX values for all config fields.
- **Saturation tests**: Overflow/underflow safety for token counts and rates.
- **Skeleton tests**: Verify that skeleton methods return `ShuttingDown`.
- **Predicate tests**: `is_fatal`, `is_retryable`, `is_active`, `is_terminal`.
- **Display tests**: Human-readable error messages.
- **Equality tests**: `PartialEq` and `Hash` for newtypes.

### 11.3 Test Conventions

- Tests use `#[cfg(test)]` mod tests pattern.
- Each test function is named descriptively (e.g., `agent_id_new`).
- `assert!` and `assert_eq!` are preferred over custom assertions.
- No `unwrap()` or `expect()` in tests (clippy denies them).

### 11.4 Property-Based Testing (Future)

In Phase 1+, property-based tests will be added using `proptest`:
- Agent ID uniqueness under concurrent creation.
- Token budget saturation properties.
- Health state transition properties.

### 11.5 Integration Testing (Future)

In Phase 1+, integration tests will verify:
- End-to-end agent lifecycle (create -> schedule -> execute -> stop).
- Tool execution with real timeouts.
- Scheduler fairness under load.
- Stats consistency under concurrent updates.

---

## 12. Ops Runbook

### 12.1 Health Monitoring

Monitor the `HealthReport` via the `health()` method on `AgentRuntime`,
`AgentExecutor`, and `AgentScheduler`:

```rust
let health = runtime.health();
match health.state {
    HealthState::Healthy => { /* nominal */ }
    HealthState::Degraded => { /* investigate */ }
    HealthState::Critical => { /* page on-call */ }
}
```

### 12.2 Key Metrics

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `tool_success_rate_bps` | Tool call success rate | < 9000 = Degraded, < 8000 = Critical |
| `active_agents` | Currently active agents | Depends on capacity |
| `context_tokens_used` | Total tokens consumed | Monitor for budget exhaustion |
| `agents_created` | Total agents created | Track for capacity planning |
| `agents_stopped` | Total agents stopped | Track for lifecycle health |

### 12.3 Common Issues

**Issue: Tool execution failures**
- Symptom: `tool_success_rate_bps` drops below 9000.
- Action: Check tool implementations for errors. Check timeouts.
- Resolution: Fix tool bugs, increase `tool_timeout_ms`.

**Issue: Scheduler at capacity**
- Symptom: `SchedulerFull` errors.
- Action: Check `active_agents` against `max_agents`.
- Resolution: Increase `max_agents` or reduce agent count.

**Issue: Token budget exhaustion**
- Symptom: `is_exhausted()` returns true for agents.
- Action: Check `context_tokens_used` growth rate.
- Resolution: Increase `context_window_tokens` or optimize prompts.

**Issue: Runtime shutting down**
- Symptom: `ShuttingDown` errors from all APIs.
- Action: Check for graceful shutdown in progress.
- Resolution: Wait for shutdown to complete, then restart.

### 12.4 Capacity Planning

| Preset | Recommended Agents | Recommended Tools | Memory |
|--------|--------------------|--------------------|--------|
| Minimal | 1-32 | 1-8 | ~1 MB |
| Standard | 32-128 | 8-32 | ~4 MB |
| Full | 128-512 | 32-128 | ~16 MB |

### 12.5 Startup and Shutdown

**Startup:**
1. Create `AgentRuntimeConfig` (via builder or preset).
2. Validate config with `validate()`.
3. Create `AgentRuntime::new(cfg)`.
4. Verify health is `Healthy`.

**Shutdown:**
1. Stop all active agents via `stop_agent()`.
2. Verify `active_agents` is 0.
3. Drop the `AgentRuntime` instance.

---

## 13. Security

### 13.1 Agent Isolation

Agents are isolated by ID. Each agent has its own `AgentContext` with a
separate token budget. In Phase 1+, agents will be sandboxed with:
- Separate memory pools.
- Tool execution in isolated tasks.
- Resource quotas per agent.

### 13.2 Session Association

Each agent is associated with a `SessionId` from the `session-manager`
crate. This provides:
- Authentication: the session must be valid.
- Authorization: the session's permissions determine what tools are available.
- Quota enforcement: the session's resource quotas apply to the agent.

### 13.3 Tool Security

Tools are registered as `ToolDescriptor`s with static names. In Phase 1+:
- Tool execution will be sandboxed.
- Tool results will be size-limited to prevent memory exhaustion.
- Tool timeouts prevent resource hogging.

### 13.4 Token Budget Security

The token budget (`context_window_tokens`) prevents:
- Runaway context growth from adversarial inputs.
- Memory exhaustion from excessive token usage.
- Cost overruns from unbounded LLM inference.

### 13.5 Error Information

Error messages do not leak sensitive information. `AgentRuntimeError`
variants use:
- Agent IDs (not sensitive, but could be used for enumeration).
- Static error strings (no user-controlled content).
- No stack traces or internal state.

---

## 14. Future Directions

### 14.1 Phase 1: Functional Implementation

- Implement `AgentRuntime` with real agent lifecycle management.
- Implement `AgentExecutor` with timeout-enforced tool execution.
- Implement `AgentScheduler` with weighted round-robin scheduling.
- Add async/await support via the Vireon reactor.
- Add property-based tests with `proptest`.

### 14.2 Phase 2: Persistence

- Add WAL (Write-Ahead Log) for agent state durability.
- Add checkpoint/restore for agent contexts.
- Add crash recovery for in-flight tool executions.

### 14.3 Phase 3: Distributed Agents

- Add cross-process agent orchestration via `rpc-framework`.
- Add distributed scheduling with work stealing.
- Add agent migration between nodes.
- Add distributed health aggregation.

### 14.4 Phase 4: Advanced Features

- Add agent composition (agents calling agents).
- Add tool chaining and pipeline execution.
- Add streaming tool results via `ai-streaming`.
- Add event-driven agent triggers via `event-bus`.
- Add agent templates and instantiation.

### 14.5 Potential Improvements

- **Tool caching**: Cache tool results to avoid redundant executions.
- **Context compression**: Compress context to extend token budget.
- **Agent prioritization**: Dynamic priority adjustment based on workload.
- **Tool versioning**: Support multiple versions of the same tool.
- **Observability**: Add histogram metrics for latency percentiles.

---

## Appendix A: Re-exported Types

### From ai-streaming

- `AiStreamId`: Unique identifier for an AI stream.
- `TokenChunk`: A chunk of tokens in an AI stream.

### From rpc-framework

- `RpcMethodId`: Unique identifier for an RPC method.
- `StreamHandle`: Handle to a streaming RPC response.

### From event-bus

- `TopicId`: Unique identifier for an event bus topic.

### From session-manager

- `SessionId`: 128-bit application-level session identifier.

---

## Appendix B: Type Conversion Table

| Source Type | Target Type | Conversion |
|-------------|-------------|------------|
| `u64` | `AgentId` | `AgentId::new(u64)` |
| `u64` | `ToolId` | `ToolId::new(u64)` |
| `AgentId` | `u64` | `AgentId::raw()` |
| `ToolId` | `u64` | `ToolId::raw()` |
| `AgentState` | `u8` | `AgentState as u8` |
| `AgentState` | `&'static str` | `AgentState::as_str()` |
| `HealthState` | `&'static str` | `HealthState::as_str()` |
| `StatsSnapshot` | `HealthReport` | `HealthReport::derive(&snap)` |
| `AgentRuntimeStats` | `StatsSnapshot` | `stats.snapshot()` |

---

## Appendix C: Constant Reference

### Agent

| Constant | Value | Location |
|----------|-------|----------|
| `AgentId::NIL` | 0 | `agent.rs` |

### Tool

| Constant | Value | Location |
|----------|-------|----------|
| `ToolId::NIL` | 0 | `tool.rs` |
| `ToolDescriptor::DEFAULT_TIMEOUT_MS` | 30_000 | `tool.rs` |

### Context

| Constant | Value | Location |
|----------|-------|----------|
| `AgentContext::DEFAULT_TOKEN_LIMIT` | 8_192 | `context.rs` |

### Config

| Constant | Value | Location |
|----------|-------|----------|
| `MIN_MAX_AGENTS` | 1 | `config.rs` |
| `MAX_MAX_AGENTS` | 10_000 | `config.rs` |
| `MIN_MAX_TOOLS_PER_AGENT` | 1 | `config.rs` |
| `MAX_MAX_TOOLS_PER_AGENT` | 1_000 | `config.rs` |
| `MIN_TOOL_TIMEOUT_MS` | 100 | `config.rs` |
| `MAX_TOOL_TIMEOUT_MS` | 600_000 | `config.rs` |
| `MIN_CONTEXT_WINDOW_TOKENS` | 128 | `config.rs` |
| `MAX_CONTEXT_WINDOW_TOKENS` | 1_000_000 | `config.rs` |

### Health

| Constant | Value | Location |
|----------|-------|----------|
| `HealthReport::TOOL_SUCCESS_DEGRADED_BPS` | 9_000 | `health.rs` |
| `HealthReport::TOOL_SUCCESS_CRITICAL_BPS` | 8_000 | `health.rs` |

---

## Appendix D: Module Dependency Graph

```
lib.rs
  +-- error.rs     (uses agent.rs for AgentId)
  +-- agent.rs     (standalone)
  +-- tool.rs      (standalone)
  +-- context.rs   (uses agent.rs, session-manager)
  +-- config.rs    (uses error.rs)
  +-- stats.rs     (standalone)
  +-- health.rs    (uses stats.rs)
  +-- builder.rs   (uses config.rs, error.rs)
  +-- runtime.rs   (uses agent.rs, config.rs, error.rs, health.rs, stats.rs)
  +-- executor.rs  (uses agent.rs, config.rs, error.rs, health.rs, stats.rs, tool.rs)
  +-- scheduler.rs (uses agent.rs, config.rs, error.rs, health.rs, stats.rs)
```

---

## Appendix E: Benchmark Descriptions

### agent_lifecycle

Measures the cost of agent lifecycle operations:
- `agent_id_new`: Creating an `AgentId` from a `u64`.
- `agent_state_as_str`: Converting all `AgentState` variants to strings.
- `runtime_health`: Deriving a `HealthReport` from `AgentRuntime`.

### tool_execute

Measures the cost of tool creation and result handling:
- `tool_descriptor_new`: Creating a `ToolDescriptor` with ID and name.
- `tool_result_success`: Creating a successful `ToolResult`.
- `tool_result_failure`: Creating a failed `ToolResult`.

### context_switch

Measures the cost of context creation and token tracking:
- `context_new`: Creating an `AgentContext` with agent and session IDs.
- `context_use_tokens`: Consuming tokens and checking remaining.
- `context_remaining_tokens`: Querying remaining tokens (no mutation).

### schedule

Measures the cost of scheduler operations:
- `scheduler_new`: Creating an `AgentScheduler` with config.
- `scheduler_health`: Deriving health from scheduler stats.
- `agent_id_is_nil`: Checking nil status for agent IDs.

---

## Appendix F: Error Decision Tree

```
Error received
    |
    +-- is_fatal() == true?
    |       |
    |       +-- AgentExists? --> Programming error; fix caller code
    |       +-- ShuttingDown? --> Runtime terminating; abort gracefully
    |
    +-- is_retryable() == true?
    |       |
    |       +-- ToolFailed? --> Back off and retry tool execution
    |       +-- SchedulerFull? --> Back off and retry scheduling
    |
    +-- Neither fatal nor retryable?
            |
            +-- AgentNotFound? --> Check agent ID; agent may have been stopped
```

---

## Appendix G: State Machine Diagram

```
                    create_agent()
                          |
                          v
                      +-------+
          +---------->| Idle  |
          |           +-------+
          |              |
          |              | schedule()
          |              v
          |          +---------+
          |          | Running |
          |          +---------+
          |              |
          |              | tool call / wait
          |              v
          |          +---------+
          |          | Waiting |
          |          +---------+
          |              |
          |              | resume
          |              v
          |          +---------+
          |          | Running |
          |          +---------+
          |
          | stop_agent()
          |              |
          |              v
          |          +---------+
          +--------->| Stopped |  (terminal)
                     +---------+
                    
          tool failure
              |
              v
          +--------+
          | Failed |  (terminal)
          +--------+
```

---

## Appendix H: Configuration Decision Guide

### Choosing a Preset

```
Need agent runtime?
    |
    +-- Development / Testing?
    |       --> Minimal (32 agents, 8 tools, 5s timeout)
    |
    +-- Production with moderate load?
    |       --> Standard (128 agents, 32 tools, 30s timeout)
    |
    +-- Production with heavy load?
    |       --> Full (512 agents, 128 tools, 60s timeout)
    |
    +-- Custom requirements?
            --> AgentRuntimeBuilder::new()
                .max_agents(...)
                .max_tools_per_agent(...)
                .tool_timeout_ms(...)
                .context_window_tokens(...)
                .build()?
```

### Tuning Guidelines

| Field | When to Increase | When to Decrease |
|-------|-----------------|-----------------|
| `max_agents` | More concurrent agents needed | Memory pressure |
| `max_tools_per_agent` | More tool variety needed | Tool registration overhead |
| `tool_timeout_ms` | Tools are slow (network I/O) | Tools are fast (local compute) |
| `context_window_tokens` | Longer conversations needed | Memory pressure, cost control |

---

*End of DESIGN.md*
