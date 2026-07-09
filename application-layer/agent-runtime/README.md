# agent-runtime

**Agent runtime** for the Vireon application layer. Provides agent lifecycle
management, tool execution, context management with token budget tracking,
and priority-based scheduling for LLM-based AI agent workloads.

## Overview

The agent runtime is the primary execution environment for AI agents in
Vireon. It manages the full agent lifecycle from creation through termination,
with typed errors, atomic observability counters, and tri-state health reports.

Key features:

- Agent lifecycle management (Idle, Running, Waiting, Stopped, Failed)
- Tool execution with configurable timeouts
- Token budget tracking with saturation safety
- Priority-based scheduling (Phase 1+)
- Typed errors (`AgentRuntimeError`) with retryable/fatal predicates
- Atomic stats counters and tri-state health reports
- Configuration presets (Minimal, Standard, Full)

## Layer Map

| #  | Module       | Phase | Responsibility                          |
|----|--------------|-------|-----------------------------------------|
| 1  | `error`      | 0     | `AgentRuntimeError` thiserror enum      |
| 2  | `config`     | 0     | `AgentRuntimeConfig`, `Preset`          |
| 3  | `agent`      | 0     | `AgentId`, `AgentState`                 |
| 4  | `tool`       | 0     | `ToolId`, `ToolDescriptor`, `ToolResult`|
| 5  | `context`    | 0     | `AgentContext` (token budget)           |
| 6  | `stats`      | 0     | `AgentRuntimeStats` atomics, `StatsSnapshot` |
| 7  | `health`     | 0     | `HealthReport`, `HealthState`           |
| 8  | `builder`    | 0     | `AgentRuntimeBuilder` (fluent)          |
| 9  | `runtime`    | 1     | `AgentRuntime` facade                   |
| 10 | `executor`   | 1     | `AgentExecutor` facade                  |
| 11 | `scheduler`  | 1     | `AgentScheduler` facade                 |

Phase 0 modules are fully implemented. Modules 9-11 ship as typed skeletons
and return `Err(AgentRuntimeError::ShuttingDown)` until Phase 1.

## Quick Start

```rust
use agent_runtime::{AgentRuntimeBuilder, AgentRuntime, Preset};

// Build config from a preset with overrides.
let cfg = AgentRuntimeBuilder::from_preset(Preset::Standard)
    .max_agents(200)
    .build()?;

// Create the agent runtime.
let runtime = AgentRuntime::new(cfg);

// Check health.
let report = runtime.health();
assert!(report.is_healthy());
```

## Presets

| Preset    | Max Agents | Max Tools | Tool Timeout | Context Window |
|-----------|------------|-----------|--------------|----------------|
| `Minimal` | 32         | 8         | 5_000 ms     | 4_096          |
| `Standard`| 128        | 32        | 30_000 ms    | 16_384         |
| `Full`    | 512        | 128       | 60_000 ms    | 65_536         |

All presets produce validated configs. Override any field with builder
methods after materializing:

```rust
let cfg = Preset::Minimal.to_config()
    .with_max_agents(50)
    .with_tool_timeout_ms(15_000);
```

## Error Handling

All fallible APIs return `Result<T, AgentRuntimeError>`. Use the built-in
predicates for retry logic:

```rust
match runtime.create_agent() {
    Ok(id) => { /* proceed */ }
    Err(e) if e.is_retryable() => { /* back off and retry */ }
    Err(e) if e.is_fatal() => { /* abort */ }
    Err(e) => { /* handle */ }
}
```

## Observability

```rust
let snapshot = runtime.stats_snapshot();
println!("active agents: {}", snapshot.active_agents());
println!("tool success rate: {} bps", snapshot.tool_success_rate_bps());

let health = runtime.health();
match health.state {
    HealthState::Healthy => { /* nominal */ }
    HealthState::Degraded => { /* investigate */ }
    HealthState::Critical => { /* page on-call */ }
}
```

## Testing

The crate has 115 tests covering all Phase 0 modules. Run them with:

```sh
cargo test
```

Benchmarks are provided via Criterion:

```sh
cargo bench
```

## Design Document

See `DESIGN.md` for the full 14-section specification covering architecture,
data structures, algorithms, failure modes, observability, configuration,
presets, performance, API reference, testing strategy, ops runbook,
security, and future directions.
