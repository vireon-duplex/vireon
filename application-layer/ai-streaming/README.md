# ai-streaming

**AI streaming framework** for the Vireon application layer. Provides token streaming, tool call aggregation, and pipeline management for LLM-based AI workloads.

## Overview

The `ai-streaming` crate is the foundational layer for streaming AI model output (tokens and tool calls) through the Vireon realtime framework. It provides typed identifiers, atomic statistics, health monitoring, and a pipeline facade for managing AI streams.

## Architecture

```
  Token Request --> [AI Stream Pipeline] --> Token Chunks
                        |
                        +- AiStreamId       (stream identity)
                        +- StreamMode       (text/tool/mixed)
                        +- TokenChunk        (streaming unit)
                        +- ToolCall          (tool invocation)
                        +- TokenAggregator   (reassembly)
                        +- AiStreamStats     (observability)
                        +- HealthReport      (health)
```

## Quick Start

```rust
use ai_streaming::{AiStreamConfig, AiStreamPipeline, Preset, StreamMode};

// Use a preset
let cfg = Preset::Chat.to_config();
let pipeline = AiStreamPipeline::new(cfg);

// Or build custom config
let cfg = ai_streaming::AiStreamBuilder::from_preset(Preset::Agent)
    .max_concurrent_streams(64)
    .tool_call_timeout_ms(30_000)
    .build()?;
```

## Key Types

| Type              | Description                              |
|-------------------|------------------------------------------|
| `AiStreamId`      | Unique stream identifier (u64 newtype)   |
| `StreamMode`      | Text, ToolCalls, or Mixed streaming      |
| `TokenChunk`      | Atomic unit of token streaming           |
| `TokenType`       | Text, ToolCall, EndOfStream, Error       |
| `ToolCall`        | Tool invocation with name and arguments  |
| `ToolStatus`      | Pending, Executing, Completed, Failed    |
| `AiStreamConfig`  | Copy config with validation and presets  |
| `AiStreamStats`   | Atomic counters for observability        |
| `HealthReport`    | Derived health (Healthy/Degraded/Critical)|

## Presets

| Preset          | Streams | Tokens  | Chunk | Timeout  | Use Case            |
|-----------------|---------|---------|-------|----------|---------------------|
| `Chat`          | 128     | 4,096   | 64    | 5s       | Chat applications   |
| `CodeAssistant` | 64      | 16,384  | 128   | 15s      | Code completion     |
| `Agent`         | 32      | 32,768  | 256   | 60s      | Autonomous agents   |

## Error Handling

All fallible APIs return `AiStreamError`, a `#[non_exhaustive]` thiserror enum with `is_fatal()` and `is_retryable()` predicates:

```rust
use ai_streaming::{AiStreamError, AiStreamId};

let err = AiStreamError::TokenFailed("buffer full");
assert!(err.is_retryable());
assert!(!err.is_fatal());

let err = AiStreamError::ShuttingDown;
assert!(err.is_fatal());
assert!(!err.is_retryable());
```

| Variant          | Fatal | Retryable |
|------------------|-------|-----------|
| `StreamNotFound` | No    | No        |
| `StreamExists`   | Yes   | No        |
| `TokenFailed`    | No    | Yes       |
| `ToolCallFailed` | No    | Yes       |
| `ShuttingDown`   | Yes   | No        |

## Health Monitoring

Health is derived from stats snapshots using a worst-signal-wins policy:

```rust
use ai_streaming::{AiStreamPipeline, AiStreamConfig};

let pipeline = AiStreamPipeline::new(AiStreamConfig::new());
let health = pipeline.health();

if health.needs_attention() {
    // Alert operator
}
```

Health states: `Healthy`, `Degraded`, `Critical`.

Thresholds are based on tool call success rate:
- Below 90% (9,000 bps): Degraded
- Below 80% (8,000 bps): Critical

## Phase 0 Status

Modules 1-8 are real implementations:
- `error` — `AiStreamError` thiserror enum
- `config` — `AiStreamConfig`, `Preset` (Chat/CodeAssistant/Agent)
- `stream` — `AiStreamId`, `StreamMode`
- `token` — `TokenChunk`, `TokenType`
- `tool` — `ToolCallId`, `ToolCall`, `ToolStatus`
- `stats` — `AiStreamStats` atomics, `StatsSnapshot`
- `health` — `HealthReport`, `HealthState`
- `builder` — `AiStreamBuilder` (fluent)

Modules 9-10 ship as typed skeletons returning `ShuttingDown` errors:
- `pipeline` — `AiStreamPipeline` facade
- `aggregator` — `TokenAggregator` facade

Functional implementations land in Phase 1+.

## Verification

```bash
cargo check
cargo clippy -- -D warnings
cargo fmt --check
cargo test
cargo bench --no-run
```

All gates pass with 113 tests and 4 Criterion bench stubs.

## Dependencies

| Dependency              | Purpose                          |
|-------------------------|----------------------------------|
| `configuration-system`  | Configuration infrastructure     |
| `middleware-pipeline`   | Request/response middleware      |
| `rpc-framework`         | RPC transport (RpcMethodId, etc) |
| `session-manager`       | Session identity                 |
| `thiserror`             | Error derive macro               |
| `tracing`               | Structured logging               |

## Documentation

See `DESIGN.md` for the full 20-section specification covering architecture, stream model, token model, tool call model, pipeline design, aggregation, backpressure, concurrency, failure modes, observability, configuration, health, performance, security, integration, testing, benchmarking, and roadmap.

## License

GNU AFFERO GENERAL PUBLIC LICENSE v3.0
