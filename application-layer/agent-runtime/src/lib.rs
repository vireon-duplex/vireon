//! # agent-runtime
//!
//! **Agent runtime** for the Vireon application layer. Provides agent
//! lifecycle management, tool execution, context management, and
//! scheduling for LLM-based AI agent workloads.
//!
//! # Architecture (one-line)
//!
//! ```text
//!   Client ──► [Agent Runtime] ──► Agent Lifecycle
//!                   │
//!                   ├─ AgentId          (agent identity)
//!                   ├─ AgentState       (lifecycle state)
//!                   ├─ ToolDescriptor   (tool registration)
//!                   ├─ ToolResult       (execution result)
//!                   ├─ AgentContext     (token budget)
//!                   ├─ AgentRuntimeStats(observability)
//!                   ├─ HealthReport     (health)
//!                   ├─ AgentExecutor    (tool dispatch)
//!                   └─ AgentScheduler   (fair scheduling)
//! ```
//!
//! See `DESIGN.md` for the full 20-section specification.
//!
//! # Layer map
//!
//! | #  | Module       | Phase | Responsibility |
//! |----|--------------|-------|----------------|
//! | 1  | [`error`]    | 0     | `AgentRuntimeError` thiserror enum |
//! | 2  | [`config`]   | 0     | `AgentRuntimeConfig`, `Preset` |
//! | 3  | [`agent`]    | 0     | `AgentId`, `AgentState` |
//! | 4  | [`tool`]     | 0     | `ToolId`, `ToolDescriptor`, `ToolResult` |
//! | 5  | [`context`]  | 0     | `AgentContext` (token budget) |
//! | 6  | [`stats`]    | 0     | `AgentRuntimeStats` atomics, `StatsSnapshot` |
//! | 7  | [`health`]   | 0     | `HealthReport`, `HealthState` |
//! | 8  | [`builder`]  | 0     | `AgentRuntimeBuilder` (fluent) |
//! | 9  | [`runtime`]  | 1     | `AgentRuntime` facade |
//! | 10 | [`executor`] | 1     | `AgentExecutor` facade |
//! | 11 | [`scheduler`]| 1     | `AgentScheduler` facade |
//!
//! # Phase 0 status
//!
//! Modules 1-8 are real implementations. Modules 9-11 ship as typed
//! skeletons. Functional runtime + executor + scheduler land in Phase 1+.

#![allow(clippy::module_inception)]

pub mod agent;
pub mod builder;
pub mod config;
pub mod context;
pub mod error;
pub mod executor;
pub mod health;
pub mod runtime;
pub mod scheduler;
pub mod stats;
pub mod tool;

// ── Re-exports ────────────────────────────────────────────────────

pub use agent::{AgentId, AgentState};
pub use builder::AgentRuntimeBuilder;
pub use config::{AgentRuntimeConfig, Preset};
pub use context::AgentContext;
pub use error::AgentRuntimeError;
pub use executor::AgentExecutor;
pub use health::{HealthReport, HealthState};
pub use runtime::AgentRuntime;
pub use scheduler::AgentScheduler;
pub use stats::{AgentRuntimeStats, StatsSnapshot};
pub use tool::{ToolDescriptor, ToolId, ToolResult};

// Re-export key adjacent-layer types for convenience.
pub use ai_streaming::{AiStreamId, TokenChunk};
pub use event_bus::TopicId;
pub use rpc_framework::{RpcMethodId, StreamHandle};
