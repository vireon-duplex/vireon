//! # ai-streaming
//!
//! **AI streaming framework** for the Vireon application layer. Provides
//! token streaming, tool call aggregation, and pipeline management for
//! LLM-based AI workloads.
//!
//! # Architecture (one-line)
//!
//! ```text
//!   Token Request ──► [AI Stream Pipeline] ──► Token Chunks
//!                          │
//!                          ├─ AiStreamId       (stream identity)
//!                          ├─ StreamMode       (text/tool/mixed)
//!                          ├─ TokenChunk        (streaming unit)
//!                          ├─ ToolCall          (tool invocation)
//!                          ├─ TokenAggregator   (reassembly)
//!                          ├─ AiStreamStats     (observability)
//!                          └─ HealthReport      (health)
//! ```
//!
//! See `DESIGN.md` for the full 20-section specification.
//!
//! # Layer map
//!
//! | #  | Module         | Phase | Responsibility |
//! |----|----------------|-------|----------------|
//! | 1  | [`error`]      | 0 ✅  | `AiStreamError` thiserror enum |
//! | 2  | [`config`]     | 0 ✅  | `AiStreamConfig`, `Preset` |
//! | 3  | [`stream`]     | 0 ✅  | `AiStreamId`, `StreamMode` |
//! | 4  | [`token`]      | 0 ✅  | `TokenChunk`, `TokenType` |
//! | 5  | [`tool`]       | 0 ✅  | `ToolCallId`, `ToolCall`, `ToolStatus` |
//! | 6  | [`stats`]      | 0 ✅  | `AiStreamStats` atomics, `StatsSnapshot` |
//! | 7  | [`health`]     | 0 ✅  | `HealthReport`, `HealthState` |
//! | 8  | [`builder`]    | 0 ✅  | `AiStreamBuilder` (fluent) |
//! | 9  | [`pipeline`]   | 1    | `AiStreamPipeline` facade |
//! | 10 | [`aggregator`] | 1    | `TokenAggregator` facade |
//!
//! # Phase 0 status
//!
//! Modules 1–8 are real implementations. Modules 9–10 ship as typed
//! skeletons. Functional pipeline + aggregator land in Phase 1+.

#![allow(clippy::module_inception)]

pub mod aggregator;
pub mod builder;
pub mod config;
pub mod error;
pub mod health;
pub mod pipeline;
pub mod stats;
pub mod stream;
pub mod token;
pub mod tool;

// ── Re-exports ────────────────────────────────────────────────────

pub use aggregator::TokenAggregator;
pub use builder::AiStreamBuilder;
pub use config::{AiStreamConfig, Preset};
pub use error::AiStreamError;
pub use health::{HealthReport, HealthState};
pub use pipeline::AiStreamPipeline;
pub use stats::{AiStreamStats, StatsSnapshot};
pub use stream::{AiStreamId, StreamMode};
pub use token::{TokenChunk, TokenType};
pub use tool::{ToolCall, ToolCallId, ToolStatus};

// Re-export key adjacent-layer types for convenience.
pub use rpc_framework::{RpcMethodId, StreamHandle};
