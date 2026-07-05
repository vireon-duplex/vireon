//! Runtime CQE Processor (Layer 13) — central event dispatcher.
//!
//! See [`DESIGN.md`](../DESIGN.md) for the full architecture specification.
//!
//! ## Quick start
//!
//! ```no_run
//! use cqe_processor::{CqeProcessorBuilder, CompletionDescriptor};
//! use io::completion::Completion;
//! use io::ops::{OpKind, encode};
//!
//! let mut proc = CqeProcessorBuilder::new()
//!     .batch_max_cqes(256)
//!     .reactor_id(0)
//!     .build()?;
//!
//! // Dispatch a synthetic completion without touching the ring.
//! let c = Completion::Timeout { id: 42 };
//! let outcome = proc.dispatch_one(encode(OpKind::Timeout, 42), &c, 0);
//! assert!(outcome.consumed);
//! assert_eq!(outcome.retire_timer, Some(42));
//! # Ok::<(), cqe_processor::CqeError>(())
//! ```
//!
//! ## Module map
//!
//! | Layer | Module | Responsibility |
//! |-------|--------|----------------|
//! | 1 | [`error`]    | `CqeError` thiserror enum |
//! | 2 | [`config`]   | `CqeProcessorConfig`, defaults, validation, `OverflowPolicy` |
//! | 3 | [`user_data`]| OpKind-aware helpers (re-export of `io::ops`) |
//! | 4 | [`completion`]| `CompletionDescriptor` |
//! | 5 | [`cache`]    | `HotState` cache-aligned counters |
//! | 6 | [`dispatcher`]| `DispatchTable`, `Handler` fn-pointer, default handlers |
//! | 7 | [`drain`]    | `CqDrainer` (raw ring read + head advance) |
//! | 8 | [`sendzc`]   | `SendZcTracker` (id → buf_id + debug pairing) |
//! | 9 | [`multishot`]| `MultishotState` (re-arm policy + cooldown) |
//! | 10 | [`recycle`] | `RecyclePipeline` (batched buffer returns) |
//! | 11 | [`lifecycle`] | `Outcome` enum, `SendQueueEvent` |
//! | 12 | [`scheduler`]| `CqeScheduler` (Task Queue bridge accumulator) |
//! | 13 | [`stats`]   | `StatsCollector` (atomic), `StatsSnapshot` (`Copy`) |
//! | 14 | [`processor`] | `CqeProcessor` facade, `TickResult` |
//! | 15 | [`builder`] | `CqeProcessorBuilder` (fluent) |

#![allow(clippy::module_inception)]

pub mod builder;
pub mod cache;
pub mod completion;
pub mod config;
pub mod dispatcher;
pub mod drain;
pub mod error;
pub mod lifecycle;
pub mod multishot;
pub mod processor;
pub mod recycle;
pub mod scheduler;
pub mod sendzc;
pub mod stats;
pub mod user_data;

// Convenience: re-export handlers sub-module from dispatcher for tests
// that want to address them by full path.
pub use dispatcher::handlers;

// ── Re-exports ─────────────────────────────────────────────────────

pub use builder::CqeProcessorBuilder;
pub use completion::CompletionDescriptor;
pub use config::{CqeProcessorConfig, OverflowPolicy};
pub use dispatcher::{DispatchTable, Handler};
pub use drain::CqDrainer;
pub use error::CqeError;
pub use lifecycle::{Outcome, SendQueueEvent};
pub use multishot::{MultishotState, RearmRequest};
pub use processor::{CqeProcessor, TickResult};
pub use recycle::RecyclePipeline;
pub use scheduler::{CqeScheduler, ScheduledTask};
pub use sendzc::SendZcTracker;
pub use stats::{StatsCollector, StatsSnapshot};
