//! # stream-scheduler
//!
//! Per-tick stream selection for the Vireon QUIC runtime. Implements
//! RFC 9218 prioritization (urgency + incremental), fair queuing,
//! and starvation prevention.
//!
//! # Architecture (one-line)
//!
//! ```text
//! Stream Manager ─► [Stream Scheduler] ─► send-queue / quic-engine
//!                      │
//!                      ├─ ReadyQueue       (8-bucket urgency)
//!                      ├─ BitmapScheduler  (O(1) band lookup)
//!                      ├─ StreamSelector   (policy-driven pick)
//!                      ├─ AgingTracker     (starvation prevention)
//!                      ├─ BatchComposer    (N-stream packet flush)
//!                      ├─ Backpressure     (flow-control integration)
//!                      └─ IncrementalSched (RFC 9218 cycle-share)
//! ```
//!
//! See `DESIGN.md` for the full architecture specification.
//!
//! # Layer map
//!
//! | # | Module | Phase | Responsibility |
//! |---|--------|-------|----------------|
//! | 1 | [`error`]    | 0 ✅ | `SchedulerError` thiserror enum |
//! | 2 | [`config`]   | 0 ✅ | `SchedulerConfig`, `Preset` |
//! | 3 | [`policy`]   | 0 ✅ | `SchedulingPolicy` enum |
//! | 4 | [`budget`]   | 0 ✅ | `SchedulingBudget` (per-tick caps) |
//! | 5 | [`aging`]    | 0 ✅ | `AgingTracker` (starvation prevention) |
//! | 6 | [`fairness`] | 0 ✅ | `FairnessMetrics` (Jain's index) |
//! | 7 | [`stats`]    | 0 ✅ | `SchedulerStats` atomics, `StatsSnapshot` |
//! | 8 | [`health`]   | 0 ✅ | `HealthReport`, `HealthState` |
//! | 9 | [`queue`]         | 1 | `ReadyQueue` (8-bucket urgency) |
//! | 10| [`bitmap`]        | 1 | `BitmapScheduler` (O(1) band lookup) |
//! | 11| [`selector`]      | 1 | `StreamSelector` (policy pick) |
//! | 12| [`batch`]         | 2 | `BatchComposer` (N-stream flush) |
//! | 13| [`backpressure`]  | 2 | `BackpressureTracker` (FC integration) |
//! | 14| [`incremental`]   | 2 | `IncrementalScheduler` (RFC 9218) |
//! | 15| [`event`]         | 1 | `SchedulingEvent` enum |
//! | 16| [`builder`]       | 1 | `SchedulerBuilder` (fluent) |
//! | 17| [`scheduler`]     | 1 | `StreamScheduler` facade |
//!
//! # Phase 0 status
//!
//! Modules 1–8 are real implementations; modules 9–17 ship as
//! typed skeletons with `//!` docs pointing at `DESIGN.md`. The
//! functional queue + selector land in Phase 1.

#![allow(clippy::module_inception)]

pub mod aging;
pub mod backpressure;
pub mod batch;
pub mod bitmap;
pub mod budget;
pub mod builder;
pub mod config;
pub mod error;
pub mod event;
pub mod fairness;
pub mod health;
pub mod incremental;
pub mod policy;
pub mod queue;
pub mod scheduler;
pub mod selector;
pub mod stats;

// ── Re-exports ────────────────────────────────────────────────────

pub use aging::AgingTracker;
pub use budget::SchedulingBudget;
pub use config::{Preset, SchedulerConfig};
pub use error::SchedulerError;
pub use fairness::FairnessMetrics;
pub use health::{HealthReport, HealthState};
pub use policy::SchedulingPolicy;
pub use stats::{SchedulerStats, StatsSnapshot};

// Re-export the priority types from stream-manager so consumers
// don't have to depend on it directly for the common types.
pub use stream_manager::{PriorityUpdate, StreamId, StreamPriority};
