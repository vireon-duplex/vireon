//! Runtime Task Queue (Layer 12+) — reactor-local execution engine.
//!
//! See [`DESIGN.md`](../DESIGN.md) for the full architecture specification.
//!
//! ## Quick start
//!
//! ```no_run
//! use task_queue::{Task, TaskContext, TaskPriority, TaskResult, TaskQueueBuilder};
//!
//! struct Echo { n: u32 }
//! impl Task for Echo {
//!     fn run(&mut self, _ctx: &mut TaskContext) -> TaskResult {
//!         # let _ = self.n;
//!         TaskResult::Done
//!     }
//! }
//!
//! let mut tq = TaskQueueBuilder::new().capacity(1024).build().expect("build");
//! let handle = tq.spawn(Echo { n: 42 }, TaskPriority::Normal).expect("spawn");
//! tq.cancel(&handle);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! ## Module map
//!
//! | Layer | Module | Responsibility |
//! |-------|--------|----------------|
//! | 1 | [`error`]    | `TaskError` thiserror enum |
//! | 2 | [`config`]   | `TaskQueueConfig`, defaults, validation |
//! | 3 | [`id`]       | `TaskId` encode/decode (slot + generation) |
//! | 4 | [`priority`] | `TaskPriority` enum + admission policy |
//! | 5 | [`state`]    | `TaskState` enum + transitions |
//! | 6 | [`metadata`] | `TaskMetadata`, `TaskFlags` |
//! | 7 | [`task`]     | `Task` trait, `TaskResult`, `TaskKind` |
//! | 8 | [`handle`]   | `TaskHandle` (cancellation token) |
//! | 9 | [`context`]  | `TaskContext` (runtime services, opaque) |
//! | 10 | [`pool`]    | `TaskPool` (slab allocator, generation-tracked) |
//! | 11 | [`inbox`]   | `MpscInbox` (bounded cross-thread ring) |
//! | 12 | [`ready`]   | `ReadyQueue` (4-band FIFO, reactor-local) |
//! | 13 | [`cancel`]  | `CancellationRegistry` (bitmap) |
//! | 14 | [`policy`]  | `TaskPolicy` trait + `FifoPolicy` |
//! | 15 | [`scheduler`] | `TaskScheduler` (select + aging + expiry) |
//! | 16 | [`executor`] | `TaskExecutor` (batch run + recycle) |
//! | 17 | [`batch`]   | `TaskBatch` (scratch buffer) |
//! | 18 | [`budget`]  | `Budget` (time + count limits) |
//! | 19 | [`aging`]   | `AgingTracker` (starvation prevention) |
//! | 20 | [`shutdown`]| `ShutdownManager` (phase machine) |
//! | 21 | [`metrics`] | `MetricsCollector` (atomic), `MetricsSnapshot` |
//! | 22 | [`queue`]   | `TaskQueue` facade (public API) |
//! | 23 | [`builder`] | `TaskQueueBuilder` (fluent) |

#![allow(clippy::module_inception)]

pub mod aging;
pub mod batch;
pub mod budget;
pub mod builder;
pub mod cancel;
pub mod config;
pub mod context;
pub mod error;
pub mod executor;
pub mod handle;
pub mod id;
pub mod inbox;
pub mod metadata;
pub mod metrics;
pub mod policy;
pub mod pool;
pub mod priority;
pub mod queue;
pub mod ready;
pub mod scheduler;
pub mod shutdown;
pub mod state;
pub mod task;

// ── Re-exports ─────────────────────────────────────────────────────

pub use builder::TaskQueueBuilder;
pub use config::{DEFAULT_CAPACITY, MAX_CAPACITY, PolicyKind, TaskQueueConfig};
pub use context::TaskContext;
pub use error::TaskError;
pub use executor::ExecuteResult;
pub use handle::TaskHandle;
pub use id::TaskId;
pub use metadata::{TaskFlags, TaskMetadata};
pub use metrics::{MetricsCollector, MetricsSnapshot};
pub use policy::{FifoPolicy, TaskPolicy};
pub mod deferred;
pub mod tests;
pub use priority::TaskPriority;
pub use queue::{ShutdownReport, TaskQueue, TickResult};
pub use state::TaskState;
pub use task::{Task, TaskKind, TaskResult};
