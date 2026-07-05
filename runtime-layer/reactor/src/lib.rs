//! # reactor
//!
//! Thread-per-core reactor orchestrator for `Vireon`.
//!
//! The [`Reactor`] owns one CPU core, one `io_uring` ring, and all
//! runtime-layer primitives (CQE processor, timer wheel, buffer pool,
//! connection shard, send queue, task queue). Each call to
//! [`EventLoop::run_one_tick`] drives every subsystem exactly once
//! in a fixed, latency-optimal order.
//!
//! # Architecture (one-line)
//!
//! ```text
//! park_on_ring → cqe_proc.drain_cq → timer_wheel.drain_expired
//!   → send_queue.flush → task_queue.tick → shard.drain_closed
//!   → driver.on_tick_complete → check_shutdown
//! ```
//!
//! See `DESIGN.md` §3 for the canonical loop ordering and §13 for
//! the io_uring opcode inventory.
//!
//! # Design choices
//!
//! - **Thread-per-core, shared-nothing.** No `Arc<Mutex>`, no work
//!   stealing. One Reactor per CPU core, pinned via
//!   `sched_setaffinity`. Matches Pingora / Seastar / Glommio.
//! - **Direct callback driver.** The protocol layer (e.g. the
//!   future `quic-engine`) implements [`ReactorDriver`]; the reactor
//!   calls it directly — no queue hop on the hot path.
//! - **eventfd cross-thread wake.** Other threads write 8 bytes to
//!   an eventfd registered with our ring; we wake on the next CQE.
//!   Pure io_uring, no `mio` on the wake path.
//! - **Provided buffer rings** for zero-copy RX, **SEND_ZC** for
//!   zero-copy TX with two-CQE lifecycle.
//! - **Deterministic tail latency.** Every tick is bounded by
//!   [`TickBudget`]; overruns are observable in
//!   [`StatsSnapshot`](stats::StatsSnapshot).
//!
//! # Layer map
//!
//! | # | Module | Responsibility |
//! |---|--------|----------------|
//! | 1 | [`error`]   | `ReactorError` thiserror enum |
//! | 2 | [`config`]  | `ReactorConfig`, `ReactorPreset`, validation |
//! | 3 | [`tick`]    | `TickResult`, `TickBudget` |
//! | 4 | [`budget`]  | per-subsystem time-slice budget |
//! | 5 | [`idle`]    | `IdleStrategy` (Block / Spin / Hybrid) |
//! | 6 | [`pin`]     | `CpuPinning` (`sched_setaffinity` + NUMA stub) |
//! | 7 | [`stats`]   | `ReactorStats` atomics, `StatsSnapshot` |
//! | 8 | [`health`]  | `HealthReport`, `HealthState` |
//! | 9 | [`driver`]  | `ReactorDriver` callback trait, `Event` enum |
//! | 10 | [`ring`]    | `RingOwner` (wraps `io::IoUringCtx`) |
//! | 11 | [`submission`] | `SubmissionSink` bridge to the ring |
//! | 12 | [`wake`]    | `EventfdWake` cross-thread wake |
//! | 13 | [`buf_ring`] | `ProvidedBufferRing` setup |
//! | 14 | [`shutdown`] | `ShutdownCoordinator` |
//! | 15 | [`reactor`] | `Reactor` facade (owns all primitives) |
//! | 16 | [`event_loop`] | `EventLoop::run_one_tick` |
//! | 17 | [`builder`] | `ReactorBuilder` (fluent + `from_preset`) |
//!
//! # Phase 0 status
//!
//! Modules 1–8 are real implementations; modules 9–17 ship as
//! typed skeletons with `//!` docs pointing at `DESIGN.md`. The
//! functional event loop lands in Phase 1.
//!
//! [`EventLoop::run_one_tick`]: event_loop::EventLoop::run_one_tick

#![allow(clippy::module_inception)]

pub mod budget;
pub mod buf_ring;
pub mod builder;
pub mod config;
pub mod driver;
pub mod error;
pub mod event_loop;
pub mod health;
pub mod idle;
pub mod pin;
pub mod reactor;
pub mod ring;
pub mod shutdown;
pub mod stats;
pub mod submission;
pub mod tick;
pub mod wake;

// ── Re-exports ────────────────────────────────────────────────────

pub use builder::ReactorBuilder;
pub use config::{ReactorConfig, ReactorPreset};
pub use driver::ReactorDriver;
pub use error::ReactorError;
pub use health::{HealthReport, HealthState};
pub use idle::IdleStrategy;
pub use pin::CpuPinning;
pub use reactor::Reactor;
pub use stats::{ReactorStats, StatsSnapshot};
pub use tick::{TickBudget, TickResult};
