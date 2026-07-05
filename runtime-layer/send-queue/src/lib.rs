//! Production-grade send queue for `Vireon`.
//!
//! A thread-local, lock-free transmit queue that sits between the
//! application stream layer and the IO Layer's [`Sender`](crate::submit::SubmissionSink).
//! The public surface is the [`SendQueue`] handle; internally, a `submit`
//! request flows through:
//!
//! ```text
//! SendQueue::submit(buf_index, len, dest, meta)
//!   -> tracking-slot allocator  (free-list, O(1))
//!   -> backpressure gate        (admit if depth < high_watermark)
//!   -> ingress ring             (SPSC, lock-free, cache-aligned)
//!
//! Reactor::flush(sink, now_ns)
//!   -> scheduler drain          (FIFO policy, reorder-capable)
//!   -> batch composer           (accumulate + flush triggers)
//!   -> submission sink          (trait → IO Layer SEND_ZC)
//!   -> in-flight table          (track two-CQE lifecycle)
//!
//! Reactor::on_send_cqe / on_reclaim_cqe
//!   -> in-flight table          (state transitions)
//!   -> tracking-slot allocator  (free on reclaim)
//! ```
//!
//! # Quick start
//!
//! ```no_run
//! use send_queue::{SendQueueBuilder, SchedulePolicyKind, PacketMeta, SubmissionSink, BatchEntry, SendQueueError};
//!
//! // Application provides a SubmissionSink implementation that bridges
//! // to the IO Layer's Sender::send_zc.
//! struct MySink;
//! impl SubmissionSink for MySink {
//!     fn submit_batch(&mut self, entries: &[BatchEntry]) -> Result<usize, SendQueueError> {
//!         // Push SEND_ZC SQEs via io_uring here.
//!         Ok(entries.len())
//!     }
//! }
//!
//! let mut sq = SendQueueBuilder::new()
//!     .capacity(4096)
//!     .policy(SchedulePolicyKind::Fifo)
//!     .build()?;
//!
//! let id = sq.submit(0, 64, "10.0.0.1:8080".parse().unwrap(), PacketMeta::new())?;
//!
//! let mut sink = MySink;
//! sq.flush(&mut sink, 0)?;
//!
//! sq.on_send_cqe(id.raw());
//! sq.on_reclaim_cqe(id.raw());
//! # Ok::<(), SendQueueError>(())
//! ```
//!
//! # Layer map
//!
//! | Layer | Module | Responsibility |
//! |-------|--------|---------------|
//! | 1 | [`error`] | `SendQueueError` enum |
//! | 2 | [`config`] | `SendQueueConfig`, `RetryConfig`, constants |
//! | 3 | [`queue`] | `IngressRing` (SPSC), `PacketSlot`, `SlotState` |
//! | 4 | [`scheduler`] | `SchedulePolicy` trait, `FifoPolicy` |
//! | 5 | [`batch`] | `BatchComposer`, `BatchEntry` |
//! | 6 | [`backpressure`] | `BackpressureGate`, `PressureLevel`, `PressureObserver` |
//! | 7 | [`retry`] | `RetryTracker`, `RetryConfig`, `DropReason` |
//! | 8 | [`lifecycle`] | `InFlightTable`, ID encode/decode |
//! | 9 | [`submit`] | `SubmissionSink` trait (IO Layer boundary) |
//! | 10 | [`stats`] | `StatsCollector`, `StatsSnapshot` |
//! | 12 | [`api`] | `SendQueue` facade, `SendQueueBuilder`, `PacketMeta` |
//!
//! # Design notes
//!
//! - **Thread-local.** Phase 1 uses `&mut self` for all mutation methods.
//!   Cross-thread SPSC is supported by the ring's `Sync` impl; full
//!   cross-thread `&self` submit is Phase 2.
//! - **Zero allocation on the hot path.** All slots, tracking entries, and
//!   batch buffers are pre-allocated at construction.
//! - **Two-CQE correctness.** Every submitted send tracks both `Send` and
//!   `SendBufferReclaim` completions via the in-flight table's generation
//!   counter.
//! - **No io_uring access.** All kernel interaction goes through the
//!   [`SubmissionSink`] trait; the IO Layer provides the implementation.

#![allow(clippy::module_inception)]

pub mod api;
pub mod backpressure;
pub mod batch;
pub mod config;
pub mod error;
pub mod lifecycle;
pub mod queue;
pub mod retry;
pub mod scheduler;
pub mod stats;
pub mod submit;

// ── Re-exports ────────────────────────────────────────────────────────

pub use api::{PacketId, PacketMeta, SendQueue, SendQueueBuilder};
pub use backpressure::{PressureLevel, PressureObserver};
pub use batch::BatchEntry;
pub use config::{DEFAULT_CAPACITY, RetryConfig, SchedulePolicyKind, SendQueueConfig};
pub use error::SendQueueError;
pub use lifecycle::{decode_generation, decode_slot, encode_id};
pub use queue::{PacketSlot, SlotState};
pub use retry::DropReason;
pub use stats::{StatsCollector, StatsSnapshot};
pub use submit::SubmissionSink;
