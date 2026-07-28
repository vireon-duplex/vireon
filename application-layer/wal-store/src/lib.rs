//! # wal-store
//!
//! Append-only write-ahead log per topic, with offset-based replay and
//! per-consumer ack tracking.
//!
//! ## Layering
//!
//! Lives at the same level as `pubsub-engine` — it does not import the
//! engine. The engine's `handle_publish` path is expected to call
//! [`WalStore::append`] for every accepted message so that consumers
//! can replay history after a disconnect, and so the runtime can
//! recover in-flight data after a crash.
//!
//! ## Storage backends
//!
//! * [`MemoryStore`] — fully in-process, used by tests, dev, and
//!   ephemeral workloads. The whole log lives in a `Vec<WalEntry>` per
//!   topic.
//! * [`FileStore`] — real persistence to disk. Each topic gets a
//!   directory under the configured root; the active segment is opened
//!   read/write and rotated when it exceeds the configured size cap.
//!   Older segments are deleted by the retention sweeper.
//!
//! Both backends implement the [`WalStore`] trait so callers can swap
//! them behind a single API.
//!
//! ## Offset semantics
//!
//! Each successful [`WalStore::append`] returns the [`Offset`] of the
//! appended entry. Offsets are:
//!
//! * Monotonic per topic.
//! * Contiguous — `append` returns `Offset(N+1)` when the previous head
//!   was `Offset(N)`.
//! * Comparable — `offset_a < offset_b` means `a` was appended first.
//!
//! [`Offset::OLDEST`] is the offset of the oldest entry currently
//! retained. After `truncate` it advances forward; new consumers can
//! start replay from there.
//!
//! ## Per-consumer ack tracking
//!
//! [`ConsumerOffsets`] is a small in-memory map from `consumer_id` to
//! the highest offset that consumer has acknowledged. Publishers don't
//! consult it; consumers use it to remember where to resume after a
//! disconnect. At-most-once and at-least-once delivery semantics are
//! the caller's responsibility — this crate only stores and reports.

#![allow(clippy::module_inception)]

pub mod consumer;
pub mod entry;
pub mod error;
pub mod file_store;
pub mod memory;
pub mod offset;
pub mod retention;
pub mod store;

// ── Re-exports ────────────────────────────────────────────────────

pub use consumer::ConsumerOffsets;
pub use entry::{WalEntry, WalRecord};
pub use error::WalError;
pub use file_store::{FileStore, SyncPolicy};
pub use memory::MemoryStore;
pub use offset::Offset;
pub use retention::{RetentionPolicy, RetentionStats};
pub use store::WalStore;
