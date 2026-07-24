//! Atomic counters surfaced by [`SendPolicyTable`](crate::SendPolicyTable).
//!
//! All counters are `AtomicU64` with `Ordering::Relaxed` — the table is
//! single-threaded per reactor (one `QuicEngine` per core), so updates
//! don't need release/acquire ordering against each other. Relaxed is
//! enough for cross-thread snapshot reads (metrics scraper), which is
//! the only cross-thread access pattern.

use std::sync::atomic::{AtomicU64, Ordering};

/// Atomic counter bag, embedded in [`SendPolicyTable`](crate::SendPolicyTable).
#[derive(Debug, Default)]
pub struct SendPolicyMetrics {
    /// Total frames successfully appended to a per-stream queue by
    /// `enqueue` (`EnqueueDecision::Queue`).
    pub frames_queued: AtomicU64,
    /// Total frames that replaced an older pending payload
    /// (`EnqueueDecision::Replace`). Only `LatestOnly` produces this.
    pub frames_replaced: AtomicU64,
    /// Total frames silently discarded by `enqueue`
    /// (`EnqueueDecision::Drop`): stale, duplicate, or displaced.
    pub frames_dropped: AtomicU64,
    /// Total frames emitted by `dequeue`. A non-zero rate here means the
    /// scheduler is actively draining the table.
    pub frames_dequeued: AtomicU64,
    /// Streams lazy-defaulted to `ReliableOrdered` because `enqueue` was
    /// called before `StreamOpen`. Persistent non-zero growth means
    /// callers are mis-configured (not binding streams first).
    pub lazy_default_inserts: AtomicU64,
    /// `bind` calls that upgraded an existing stream's policy. The
    /// upgrade path flushes queued frames — a high rate suggests callers
    /// are re-binding streams they already opened.
    pub bind_upgrades: AtomicU64,
}

impl SendPolicyMetrics {
    /// Construct a fresh counter bag with everything zeroed.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Take a point-in-time snapshot. Relaxed loads — counters are
    /// monotonic and we don't need a consistent cross-field cut.
    #[must_use]
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            frames_queued: self.frames_queued.load(Ordering::Relaxed),
            frames_replaced: self.frames_replaced.load(Ordering::Relaxed),
            frames_dropped: self.frames_dropped.load(Ordering::Relaxed),
            frames_dequeued: self.frames_dequeued.load(Ordering::Relaxed),
            lazy_default_inserts: self.lazy_default_inserts.load(Ordering::Relaxed),
            bind_upgrades: self.bind_upgrades.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time copy of [`SendPolicyMetrics`]. Cheap to pass across
/// threads; the engine typically exposes this via an `Arc` to a metrics
/// scraper.
#[derive(Debug, Default, Clone, Copy)]
pub struct MetricsSnapshot {
    pub frames_queued: u64,
    pub frames_replaced: u64,
    pub frames_dropped: u64,
    pub frames_dequeued: u64,
    pub lazy_default_inserts: u64,
    pub bind_upgrades: u64,
}
