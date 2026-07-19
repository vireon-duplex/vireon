//! Atomic counters surfaced by [`DeliveryPolicyTable`](crate::DeliveryPolicyTable).
//!
//! All counters are `AtomicU64` with `Ordering::Relaxed` — the table is
//! single-threaded per reactor (one `QuicEngine` per core), so updates
//! don't need release/acquire ordering against each other. Relaxed is
//! enough for cross-thread snapshot reads (metrics scraper), which is
//! the only cross-thread access pattern.
//!
//! State-internal events such as reorder-window eviction and dedup-set
//! slide are not counted here — they are already surfaced via
//! `tracing::warn!`. Adding them would require plumbing `&metrics` into
//! `state.rs`, which is a marginal win for observability.

use std::sync::atomic::{AtomicU64, Ordering};

/// Atomic counter bag, embedded in [`DeliveryPolicyTable`](crate::DeliveryPolicyTable).
#[derive(Debug, Default)]
pub struct DeliveryPolicyMetrics {
    /// Total frames emitted synchronously by `apply` (`Decision::Deliver`).
    pub frames_delivered: AtomicU64,
    /// Total frames held in reorder / pending by `apply` (`Decision::Buffer`).
    pub frames_buffered: AtomicU64,
    /// Total frames silently discarded by `apply` (`Decision::Drop`):
    /// stale, duplicate, or displaced.
    pub frames_dropped: AtomicU64,
    /// Total frames released later by `drain_ready_for_connection`
    /// (gap-fill delivery + LatestOnly slot flush).
    pub drain_emitted: AtomicU64,
    /// Frames dropped at the table level because `apply` hit the
    /// `MAX_TRACKED_STREAMS` cap on the lazy-default path. A non-zero
    /// rate here means the peer is flooding data frames for unknown
    /// streams faster than the application opens them.
    pub table_full_drops: AtomicU64,
    /// Streams lazy-defaulted to `ReliableOrdered` because a data frame
    /// arrived before `StreamOpen`. Persistent non-zero growth means
    /// peers are mis-configured (not sending StreamOpen first).
    pub lazy_default_inserts: AtomicU64,
    /// `bind` calls that upgraded an existing stream's policy. The
    /// upgrade path flushes buffered frames — a high rate suggests
    /// applications are re-binding streams they already opened.
    pub bind_upgrades: AtomicU64,
}

impl DeliveryPolicyMetrics {
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
            frames_delivered: self.frames_delivered.load(Ordering::Relaxed),
            frames_buffered: self.frames_buffered.load(Ordering::Relaxed),
            frames_dropped: self.frames_dropped.load(Ordering::Relaxed),
            drain_emitted: self.drain_emitted.load(Ordering::Relaxed),
            table_full_drops: self.table_full_drops.load(Ordering::Relaxed),
            lazy_default_inserts: self.lazy_default_inserts.load(Ordering::Relaxed),
            bind_upgrades: self.bind_upgrades.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time copy of [`DeliveryPolicyMetrics`]. Cheap to pass
/// across threads; the engine typically exposes this via an `Arc` to a
/// metrics scraper.
#[derive(Debug, Default, Clone, Copy)]
pub struct MetricsSnapshot {
    pub frames_delivered: u64,
    pub frames_buffered: u64,
    pub frames_dropped: u64,
    pub drain_emitted: u64,
    pub table_full_drops: u64,
    pub lazy_default_inserts: u64,
    pub bind_upgrades: u64,
}
