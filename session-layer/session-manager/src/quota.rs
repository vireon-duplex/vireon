//! [`ResourceQuota`] + [`QuotaSnapshot`] — per-session resource caps.
//!
//! See `DESIGN.md` Part 11 (resource management).
//!
//! # Why quotas?
//!
//! Without per-session resource caps, a single misbehaving session
//! can exhaust the connection's memory, open too many streams, or
//! hog bandwidth. The `ResourceQuota` enforces fair sharing:
//!
//! - **Stream count** — caps concurrent streams per session.
//! - **Memory** — caps total buffered bytes (send + recv).
//! - **Bandwidth** — caps bytes/sec (rate limit).
//! - **CPU budget** — caps reactor-tick time per session.
//!
//! All counters are atomic (single-threaded in Vireon, but atomics
//! make the intent explicit and future-proof for multi-threaded
//! profiling).

use std::sync::atomic::{AtomicU64, Ordering};

// ── Defaults ────────────────────────────────────────────────────

/// Default max concurrent streams per session.
pub const DEFAULT_MAX_STREAMS: u32 = 100;
/// Default max memory per session (1 MiB).
pub const DEFAULT_MAX_MEMORY_BYTES: u64 = 1024 * 1024;
/// Default max bandwidth per session (10 Mbps).
pub const DEFAULT_MAX_BANDWIDTH_BPS: u64 = 10 * 1024 * 1024 / 8;
/// Default max CPU budget per tick per session (0.1%).
pub const DEFAULT_MAX_CPU_BUDGET_PCT: u8 = 1;

/// Per-session resource quota. Tracks consumption with atomics.
#[derive(Debug)]
pub struct ResourceQuota {
    // ── Caps ──
    max_streams: u32,
    max_memory_bytes: u64,
    max_bandwidth_bps: u64,
    max_cpu_budget_pct: u8,
    // ── Consumed (atomics) ──
    streams_used: AtomicU64,
    memory_used: AtomicU64,
    bandwidth_used_bps: AtomicU64,
}

impl Default for ResourceQuota {
    fn default() -> Self {
        Self::new(
            DEFAULT_MAX_STREAMS,
            DEFAULT_MAX_MEMORY_BYTES,
            DEFAULT_MAX_BANDWIDTH_BPS,
            DEFAULT_MAX_CPU_BUDGET_PCT,
        )
    }
}

impl ResourceQuota {
    /// Construct a quota with explicit caps.
    #[must_use]
    pub const fn new(
        max_streams: u32,
        max_memory_bytes: u64,
        max_bandwidth_bps: u64,
        max_cpu_budget_pct: u8,
    ) -> Self {
        Self {
            max_streams,
            max_memory_bytes,
            max_bandwidth_bps,
            max_cpu_budget_pct,
            streams_used: AtomicU64::new(0),
            memory_used: AtomicU64::new(0),
            bandwidth_used_bps: AtomicU64::new(0),
        }
    }

    /// Maximum concurrent streams.
    #[must_use]
    pub const fn max_streams(&self) -> u32 {
        self.max_streams
    }

    /// Maximum memory (bytes).
    #[must_use]
    pub const fn max_memory_bytes(&self) -> u64 {
        self.max_memory_bytes
    }

    /// Maximum bandwidth (bytes/sec).
    #[must_use]
    pub const fn max_bandwidth_bps(&self) -> u64 {
        self.max_bandwidth_bps
    }

    /// Maximum CPU budget per tick (percent).
    #[must_use]
    pub const fn max_cpu_budget_pct(&self) -> u8 {
        self.max_cpu_budget_pct
    }

    /// Streams currently in use.
    #[must_use]
    pub fn streams_used(&self) -> u32 {
        self.streams_used.load(Ordering::Relaxed) as u32
    }

    /// Memory currently in use (bytes).
    #[must_use]
    pub fn memory_used(&self) -> u64 {
        self.memory_used.load(Ordering::Relaxed)
    }

    /// Bandwidth currently in use (bytes/sec).
    #[must_use]
    pub fn bandwidth_used_bps(&self) -> u64 {
        self.bandwidth_used_bps.load(Ordering::Relaxed)
    }

    /// Remaining stream slots.
    #[must_use]
    pub fn remaining_streams(&self) -> u32 {
        self.max_streams.saturating_sub(self.streams_used())
    }

    /// Remaining memory (bytes).
    #[must_use]
    pub fn remaining_memory(&self) -> u64 {
        self.max_memory_bytes.saturating_sub(self.memory_used())
    }

    /// Remaining bandwidth (bytes/sec).
    #[must_use]
    pub fn remaining_bandwidth_bps(&self) -> u64 {
        self.max_bandwidth_bps
            .saturating_sub(self.bandwidth_used_bps())
    }

    /// Whether any quota is exceeded.
    #[must_use]
    pub fn is_exceeded(&self) -> bool {
        self.streams_used() >= self.max_streams
            || self.memory_used() >= self.max_memory_bytes
            || self.bandwidth_used_bps() >= self.max_bandwidth_bps
    }

    /// Attempt to acquire one stream slot. Returns `false` if the
    /// stream quota is exhausted.
    pub fn try_acquire_stream(&self) -> bool {
        let current = self.streams_used.load(Ordering::Relaxed);
        if current >= self.max_streams as u64 {
            return false;
        }
        self.streams_used.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Attempt to acquire `bytes` of memory. Returns `false` if the
    /// memory quota would be exceeded. No partial acquisition.
    pub fn try_acquire_memory(&self, bytes: u64) -> bool {
        let current = self.memory_used.load(Ordering::Relaxed);
        let new_val = current.saturating_add(bytes);
        if new_val > self.max_memory_bytes {
            return false;
        }
        self.memory_used.store(new_val, Ordering::Relaxed);
        true
    }

    /// Release one stream slot.
    pub fn release_stream(&self) {
        self.streams_used.fetch_sub(1, Ordering::Relaxed);
    }

    /// Release `bytes` of memory.
    pub fn release_memory(&self, bytes: u64) {
        let current = self.memory_used.load(Ordering::Relaxed);
        let new_val = current.saturating_sub(bytes);
        self.memory_used.store(new_val, Ordering::Relaxed);
    }

    /// Record bandwidth usage (called by the scheduler per tick).
    pub fn record_bandwidth(&self, bytes_this_second: u64) {
        self.bandwidth_used_bps
            .store(bytes_this_second, Ordering::Relaxed);
    }

    /// Snapshot the quota state (per-field, not cross-field
    /// consistent).
    #[must_use]
    pub fn snapshot(&self) -> QuotaSnapshot {
        QuotaSnapshot {
            max_streams: self.max_streams,
            max_memory_bytes: self.max_memory_bytes,
            max_bandwidth_bps: self.max_bandwidth_bps,
            streams_used: self.streams_used(),
            memory_used: self.memory_used(),
            bandwidth_used_bps: self.bandwidth_used_bps(),
        }
    }
}

/// `Copy` view of [`ResourceQuota`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QuotaSnapshot {
    /// Maximum concurrent streams.
    pub max_streams: u32,
    /// Maximum memory (bytes).
    pub max_memory_bytes: u64,
    /// Maximum bandwidth (bytes/sec).
    pub max_bandwidth_bps: u64,
    /// Streams currently in use.
    pub streams_used: u32,
    /// Memory currently in use (bytes).
    pub memory_used: u64,
    /// Bandwidth currently in use (bytes/sec).
    pub bandwidth_used_bps: u64,
}

impl QuotaSnapshot {
    /// Whether the stream quota is exhausted.
    #[must_use]
    pub const fn streams_exhausted(&self) -> bool {
        self.streams_used >= self.max_streams
    }

    /// Whether the memory quota is exhausted.
    #[must_use]
    pub const fn memory_exhausted(&self) -> bool {
        self.memory_used >= self.max_memory_bytes
    }

    /// Memory utilization fraction (0.0..=1.0).
    #[must_use]
    pub fn memory_utilization(&self) -> f64 {
        if self.max_memory_bytes == 0 {
            0.0
        } else {
            self.memory_used as f64 / self.max_memory_bytes as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_not_exceeded() {
        let q = ResourceQuota::default();
        assert!(!q.is_exceeded());
        assert!(q.remaining_streams() > 0);
        assert!(q.remaining_memory() > 0);
    }

    #[test]
    fn acquire_stream_increments() {
        let q = ResourceQuota::new(2, 1024, 1024, 1);
        assert!(q.try_acquire_stream());
        assert!(q.try_acquire_stream());
        assert!(!q.try_acquire_stream()); // exhausted
        assert_eq!(q.streams_used(), 2);
    }

    #[test]
    fn acquire_memory_checks_limit() {
        let q = ResourceQuota::new(100, 1000, 1024, 1);
        assert!(q.try_acquire_memory(600));
        assert!(q.try_acquire_memory(400));
        assert!(!q.try_acquire_memory(1)); // exhausted
        assert_eq!(q.memory_used(), 1000);
    }

    #[test]
    fn release_restores() {
        let q = ResourceQuota::new(1, 100, 1024, 1);
        q.try_acquire_stream();
        q.try_acquire_memory(50);
        q.release_stream();
        q.release_memory(50);
        assert_eq!(q.streams_used(), 0);
        assert_eq!(q.memory_used(), 0);
    }

    #[test]
    fn snapshot_reflects_state() {
        let q = ResourceQuota::new(10, 1000, 1024, 1);
        q.try_acquire_memory(300);
        let snap = q.snapshot();
        assert_eq!(snap.memory_used, 300);
        assert!((snap.memory_utilization() - 0.3).abs() < 1e-9);
    }

    #[test]
    fn exceeded_detection() {
        let q = ResourceQuota::new(0, 0, 0, 1);
        assert!(q.is_exceeded());
    }

    #[test]
    fn record_bandwidth_updates() {
        let q = ResourceQuota::default();
        q.record_bandwidth(500);
        assert_eq!(q.bandwidth_used_bps(), 500);
    }
}
