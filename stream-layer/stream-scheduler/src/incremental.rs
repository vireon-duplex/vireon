//! [`IncrementalScheduler`] — RFC 9218 incremental streams.
//!
//! See `DESIGN.md` Part 4 (RFC 9218) and Part 5 (algorithm).
//!
//! # RFC 9218 incremental
//!
//! An incremental stream benefits from **partial delivery** rather
//! than waiting for a full chunk. The scheduler gives incremental
//! streams a configurable share of each tick's budget (default 25%),
//! cycling through them round-robin within their urgency band.
//!
//! ```text
//! tick_budget = 1000 bytes
//! incremental_share = 25%  → 250 bytes reserved
//!
//! non_incremental streams compete for 750 bytes (strict priority)
//! incremental   streams compete for 250 bytes (round-robin)
//! ```
//!
//! # Phase 0 status
//!
//! Phase 0 ships the share-tracking type. Functional cycle-share
//! selection lands in Phase 2.

/// Default share of each tick reserved for incremental streams.
pub const DEFAULT_INCREMENTAL_SHARE_PCT: u8 = 25;

use profiler::profile;

/// Tracks the incremental-stream budget share within a tick.
#[derive(Clone, Copy, Debug)]
pub struct IncrementalScheduler {
    /// Configured share percentage (0..=100).
    share_pct: u8,
    /// Bytes allotted to incremental streams this tick.
    allotted_bytes: u64,
    /// Bytes consumed by incremental streams this tick.
    consumed_bytes: u64,
}

impl IncrementalScheduler {
    /// Construct a scheduler with the given share percentage.
    #[must_use]
    pub const fn new(share_pct: u8) -> Self {
        Self {
            share_pct,
            allotted_bytes: 0,
            consumed_bytes: 0,
        }
    }

    /// The configured share percentage.
    #[must_use]
    pub const fn share_pct(&self) -> u8 {
        self.share_pct
    }

    /// Allotted bytes for incremental streams this tick.
    #[must_use]
    pub const fn allotted_bytes(&self) -> u64 {
        self.allotted_bytes
    }

    /// Consumed bytes by incremental streams this tick.
    #[must_use]
    pub const fn consumed_bytes(&self) -> u64 {
        self.consumed_bytes
    }

    /// Remaining incremental budget this tick.
    #[must_use]
    pub const fn remaining(&self) -> u64 {
        self.allotted_bytes.saturating_sub(self.consumed_bytes)
    }

    /// Start a new tick: compute the incremental share from the
    /// total tick byte budget.
    #[profile]
    pub fn start_tick(&mut self, total_tick_bytes: u64) {
        self.allotted_bytes = (total_tick_bytes * self.share_pct as u64) / 100;
        self.consumed_bytes = 0;
    }

    /// Attempt to consume `bytes` from the incremental share.
    /// Returns `true` if the consumption succeeded.
    #[profile]
    pub fn consume(&mut self, bytes: u64) -> bool {
        if bytes > self.remaining() {
            return false;
        }
        self.consumed_bytes += bytes;
        true
    }

    /// Whether the incremental share is exhausted this tick.
    #[must_use]
    pub const fn is_exhausted(&self) -> bool {
        self.consumed_bytes >= self.allotted_bytes
    }
}

impl Default for IncrementalScheduler {
    fn default() -> Self {
        Self::new(DEFAULT_INCREMENTAL_SHARE_PCT)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_share_is_25_pct() {
        let s = IncrementalScheduler::default();
        assert_eq!(s.share_pct(), 25);
    }

    #[test]
    fn start_tick_allots_share() {
        let mut s = IncrementalScheduler::new(25);
        s.start_tick(1000);
        assert_eq!(s.allotted_bytes(), 250);
        assert_eq!(s.remaining(), 250);
    }

    #[test]
    fn consume_decrements_remaining() {
        let mut s = IncrementalScheduler::new(50);
        s.start_tick(1000);
        assert!(s.consume(300));
        assert_eq!(s.remaining(), 200);
        assert!(s.consume(200));
        assert!(s.is_exhausted());
        assert!(!s.consume(1));
    }

    #[test]
    fn start_tick_resets_consumed() {
        let mut s = IncrementalScheduler::new(10);
        s.start_tick(1000);
        s.consume(50);
        s.start_tick(2000); // reset
        assert_eq!(s.consumed_bytes(), 0);
        assert_eq!(s.allotted_bytes(), 200);
    }

    #[test]
    fn zero_share_allots_nothing() {
        let mut s = IncrementalScheduler::new(0);
        s.start_tick(1000);
        assert_eq!(s.allotted_bytes(), 0);
        assert!(s.is_exhausted());
    }
}
