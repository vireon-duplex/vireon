//! [`RegistryGc`] — expired-lease garbage collector.
//!
//! See `DESIGN.md` Part 10 (garbage collection).
//!
//! # Role
//!
//! Leases expire. When they do, the corresponding ownership entry
//! becomes stale: the recorded owner no longer has the right to
//! serve the session. The GC sweeps the ownership table and:
//!
//! 1. Marks expired entries as reclaimable.
//! 2. Notifies the rebalance planner that those sessions need new
//!    owners.
//! 3. Frees the table slot.
//!
//! # Phase 0
//!
//! Skeleton only.

use crate::ownership::OwnershipTable;

/// Expired-lease garbage collector.
///
/// Phase 0 ships only a typed skeleton.
#[derive(Debug, Default)]
pub struct RegistryGc {
    /// Interval between sweeps (ns).
    #[allow(dead_code)]
    sweep_interval_ns: u64,
}

impl RegistryGc {
    /// Construct a GC with the given sweep interval.
    #[must_use]
    pub const fn new(sweep_interval_ns: u64) -> Self {
        Self { sweep_interval_ns }
    }

    /// Sweep the ownership table and reclaim expired entries.
    ///
    /// Returns the number of entries reclaimed.
    ///
    /// # Phase 0
    ///
    /// Always returns 0 — the ownership table is a skeleton.
    #[must_use]
    pub fn collect_expired(&self, _table: &mut OwnershipTable, _now_ns: u64) -> u64 {
        // TODO(phase-1): iterate table, find expired entries,
        // remove them, return count.
        0
    }

    /// Whether the GC is due to run at `now_ns`, given the last
    /// sweep happened at `last_sweep_ns`.
    ///
    /// Pure helper — does not require the table.
    #[must_use]
    pub const fn is_due(&self, last_sweep_ns: u64, now_ns: u64) -> bool {
        now_ns.saturating_sub(last_sweep_ns) >= self.sweep_interval_ns
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_gc_has_interval() {
        let gc = RegistryGc::new(1_000_000_000); // 1 s
        // The field is dead_code-suppressed; just exercise construction.
        let _ = format!("{gc:?}");
    }

    #[test]
    fn collect_expired_returns_zero_in_phase0() {
        let gc = RegistryGc::new(1_000_000_000);
        let mut table = OwnershipTable::new();
        assert_eq!(gc.collect_expired(&mut table, 0), 0);
    }

    #[test]
    fn is_due_when_interval_elapsed() {
        let gc = RegistryGc::new(1_000_000_000); // 1 s
        assert!(gc.is_due(0, 1_000_000_000));
        assert!(gc.is_due(0, 2_000_000_000));
    }

    #[test]
    fn is_not_due_before_interval_elapsed() {
        let gc = RegistryGc::new(1_000_000_000); // 1 s
        assert!(!gc.is_due(0, 999_999_999));
    }

    #[test]
    fn is_due_handles_time_going_backwards() {
        // saturating_sub should prevent overflow.
        let gc = RegistryGc::new(1_000_000_000);
        assert!(!gc.is_due(2_000_000_000, 1_000_000_000));
    }
}
