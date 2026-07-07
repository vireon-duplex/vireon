//! [`MigrationPlan`], [`MigrationFlags`] — declarative plan for a
//! single session migration.
//!
//! See `DESIGN.md` Part 2 (migration lifecycle) and Part 8 (deadline
//! & timeout management).
//!
//! # Role
//!
//! A `MigrationPlan` is the immutable contract between the migration
//! initiator (operator, rebalancer, failover controller) and the
//! [`MigrationCoordinator`](crate::coordinator::MigrationCoordinator).
//! It captures the source, target, deadline, and behavioral flags.

use session_manager::SessionId;
use session_registry::{Generation, NodeId, Region};

// ── MigrationFlags ──────────────────────────────────────────────

/// Behavioral flags that modify how a migration is executed.
///
/// All flags default to `false` for maximum safety.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MigrationFlags {
    /// Preserve stream ordering during migration (sequence numbers,
    /// replay buffer). Required for ordered streams.
    pub preserve_ordering: bool,
    /// Preserve QUIC stream state (flow control, stream limits).
    pub preserve_streams: bool,
    /// Use zero-downtime migration (dual-ownership, redirect,
    /// cutover). See `DESIGN.md` Part 7.
    pub zero_downtime: bool,
    /// Force migration even if the source/target are in suspect or
    /// degraded states. Use with caution.
    pub force: bool,
}

impl MigrationFlags {
    /// Construct flags with all booleans set to `false`.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            preserve_ordering: false,
            preserve_streams: false,
            zero_downtime: false,
            force: false,
        }
    }

    /// Construct flags suitable for zero-downtime migration.
    #[must_use]
    pub const fn zero_downtime() -> Self {
        Self {
            preserve_ordering: true,
            preserve_streams: true,
            zero_downtime: true,
            force: false,
        }
    }

    /// Construct flags suitable for forced (drain) migration.
    #[must_use]
    pub const fn forced() -> Self {
        Self {
            preserve_ordering: true,
            preserve_streams: false,
            zero_downtime: false,
            force: true,
        }
    }
}

// ── MigrationPlan ───────────────────────────────────────────────

/// The immutable migration plan for a single session.
///
/// Created by the initiator, consumed by the coordinator. The
/// coordinator reads the plan to determine timeout, source, target,
/// and behavioral constraints.
#[derive(Clone, Debug)]
pub struct MigrationPlan {
    /// The session to migrate.
    pub session_id: SessionId,
    /// The current owning node (source).
    pub source_node: NodeId,
    /// The new owning node (target).
    pub target_node: NodeId,
    /// Absolute deadline in nanoseconds since cluster epoch.
    pub deadline_ns: u64,
    /// When the migration was initiated (ns since cluster epoch).
    pub initiated_ns: u64,
    /// Behavioral flags.
    pub flags: MigrationFlags,
    /// Ownership generation at initiation time.
    pub generation: Generation,
}

impl MigrationPlan {
    /// Construct a new migration plan.
    #[must_use]
    pub const fn new(
        session_id: SessionId,
        source_node: NodeId,
        target_node: NodeId,
        deadline_ns: u64,
        initiated_ns: u64,
        flags: MigrationFlags,
        generation: Generation,
    ) -> Self {
        Self {
            session_id,
            source_node,
            target_node,
            deadline_ns,
            initiated_ns,
            flags,
            generation,
        }
    }

    /// Whether the migration deadline has passed at `now_ns`.
    #[must_use]
    pub const fn is_expired(&self, now_ns: u64) -> bool {
        now_ns >= self.deadline_ns
    }

    /// Remaining nanoseconds before the deadline. Returns 0 if
    /// already expired.
    #[must_use]
    pub const fn remaining_ns(&self, now_ns: u64) -> u64 {
        self.deadline_ns.saturating_sub(now_ns)
    }

    /// Whether the source and target regions differ, indicating a
    /// cross-region migration (higher latency, longer timeout
    /// recommended).
    #[must_use]
    pub const fn is_cross_region(&self, source_region: Region, target_region: Region) -> bool {
        source_region.raw() != target_region.raw()
    }
}

impl PartialEq for MigrationPlan {
    fn eq(&self, other: &Self) -> bool {
        self.session_id == other.session_id
            && self.source_node.raw() == other.source_node.raw()
            && self.target_node.raw() == other.target_node.raw()
            && self.deadline_ns == other.deadline_ns
            && self.initiated_ns == other.initiated_ns
            && self.flags == other.flags
            && self.generation == other.generation
    }
}

impl Eq for MigrationPlan {}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_plan() -> MigrationPlan {
        MigrationPlan::new(
            SessionId::from_pair(1, 1),
            NodeId::new(1),
            NodeId::new(2),
            10_000_000_000,
            0,
            MigrationFlags::default(),
            Generation::INITIAL,
        )
    }

    #[test]
    fn plan_constructs_correctly() {
        let p = sample_plan();
        assert_eq!(p.source_node.raw(), 1);
        assert_eq!(p.target_node.raw(), 2);
        assert_eq!(p.deadline_ns, 10_000_000_000);
        assert_eq!(p.initiated_ns, 0);
    }

    #[test]
    fn is_expired_past_deadline() {
        let p = sample_plan();
        assert!(p.is_expired(10_000_000_000));
        assert!(p.is_expired(11_000_000_000));
        assert!(!p.is_expired(9_999_999_999));
    }

    #[test]
    fn remaining_ns_before_deadline() {
        let p = sample_plan();
        assert_eq!(p.remaining_ns(0), 10_000_000_000);
        assert_eq!(p.remaining_ns(5_000_000_000), 5_000_000_000);
    }

    #[test]
    fn remaining_ns_zero_after_deadline() {
        let p = sample_plan();
        assert_eq!(p.remaining_ns(10_000_000_000), 0);
        assert_eq!(p.remaining_ns(20_000_000_000), 0);
    }

    #[test]
    fn is_cross_region_detects_difference() {
        let p = sample_plan();
        assert!(p.is_cross_region(Region::new(1), Region::new(2)));
        assert!(!p.is_cross_region(Region::new(1), Region::new(1)));
        assert!(!p.is_cross_region(Region::GLOBAL, Region::GLOBAL));
    }

    #[test]
    fn flags_presets() {
        let zd = MigrationFlags::zero_downtime();
        assert!(zd.preserve_ordering);
        assert!(zd.preserve_streams);
        assert!(zd.zero_downtime);
        assert!(!zd.force);

        let forced = MigrationFlags::forced();
        assert!(forced.preserve_ordering);
        assert!(!forced.preserve_streams);
        assert!(!forced.zero_downtime);
        assert!(forced.force);

        let default = MigrationFlags::default();
        assert!(!default.preserve_ordering);
        assert!(!default.zero_downtime);
        assert!(!default.force);
    }
}
