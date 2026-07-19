//! [`RebalanceStrategy`] — pluggable assignment algorithm.
//!
//! A strategy takes the *sorted* list of member ids and the partition
//! count and decides which consumer owns which partition. All
//! strategies produce a full assignment — no partition is left
//! unassigned as long as the member list is non-empty.
//!
//! Two strategies ship here:
//!
//! * [`RoundRobin`] — partition *i* goes to member *(i mod n)*.
//!   Simplest; spreads partitions as evenly as possible.
//! * [`Range`] — partitions are split into *n* contiguous runs, one
//!   per member. Members with ids that sort earlier get the first
//!   runs; this is what Kafka's `RangeAssignor` does and works well
//!   when consumers prefer to keep adjacent partitions together for
//!   batched fetches.

use rustc_hash::FxHashMap;

use crate::id::{ConsumerId, Partition};

/// Output of a strategy run: maps each member to the partitions it
/// now owns.
pub type Assignment = FxHashMap<ConsumerId, Vec<Partition>>;

/// A partition-assignment algorithm. Implementations are pure — they
/// take the member list and partition count, return the assignment.
/// Generation bookkeeping and persistence live in
/// [`crate::ConsumerGroup`].
pub trait RebalanceStrategy: Send + Sync {
    /// Compute the assignment.
    ///
    /// `members` MUST be sorted by `ConsumerId` — strategies rely on
    /// deterministic ordering so two coordinators that observe the
    /// same membership compute the same plan. The coordinator sorts
    /// before calling this.
    fn assign(&self, members: &[ConsumerId], partitions: u32) -> Assignment;
}

/// Round-robin strategy: partition *i* goes to member *(i mod n)*.
#[derive(Debug, Default, Clone, Copy)]
pub struct RoundRobin;

impl RoundRobin {
    /// Construct.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl RebalanceStrategy for RoundRobin {
    fn assign(&self, members: &[ConsumerId], partitions: u32) -> Assignment {
        let mut out: Assignment =
            FxHashMap::with_capacity_and_hasher(members.len(), Default::default());
        // Pre-insert every member so even those that receive zero
        // partitions (only when partitions < members) appear with an
        // empty Vec — the caller can distinguish "in group, owns
        // nothing" from "not in group".
        for m in members {
            out.entry(m.clone()).or_default();
        }
        if members.is_empty() || partitions == 0 {
            return out;
        }
        let n = members.len() as u32;
        for p in 0..partitions {
            let owner = &members[(p % n) as usize];
            out.entry(owner.clone())
                .or_default()
                .push(Partition::new(p));
        }
        out
    }
}

/// Range strategy: partitions are split into contiguous runs.
///
/// Partition *p* goes to member *i* where *i* is determined by the
/// run boundaries. If partitions don't divide evenly, earlier members
/// (in sort order) get one extra partition.
#[derive(Debug, Default, Clone, Copy)]
pub struct Range;

impl Range {
    /// Construct.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl RebalanceStrategy for Range {
    fn assign(&self, members: &[ConsumerId], partitions: u32) -> Assignment {
        let mut out: Assignment =
            FxHashMap::with_capacity_and_hasher(members.len(), Default::default());
        for m in members {
            out.entry(m.clone()).or_default();
        }
        if members.is_empty() || partitions == 0 {
            return out;
        }
        let n = members.len() as u32;
        // Each member gets `base` partitions; the first `rem` members
        // get one extra. Mirrors Kafka's RangeAssignor.
        let base = partitions / n;
        let rem = partitions % n;
        let mut start = 0u32;
        for (i, m) in members.iter().enumerate() {
            let count = base + if (i as u32) < rem { 1 } else { 0 };
            let v = out.entry(m.clone()).or_default();
            for p in start..start + count {
                v.push(Partition::new(p));
            }
            start += count;
        }
        out
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn members(n: usize) -> Vec<ConsumerId> {
        (0..n)
            .map(|i| ConsumerId::new(&format!("c{i}")).unwrap())
            .collect()
    }

    // ── RoundRobin ─────────────────────────────────────────────────

    #[test]
    fn round_robin_distributes_evenly() {
        let m = members(3);
        let a = RoundRobin::new().assign(&m, 6);
        for member in &m {
            let owned = a.get(member).unwrap();
            assert_eq!(owned.len(), 2, "each member should own 2 of 6");
        }
    }

    #[test]
    fn round_robin_handles_more_partitions_than_members() {
        let m = members(2);
        let a = RoundRobin::new().assign(&m, 5);
        assert_eq!(a.get(&m[0]).unwrap().len(), 3); // partitions 0, 2, 4
        assert_eq!(a.get(&m[1]).unwrap().len(), 2); // partitions 1, 3
    }

    #[test]
    fn round_robin_handles_fewer_partitions_than_members() {
        let m = members(5);
        let a = RoundRobin::new().assign(&m, 2);
        // Only members[0] and members[1] get a partition.
        assert_eq!(a.get(&m[0]).unwrap().len(), 1);
        assert_eq!(a.get(&m[1]).unwrap().len(), 1);
        for member in &m[2..] {
            assert!(a.get(member).unwrap().is_empty());
        }
    }

    #[test]
    fn round_robin_empty_members_returns_empty_map() {
        let a = RoundRobin::new().assign(&[], 3);
        assert!(a.is_empty());
    }

    #[test]
    fn round_robin_zero_partitions_assigns_nothing() {
        let m = members(3);
        let a = RoundRobin::new().assign(&m, 0);
        for member in &m {
            assert!(a.get(member).unwrap().is_empty());
        }
    }

    // ── Range ──────────────────────────────────────────────────────

    #[test]
    fn range_distributes_evenly() {
        let m = members(3);
        let a = Range::new().assign(&m, 6);
        for member in &m {
            assert_eq!(a.get(member).unwrap().len(), 2);
        }
    }

    #[test]
    fn range_gives_extra_to_earlier_members() {
        let m = members(3);
        let a = Range::new().assign(&m, 7);
        // 7 / 3 = 2 each, remainder 1 → member[0] gets 3.
        assert_eq!(a.get(&m[0]).unwrap().len(), 3);
        assert_eq!(a.get(&m[1]).unwrap().len(), 2);
        assert_eq!(a.get(&m[2]).unwrap().len(), 2);
    }

    #[test]
    fn range_partitions_are_contiguous_per_member() {
        let m = members(3);
        let a = Range::new().assign(&m, 9);
        // Each member should own contiguous partitions.
        for member in &m {
            let owned = a.get(member).unwrap();
            for i in 1..owned.len() {
                assert_eq!(owned[i].get(), owned[i - 1].get() + 1, "non-contiguous");
            }
        }
    }

    #[test]
    fn range_empty_members_returns_empty_map() {
        let a = Range::new().assign(&[], 3);
        assert!(a.is_empty());
    }
}
