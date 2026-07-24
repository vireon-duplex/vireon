//! [`ConsumerGroup`] and [`GroupCoordinator`] — the per-group and
//! multi-group state machines.
//!
//! Each `ConsumerGroup` owns:
//!
//! * Its member set, sorted by `ConsumerId` for deterministic
//!   assignment.
//! * Its current partition count (set on creation, can be bumped by
//!   `set_partition_count` — which also triggers a rebalance).
//! * A monotonic `generation: u32`. Bumped on every rebalance.
//! * The current [`Assignment`] and the [`RebalanceStrategy`] that
//!   produced it.
//!
//! Joining or leaving a member triggers a rebalance synchronously.
//! Generation bump + reassignment is atomic from the caller's point
//! of view.
//!
//! ## Heartbeats & staleness
//!
//! Members are expected to heartbeat periodically (the runtime sets
//! the cadence, typically 3s). A member whose heartbeat is older than
//! `max_idle_ns` is evicted; eviction triggers a rebalance. The
//! coordinator runs the eviction sweep on `tick(now_ns)`.

use std::collections::hash_map::Entry;
use std::time::Duration;

use rustc_hash::FxHashMap;

use crate::error::GroupError;
use crate::id::{ConsumerId, GroupId, Partition};
use crate::strategy::{Assignment, RebalanceStrategy, RoundRobin};

/// Default heartbeat timeout — a member that does not heartbeat within
/// this window is evicted. Matches Kafka's default
/// `session.timeout.ms` (3000 ms in modern releases).
pub const DEFAULT_MAX_IDLE_NS: u64 = 3_000_000_000;

/// One consumer's view of its group membership.
#[derive(Clone, Debug)]
pub struct Member {
    /// The consumer id.
    pub id: ConsumerId,
    /// Last heartbeat observed, in nanoseconds since UNIX epoch.
    pub last_heartbeat_ns: u64,
}

/// Immutable snapshot of a group's state. Returned by
/// [`ConsumerGroup::snapshot`] so callers can inspect without holding
/// a lock.
#[derive(Clone, Debug)]
pub struct MembershipSnapshot {
    /// Group id.
    pub group: GroupId,
    /// Monotonic generation counter.
    pub generation: u32,
    /// Members, sorted by id.
    pub members: Vec<Member>,
    /// Partition count for the topic this group consumes.
    pub partitions: u32,
    /// Current assignment, by member.
    pub assignment: Assignment,
}

/// Per-group state machine.
pub struct ConsumerGroup {
    group: GroupId,
    partitions: u32,
    members: FxHashMap<ConsumerId, u64>,
    generation: u32,
    assignment: Assignment,
    strategy: Box<dyn RebalanceStrategy>,
    max_idle_ns: u64,
}

impl ConsumerGroup {
    /// Construct a new group with no members and the given strategy.
    /// `partitions` must be ≥ 1.
    pub fn new(
        group: GroupId,
        partitions: u32,
        strategy: Box<dyn RebalanceStrategy>,
    ) -> Result<Self, GroupError> {
        if partitions == 0 {
            return Err(GroupError::ZeroPartitions(0));
        }
        Ok(Self {
            group,
            partitions,
            members: FxHashMap::default(),
            generation: 0,
            assignment: FxHashMap::default(),
            strategy,
            max_idle_ns: DEFAULT_MAX_IDLE_NS,
        })
    }

    /// Replace the heartbeat timeout. A member whose
    /// `last_heartbeat_ns` is older than `now - max_idle_ns` is
    /// considered stale and evicted on the next `tick`.
    pub fn set_max_idle(&mut self, max_idle_ns: u64) {
        self.max_idle_ns = max_idle_ns;
    }

    /// Add a member. Triggers a rebalance (generation bump + new
    /// assignment). Returns the new generation.
    pub fn join(&mut self, id: ConsumerId, now_ns: u64) -> u32 {
        self.members.insert(id, now_ns);
        self.rebalance();
        self.generation
    }

    /// Remove a member. Triggers a rebalance. Returns the new
    /// generation, or `None` if the member was not in the group.
    pub fn leave(&mut self, id: &ConsumerId) -> Option<u32> {
        if self.members.remove(id).is_some() {
            self.rebalance();
            Some(self.generation)
        } else {
            None
        }
    }

    /// Refresh a member's heartbeat. Returns `Err` if the member is
    /// not in the group OR if `provided_generation` is stale.
    pub fn heartbeat(
        &mut self,
        id: &ConsumerId,
        provided_generation: u32,
        now_ns: u64,
    ) -> Result<u32, GroupError> {
        if provided_generation != self.generation {
            return Err(GroupError::StaleGeneration {
                provided: provided_generation,
                current: self.generation,
            });
        }
        match self.members.get_mut(id) {
            Some(ts) => {
                *ts = now_ns;
                Ok(self.generation)
            }
            None => Err(GroupError::EmptyGroup {
                group: self.group.as_str().to_owned(),
            }),
        }
    }

    /// Sweep stale members and rebalance if any were evicted. Returns
    /// the number of members evicted.
    pub fn tick(&mut self, now_ns: u64) -> usize {
        if self.members.is_empty() {
            return 0;
        }
        let cutoff = now_ns.saturating_sub(self.max_idle_ns);
        let stale: Vec<ConsumerId> = self
            .members
            .iter()
            .filter(|(_, ts)| **ts < cutoff)
            .map(|(id, _)| id.clone())
            .collect();
        let evicted = stale.len();
        for id in &stale {
            self.members.remove(id);
        }
        if evicted > 0 {
            self.rebalance();
        }
        evicted
    }

    /// Change the partition count. Triggers a rebalance.
    pub fn set_partition_count(&mut self, partitions: u32) -> Result<(), GroupError> {
        if partitions == 0 {
            return Err(GroupError::ZeroPartitions(0));
        }
        if partitions != self.partitions {
            self.partitions = partitions;
            self.rebalance();
        }
        Ok(())
    }

    /// Partitions owned by `id` at the current generation, or `None`
    /// if the member is not in the group.
    #[must_use]
    pub fn owned_partitions(&self, id: &ConsumerId) -> Option<&[Partition]> {
        self.assignment.get(id).map(Vec::as_slice).or_else(|| {
            if self.members.contains_key(id) {
                // Member is in the group but received no partitions
                // (only happens when partitions < members). Return
                // an empty slice so callers can distinguish from
                // "not in group".
                Some(&[][..])
            } else {
                None
            }
        })
    }

    /// Current generation.
    #[must_use]
    pub fn generation(&self) -> u32 {
        self.generation
    }

    /// Member count.
    #[must_use]
    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    /// Snapshot for inspection.
    #[must_use]
    pub fn snapshot(&self) -> MembershipSnapshot {
        let mut members: Vec<Member> = self
            .members
            .iter()
            .map(|(id, ts)| Member {
                id: id.clone(),
                last_heartbeat_ns: *ts,
            })
            .collect();
        members.sort_by(|a, b| a.id.cmp(&b.id));
        MembershipSnapshot {
            group: self.group.clone(),
            generation: self.generation,
            members,
            partitions: self.partitions,
            assignment: self.assignment.clone(),
        }
    }

    /// Swap the strategy. Triggers a rebalance so the new strategy's
    /// assignment takes effect immediately.
    pub fn set_strategy(&mut self, strategy: Box<dyn RebalanceStrategy>) {
        self.strategy = strategy;
        self.rebalance();
    }

    fn rebalance(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        let mut members: Vec<ConsumerId> = self.members.keys().cloned().collect();
        members.sort();
        self.assignment = self.strategy.assign(&members, self.partitions);
    }
}

/// Multi-group coordinator. Tracks one [`ConsumerGroup`] per `(topic,
/// group_id)` pair so the same group id can be reused across topics
/// without collision.
pub struct GroupCoordinator {
    groups: FxHashMap<(String, String), ConsumerGroup>,
    /// Strategy factory: produces a fresh strategy per new group so
    /// different groups can use different strategies. Default is
    /// [`RoundRobin`].
    strategy_factory: Box<dyn Fn() -> Box<dyn RebalanceStrategy> + Send + Sync>,
    max_idle_ns: u64,
}

impl std::fmt::Debug for GroupCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupCoordinator")
            .field("groups", &self.groups.len())
            .field("max_idle_ns", &self.max_idle_ns)
            .finish()
    }
}

impl GroupCoordinator {
    /// Construct with the default [`RoundRobin`] strategy.
    #[must_use]
    pub fn new() -> Self {
        Self::with_strategy_factory(|| Box::new(RoundRobin::new()))
    }

    /// Construct with a custom strategy factory. Called once per new
    /// group; existing groups keep their original strategy.
    pub fn with_strategy_factory<F>(factory: F) -> Self
    where
        F: Fn() -> Box<dyn RebalanceStrategy> + Send + Sync + 'static,
    {
        Self {
            groups: FxHashMap::default(),
            strategy_factory: Box::new(factory),
            max_idle_ns: DEFAULT_MAX_IDLE_NS,
        }
    }

    /// Override the default heartbeat timeout for newly-created groups.
    pub fn set_max_idle(&mut self, max_idle_ns: u64) {
        self.max_idle_ns = max_idle_ns;
    }

    /// Join a consumer to a group on `topic`. Creates the group if
    /// this is its first member. Returns the new generation.
    pub fn join(
        &mut self,
        topic: &str,
        group: &GroupId,
        consumer: &ConsumerId,
        partitions: u32,
        now_ns: u64,
    ) -> Result<u32, GroupError> {
        let key = (topic.to_owned(), group.as_str().to_owned());
        let g = match self.groups.entry(key) {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => {
                let mut new_group =
                    ConsumerGroup::new(group.clone(), partitions, (self.strategy_factory)())?;
                new_group.set_max_idle(self.max_idle_ns);
                v.insert(new_group)
            }
        };
        // If the group already existed with a different partition
        // count, update it (and rebalance).
        if g.snapshot().partitions != partitions {
            g.set_partition_count(partitions)?;
        }
        Ok(g.join(consumer.clone(), now_ns))
    }

    /// Remove a consumer from a group. Returns `Some(generation)` if
    /// the consumer was in the group, else `None`.
    pub fn leave(&mut self, topic: &str, group: &GroupId, consumer: &ConsumerId) -> Option<u32> {
        let key = &(topic.to_owned(), group.as_str().to_owned());
        let g = self.groups.get_mut(key)?;
        let result = g.leave(consumer);
        // Drop the group if now empty — fresh consumers re-create it.
        if g.member_count() == 0 {
            self.groups.remove(key);
        }
        result
    }

    /// Refresh a member's heartbeat.
    pub fn heartbeat(
        &mut self,
        topic: &str,
        group: &GroupId,
        consumer: &ConsumerId,
        generation: u32,
        now_ns: u64,
    ) -> Result<u32, GroupError> {
        let key = (topic.to_owned(), group.as_str().to_owned());
        let g = self.groups.get_mut(&key).ok_or(GroupError::EmptyGroup {
            group: group.as_str().to_owned(),
        })?;
        g.heartbeat(consumer, generation, now_ns)
    }

    /// Sweep all groups for stale members. Returns the total number
    /// of members evicted across all groups.
    pub fn tick(&mut self, now_ns: u64) -> usize {
        let mut total = 0;
        let mut empty_keys: Vec<(String, String)> = Vec::new();
        for (key, g) in self.groups.iter_mut() {
            total += g.tick(now_ns);
            if g.member_count() == 0 {
                empty_keys.push(key.clone());
            }
        }
        for k in empty_keys {
            self.groups.remove(&k);
        }
        total
    }

    /// Partitions owned by `consumer` in `group` on `topic`, or `None`
    /// if either the group or the consumer is unknown.
    #[must_use]
    pub fn owned_partitions(
        &self,
        topic: &str,
        group: &GroupId,
        consumer: &ConsumerId,
    ) -> Option<&[Partition]> {
        let key = (topic.to_owned(), group.as_str().to_owned());
        self.groups.get(&key)?.owned_partitions(consumer)
    }

    /// Number of groups currently tracked.
    #[must_use]
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Total members across all groups.
    #[must_use]
    pub fn member_count(&self) -> usize {
        self.groups.values().map(ConsumerGroup::member_count).sum()
    }
}

impl Default for GroupCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

// Convenience: a Duration-based heartbeat setter, since callers often
// have a Duration rather than raw ns.
impl ConsumerGroup {
    /// Same as [`Self::set_max_idle`] but takes a `Duration`.
    pub fn set_max_idle_duration(&mut self, d: Duration) {
        self.set_max_idle(d.as_nanos() as u64);
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::strategy::Range;

    fn cid(s: &str) -> ConsumerId {
        ConsumerId::new(s).unwrap()
    }
    fn gid(s: &str) -> GroupId {
        GroupId::new(s).unwrap()
    }

    // ── ConsumerGroup basics ───────────────────────────────────────

    #[test]
    fn new_group_starts_at_generation_zero() {
        let g = ConsumerGroup::new(gid("g"), 4, Box::new(RoundRobin::new())).unwrap();
        assert_eq!(g.generation(), 0);
        assert_eq!(g.member_count(), 0);
    }

    #[test]
    fn zero_partitions_rejected() {
        assert!(ConsumerGroup::new(gid("g"), 0, Box::new(RoundRobin::new())).is_err());
    }

    #[test]
    fn join_increments_generation_and_assigns_partitions() {
        let mut g = ConsumerGroup::new(gid("g"), 4, Box::new(RoundRobin::new())).unwrap();
        let cur_gen = g.join(cid("c1"), 0);
        assert_eq!(cur_gen, 1);
        assert_eq!(g.member_count(), 1);
        // Single member owns all 4 partitions.
        let owned = g.owned_partitions(&cid("c1")).unwrap();
        assert_eq!(owned.len(), 4);
    }

    #[test]
    fn leave_triggers_rebalance() {
        let mut g = ConsumerGroup::new(gid("g"), 4, Box::new(RoundRobin::new())).unwrap();
        let _ = g.join(cid("c1"), 0);
        let gen2 = g.join(cid("c2"), 0);
        // After both joins, generation is 2.
        assert_eq!(gen2, 2);

        let gen3 = g.leave(&cid("c1")).unwrap();
        assert_eq!(gen3, 3);
        // c2 now owns everything.
        let owned = g.owned_partitions(&cid("c2")).unwrap();
        assert_eq!(owned.len(), 4);
    }

    #[test]
    fn leave_unknown_returns_none() {
        let mut g = ConsumerGroup::new(gid("g"), 4, Box::new(RoundRobin::new())).unwrap();
        assert!(g.leave(&cid("nobody")).is_none());
    }

    #[test]
    fn heartbeat_rejects_stale_generation() {
        let mut g = ConsumerGroup::new(gid("g"), 4, Box::new(RoundRobin::new())).unwrap();
        let cur_gen = g.join(cid("c1"), 0);
        // Try to heartbeat with an old generation.
        assert!(matches!(
            g.heartbeat(&cid("c1"), cur_gen - 1, 100),
            Err(GroupError::StaleGeneration { .. })
        ));
        // Fresh generation works.
        assert_eq!(g.heartbeat(&cid("c1"), cur_gen, 100).unwrap(), cur_gen);
    }

    #[test]
    fn heartbeat_unknown_member_errors() {
        let mut g = ConsumerGroup::new(gid("g"), 4, Box::new(RoundRobin::new())).unwrap();
        let cur_gen = g.join(cid("c1"), 0);
        assert!(matches!(
            g.heartbeat(&cid("imposter"), cur_gen, 100),
            Err(GroupError::EmptyGroup { .. })
        ));
    }

    #[test]
    fn tick_evicts_stale_members() {
        let mut g = ConsumerGroup::new(gid("g"), 4, Box::new(RoundRobin::new())).unwrap();
        g.set_max_idle(1_000_000_000); // 1s
        let _ = g.join(cid("c1"), 0);
        let _ = g.join(cid("c2"), 0);
        // Heartbeat c1 just before the tick, leave c2 stale.
        let _ = g
            .heartbeat(&cid("c1"), g.generation(), 4_500_000_000)
            .unwrap();
        // Tick at 5s — cutoff is 4s. c2 (ts=0) should be evicted; c1 (ts=4.5s) survives.
        let evicted = g.tick(5_000_000_000);
        assert_eq!(evicted, 1);
        assert_eq!(g.member_count(), 1);
    }

    #[test]
    fn set_partition_count_rebalances() {
        let mut g = ConsumerGroup::new(gid("g"), 2, Box::new(RoundRobin::new())).unwrap();
        let _ = g.join(cid("c1"), 0);
        let gen_before = g.generation();
        g.set_partition_count(8).unwrap();
        assert!(g.generation() > gen_before);
        let owned = g.owned_partitions(&cid("c1")).unwrap();
        assert_eq!(owned.len(), 8);
    }

    #[test]
    fn set_strategy_rebalances() {
        let mut g = ConsumerGroup::new(gid("g"), 6, Box::new(RoundRobin::new())).unwrap();
        let _ = g.join(cid("c1"), 0);
        let _ = g.join(cid("c2"), 0);
        let _ = g.join(cid("c3"), 0);
        let gen_before = g.generation();
        g.set_strategy(Box::new(Range::new()));
        assert!(g.generation() > gen_before);
        // Range strategy: 6/3=2 per member, contiguous.
        let owned = g.owned_partitions(&cid("c1")).unwrap();
        // members are sorted; c1 is first.
        assert_eq!(owned.len(), 2);
        assert_eq!(owned[0].get(), 0);
        assert_eq!(owned[1].get(), 1);
    }

    #[test]
    fn owned_partitions_for_member_with_zero_returns_empty_slice() {
        let mut g = ConsumerGroup::new(gid("g"), 2, Box::new(RoundRobin::new())).unwrap();
        let _ = g.join(cid("a"), 0);
        let _ = g.join(cid("b"), 0);
        let _ = g.join(cid("c"), 0);
        // 2 partitions, 3 members → one member gets nothing.
        let total: usize = [cid("a"), cid("b"), cid("c")]
            .iter()
            .map(|c| g.owned_partitions(c).map_or(0, <[Partition]>::len))
            .sum();
        assert_eq!(total, 2, "only 2 partitions distributed");
        // Every member should get Some(_) (maybe empty).
        for c in &[cid("a"), cid("b"), cid("c")] {
            assert!(g.owned_partitions(c).is_some());
        }
    }

    // ── GroupCoordinator ───────────────────────────────────────────

    #[test]
    fn coordinator_join_creates_group() {
        let mut c = GroupCoordinator::new();
        let cur_gen = c.join("t", &gid("g"), &cid("c1"), 4, 0).unwrap();
        assert_eq!(cur_gen, 1);
        assert_eq!(c.group_count(), 1);
        assert_eq!(c.member_count(), 1);
    }

    #[test]
    fn coordinator_leave_drops_empty_group() {
        let mut c = GroupCoordinator::new();
        let _ = c.join("t", &gid("g"), &cid("c1"), 4, 0).unwrap();
        assert_eq!(c.group_count(), 1);
        let _ = c.leave("t", &gid("g"), &cid("c1"));
        assert_eq!(c.group_count(), 0, "empty group should be dropped");
    }

    #[test]
    fn coordinator_reuses_group_across_topics() {
        let mut c = GroupCoordinator::new();
        let _ = c.join("t1", &gid("g"), &cid("c1"), 4, 0).unwrap();
        let _ = c.join("t2", &gid("g"), &cid("c1"), 4, 0).unwrap();
        // Same group id on different topics → two groups.
        assert_eq!(c.group_count(), 2);
    }

    #[test]
    fn coordinator_tick_evicts_globally() {
        let mut c = GroupCoordinator::new();
        c.set_max_idle(1_000_000_000);
        let _ = c.join("t1", &gid("g1"), &cid("c1"), 4, 0).unwrap();
        let _ = c.join("t2", &gid("g2"), &cid("c2"), 4, 0).unwrap();
        let evicted = c.tick(5_000_000_000);
        assert_eq!(evicted, 2);
        assert_eq!(c.member_count(), 0);
        assert_eq!(c.group_count(), 0);
    }

    #[test]
    fn coordinator_owned_partitions_returns_assignment() {
        let mut c = GroupCoordinator::new();
        let _ = c.join("t", &gid("g"), &cid("c1"), 4, 0).unwrap();
        let owned = c.owned_partitions("t", &gid("g"), &cid("c1")).unwrap();
        assert_eq!(owned.len(), 4);
        // Unknown consumer returns None.
        assert!(c.owned_partitions("t", &gid("g"), &cid("nobody")).is_none());
    }
}
