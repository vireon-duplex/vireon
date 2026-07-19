//! # consumer-groups
//!
//! Consumer-group membership and partition assignment for the Vireon
//! pub/sub runtime.
//!
//! A consumer group is a named collection of subscribers that share
//! ownership of a topic's partitions. Each partition is assigned to
//! exactly one consumer in the group at any time; when members join
//! or leave, the assignment is rebalanced.
//!
//! ## What this crate provides
//!
//! * [`ConsumerGroup`] — single-group state: members, partition count,
//!   current assignment, monotonic generation counter.
//! * [`GroupCoordinator`] — multi-group registry; routes joins/leaves
//!   and triggers rebalances.
//! * [`RebalanceStrategy`] — pluggable assignment algorithm. Two
//!   strategies ship in this crate: [`RoundRobin`] and [`Range`]. A
//!   `Sticky` strategy is sketched for a follow-up.
//! * Per-consumer offset tracking is *not* in this crate — it lives in
//!   [`wal-store::ConsumerOffsets`]. The group's job is ownership; the
//!   consumer's job is its ack cursor.
//!
//! ## Generations
//!
//! Every successful rebalance bumps a `u32` generation counter (à la
//! Kafka's `group_generation_id`). A consumer that responds with a
//! stale generation is rejected; this prevents a slow consumer from
//! committing offsets for a partition it no longer owns.

#![allow(clippy::module_inception)]

pub mod error;
pub mod group;
pub mod id;
pub mod strategy;

// ── Re-exports ────────────────────────────────────────────────────

pub use error::GroupError;
pub use group::{ConsumerGroup, GroupCoordinator, Member, MembershipSnapshot};
pub use id::{ConsumerId, GroupId, Partition};
pub use strategy::{Assignment, Range, RebalanceStrategy, RoundRobin};
