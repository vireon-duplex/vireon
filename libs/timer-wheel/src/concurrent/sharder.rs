//! Shard router (Layer 6).
//!
//! [`Sharder`] maps a `task_id` to a [`ShardId`] via Fibonacci hashing — a
//! multiply by the golden ratio then reduce modulo the shard count. This
//! distributes sequential identifiers evenly across shards without the cost
//! of a general-purpose hash.

use std::num::NonZeroU32;

use crate::error::ShardId;

/// 2^64 / phi, rounded — the standard Fibonacci-hashing multiplier.
const GOLDEN_RATIO: u64 = 0x9E3779B9_7F4A7C15;

/// Routes `task_id` values to shards.
#[derive(Clone, Copy, Debug)]
pub struct Sharder {
    shards: NonZeroU32,
}

impl Sharder {
    /// Creates a sharder for `shards` buckets.
    #[must_use]
    pub fn new(shards: NonZeroU32) -> Self {
        Self { shards }
    }

    /// Number of shards this router targets.
    #[inline]
    #[must_use]
    pub fn shards(&self) -> NonZeroU32 {
        self.shards
    }

    /// Returns the shard responsible for `task_id`.
    ///
    /// The result is `(task_id * GOLDEN_RATIO) mod shards`, which spreads
    /// sequential or clustered identifiers evenly.
    #[inline]
    #[must_use]
    pub fn route(&self, task_id: u64) -> ShardId {
        let mixed = task_id.wrapping_mul(GOLDEN_RATIO);
        // `mixed % shards` distributes uniformly when shards is not a power of
        // two; for power-of-two shard counts the low bits are equally mixed.
        (mixed % u64::from(self.shards.get())) as ShardId
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_returns_valid_shard() {
        let s = Sharder::new(NonZeroU32::new(4).expect("nz"));
        for id in 0..1_000u64 {
            let shard = s.route(id);
            assert!(shard < 4, "shard {shard} out of range for id {id}");
        }
    }

    #[test]
    fn route_covers_every_shard() {
        let n = 16u32;
        let s = Sharder::new(NonZeroU32::new(n).expect("nz"));
        let mut seen = vec![false; n as usize];
        for id in 0..100_000u64 {
            seen[s.route(id) as usize] = true;
        }
        assert!(seen.iter().all(|&b| b), "some shard never selected");
    }

    #[test]
    fn distribution_is_reasonably_even() {
        let n = 8u32;
        let s = Sharder::new(NonZeroU32::new(n).expect("nz"));
        let mut counts = vec![0u64; n as usize];
        const SAMPLES: u64 = 80_000;
        for id in 0..SAMPLES {
            counts[s.route(id) as usize] += 1;
        }
        let expected = SAMPLES / u64::from(n);
        // allow ±20% deviation per shard
        for &c in &counts {
            let dev = (c as i64 - expected as i64).unsigned_abs();
            assert!(
                dev * 5 < expected,
                "shard count {c} too far from {expected}"
            );
        }
    }

    #[test]
    fn single_shard_routes_everything_to_zero() {
        let s = Sharder::new(NonZeroU32::new(1).expect("nz"));
        assert_eq!(s.route(0), 0);
        assert_eq!(s.route(u64::MAX), 0);
    }

    #[test]
    fn sequential_ids_spread_across_shards() {
        let s = Sharder::new(NonZeroU32::new(4).expect("nz"));
        let first_shard = s.route(0);
        let mut spread = false;
        for id in 1..16u64 {
            if s.route(id) != first_shard {
                spread = true;
                break;
            }
        }
        assert!(spread, "sequential ids all landed on one shard");
    }
}
