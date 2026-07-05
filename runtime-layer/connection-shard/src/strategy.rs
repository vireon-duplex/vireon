//! [`ShardingStrategy`] — pluggable connection placement (Layer 10).
//!
//! The strategy decides which shard should own a given `stream_id`. In a
//! thread-per-core + RSS architecture the kernel already steers incoming
//! packets to the correct core, so the default [`LocalSharding`] strategy
//! simply returns the current reactor id. Other strategies are useful
//! for explicit placement (e.g. when creating outbound connections) or
//! for load-balancing decisions.

use std::fmt::Debug;

/// A strategy for mapping a `stream_id` to a shard index.
///
/// Implementations must be deterministic and cheap (called on the fast
/// path in some configurations). The trait is `Send + Sync` because
/// strategies are shared (by reference) across reactor threads.
pub trait ShardingStrategy: Send + Sync + Debug {
    /// Returns the shard index that should own `stream_id`.
    ///
    /// `num_shards` is the total number of shards (typically the CPU
    /// count). The return value must be in `[0, num_shards)`.
    fn shard_for_stream(&self, stream_id: u64, num_shards: u32) -> u32;
}

// ════════════════════════════════════════════════════════════════
//  LocalSharding — the RSS default
// ════════════════════════════════════════════════════════════════

/// Always returns the owning reactor's id.
///
/// This is the default strategy for thread-per-core + RSS: the kernel
/// already steers packets to the correct core via the RSS indirection
/// table, so the shard simply claims ownership of any stream that
/// arrives on its core.
#[derive(Clone, Copy, Debug, Default)]
pub struct LocalSharding {
    reactor_id: u32,
}

impl LocalSharding {
    /// Creates a strategy bound to `reactor_id`.
    #[must_use]
    pub const fn new(reactor_id: u32) -> Self {
        Self { reactor_id }
    }
}

impl ShardingStrategy for LocalSharding {
    #[inline]
    fn shard_for_stream(&self, _stream_id: u64, num_shards: u32) -> u32 {
        if num_shards == 0 {
            return 0;
        }
        self.reactor_id.min(num_shards - 1)
    }
}

// ════════════════════════════════════════════════════════════════
//  ModuloSharding — simple round-robin
// ════════════════════════════════════════════════════════════════

/// `stream_id % num_shards`.
///
/// Simple, fast, uniformly distributed if `stream_id` is well-distributed.
/// Not stable under shard-count changes (adding a shard reshuffles
/// nearly all mappings).
#[derive(Clone, Copy, Debug, Default)]
pub struct ModuloSharding;

impl ShardingStrategy for ModuloSharding {
    #[inline]
    fn shard_for_stream(&self, stream_id: u64, num_shards: u32) -> u32 {
        if num_shards == 0 {
            return 0;
        }
        // Folding to avoid bias when stream_id > 2^53.
        let folded = stream_id.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        (folded % u64::from(num_shards)) as u32
    }
}

// ════════════════════════════════════════════════════════════════
//  JumpHashSharding — Google's jump consistent hash
// ════════════════════════════════════════════════════════════════

/// Jump consistent hash (Lamping & Veach 2014).
///
/// Produces a near-uniform distribution with minimal memory (no tables).
/// When `num_shards` increases, only `~1/n` of keys move — much better
/// than modulo. The trade-off: it requires a loop proportional to the
/// shard count in the worst case.
#[derive(Clone, Copy, Debug, Default)]
pub struct JumpHashSharding;

impl JumpHashSharding {
    /// Computes the jump consistent hash.
    #[must_use]
    pub fn hash(mut key: u64, num_buckets: u32) -> u32 {
        if num_buckets == 0 {
            return 0;
        }
        let num = i64::from(num_buckets);
        let mut b: i64 = -1;
        let mut j: i64 = 0;
        while j < num {
            b = j;
            key = key.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1);
            let denom = f64::from((key >> 33) as u32) + 1.0;
            j = (((b + 1) as f64) * (f64::from(1u32 << 31) / denom)) as i64;
        }
        b as u32
    }
}

impl ShardingStrategy for JumpHashSharding {
    #[inline]
    fn shard_for_stream(&self, stream_id: u64, num_shards: u32) -> u32 {
        Self::hash(stream_id, num_shards)
    }
}

// ════════════════════════════════════════════════════════════════
//  RendezvousSharding — Highest Random Weight (HRW)
// ════════════════════════════════════════════════════════════════

/// Rendezvous (HRW) hashing.
///
/// For each candidate shard, computes `hash(stream_id, shard_id)` and
/// picks the highest. This is **stable under shard-count changes**:
/// adding or removing a shard only moves the keys that mapped to the
/// changed shard. The trade-off is O(n) per lookup (n = shard count),
/// which is fine for small n (< 256).
#[derive(Clone, Debug, Default)]
pub struct RendezvousSharding {
    shard_ids: Vec<u32>,
}

impl RendezvousSharding {
    /// Creates a strategy with the given candidate shard ids.
    ///
    /// The shard ids do not need to be contiguous — this is useful for
    /// heterogeneous clusters.
    #[must_use]
    pub fn new(shard_ids: Vec<u32>) -> Self {
        Self { shard_ids }
    }

    /// Creates a strategy with `n` contiguous shard ids `[0, n)`.
    #[must_use]
    pub fn contiguous(n: u32) -> Self {
        Self {
            shard_ids: (0..n).collect(),
        }
    }

    /// Computes the HRW weight for `(key, shard_id)`.
    fn weight(key: u64, shard_id: u32) -> u64 {
        // Mix the key and shard_id into a 64-bit hash.
        let mixed = key
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .wrapping_add(u64::from(shard_id).wrapping_mul(0xD1B5_4A32_D192_ED03));
        // Finalizer from SplitMix64.
        let mut z = mixed;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

impl ShardingStrategy for RendezvousSharding {
    fn shard_for_stream(&self, stream_id: u64, _num_shards: u32) -> u32 {
        if self.shard_ids.is_empty() {
            return 0;
        }
        self.shard_ids
            .iter()
            .copied()
            .map(|sid| (sid, Self::weight(stream_id, sid)))
            .max_by_key(|&(_, w)| w)
            .map_or(0, |(sid, _)| sid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn local_returns_reactor_id() {
        let s = LocalSharding::new(3);
        assert_eq!(s.shard_for_stream(0, 8), 3);
        assert_eq!(s.shard_for_stream(u64::MAX, 8), 3);
    }

    #[test]
    fn local_clamps_to_num_shards() {
        let s = LocalSharding::new(10);
        assert_eq!(s.shard_for_stream(0, 4), 3);
    }

    #[test]
    fn local_zero_shards() {
        let s = LocalSharding::new(0);
        assert_eq!(s.shard_for_stream(0, 0), 0);
    }

    #[test]
    fn modulo_distributes() {
        let s = ModuloSharding;
        let mut counts = [0u32; 8];
        for i in 0..10_000u64 {
            let shard = s.shard_for_stream(i, 8);
            assert!(shard < 8);
            counts[shard as usize] += 1;
        }
        // Each shard should get roughly 1250 ± 200.
        for &c in &counts {
            assert!(c > 1000 && c < 1500, "shard got {c}");
        }
    }

    #[test]
    fn jump_hash_distributes() {
        let s = JumpHashSharding;
        let mut counts = HashMap::<u32, u32>::new();
        for i in 0..10_000u64 {
            let shard = s.shard_for_stream(i, 8);
            assert!(shard < 8);
            *counts.entry(shard).or_insert(0) += 1;
        }
        for &c in counts.values() {
            assert!(c > 1000 && c < 1500, "shard got {c}");
        }
    }

    #[test]
    fn jump_hash_stability_on_add() {
        // When going from 7 → 8 shards, only ~1/8 of keys should move.
        let mut moved = 0u32;
        for i in 0..10_000u64 {
            let before = JumpHashSharding::hash(i, 7);
            let after = JumpHashSharding::hash(i, 8);
            if before != after {
                moved += 1;
            }
        }
        // Expect ~1250 (1/8 of 10000). Allow generous bounds.
        assert!(moved < 2000, "too many keys moved: {moved}");
        assert!(moved > 500, "too few keys moved: {moved}");
    }

    #[test]
    fn rendezvous_contiguous_distributes() {
        let s = RendezvousSharding::contiguous(8);
        let mut counts = [0u32; 8];
        for i in 0..10_000u64 {
            let shard = s.shard_for_stream(i, 8);
            assert!(shard < 8);
            counts[shard as usize] += 1;
        }
        for &c in &counts {
            assert!(c > 900 && c < 1600, "shard got {c}");
        }
    }

    #[test]
    fn rendezvous_stability_on_remove() {
        // When removing one shard from 8 → 7, only the keys that were
        // on the removed shard should move.
        let s8 = RendezvousSharding::contiguous(8);
        let s7 = RendezvousSharding::contiguous(7);
        let mut moved = 0u32;
        for i in 0..10_000u64 {
            let before = s8.shard_for_stream(i, 8);
            let after = s7.shard_for_stream(i, 7);
            if before != after {
                moved += 1;
            }
        }
        // Only keys on shard 7 should move to one of [0,7).
        assert!(moved < 2000, "too many moved: {moved}");
    }

    #[test]
    fn rendezvous_empty_shards() {
        let s = RendezvousSharding::new(vec![]);
        assert_eq!(s.shard_for_stream(0, 8), 0);
    }

    #[test]
    fn modulo_zero_shards() {
        let s = ModuloSharding;
        assert_eq!(s.shard_for_stream(0, 0), 0);
    }

    #[test]
    fn jump_zero_shards() {
        let s = JumpHashSharding;
        assert_eq!(s.shard_for_stream(0, 0), 0);
    }
}
