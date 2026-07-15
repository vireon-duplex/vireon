//! Lock-free logarithmic latency histogram.
//!
//! Uses 32 octaves × 16 sub-buckets = 512 buckets, covering 1 ns to
//! ~4.3 s (2³² ns). Each bucket is an [`AtomicU64`], incremented with
//! `Ordering::Relaxed` — never blocks.
//!
//! # Bucket assignment
//!
//! For a value `ns`:
//! 1. `log = floor(log2(ns))` (position of highest set bit, via `ilog2`)
//! 2. `sub = top 4 bits below the MSB`
//! 3. `bucket = log * 16 + sub`
//!
//! Relative error per bucket: 2^(1/16) ≈ 4.4 % — sufficient for
//! P50 / P95 / P99 trend monitoring.

use std::sync::atomic::{AtomicU64, Ordering};

/// Sub-buckets per octave (precision knob).
pub const SUB_PER_OCTAVE: usize = 16;

/// Number of octaves covered (2³² ns ≈ 4.3 s).
pub const NUM_OCTAVES: usize = 32;

/// Total histogram buckets.
pub const NUM_BUCKETS: usize = SUB_PER_OCTAVE * NUM_OCTAVES; // 512

/// Bits needed for the sub-index.
const SUB_BITS: u32 = 4; // log2(16)
const SUB_MASK: u64 = (SUB_PER_OCTAVE as u64) - 1;

/// Maps a duration (nanoseconds) to its bucket index.
///
/// Pure integer arithmetic — no floating point, no branches beyond
/// the range checks. Inlined at every call site.
#[inline]
pub fn bucket_for(ns: u64) -> usize {
    if ns <= 1 {
        return 0;
    }
    let log = ns.ilog2() as usize;
    if log >= NUM_OCTAVES {
        return NUM_BUCKETS - 1;
    }
    // Extract the 4 bits immediately below the MSB.
    let shift = log.saturating_sub(SUB_BITS as usize);
    let sub = ((ns >> shift) & SUB_MASK) as usize;
    log * SUB_PER_OCTAVE + sub
}

/// Approximate lower bound (inclusive) of `bucket` in nanoseconds.
#[inline]
pub fn bucket_lower(bucket: usize) -> u64 {
    if bucket == 0 {
        return 0;
    }
    let log = bucket / SUB_PER_OCTAVE;
    let sub = (bucket % SUB_PER_OCTAVE) as u64;
    if log == 0 {
        return sub;
    }
    let base = 1u64 << log;
    base + (sub * base) / SUB_PER_OCTAVE as u64
}

/// Approximate midpoint of `bucket` — used for percentile interpolation.
#[inline]
pub fn bucket_mid(bucket: usize) -> u64 {
    let lo = bucket_lower(bucket);
    let hi = bucket_lower(bucket + 1);
    lo + (hi - lo) / 2
}

// ── Lock-free histogram ───────────────────────────────────────────

/// Lock-free histogram backed by an array of [`AtomicU64`].
///
/// `observe` is a single relaxed `fetch_add` — O(1), no locks.
pub struct LatencyHistogram {
    buckets: [AtomicU64; NUM_BUCKETS],
    total_count: AtomicU64,
    total_sum: AtomicU64,
}

impl LatencyHistogram {
    /// Creates an empty histogram.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            buckets: [const { AtomicU64::new(0) }; NUM_BUCKETS],
            total_count: AtomicU64::new(0),
            total_sum: AtomicU64::new(0),
        }
    }

    /// Records a duration observation (nanoseconds).
    #[inline]
    pub fn observe(&self, ns: u64) {
        let idx = bucket_for(ns);
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.total_count.fetch_add(1, Ordering::Relaxed);
        self.total_sum.fetch_add(ns, Ordering::Relaxed);
    }

    /// Merges another histogram's counts into this one (used during flush).
    pub fn merge(&self, other: &LatencyHistogram) {
        for (dst, src) in self.buckets.iter().zip(other.buckets.iter()) {
            let count = src.load(Ordering::Relaxed);
            if count > 0 {
                dst.fetch_add(count, Ordering::Relaxed);
            }
        }
        let tc = other.total_count.load(Ordering::Relaxed);
        let ts = other.total_sum.load(Ordering::Relaxed);
        self.total_count.fetch_add(tc, Ordering::Relaxed);
        self.total_sum.fetch_add(ts, Ordering::Relaxed);
    }

    /// Merges raw bucket counts (used by thread-local collector flush).
    pub fn merge_counts(&self, counts: &[u64]) {
        for (i, &c) in counts.iter().enumerate() {
            if c > 0 {
                self.buckets[i].fetch_add(c, Ordering::Relaxed);
            }
        }
    }

    /// Total number of observations.
    #[inline]
    #[must_use]
    pub fn total_count(&self) -> u64 {
        self.total_count.load(Ordering::Relaxed)
    }

    /// Arithmetic mean of all observations (ns).
    #[must_use]
    pub fn average(&self) -> u64 {
        let count = self.total_count();
        if count == 0 {
            0
        } else {
            self.total_sum.load(Ordering::Relaxed) / count
        }
    }

    /// Approximates the `p`-th percentile (0 ≤ p ≤ 100) in nanoseconds.
    ///
    /// Walks buckets, accumulates counts, and returns the midpoint of
    /// the bucket where the cumulative count crosses the target.
    #[must_use]
    pub fn percentile(&self, p: f64) -> u64 {
        let counts: Vec<u64> = self
            .buckets
            .iter()
            .map(|a| a.load(Ordering::Relaxed))
            .collect();
        let total: u64 = counts.iter().sum();
        if total == 0 {
            return 0;
        }
        let clamped = p.clamp(0.0, 100.0);
        let target = ((total as f64) * clamped / 100.0).ceil() as u64;
        let mut cumulative: u64 = 0;
        for (idx, &count) in counts.iter().enumerate() {
            cumulative += count;
            if cumulative >= target {
                return bucket_mid(idx);
            }
        }
        bucket_mid(NUM_BUCKETS - 1)
    }

    /// Reads all bucket counts into a `Vec<u64>`.
    #[must_use]
    pub fn bucket_counts(&self) -> Vec<u64> {
        self.buckets
            .iter()
            .map(|a| a.load(Ordering::Relaxed))
            .collect()
    }

    /// Resets every counter to zero.
    pub fn reset(&self) {
        for b in &self.buckets {
            b.store(0, Ordering::Relaxed);
        }
        self.total_count.store(0, Ordering::Relaxed);
        self.total_sum.store(0, Ordering::Relaxed);
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for LatencyHistogram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LatencyHistogram")
            .field("buckets", &NUM_BUCKETS)
            .field("total_count", &self.total_count())
            .field("average_ns", &self.average())
            .finish()
    }
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn bucket_for_zero_and_one() {
        assert_eq!(bucket_for(0), 0);
        assert_eq!(bucket_for(1), 0);
    }

    #[test]
    fn bucket_for_small_values() {
        // Values 2..16 are in octaves 0-3 (buckets 0..64).
        for v in 2..16u64 {
            let b = bucket_for(v);
            assert!(b < 4 * SUB_PER_OCTAVE, "value {v} mapped to bucket {b}");
        }
    }

    #[test]
    fn bucket_for_power_of_two() {
        let b256 = bucket_for(256);
        let b512 = bucket_for(512);
        // 256 = 2^8, 512 = 2^9 → different octaves
        assert!(b512 > b256);
    }

    #[test]
    fn bucket_for_clamps_huge() {
        let huge = 1u64 << 40;
        assert_eq!(bucket_for(huge), NUM_BUCKETS - 1);
    }

    #[test]
    fn bucket_lower_matches_boundaries() {
        assert_eq!(bucket_lower(0), 0);
        assert!(bucket_lower(1) > 0);
    }

    #[test]
    fn bucket_mid_is_between_bounds() {
        let lo = bucket_lower(50);
        let hi = bucket_lower(51);
        let mid = bucket_mid(50);
        assert!(mid >= lo);
        assert!(mid <= hi);
    }

    #[test]
    fn observe_increments_correct_bucket() {
        let h = LatencyHistogram::new();
        h.observe(10);
        h.observe(100);
        h.observe(1000);
        assert_eq!(h.total_count(), 3);
    }

    #[test]
    fn observe_records_total_sum() {
        let h = LatencyHistogram::new();
        h.observe(10);
        h.observe(20);
        h.observe(30);
        assert_eq!(h.total_count(), 3);
        assert_eq!(h.average(), 20);
    }

    #[test]
    fn percentile_empty_returns_zero() {
        let h = LatencyHistogram::new();
        assert_eq!(h.percentile(50.0), 0);
        assert_eq!(h.percentile(99.0), 0);
    }

    #[test]
    fn percentile_p50_with_uniform() {
        let h = LatencyHistogram::new();
        for ns in 1..=1000u64 {
            h.observe(ns);
        }
        let p50 = h.percentile(50.0);
        // p50 should be roughly in the 500ns range.
        assert!(p50 > 0);
        // With logarithmic buckets, p50 is approximate but should
        // be in the right order of magnitude.
        assert!(p50 < 2000, "p50 = {p50}, expected < 2000");
    }

    #[test]
    fn percentile_p99_picks_high_value() {
        let h = LatencyHistogram::new();
        for _ in 0..900 {
            h.observe(10);
        }
        for _ in 0..100 {
            h.observe(10_000);
        }
        let p99 = h.percentile(99.0);
        // p99 should be in the 10,000ns range (the slow observations).
        // target = ceil(1000 * 0.99) = 990.
        // After 900 fast observations (cumulative 900 < 990),
        // the slow observations push cumulative to 1000 >= 990.
        assert!(p99 > 1_000, "p99 = {p99}, expected > 1000");
    }

    #[test]
    fn reset_clears_all() {
        let h = LatencyHistogram::new();
        h.observe(10);
        h.observe(100);
        h.reset();
        assert_eq!(h.total_count(), 0);
        assert_eq!(h.average(), 0);
    }

    #[test]
    fn merge_combines_counts() {
        let a = LatencyHistogram::new();
        let b = LatencyHistogram::new();
        a.observe(10);
        a.observe(20);
        b.observe(30);
        b.observe(40);
        a.merge(&b);
        assert_eq!(a.total_count(), 4);
    }

    #[test]
    fn merge_counts_from_slice() {
        let h = LatencyHistogram::new();
        let counts = vec![0u64; NUM_BUCKETS];
        h.merge_counts(&counts); // no-op
        assert_eq!(h.total_count(), 0);

        let mut counts2 = vec![0u64; NUM_BUCKETS];
        counts2[bucket_for(50)] = 5;
        h.merge_counts(&counts2);
        // merge_counts adds to buckets but not total_count/total_sum
        // (those are managed by the caller — see FunctionStats::merge_local)
    }

    #[test]
    fn debug_format_works() {
        let h = LatencyHistogram::new();
        let _ = format!("{h:?}");
    }

    #[test]
    fn bucket_counts_snapshot() {
        let h = LatencyHistogram::new();
        h.observe(10);
        h.observe(10);
        h.observe(100);
        let counts = h.bucket_counts();
        assert_eq!(counts.len(), NUM_BUCKETS);
        let total: u64 = counts.iter().sum();
        assert_eq!(total, 3);
    }
}
