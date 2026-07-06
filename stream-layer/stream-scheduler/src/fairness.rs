//! [`FairnessMetrics`] — Jain's fairness index over recent ticks.
//!
//! See `DESIGN.md` Part 6 (fairness) and Part 15 (observability).
//!
//! # Jain's fairness index
//!
//! Given `n` streams each receiving `x_i` bytes over the window:
//!
//! ```text
//! J = (Σ x_i)² / (n · Σ x_i²)
//! ```
//!
//! - `J = 1` → perfectly fair (all streams got equal bytes).
//! - `J → 1/n` → maximally unfair (one stream got everything).
//!
//! The index is computed over a sliding window of the last `N`
//! ticks. We track per-stream byte totals and recompute on demand.
//!
//! # Min/max ratio
//!
//! A cheaper liveness signal: the ratio of the smallest to the
//! largest share. `1.0` = perfectly fair; `0.0` = at least one
//! stream got nothing.

use std::collections::HashMap;

/// Default window size (number of recent streams tracked).
pub const DEFAULT_WINDOW_SIZE: usize = 64;
/// Minimum number of samples before fairness is reported.
pub const MIN_SAMPLES_FOR_REPORT: usize = 2;

/// Per-stream fairness tracking. Records bytes scheduled per stream
/// and computes Jain's index + min/max ratio.
#[derive(Clone, Debug)]
pub struct FairnessMetrics {
    window_size: usize,
    /// `stream_id.raw() → bytes scheduled in the window`.
    bytes_per_stream: HashMap<u64, u64>,
}

impl FairnessMetrics {
    /// Construct a tracker with the given window size.
    #[must_use]
    pub fn new(window_size: usize) -> Self {
        Self {
            window_size,
            bytes_per_stream: HashMap::with_capacity(window_size),
        }
    }

    /// Construct with default window size.
    #[must_use]
    pub fn default_metrics() -> Self {
        Self::new(DEFAULT_WINDOW_SIZE)
    }

    /// The configured window size.
    #[must_use]
    pub const fn window_size(&self) -> usize {
        self.window_size
    }

    /// Number of distinct streams tracked in the current window.
    #[must_use]
    pub fn tracked_streams(&self) -> usize {
        self.bytes_per_stream.len()
    }

    /// Record `bytes_sent` for the given stream.
    pub fn record(&mut self, stream_id: stream_manager::StreamId, bytes_sent: u64) {
        let key = stream_id.raw();
        if let Some(v) = self.bytes_per_stream.get_mut(&key) {
            *v = v.saturating_add(bytes_sent);
            return;
        }
        // New entry — evict if window full.
        if self.bytes_per_stream.len() >= self.window_size {
            self.evict_smallest();
        }
        self.bytes_per_stream.insert(key, bytes_sent);
    }

    /// Jain's fairness index over the current window. Returns `1.0`
    /// when there are fewer than [`MIN_SAMPLES_FOR_REPORT`] samples.
    #[must_use]
    pub fn jain_index(&self) -> f64 {
        if self.bytes_per_stream.len() < MIN_SAMPLES_FOR_REPORT {
            return 1.0;
        }
        let mut sum: u64 = 0;
        let mut sum_sq: u64 = 0;
        let n = self.bytes_per_stream.len() as f64;
        for &x in self.bytes_per_stream.values() {
            sum = sum.saturating_add(x);
            sum_sq = sum_sq.saturating_add(x.saturating_mul(x));
        }
        if sum == 0 || sum_sq == 0 {
            return 1.0;
        }
        let sum_f = sum as f64;
        let sum_sq_f = sum_sq as f64;
        (sum_f * sum_f) / (n * sum_sq_f)
    }

    /// Min/max ratio (smallest share / largest share). Returns `0.0`
    /// when there are no samples and `1.0` when only one sample.
    #[must_use]
    pub fn min_max_ratio(&self) -> f64 {
        let mut iter = self.bytes_per_stream.values().copied();
        let Some(first) = iter.next() else {
            return 0.0;
        };
        let mut min = first;
        let mut max = first;
        for x in iter {
            if x < min {
                min = x;
            }
            if x > max {
                max = x;
            }
        }
        if max == 0 {
            return 0.0;
        }
        min as f64 / max as f64
    }

    /// Clear all recorded bytes (called on window rollover).
    pub fn reset(&mut self) {
        self.bytes_per_stream.clear();
    }

    fn evict_smallest(&mut self) {
        let min_key = self
            .bytes_per_stream
            .iter()
            .min_by_key(|(_, v)| *v)
            .map(|(k, _)| *k);
        if let Some(key) = min_key {
            self.bytes_per_stream.remove(&key);
        }
    }
}

impl Default for FairnessMetrics {
    fn default() -> Self {
        Self::default_metrics()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stream_manager::StreamId;

    #[test]
    fn empty_reports_one() {
        let m = FairnessMetrics::default();
        assert_eq!(m.jain_index(), 1.0);
    }

    #[test]
    fn single_sample_reports_one() {
        let mut m = FairnessMetrics::default();
        m.record(StreamId::new_unchecked(0), 1000);
        assert_eq!(m.jain_index(), 1.0);
    }

    #[test]
    fn equal_shares_are_perfectly_fair() {
        let mut m = FairnessMetrics::default();
        m.record(StreamId::new_unchecked(0), 1000);
        m.record(StreamId::new_unchecked(4), 1000);
        m.record(StreamId::new_unchecked(8), 1000);
        let j = m.jain_index();
        assert!((j - 1.0).abs() < 1e-9, "expected 1.0 got {j}");
    }

    #[test]
    fn unequal_shares_reduce_index() {
        let mut m = FairnessMetrics::default();
        m.record(StreamId::new_unchecked(0), 100);
        m.record(StreamId::new_unchecked(4), 100);
        m.record(StreamId::new_unchecked(8), 100);
        m.record(StreamId::new_unchecked(12), 700);
        // Jain = (1000)^2 / (4 * (3*10000 + 490000)) = 1_000_000 / 2_080_000
        let j = m.jain_index();
        assert!(j < 0.6, "expected < 0.6 got {j}");
        assert!(j > 0.4);
    }

    #[test]
    fn min_max_ratio_extremes() {
        let mut m = FairnessMetrics::default();
        m.record(StreamId::new_unchecked(0), 100);
        m.record(StreamId::new_unchecked(4), 900);
        let r = m.min_max_ratio();
        assert!((r - (100.0 / 900.0)).abs() < 1e-9);
    }

    #[test]
    fn reset_clears_window() {
        let mut m = FairnessMetrics::default();
        m.record(StreamId::new_unchecked(0), 100);
        m.reset();
        assert_eq!(m.tracked_streams(), 0);
    }

    #[test]
    fn window_eviction_keeps_bounded() {
        let mut m = FairnessMetrics::new(3);
        m.record(StreamId::new_unchecked(0), 100);
        m.record(StreamId::new_unchecked(4), 200);
        m.record(StreamId::new_unchecked(8), 300);
        m.record(StreamId::new_unchecked(12), 400);
        assert!(m.tracked_streams() <= 3);
    }
}
