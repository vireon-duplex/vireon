//! Adaptive tuning (Layer 17).
//!
//! [`analyze`] reads pool statistics and produces a [`TuningReport`] with
//! recommended parameters. The recommendations are advisory — the worker logs
//! them and, when applicable, the pool applies them.

use crate::stats::StatsSnapshot;

/// Per-tick tuning recommendations derived from pool statistics.
#[derive(Debug, Clone)]
pub struct TuningReport {
    /// Fraction of requests served from the thread-local cache.
    pub hit_rate: f64,
    /// Fraction of requests that missed the thread-local cache.
    pub miss_rate: f64,
    /// Suggested batch size for global ↔ thread-local transfers.
    pub recommended_batch: usize,
}

/// Maximum batch size the tuner will recommend.
pub const MAX_BATCH: usize = 64;
/// Minimum batch size the tuner will recommend.
pub const MIN_BATCH: usize = 4;

/// Analyzes `stats` and produces a [`TuningReport`].
///
/// - High miss rate (> 50 %) → double the batch (refill more aggressively).
/// - Low miss rate (< 10 %) → halve the batch (avoid over-fetching).
/// - Otherwise → keep the current batch.
pub fn analyze(stats: &StatsSnapshot, current_batch: usize) -> TuningReport {
    let total = stats.counters.hits + stats.counters.misses + stats.counters.reuses;
    let (hit_rate, miss_rate) = if total == 0 {
        (0.0, 0.0)
    } else {
        (
            stats.counters.hits as f64 / total as f64,
            stats.counters.misses as f64 / total as f64,
        )
    };

    let recommended_batch = if total == 0 {
        current_batch
    } else if miss_rate > 0.5 {
        (current_batch * 2).min(MAX_BATCH)
    } else if miss_rate < 0.1 {
        (current_batch / 2).max(MIN_BATCH)
    } else {
        current_batch
    };

    TuningReport {
        hit_rate,
        miss_rate,
        recommended_batch,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::CountersSnapshot;

    fn snap(hits: u64, misses: u64, reuses: u64) -> StatsSnapshot {
        StatsSnapshot {
            counters: CountersSnapshot {
                hits,
                misses,
                reuses,
                ..CountersSnapshot::default()
            },
            ..StatsSnapshot::default()
        }
    }

    #[test]
    fn zero_traffic_returns_zero_rates() {
        let report = analyze(&StatsSnapshot::default(), 16);
        assert_eq!(report.hit_rate, 0.0);
        assert_eq!(report.miss_rate, 0.0);
        assert_eq!(report.recommended_batch, 16);
    }

    #[test]
    fn high_hit_rate_keeps_batch() {
        // miss_rate = 0.15 (between 0.1 and 0.5) → keep batch.
        let report = analyze(&snap(80, 15, 5), 16);
        assert!((report.hit_rate - 0.80).abs() < 1e-9);
        assert_eq!(report.recommended_batch, 16);
    }

    #[test]
    fn high_miss_rate_doubles_batch() {
        let report = analyze(&snap(10, 60, 30), 16);
        assert!(report.miss_rate > 0.5);
        assert_eq!(report.recommended_batch, 32);
    }

    #[test]
    fn low_miss_rate_halves_batch() {
        let report = analyze(&snap(90, 3, 7), 16);
        assert!(report.miss_rate < 0.1);
        assert_eq!(report.recommended_batch, 8);
    }

    #[test]
    fn batch_capped_at_max() {
        let report = analyze(&snap(0, 100, 0), 64);
        assert_eq!(report.recommended_batch, MAX_BATCH);
    }

    #[test]
    fn batch_floored_at_min() {
        let report = analyze(&snap(100, 0, 0), 4);
        assert_eq!(report.recommended_batch, MIN_BATCH);
    }
}
