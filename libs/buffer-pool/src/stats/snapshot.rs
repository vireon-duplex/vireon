//! Snapshot types for pool statistics (Layer 13-14).
//!
//! These are plain value types so they can be returned across thread
//! boundaries, serialised, and logged without holding any locks on the live
//! counters or histogram.

/// Aggregated counters at a point in time.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CountersSnapshot {
    /// Thread-local cache hits.
    pub hits: u64,
    /// Requests that missed the thread-local cache (may still hit global).
    pub misses: u64,
    /// Served from the global shard cache.
    pub reuses: u64,
    /// New allocations from the arena or huge allocator.
    pub allocations: u64,
    /// Buffers released back to the pool.
    pub frees: u64,
    /// Currently outstanding (live) buffers.
    pub outstanding: u64,
    /// High-water mark of `outstanding`.
    pub peak_outstanding: u64,
    /// Cumulative bytes allocated.
    pub bytes_allocated: u64,
    /// Cumulative bytes freed.
    pub bytes_freed: u64,
    /// Currently outstanding bytes (live buffers).
    pub bytes_outstanding: u64,
}

/// Common latency percentiles derived from the histogram.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Percentiles {
    /// 50th percentile (median) in nanoseconds.
    pub p50_ns: u64,
    /// 90th percentile in nanoseconds.
    pub p90_ns: u64,
    /// 99th percentile in nanoseconds.
    pub p99_ns: u64,
}

/// Histogram snapshot: bucket counts plus aggregate metrics.
#[derive(Debug, Clone, Default)]
pub struct HistogramSnapshot {
    /// Count per bucket (see [`bucket_lower`](crate::stats::histogram::bucket_lower)).
    pub buckets: Vec<u64>,
    /// Total observations.
    pub total_count: u64,
    /// Sum of all observed latencies (nanoseconds).
    pub total_sum: u64,
    /// Arithmetic mean latency (nanoseconds).
    pub average_ns: u64,
    /// Pre-computed percentiles.
    pub percentiles: Percentiles,
}

/// Complete statistics snapshot for a pool.
#[derive(Debug, Clone, Default)]
pub struct StatsSnapshot {
    /// Counter values.
    pub counters: CountersSnapshot,
    /// Latency distribution.
    pub latency: HistogramSnapshot,
    /// Bytes retained by the arena (slab backing).
    pub arena_bytes: u64,
    /// Bytes held in the global cache.
    pub cached_bytes: u64,
}

impl StatsSnapshot {
    /// Hit ratio of the thread-local cache: `hits / (hits + misses + reuses)`.
    ///
    /// Returns `0.0` when no requests have been served.
    pub fn hit_rate(&self) -> f64 {
        let total = self.counters.hits + self.counters.misses + self.counters.reuses;
        if total == 0 {
            return 0.0;
        }
        self.counters.hits as f64 / total as f64
    }

    /// Effective reuse rate: `(hits + reuses) / total_requests`.
    pub fn reuse_rate(&self) -> f64 {
        let total = self.counters.hits + self.counters.misses + self.counters.reuses;
        if total == 0 {
            return 0.0;
        }
        (self.counters.hits + self.counters.reuses) as f64 / total as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hit_rate_zero_when_no_traffic() {
        let snap = StatsSnapshot::default();
        assert_eq!(snap.hit_rate(), 0.0);
        assert_eq!(snap.reuse_rate(), 0.0);
    }

    #[test]
    fn hit_rate_computes_ratio() {
        let snap = StatsSnapshot {
            counters: CountersSnapshot {
                hits: 80,
                misses: 15,
                reuses: 5,
                ..CountersSnapshot::default()
            },
            ..StatsSnapshot::default()
        };
        assert!((snap.hit_rate() - 0.80).abs() < 1e-9);
        assert!((snap.reuse_rate() - 0.85).abs() < 1e-9);
    }
}
