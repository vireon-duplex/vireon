//! [`QueryRange`], [`QueryAggregation`] — query specification types
//! for the ops-dashboard.
//!
//! See `DESIGN.md` Part 6 (query model).
//!
//! # Design
//!
//! - `QueryRange`: A half-open time range [start_ms, end_ms) for
//!   metric queries. All times are epoch milliseconds.
//! - `QueryAggregation`: The aggregation function to apply to raw
//!   data points within a query range.

// ── QueryRange ──────────────────────────────────────────────────

/// A time range for a metric query.
///
/// Represents the half-open interval `[start_ms, end_ms)`. The
/// duration is `end_ms - start_ms`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct QueryRange {
    /// Start of the range in epoch milliseconds (inclusive).
    pub start_ms: u64,
    /// End of the range in epoch milliseconds (exclusive).
    pub end_ms: u64,
}

impl QueryRange {
    /// Construct a new query range.
    ///
    /// Does NOT validate; call [`QueryRange::is_valid`] to check.
    #[must_use]
    pub const fn new(start_ms: u64, end_ms: u64) -> Self {
        Self { start_ms, end_ms }
    }

    /// Duration of the range in milliseconds.
    ///
    /// Returns 0 if `end_ms <= start_ms`.
    #[must_use]
    pub const fn duration_ms(&self) -> u64 {
        self.end_ms.saturating_sub(self.start_ms)
    }

    /// Whether the range is valid (start_ms < end_ms, non-zero duration).
    #[must_use]
    pub const fn is_valid(&self) -> bool {
        self.end_ms > self.start_ms
    }
}

// ── QueryAggregation ────────────────────────────────────────────

/// The aggregation function to apply to raw data points.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum QueryAggregation {
    /// No aggregation; return raw data points.
    None = 0,
    /// Average of all data points.
    Avg = 1,
    /// Sum of all data points.
    Sum = 2,
    /// Maximum of all data points.
    Max = 3,
    /// Minimum of all data points.
    Min = 4,
    /// 95th percentile of all data points.
    P95 = 5,
    /// 99th percentile of all data points.
    P99 = 6,
}

impl QueryAggregation {
    /// Human-readable name for the aggregation type.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Avg => "avg",
            Self::Sum => "sum",
            Self::Max => "max",
            Self::Min => "min",
            Self::P95 => "p95",
            Self::P99 => "p99",
        }
    }
}

impl Default for QueryAggregation {
    fn default() -> Self {
        Self::None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_range() {
        let r = QueryRange::new(1_000, 5_000);
        assert!(r.is_valid());
        assert_eq!(r.duration_ms(), 4_000);
    }

    #[test]
    fn invalid_range_zero_duration() {
        let r = QueryRange::new(1_000, 1_000);
        assert!(!r.is_valid());
        assert_eq!(r.duration_ms(), 0);
    }

    #[test]
    fn invalid_range_inverted() {
        let r = QueryRange::new(5_000, 1_000);
        assert!(!r.is_valid());
        assert_eq!(r.duration_ms(), 0);
    }

    #[test]
    fn default_range_is_invalid() {
        let r = QueryRange::default();
        assert!(!r.is_valid());
        assert_eq!(r.duration_ms(), 0);
    }

    #[test]
    fn range_copy_and_eq() {
        let a = QueryRange::new(100, 200);
        let b = a;
        assert_eq!(a, b);
        assert_eq!(a.duration_ms(), 100);
    }

    #[test]
    fn aggregation_repr_values() {
        assert_eq!(QueryAggregation::None as u8, 0);
        assert_eq!(QueryAggregation::Avg as u8, 1);
        assert_eq!(QueryAggregation::Sum as u8, 2);
        assert_eq!(QueryAggregation::Max as u8, 3);
        assert_eq!(QueryAggregation::Min as u8, 4);
        assert_eq!(QueryAggregation::P95 as u8, 5);
        assert_eq!(QueryAggregation::P99 as u8, 6);
    }

    #[test]
    fn aggregation_names() {
        assert_eq!(QueryAggregation::None.as_str(), "none");
        assert_eq!(QueryAggregation::Avg.as_str(), "avg");
        assert_eq!(QueryAggregation::Sum.as_str(), "sum");
        assert_eq!(QueryAggregation::Max.as_str(), "max");
        assert_eq!(QueryAggregation::Min.as_str(), "min");
        assert_eq!(QueryAggregation::P95.as_str(), "p95");
        assert_eq!(QueryAggregation::P99.as_str(), "p99");
    }

    #[test]
    fn default_aggregation_is_none() {
        assert_eq!(QueryAggregation::default(), QueryAggregation::None);
    }
}
