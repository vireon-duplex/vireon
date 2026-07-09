//! [`MetricKey`], [`MetricType`], [`MetricValue`] — metric types and values.
//!
//! See `DESIGN.md` Part 5 (metrics).
//!
//! # Design
//!
//! Metrics are typed (counter/gauge/histogram) and identified by a
//! string key up to 63 bytes. Values are enum-wrapped to support
//! different metric types in a single registry.

use crate::error::ObservabilityError;

// ── Constants ───────────────────────────────────────────────────

/// Maximum length of a metric key in bytes (including null terminator).
pub const MAX_LEN: usize = 63;

// ── MetricKey ───────────────────────────────────────────────────

/// A metric identifier (string key up to 63 bytes).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct MetricKey {
    bytes: [u8; Self::MAX],
    len: u8,
}

impl MetricKey {
    const MAX: usize = MAX_LEN;

    /// The nil/unset metric key.
    pub const NIL: Self = Self {
        bytes: [0u8; Self::MAX],
        len: 0,
    };

    /// Construct a new metric key from a string.
    ///
    /// # Errors
    /// Returns [`ObservabilityError::InvalidLabel`] if the string is
    /// empty, too long, or contains null bytes.
    pub fn new(s: &str) -> Result<Self, ObservabilityError> {
        if s.is_empty() {
            return Err(ObservabilityError::InvalidLabel(
                "metric key cannot be empty",
            ));
        }
        let bytes = s.as_bytes();
        if bytes.len() > Self::MAX {
            return Err(ObservabilityError::InvalidLabel("metric key too long"));
        }
        if bytes.contains(&0) {
            return Err(ObservabilityError::InvalidLabel(
                "metric key contains null byte",
            ));
        }
        let mut buf = [0u8; Self::MAX];
        buf[..bytes.len()].copy_from_slice(bytes);
        Ok(Self {
            bytes: buf,
            len: bytes.len() as u8,
        })
    }

    /// Construct the nil metric key.
    #[must_use]
    pub const fn nil() -> Self {
        Self::NIL
    }

    /// Whether this key is nil.
    #[must_use]
    pub const fn is_nil(&self) -> bool {
        self.len == 0
    }

    /// Get the underlying bytes (including trailing nulls).
    #[must_use]
    pub const fn raw(&self) -> ([u8; Self::MAX], u8) {
        (self.bytes, self.len)
    }

    /// Convert to a string slice (lossy; may contain trailing garbage).
    #[must_use]
    pub fn as_str(&self) -> &str {
        let end = self.len as usize;
        // SAFETY: we only store valid UTF-8 and length is within bounds.
        unsafe { std::str::from_utf8_unchecked(&self.bytes[..end]) }
    }
}

// ── MetricType ─────────────────────────────────────────────────

/// The type of a metric.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum MetricType {
    /// Counter: monotonically increasing value.
    Counter = 0,
    /// Gauge: point-in-time value (can go up or down).
    Gauge = 1,
    /// Histogram: distribution of values (count, sum, min, max).
    Histogram = 2,
}

impl MetricType {
    /// Human-readable name for the metric type.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
            Self::Histogram => "histogram",
        }
    }
}

// ── MetricValue ─────────────────────────────────────────────────

/// The value of a metric.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetricValue {
    /// Counter value (monotonically increasing).
    Counter(u64),
    /// Gauge value (can go up or down).
    Gauge(i64),
    /// Histogram value (distribution).
    Histogram {
        /// Number of samples.
        count: u64,
        /// Sum of all samples.
        sum: i64,
        /// Minimum sample.
        min: i64,
        /// Maximum sample.
        max: i64,
    },
}

impl MetricValue {
    /// Get the type of this metric value.
    #[must_use]
    pub const fn type_of(&self) -> MetricType {
        match self {
            Self::Counter(_) => MetricType::Counter,
            Self::Gauge(_) => MetricType::Gauge,
            Self::Histogram { .. } => MetricType::Histogram,
        }
    }

    /// Extract the counter value if this is a counter.
    #[must_use]
    pub const fn as_counter(&self) -> Option<u64> {
        match self {
            Self::Counter(v) => Some(*v),
            _ => None,
        }
    }

    /// Extract the gauge value if this is a gauge.
    #[must_use]
    pub const fn as_gauge(&self) -> Option<i64> {
        match self {
            Self::Gauge(v) => Some(*v),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nil_key_is_nil() {
        let key = MetricKey::nil();
        assert!(key.is_nil());
        assert_eq!(key.len, 0);
    }

    #[test]
    fn new_key_from_valid_string() {
        let key = MetricKey::new("request_count").unwrap();
        assert!(!key.is_nil());
        assert_eq!(key.as_str(), "request_count");
        assert_eq!(key.len, 13);
    }

    #[test]
    fn new_key_rejects_empty_string() {
        assert!(MetricKey::new("").is_err());
    }

    #[test]
    fn new_key_rejects_too_long() {
        let long = "a".repeat(64);
        assert!(MetricKey::new(&long).is_err());
    }

    #[test]
    fn new_key_rejects_null_byte() {
        assert!(MetricKey::new("bad\0key").is_err());
    }

    #[test]
    fn key_at_max_length() {
        let max = "a".repeat(63);
        let key = MetricKey::new(&max).unwrap();
        assert_eq!(key.len, 63);
        assert_eq!(key.as_str(), &max);
    }

    #[test]
    fn metric_type_names() {
        assert_eq!(MetricType::Counter.as_str(), "counter");
        assert_eq!(MetricType::Gauge.as_str(), "gauge");
        assert_eq!(MetricType::Histogram.as_str(), "histogram");
    }

    #[test]
    fn metric_value_type() {
        assert_eq!(MetricValue::Counter(42).type_of(), MetricType::Counter);
        assert_eq!(MetricValue::Gauge(-5).type_of(), MetricType::Gauge);
        assert_eq!(
            MetricValue::Histogram {
                count: 10,
                sum: 100,
                min: 5,
                max: 20
            }
            .type_of(),
            MetricType::Histogram
        );
    }

    #[test]
    fn extract_counter_value() {
        let val = MetricValue::Counter(123);
        assert_eq!(val.as_counter(), Some(123));
        assert_eq!(val.as_gauge(), None);
    }

    #[test]
    fn extract_gauge_value() {
        let val = MetricValue::Gauge(-42);
        assert_eq!(val.as_gauge(), Some(-42));
        assert_eq!(val.as_counter(), None);
    }

    #[test]
    fn histogram_value_structure() {
        let val = MetricValue::Histogram {
            count: 100,
            sum: 5_000,
            min: 10,
            max: 100,
        };
        assert_eq!(val.type_of(), MetricType::Histogram);
        assert_eq!(val.as_counter(), None);
        assert_eq!(val.as_gauge(), None);
    }
}
