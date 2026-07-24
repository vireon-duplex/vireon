//! [`Metric`] / [`MetricKind`] / [`Sample`] / [`Label`] — the data model
//! that collectors return and the encoder consumes.

use crate::error::ExporterError;
use crate::escape::{validate_label_key, validate_metric_name};

/// Variants of metric family, mirroring the Prometheus type line.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetricKind {
    /// Monotonically-increasing counter.
    Counter,
    /// Value that can go up or down.
    Gauge,
    /// Histogram (bucketed observations).
    Histogram,
}

impl MetricKind {
    /// Lowercase keyword emitted on the `# TYPE` line.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
            Self::Histogram => "histogram",
        }
    }
}

/// One key/value pair attached to a [`Sample`]. Order matters: it's
/// preserved on encode, so callers must insert labels deterministically.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Label {
    /// `[a-zA-Z_][a-zA-Z0-9_]*` — validated at construction.
    pub key: String,
    /// Any UTF-8 string; the encoder escapes `\`, `"`, `\n`.
    pub value: String,
}

impl Label {
    /// Construct without validation — caller asserts the key matches
    /// `[a-zA-Z_][a-zA-Z0-9_]*`. Used by the encoder's own tests; in
    /// production code prefer [`Metric::counter`] / friends, which
    /// validate.
    #[must_use]
    pub fn new_unchecked(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }

    /// Construct with validation.
    ///
    /// # Errors
    /// Returns [`ExporterError`] if the key violates the Prometheus
    /// label-key regex.
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Result<Self, ExporterError> {
        let key_str = key.into();
        validate_label_key(&key_str)?;
        Ok(Self {
            key: key_str,
            value: value.into(),
        })
    }
}

/// A single observed value for a metric, optionally partitioned by labels.
#[derive(Clone, Debug, PartialEq)]
pub struct Sample {
    /// Empty `Vec` means "unlabeled" — the encoder omits the `{}`.
    pub labels: Vec<Label>,
    /// Prometheus uses `f64` everywhere; we follow suit.
    pub value: f64,
}

impl Sample {
    /// Unlabeled sample.
    #[must_use]
    pub fn unlabeled(value: f64) -> Self {
        Self {
            labels: Vec::new(),
            value,
        }
    }

    /// Single-label convenience.
    ///
    /// # Errors
    /// Propagates validation errors from [`Label::new`].
    pub fn labeled(
        key: impl Into<String>,
        value_str: impl Into<String>,
        value: f64,
    ) -> Result<Self, ExporterError> {
        Ok(Self {
            labels: vec![Label::new(key, value_str)?],
            value,
        })
    }
}

/// A metric family: one name, one help, one type, zero or more samples.
#[derive(Clone, Debug, PartialEq)]
pub struct Metric {
    /// `[a-zA-Z_:][a-zA-Z0-9_:]*`.
    pub name: String,
    /// Human-readable description; the encoder escapes `\` and `\n`.
    pub help: String,
    /// Counter / Gauge / Histogram.
    pub kind: MetricKind,
    /// Observed values.
    pub samples: Vec<Sample>,
}

impl Metric {
    /// Construct a counter family. Validates name; samples are taken as-is.
    ///
    /// # Errors
    /// Propagates [`validate_metric_name`] errors.
    pub fn counter(name: &str, help: &str, samples: Vec<Sample>) -> Result<Self, ExporterError> {
        Self::new_inner(name, help, MetricKind::Counter, samples)
    }

    /// Construct a gauge family.
    ///
    /// # Errors
    /// Propagates [`validate_metric_name`] errors.
    pub fn gauge(name: &str, help: &str, samples: Vec<Sample>) -> Result<Self, ExporterError> {
        Self::new_inner(name, help, MetricKind::Gauge, samples)
    }

    /// Construct a histogram family.
    ///
    /// # Errors
    /// Propagates [`validate_metric_name`] errors.
    pub fn histogram(name: &str, help: &str, samples: Vec<Sample>) -> Result<Self, ExporterError> {
        Self::new_inner(name, help, MetricKind::Histogram, samples)
    }

    fn new_inner(
        name: &str,
        help: &str,
        kind: MetricKind,
        samples: Vec<Sample>,
    ) -> Result<Self, ExporterError> {
        validate_metric_name(name)?;
        Ok(Self {
            name: name.to_owned(),
            help: help.to_owned(),
            kind,
            samples,
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn constructors_set_correct_kind() {
        let c = Metric::counter("a_total", "x", vec![]).unwrap();
        assert_eq!(c.kind, MetricKind::Counter);
        assert_eq!(c.name, "a_total");

        let g = Metric::gauge("b", "x", vec![]).unwrap();
        assert_eq!(g.kind, MetricKind::Gauge);

        let h = Metric::histogram("c_bucket", "x", vec![]).unwrap();
        assert_eq!(h.kind, MetricKind::Histogram);
    }

    #[test]
    fn kind_as_str() {
        assert_eq!(MetricKind::Counter.as_str(), "counter");
        assert_eq!(MetricKind::Gauge.as_str(), "gauge");
        assert_eq!(MetricKind::Histogram.as_str(), "histogram");
    }

    #[test]
    fn rejects_empty_name() {
        assert_eq!(
            Metric::counter("", "x", vec![]),
            Err(ExporterError::EmptyName)
        );
    }

    #[test]
    fn rejects_overlong_name() {
        let long = "a".repeat(300);
        assert!(matches!(
            Metric::counter(&long, "x", vec![]),
            Err(ExporterError::OverlongName(_, _))
        ));
    }

    #[test]
    fn rejects_invalid_chars() {
        assert!(Metric::counter("my-metric", "x", vec![]).is_err());
        assert!(Metric::counter("1leading_digit", "x", vec![]).is_err());
        assert!(Metric::counter("with space", "x", vec![]).is_err());
    }

    #[test]
    fn accepts_underscore_and_colon() {
        assert!(Metric::counter("_ok", "x", vec![]).is_ok());
        assert!(Metric::counter(":ok", "x", vec![]).is_ok());
        assert!(Metric::counter("a:b", "x", vec![]).is_ok());
    }

    #[test]
    fn label_rejects_colon_in_key() {
        assert!(Label::new("a:b", "v").is_err());
        assert!(Label::new("valid_key", "v").is_ok());
        assert!(Label::new("", "v").is_err());
        assert!(Label::new("1bad", "v").is_err());
    }

    #[test]
    fn sample_constructors() {
        let u = Sample::unlabeled(3.0);
        assert!(u.labels.is_empty());
        assert_eq!(u.value, 3.0);

        let l = Sample::labeled("k", "v", 4.0).unwrap();
        assert_eq!(l.labels.len(), 1);
        assert_eq!(l.labels[0].key, "k");
        assert_eq!(l.labels[0].value, "v");
        assert_eq!(l.value, 4.0);
    }

    #[test]
    fn sample_round_trip_with_labels() {
        let mut s = Sample::unlabeled(1.0);
        s.labels.push(Label::new_unchecked("a", "1"));
        s.labels.push(Label::new_unchecked("b", "2"));
        assert_eq!(s.labels.len(), 2);
        assert_eq!(s.value, 1.0);
    }
}
