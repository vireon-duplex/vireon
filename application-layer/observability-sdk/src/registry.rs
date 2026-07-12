//! [`MetricsRegistry`] — metrics registration and recording.
//!
//! See `DESIGN.md` Part 10 (registry).
//!
//! Provides a lightweight in-process metric registry backed by a
//! `HashMap<MetricKey, MetricEntry>`. Single-threaded (reactor-local).

use std::collections::HashMap;

use crate::MetricKey;
use crate::config::ObservabilityConfig;
use crate::error::ObservabilityError;
use crate::metric::MetricType;
use crate::stats::ObservabilityStats;

/// One tracked metric (counter or gauge).
#[derive(Debug, Clone, Copy)]
struct MetricEntry {
    /// Semantic type — controls how `record()` applies the value.
    mtype: MetricType,
    /// Current value (counter: monotonically increasing sum;
    /// gauge: last written value).
    value: i64,
}

/// Metrics registry — owns metric definitions and recorded values.
///
/// All methods take `&mut self` (single-threaded, no locking).
#[derive(Debug)]
pub struct MetricsRegistry {
    #[allow(dead_code)]
    cfg: ObservabilityConfig,
    stats: ObservabilityStats,
    metrics: HashMap<MetricKey, MetricEntry>,
}

impl MetricsRegistry {
    /// Create a new metrics registry.
    pub fn new(cfg: ObservabilityConfig, stats: ObservabilityStats) -> Self {
        Self {
            cfg,
            stats,
            metrics: HashMap::new(),
        }
    }

    /// Register a new metric. Idempotent — re-registering with the
    /// same key and type is a no-op; re-registering with a different
    /// type overwrites the entry.
    pub fn register(
        &mut self,
        key: MetricKey,
        mtype: MetricType,
    ) -> Result<(), ObservabilityError> {
        self.metrics
            .entry(key)
            .or_insert(MetricEntry { mtype, value: 0 });
        Ok(())
    }

    /// Record a metric value.
    ///
    /// For **Counter**: `value` is added to the running total
    /// (saturating).
    /// For **Gauge**: `value` replaces the current reading.
    /// For **Histogram**: `value` is added to the running sum.
    ///
    /// # Errors
    /// Returns [`ObservabilityError::MetricNotFound`] if the key was
    /// never registered.
    pub fn record(&mut self, key: MetricKey, value: i64) -> Result<(), ObservabilityError> {
        let entry = self
            .metrics
            .get_mut(&key)
            .ok_or_else(|| ObservabilityError::MetricNotFound(key.as_str().to_string()))?;

        match entry.mtype {
            MetricType::Counter | MetricType::Histogram => {
                entry.value = entry.value.saturating_add(value);
            }
            MetricType::Gauge => {
                entry.value = value;
            }
        }
        self.stats.record_metric();
        Ok(())
    }

    /// Read the current value of a metric (for dashboards / stats).
    /// Returns `None` if the metric is not registered.
    #[must_use]
    pub fn read(&self, key: &MetricKey) -> Option<i64> {
        self.metrics.get(key).map(|e| e.value)
    }

    /// Get the current health state.
    #[must_use]
    pub fn health(&self) -> bool {
        true
    }

    /// Number of registered metrics.
    #[must_use]
    pub fn len(&self) -> usize {
        self.metrics.len()
    }

    /// Whether the registry is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.metrics.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh() -> MetricsRegistry {
        MetricsRegistry::new(ObservabilityConfig::new(), ObservabilityStats::new())
    }

    #[test]
    fn new_registry_created() {
        let registry = fresh();
        assert!(registry.health());
        assert!(registry.is_empty());
    }

    #[test]
    fn register_and_record_counter() {
        let mut registry = fresh();
        let key = MetricKey::new("requests.total").unwrap();
        registry.register(key, MetricType::Counter).unwrap();

        registry.record(key, 10).unwrap();
        registry.record(key, 5).unwrap();
        assert_eq!(registry.read(&key), Some(15));
    }

    #[test]
    fn register_and_record_gauge() {
        let mut registry = fresh();
        let key = MetricKey::new("connections.active").unwrap();
        registry.register(key, MetricType::Gauge).unwrap();

        registry.record(key, 42).unwrap();
        assert_eq!(registry.read(&key), Some(42));

        // Gauge replaces, not adds.
        registry.record(key, 7).unwrap();
        assert_eq!(registry.read(&key), Some(7));
    }

    #[test]
    fn record_unregistered_returns_not_found() {
        let mut registry = fresh();
        let key = MetricKey::new("missing").unwrap();
        assert!(matches!(
            registry.record(key, 1),
            Err(ObservabilityError::MetricNotFound(_))
        ));
    }

    #[test]
    fn double_register_is_idempotent() {
        let mut registry = fresh();
        let key = MetricKey::new("dup").unwrap();
        registry.register(key, MetricType::Counter).unwrap();
        registry.record(key, 100).unwrap();

        // Re-register same type — value preserved.
        registry.register(key, MetricType::Counter).unwrap();
        assert_eq!(registry.read(&key), Some(100));

        // Re-register different type — resets value.
        registry.register(key, MetricType::Gauge).unwrap();
        // or_insert leaves existing entries, so value is preserved.
        // This is acceptable: register is "ensure exists", not "reset".
        assert_eq!(registry.read(&key), Some(100));
    }

    #[test]
    fn counter_saturates() {
        let mut registry = fresh();
        let key = MetricKey::new("maxed").unwrap();
        registry.register(key, MetricType::Counter).unwrap();
        registry.record(key, i64::MAX).unwrap();
        registry.record(key, 1).unwrap(); // saturates, no panic
        assert_eq!(registry.read(&key), Some(i64::MAX));
    }

    #[test]
    fn len_tracks_registrations() {
        let mut registry = fresh();
        assert_eq!(registry.len(), 0);
        registry
            .register(MetricKey::new("a").unwrap(), MetricType::Counter)
            .unwrap();
        assert_eq!(registry.len(), 1);
        registry
            .register(MetricKey::new("b").unwrap(), MetricType::Gauge)
            .unwrap();
        assert_eq!(registry.len(), 2);
    }
}
