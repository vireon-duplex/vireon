//! [`Collector`] trait + [`ClosureCollector`] convenience adapter.

use crate::metric::Metric;

/// Implemented by anything that can produce metric families on demand.
///
/// The trait is the integration point between Vireon's various stat
/// structs (`PubSubStats`, `AuthStats`, …) and the Prometheus
/// [`crate::registry::PrometheusRegistry`]. A collector is called once
/// per scrape, so implementations must be cheap (<1ms typical) and
/// side-effect-free.
///
/// Implementations are `Send + Sync` so they can live behind an `Arc`
/// shared across worker threads.
pub trait Collector: Send + Sync {
    /// Return zero or more metric families. Each scrape builds a fresh
    /// `Vec`; ordering within a single collector is preserved by the
    /// encoder.
    fn collect(&self) -> Vec<Metric>;

    /// Optional human-readable name for diagnostics / logs.
    fn name(&self) -> &'static str {
        "anonymous"
    }
}

/// Adapter that turns `Fn() -> Vec<Metric>` into a [`Collector`].
///
/// Useful for the common case where a crate already has a stats struct
/// and wants to expose it without writing a named `impl Collector for …`.
///
/// # Example
/// ```
/// use observability_exporters::{ClosureCollector, Collector, Metric, Sample};
///
/// let counter_value = std::sync::atomic::AtomicU64::new(0);
/// let c = ClosureCollector::new("requests", move || {
///     vec![
///         Metric::counter(
///             "requests_total",
///             "Total requests handled.",
///             vec![Sample::unlabeled(
///                 counter_value.load(std::sync::atomic::Ordering::Relaxed) as f64,
///             )],
///         ).unwrap(),
///     ]
/// });
/// let _m = c.collect();
/// assert_eq!(c.name(), "requests");
/// ```
pub struct ClosureCollector<F>
where
    F: Fn() -> Vec<Metric> + Send + Sync,
{
    name_str: &'static str,
    f: F,
}

impl<F> ClosureCollector<F>
where
    F: Fn() -> Vec<Metric> + Send + Sync,
{
    /// Construct with a diagnostic name and the closure that produces metrics.
    #[must_use]
    pub fn new(name: &'static str, f: F) -> Self {
        Self { name_str: name, f }
    }
}

impl<F> Collector for ClosureCollector<F>
where
    F: Fn() -> Vec<Metric> + Send + Sync,
{
    fn collect(&self) -> Vec<Metric> {
        (self.f)()
    }

    fn name(&self) -> &'static str {
        self.name_str
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::metric::Sample;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn closure_collector_invokes_closure() {
        let c = ClosureCollector::new("static", || {
            vec![Metric::counter("a_total", "x", vec![Sample::unlabeled(42.0)]).unwrap()]
        });
        let m = c.collect();
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].name, "a_total");
        assert_eq!(c.name(), "static");
    }

    #[test]
    fn closure_collector_sees_atomic_updates() {
        let counter = Arc::new(AtomicU64::new(0));
        let counter_for_closure = Arc::clone(&counter);
        let c = ClosureCollector::new("counter", move || {
            vec![
                Metric::counter(
                    "v_total",
                    "x",
                    vec![Sample::unlabeled(
                        counter_for_closure.load(Ordering::Relaxed) as f64,
                    )],
                )
                .unwrap(),
            ]
        });
        assert_eq!(c.collect()[0].samples[0].value, 0.0);
        counter.fetch_add(7, Ordering::Relaxed);
        assert_eq!(c.collect()[0].samples[0].value, 7.0);
    }

    #[test]
    fn closure_collector_returns_empty_vec() {
        let c = ClosureCollector::new("empty", Vec::new);
        assert!(c.collect().is_empty());
    }

    #[test]
    fn closure_collector_with_labels() {
        let c = ClosureCollector::new("labeled", || {
            vec![
                Metric::gauge(
                    "g",
                    "x",
                    vec![
                        Sample::labeled("a", "1", 10.0).unwrap(),
                        Sample::labeled("a", "2", 20.0).unwrap(),
                    ],
                )
                .unwrap(),
            ]
        });
        let m = c.collect();
        assert_eq!(m[0].samples.len(), 2);
    }

    #[test]
    fn default_name_is_anonymous() {
        struct Custom;
        impl Collector for Custom {
            fn collect(&self) -> Vec<Metric> {
                Vec::new()
            }
        }
        let c = Custom;
        assert_eq!(c.name(), "anonymous");
    }
}
