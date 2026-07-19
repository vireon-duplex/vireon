//! [`PrometheusRegistry`] — holds collectors, pulls on scrape.

use crate::collector::Collector;
use crate::encoder::encode_prometheus;
use crate::metric::Metric;

/// Central registry of [`Collector`]s. On scrape it calls each
/// collector in registration order and concatenates the returned
/// metric families.
///
/// The registry owns its collectors as `Box<dyn Collector>` so that
/// heterogeneous collectors can coexist.
#[derive(Default)]
pub struct PrometheusRegistry {
    collectors: Vec<Box<dyn Collector>>,
}

impl PrometheusRegistry {
    /// Construct empty.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a collector. Registration order is preserved on scrape.
    pub fn register<C: Collector + 'static>(&mut self, collector: C) {
        self.collectors.push(Box::new(collector));
    }

    /// How many collectors are registered.
    #[must_use]
    pub fn collector_count(&self) -> usize {
        self.collectors.len()
    }

    /// Snapshot every collector in registration order. Each collector's
    /// metrics are concatenated; within a collector, order is preserved.
    #[must_use]
    pub fn scrape(&self) -> Vec<Metric> {
        let mut out = Vec::new();
        for c in &self.collectors {
            out.extend(c.collect());
        }
        out
    }

    /// Convenience: [`Self::scrape`] + [`encode_prometheus`] in one call.
    #[must_use]
    pub fn scrape_text(&self) -> String {
        encode_prometheus(&self.scrape())
    }
}

impl std::fmt::Debug for PrometheusRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrometheusRegistry")
            .field("collector_count", &self.collectors.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::collector::ClosureCollector;
    use crate::metric::Sample;

    fn counter_metric(name: &str, value: f64) -> Metric {
        Metric::counter(name, "test counter", vec![Sample::unlabeled(value)]).unwrap()
    }

    #[test]
    fn empty_registry() {
        let r = PrometheusRegistry::new();
        assert_eq!(r.collector_count(), 0);
        assert!(r.scrape().is_empty());
        assert_eq!(r.scrape_text(), "");
    }

    #[test]
    fn registers_and_counts() {
        let mut r = PrometheusRegistry::new();
        r.register(ClosureCollector::new("a", Vec::new));
        assert_eq!(r.collector_count(), 1);
        r.register(ClosureCollector::new("b", Vec::new));
        assert_eq!(r.collector_count(), 2);
    }

    #[test]
    fn scrape_returns_metrics_in_registration_order() {
        let mut r = PrometheusRegistry::new();
        r.register(ClosureCollector::new("a", move || {
            vec![counter_metric("a", 1.0)]
        }));
        r.register(ClosureCollector::new("b", move || {
            vec![counter_metric("b", 2.0)]
        }));
        r.register(ClosureCollector::new("c", move || {
            vec![counter_metric("c", 3.0)]
        }));

        let scraped = r.scrape();
        assert_eq!(scraped.len(), 3);
        assert_eq!(scraped[0].name, "a");
        assert_eq!(scraped[1].name, "b");
        assert_eq!(scraped[2].name, "c");
    }

    #[test]
    fn scrape_concatenates_multiple_metrics_per_collector() {
        let mut r = PrometheusRegistry::new();
        r.register(ClosureCollector::new("multi", move || {
            vec![counter_metric("x", 1.0), counter_metric("y", 2.0)]
        }));
        r.register(ClosureCollector::new("single", move || {
            vec![counter_metric("z", 3.0)]
        }));

        let scraped = r.scrape();
        assert_eq!(scraped.len(), 3);
        assert_eq!(scraped[0].name, "x");
        assert_eq!(scraped[1].name, "y");
        assert_eq!(scraped[2].name, "z");
    }

    #[test]
    fn scrape_text_matches_encode_of_scrape() {
        let mut r = PrometheusRegistry::new();
        r.register(ClosureCollector::new("a", move || {
            vec![counter_metric("a_total", 5.0)]
        }));

        let from_text = r.scrape_text();
        let from_encode = encode_prometheus(&r.scrape());
        assert_eq!(from_text, from_encode);
        // Sanity: actually contains the metric.
        assert!(from_text.contains("a_total"));
    }

    #[test]
    fn debug_shows_collector_count() {
        let mut r = PrometheusRegistry::new();
        r.register(ClosureCollector::new("a", Vec::new));
        let s = format!("{r:?}");
        assert!(s.contains("PrometheusRegistry"));
        assert!(s.contains("1"));
    }

    #[test]
    fn custom_collector_struct_works() {
        struct MyCollector;
        impl Collector for MyCollector {
            fn collect(&self) -> Vec<Metric> {
                vec![counter_metric("custom_total", 99.0)]
            }
            fn name(&self) -> &'static str {
                "custom"
            }
        }
        let mut r = PrometheusRegistry::new();
        r.register(MyCollector);
        let m = r.scrape();
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].name, "custom_total");
    }

    #[test]
    fn empty_collector_then_populated_one() {
        let mut r = PrometheusRegistry::new();
        // First collector returns nothing (e.g. a histogram with no
        // observations yet).
        r.register(ClosureCollector::new("empty", Vec::new));
        r.register(ClosureCollector::new("ok", move || {
            vec![counter_metric("ok", 1.0)]
        }));

        let scraped = r.scrape();
        assert_eq!(scraped.len(), 1);
        assert_eq!(scraped[0].name, "ok");
    }
}
