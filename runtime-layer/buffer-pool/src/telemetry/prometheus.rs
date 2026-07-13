//! Prometheus exporter (Layer 15).
//!
//! Requires the `prometheus` cargo feature.
//!
//! [`PrometheusExporter`] registers a set of counters and gauges on a
//! [`Registry`] and updates them on every call to
//! [`export`](PrometheusExporter::export). Counter deltas are computed
//! internally so that a [`Counters::reset`](crate::stats::Counters::reset) is
//! treated as a fresh start rather than a negative spike.

use parking_lot::Mutex;

use prometheus::{
    IntCounter, IntGauge, Registry, register_int_counter_with_registry,
    register_int_gauge_with_registry,
};

use crate::stats::CountersSnapshot;
use crate::stats::StatsSnapshot;
use crate::telemetry::metrics::MetricsExporter;

/// Prometheus-backed metrics exporter.
pub struct PrometheusExporter {
    registry: Registry,

    // Monotone counters (tracked via delta from last export).
    hits: IntCounter,
    misses: IntCounter,
    reuses: IntCounter,
    allocations: IntCounter,
    frees: IntCounter,
    bytes_allocated: IntCounter,
    bytes_freed: IntCounter,

    // Gauges (absolute values).
    outstanding: IntGauge,
    peak_outstanding: IntGauge,
    bytes_outstanding: IntGauge,
    arena_bytes: IntGauge,
    cached_bytes: IntGauge,

    // Percentile gauges for latency.
    latency_p50_ns: IntGauge,
    latency_p90_ns: IntGauge,
    latency_p99_ns: IntGauge,
    latency_avg_ns: IntGauge,

    last: Mutex<CountersSnapshot>,
}

impl PrometheusExporter {
    /// Creates a new exporter that registers metrics on the given [`Registry`].
    pub fn new(registry: Registry) -> Result<Self, prometheus::Error> {
        let hits = register_int_counter_with_registry!(
            "buffer_pool_hits_total",
            "Total thread-local cache hits",
            registry
        )?;
        let misses = register_int_counter_with_registry!(
            "buffer_pool_misses_total",
            "Total thread-local cache misses",
            registry
        )?;
        let reuses = register_int_counter_with_registry!(
            "buffer_pool_reuses_total",
            "Total global-cache reuses",
            registry
        )?;
        let allocations = register_int_counter_with_registry!(
            "buffer_pool_allocations_total",
            "Total fresh allocations from the arena",
            registry
        )?;
        let frees = register_int_counter_with_registry!(
            "buffer_pool_frees_total",
            "Total releases",
            registry
        )?;
        let bytes_allocated = register_int_counter_with_registry!(
            "buffer_pool_bytes_allocated_total",
            "Total bytes freshly allocated",
            registry
        )?;
        let bytes_freed = register_int_counter_with_registry!(
            "buffer_pool_bytes_freed_total",
            "Total bytes released",
            registry
        )?;

        let outstanding = register_int_gauge_with_registry!(
            "buffer_pool_outstanding",
            "Currently live buffers",
            registry
        )?;
        let peak_outstanding = register_int_gauge_with_registry!(
            "buffer_pool_peak_outstanding",
            "Peak live buffer count",
            registry
        )?;
        let bytes_outstanding = register_int_gauge_with_registry!(
            "buffer_pool_bytes_outstanding",
            "Bytes in currently-live buffers",
            registry
        )?;
        let arena_bytes = register_int_gauge_with_registry!(
            "buffer_pool_arena_bytes",
            "Memory retained by the arena",
            registry
        )?;
        let cached_bytes = register_int_gauge_with_registry!(
            "buffer_pool_cached_bytes",
            "Memory held in caches",
            registry
        )?;

        let latency_p50_ns = register_int_gauge_with_registry!(
            "buffer_pool_latency_p50_ns",
            "Acquire latency p50 (ns)",
            registry
        )?;
        let latency_p90_ns = register_int_gauge_with_registry!(
            "buffer_pool_latency_p90_ns",
            "Acquire latency p90 (ns)",
            registry
        )?;
        let latency_p99_ns = register_int_gauge_with_registry!(
            "buffer_pool_latency_p99_ns",
            "Acquire latency p99 (ns)",
            registry
        )?;
        let latency_avg_ns = register_int_gauge_with_registry!(
            "buffer_pool_latency_avg_ns",
            "Acquire latency average (ns)",
            registry
        )?;

        Ok(Self {
            registry,
            hits,
            misses,
            reuses,
            allocations,
            frees,
            bytes_allocated,
            bytes_freed,
            outstanding,
            peak_outstanding,
            bytes_outstanding,
            arena_bytes,
            cached_bytes,
            latency_p50_ns,
            latency_p90_ns,
            latency_p99_ns,
            latency_avg_ns,
            last: Mutex::new(CountersSnapshot::default()),
        })
    }

    /// Creates an exporter on the default global [`Registry`].
    pub fn with_default_registry() -> Result<Self, prometheus::Error> {
        Self::new(prometheus::default_registry().clone())
    }

    /// Returns a reference to the underlying [`Registry`].
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Encodes all registered metrics in Prometheus text format.
    pub fn encode_text(&self) -> Result<String, prometheus::Error> {
        use prometheus::Encoder;
        let metric_families = self.registry.gather();
        let mut buf = Vec::new();
        let encoder = prometheus::TextEncoder::new();
        encoder.encode(&metric_families, &mut buf)?;
        String::from_utf8(buf)
            .map_err(|_| prometheus::Error::Msg("metric output was not UTF-8".into()))
    }
}

impl MetricsExporter for PrometheusExporter {
    fn export(&self, snapshot: &StatsSnapshot) {
        // Update counters via delta.
        let mut last = self.last.lock();
        apply_delta(&self.hits, &mut last.hits, snapshot.counters.hits);
        apply_delta(&self.misses, &mut last.misses, snapshot.counters.misses);
        apply_delta(&self.reuses, &mut last.reuses, snapshot.counters.reuses);
        apply_delta(
            &self.allocations,
            &mut last.allocations,
            snapshot.counters.allocations,
        );
        apply_delta(&self.frees, &mut last.frees, snapshot.counters.frees);
        apply_delta(
            &self.bytes_allocated,
            &mut last.bytes_allocated,
            snapshot.counters.bytes_allocated,
        );
        apply_delta(
            &self.bytes_freed,
            &mut last.bytes_freed,
            snapshot.counters.bytes_freed,
        );

        // Update gauges.
        self.outstanding.set(snapshot.counters.outstanding as i64);
        self.peak_outstanding
            .set(snapshot.counters.peak_outstanding as i64);
        self.bytes_outstanding
            .set(snapshot.counters.bytes_outstanding as i64);
        self.arena_bytes.set(snapshot.arena_bytes as i64);
        self.cached_bytes.set(snapshot.cached_bytes as i64);

        // Latency percentiles.
        self.latency_p50_ns
            .set(snapshot.latency.percentiles.p50_ns as i64);
        self.latency_p90_ns
            .set(snapshot.latency.percentiles.p90_ns as i64);
        self.latency_p99_ns
            .set(snapshot.latency.percentiles.p99_ns as i64);
        self.latency_avg_ns.set(snapshot.latency.average_ns as i64);
    }
}

/// Increments `counter` by `current - *last`, treating a reset (current < last)
/// as a fresh start.
fn apply_delta(counter: &IntCounter, last: &mut u64, current: u64) {
    if current >= *last {
        counter.inc_by(current - *last);
    } else {
        counter.inc_by(current);
    }
    *last = current;
}
