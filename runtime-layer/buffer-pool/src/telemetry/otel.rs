//! OpenTelemetry metrics collector (Layer 15).
//!
//! Requires the `otel` cargo feature.
//!
//! [`OtelCollector`] uses the OpenTelemetry metrics API to emit pool
//! statistics. It does **not** create its own `MeterProvider` — callers are
//! expected to install one (e.g. via `opentelemetry_sdk::init`) before
//! constructing the collector.

use opentelemetry::metrics::{Counter, Gauge, Meter};

use crate::stats::StatsSnapshot;
use crate::telemetry::metrics::MetricsExporter;

/// OpenTelemetry-backed metrics collector.
pub struct OtelCollector {
    // Counters (monotone).
    hits: Counter<u64>,
    misses: Counter<u64>,
    reuses: Counter<u64>,
    allocations: Counter<u64>,
    frees: Counter<u64>,
    bytes_allocated: Counter<u64>,
    bytes_freed: Counter<u64>,

    // Gauges.
    outstanding: Gauge<u64>,
    peak_outstanding: Gauge<u64>,
    bytes_outstanding: Gauge<u64>,
    arena_bytes: Gauge<u64>,
    cached_bytes: Gauge<u64>,
    latency_p50_ns: Gauge<u64>,
    latency_p90_ns: Gauge<u64>,
    latency_p99_ns: Gauge<u64>,
    latency_avg_ns: Gauge<u64>,
}

impl OtelCollector {
    /// Creates a collector using the given [`Meter`].
    ///
    /// Use [`opentelemetry::global::meter`] to obtain a meter from the
    /// globally-installed provider.
    pub fn new(meter: Meter) -> Self {
        Self {
            hits: meter
                .u64_counter("buffer_pool.hits")
                .with_description("Thread-local cache hits")
                .build(),
            misses: meter
                .u64_counter("buffer_pool.misses")
                .with_description("Thread-local cache misses")
                .build(),
            reuses: meter
                .u64_counter("buffer_pool.reuses")
                .with_description("Global-cache reuses")
                .build(),
            allocations: meter
                .u64_counter("buffer_pool.allocations")
                .with_description("Fresh allocations from the arena")
                .build(),
            frees: meter
                .u64_counter("buffer_pool.frees")
                .with_description("Total releases")
                .build(),
            bytes_allocated: meter
                .u64_counter("buffer_pool.bytes_allocated")
                .with_description("Total bytes freshly allocated")
                .build(),
            bytes_freed: meter
                .u64_counter("buffer_pool.bytes_freed")
                .with_description("Total bytes released")
                .build(),

            outstanding: meter
                .u64_gauge("buffer_pool.outstanding")
                .with_description("Currently live buffers")
                .build(),
            peak_outstanding: meter
                .u64_gauge("buffer_pool.peak_outstanding")
                .with_description("Peak live buffer count")
                .build(),
            bytes_outstanding: meter
                .u64_gauge("buffer_pool.bytes_outstanding")
                .with_description("Bytes in currently-live buffers")
                .build(),
            arena_bytes: meter
                .u64_gauge("buffer_pool.arena_bytes")
                .with_description("Memory retained by the arena")
                .build(),
            cached_bytes: meter
                .u64_gauge("buffer_pool.cached_bytes")
                .with_description("Memory held in caches")
                .build(),
            latency_p50_ns: meter
                .u64_gauge("buffer_pool.latency_p50_ns")
                .with_description("Acquire latency p50 (ns)")
                .build(),
            latency_p90_ns: meter
                .u64_gauge("buffer_pool.latency_p90_ns")
                .with_description("Acquire latency p90 (ns)")
                .build(),
            latency_p99_ns: meter
                .u64_gauge("buffer_pool.latency_p99_ns")
                .with_description("Acquire latency p99 (ns)")
                .build(),
            latency_avg_ns: meter
                .u64_gauge("buffer_pool.latency_avg_ns")
                .with_description("Acquire latency average (ns)")
                .build(),
        }
    }

    /// Convenience constructor using the global meter provider.
    pub fn with_global_meter() -> Self {
        Self::new(opentelemetry::global::meter("buffer-pool"))
    }
}

impl MetricsExporter for OtelCollector {
    fn export(&self, snapshot: &StatsSnapshot) {
        let c = &snapshot.counters;

        self.hits.add(c.hits, &[]);
        self.misses.add(c.misses, &[]);
        self.reuses.add(c.reuses, &[]);
        self.allocations.add(c.allocations, &[]);
        self.frees.add(c.frees, &[]);
        self.bytes_allocated.add(c.bytes_allocated, &[]);
        self.bytes_freed.add(c.bytes_freed, &[]);

        self.outstanding.record(c.outstanding, &[]);
        self.peak_outstanding.record(c.peak_outstanding, &[]);
        self.bytes_outstanding.record(c.bytes_outstanding, &[]);
        self.arena_bytes.record(snapshot.arena_bytes, &[]);
        self.cached_bytes.record(snapshot.cached_bytes, &[]);

        self.latency_p50_ns
            .record(snapshot.latency.percentiles.p50_ns, &[]);
        self.latency_p90_ns
            .record(snapshot.latency.percentiles.p90_ns, &[]);
        self.latency_p99_ns
            .record(snapshot.latency.percentiles.p99_ns, &[]);
        self.latency_avg_ns.record(snapshot.latency.average_ns, &[]);
    }
}
