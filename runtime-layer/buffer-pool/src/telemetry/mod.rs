//! Telemetry subsystem (Layer 15).
//!
//! - [`metrics`] — the [`MetricsExporter`] trait and a no-op default.
//! - [`prometheus`] — Prometheus backend (feature `prometheus`).
//! - [`otel`] — OpenTelemetry backend (feature `otel`).

pub mod metrics;

#[cfg(feature = "prometheus")]
pub mod prometheus;

#[cfg(feature = "otel")]
pub mod otel;

pub use metrics::{MetricsExporter, NoopExporter};

#[cfg(feature = "prometheus")]
pub use prometheus::PrometheusExporter;

#[cfg(feature = "otel")]
pub use otel::OtelCollector;
