//! # observability-exporters
//!
//! Prometheus text format encoder + [`Collector`] trait for Vireon metrics.
//!
//! Phase 8 introduces a unified export path. The model:
//!
//! * Each crate that wants to expose metrics implements [`Collector`]
//!   for a small adapter struct (or uses [`ClosureCollector`]).
//! * A [`PrometheusRegistry`] holds `Box<dyn Collector>`s and pulls on
//!   [`PrometheusRegistry::scrape`].
//! * The encoder ([`encode_prometheus`]) turns a `&[Metric]` into the
//!   `text/plain; version=0.0.4` exposition format.
//!
//! This crate deliberately ships **no HTTP server**. How bytes get onto
//! the wire (HTTP, QUIC frame, admin API, file) is the operator's
//! choice.
//!
//! # Example
//!
//! ```
//! use observability_exporters::{
//!     ClosureCollector, PrometheusRegistry, encode_prometheus,
//!     Metric, Sample,
//! };
//! use std::sync::atomic::{AtomicU64, Ordering};
//! use std::sync::Arc;
//!
//! let requests = Arc::new(AtomicU64::new(0));
//! let r_clone = Arc::clone(&requests);
//!
//! let mut registry = PrometheusRegistry::new();
//! registry.register(ClosureCollector::new("requests", move || {
//!     vec![Metric::counter(
//!         "requests_total",
//!         "Total requests handled.",
//!         vec![Sample::unlabeled(r_clone.load(Ordering::Relaxed) as f64)],
//!     ).unwrap()]
//! }));
//!
//! requests.fetch_add(7, Ordering::Relaxed);
//!
//! let text = registry.scrape_text();
//! assert!(text.contains("requests_total 7"));
//! ```

pub mod collector;
pub mod encoder;
pub mod error;
pub mod escape;
pub mod metric;
pub mod registry;

// ── Re-exports ────────────────────────────────────────────────────

pub use collector::{ClosureCollector, Collector};
pub use encoder::encode_prometheus;
pub use error::ExporterError;
pub use metric::{Label, Metric, MetricKind, Sample};
pub use registry::PrometheusRegistry;
