//! # observability-sdk
//!
//! The **Observability SDK** for the Vireon QUIC runtime. Provides
//! metrics (counter/gauge/histogram), distributed tracing (trace/span IDs),
//! and structured logging for application-layer observability.
//!
//! # Architecture (one-line)
//!
//! ```text
//! Application ─► [Observability SDK] ─► Export Pipeline
//!                     │
//!                     ├─ MetricsRegistry  (metric registration & recording)
//!                     ├─ Tracer          (span lifecycle management)
//!                     ├─ MetricsExporter (batch export to backend)
//!                     ├─ LogLevel         (structured severity levels)
//!                     └─ ObservabilityStats (atomic counters)
//! ```
//!
//! See `DESIGN.md` for the full architecture specification.
//!
//! # Layer map
//!
//! | #  | Module          | Phase | Responsibility |
//! |----|-----------------|-------|----------------|
//! | 1  | [`error`]       | 0 ✅  | `ObservabilityError` thiserror enum |
//! | 2  | [`config`]      | 0 ✅  | `ObservabilityConfig`, `Preset` |
//! | 3  | [`metric`]      | 0 ✅  | `MetricKey`, `MetricType`, `MetricValue` |
//! | 4  | [`trace`]       | 0 ✅  | `TraceId`, `SpanId`, `SpanKind` |
//! | 5  | [`log`]         | 0 ✅  | `LogLevel`, `LogRecord` |
//! | 6  | [`stats`]       | 0 ✅  | `ObservabilityStats` atomics, `StatsSnapshot` |
//! | 7  | [`health`]      | 0 ✅  | `HealthReport`, `HealthState` |
//! | 8  | [`builder`]     | 0 ✅  | `ObservabilityBuilder` (fluent) |
//! | 9  | [`exporter`]    | 1    | `MetricsExporter` skeleton |
//! | 10 | [`registry`]    | 1    | `MetricsRegistry` skeleton |
//! | 11 | [`tracer`]      | 1    | `Tracer` skeleton |
//!
//! # Phase 0 status
//!
//! Modules 1–8 are real implementations. Modules 9–11 ship as
//! typed skeletons. Functional registry + exporter + tracer land in Phase 1.

#![allow(clippy::module_inception)]

pub mod builder;
pub mod config;
pub mod error;
pub mod exporter;
pub mod health;
pub mod log;
pub mod metric;
pub mod registry;
pub mod stats;
pub mod trace;
pub mod tracer;

// ── Re-exports ────────────────────────────────────────────────────

pub use builder::ObservabilityBuilder;
pub use config::{ObservabilityConfig, Preset};
pub use error::ObservabilityError;
pub use health::{HealthReport, HealthState};
pub use log::{LogLevel, LogRecord};
pub use metric::{MetricKey, MetricType, MetricValue};
pub use stats::{ObservabilityStats, StatsSnapshot};
pub use trace::{SpanId, SpanKind, TraceId};

// Re-export key adjacent-layer types for convenience.
pub use session_manager::SessionId;
