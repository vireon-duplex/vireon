//! # ops-dashboard
//!
//! The **Operations Dashboard** for the Vireon QUIC runtime. Provides
//! widget rendering, metrics collection, and health monitoring for
//! real-time operational visibility.
//!
//! # Architecture (one-line)
//!
//! ```text
//!   WidgetRequest ──► [OpsDashboardService] ──► RenderedWidget
//!                         │
//!                         ├─ OpsDashboardService  (widget rendering & refresh)
//!                         ├─ MetricsCollector     (metric collection & queries)
//!                         ├─ DashboardWidgetId    (widget identification)
//!                         ├─ QueryRange           (time-range queries)
//!                         ├─ OpsDashboardStats    (atomic counters)
//!                         └─ HealthReport         (health derivation)
//! ```
//!
//! See `DESIGN.md` for the full architecture specification.
//!
//! # Layer map
//!
//! | #  | Module        | Phase | Responsibility |
//! |----|---------------|-------|----------------|
//! | 1  | [`error`]     | 0 ✅  | `OpsDashboardError` thiserror enum |
//! | 2  | [`config`]    | 0 ✅  | `OpsDashboardConfig`, `Preset` |
//! | 3  | [`widget`]    | 0 ✅  | `DashboardWidgetId`, `WidgetType` |
//! | 4  | [`query`]     | 0 ✅  | `QueryRange`, `QueryAggregation` |
//! | 5  | [`stats`]     | 0 ✅  | `OpsDashboardStats` atomics, `StatsSnapshot` |
//! | 6  | [`health`]    | 0 ✅  | `HealthReport`, `HealthState` |
//! | 7  | [`builder`]   | 0 ✅  | `OpsDashboardBuilder` (fluent) |
//! | 8  | [`service`]   | 1    | `OpsDashboardService` facade |
//! | 9  | [`collector`] | 1    | `MetricsCollector` facade |
//!
//! # Phase 0 status
//!
//! Modules 1–7 are real implementations. Modules 8–9 ship as typed
//! skeletons. Functional service + collector land in Phase 1.

#![allow(clippy::module_inception)]

pub mod builder;
pub mod collector;
pub mod config;
pub mod error;
pub mod health;
pub mod query;
pub mod service;
pub mod stats;
pub mod widget;

// ── Re-exports ────────────────────────────────────────────────────

pub use builder::OpsDashboardBuilder;
pub use collector::MetricsCollector;
pub use config::{OpsDashboardConfig, Preset};
pub use error::OpsDashboardError;
pub use health::{HealthReport, HealthState};
pub use query::{QueryAggregation, QueryRange};
pub use service::OpsDashboardService;
pub use stats::{OpsDashboardStats, StatsSnapshot};
pub use widget::{DashboardWidgetId, WidgetType};

// Re-export key adjacent-layer types for convenience.
pub use configuration_system::{ConfigKey, ConfigSource, ConfigValue};
pub use observability_sdk::{
    HealthReport as ObservabilityHealthReport, HealthState as ObservabilityHealthState, LogLevel,
    LogRecord, MetricKey, MetricType, MetricValue, ObservabilityStats as ObservabilityStatsInner,
    SpanId, SpanKind, TraceId,
};
pub use service_registry::{
    NodeId, NodeInfo, NodeState, Region, ServiceEndpoint, ServiceId, ServiceInfo, ServiceState,
    Zone,
};
