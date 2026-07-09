//! [`DashboardWidgetId`], [`WidgetType`] — widget identification and
//! classification types for the ops-dashboard.
//!
//! See `DESIGN.md` Part 5 (widgets).
//!
//! # Design
//!
//! - `DashboardWidgetId`: 32-bit identifier for a dashboard widget.
//! - `WidgetType`: Classification of widget rendering strategy
//!   (chart, table, gauge, log, alert).

// ── DashboardWidgetId ───────────────────────────────────────────

/// A dashboard widget identifier (32-bit).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct DashboardWidgetId(u32);

impl DashboardWidgetId {
    /// The nil/unset widget ID (zero).
    pub const NIL: Self = Self(0);

    /// Construct a new widget ID from a u32.
    #[must_use]
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    /// Construct the nil widget ID.
    #[must_use]
    pub const fn nil() -> Self {
        Self::NIL
    }

    /// Whether this widget ID is nil.
    #[must_use]
    pub const fn is_nil(&self) -> bool {
        self.0 == 0
    }

    /// Get the raw u32 value.
    #[must_use]
    pub const fn raw(&self) -> u32 {
        self.0
    }
}

// ── WidgetType ──────────────────────────────────────────────────

/// The type/classification of a dashboard widget.
///
/// Determines how the widget is rendered and whether it requires
/// streaming data.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum WidgetType {
    /// Chart widget: time-series graph, bar chart, etc.
    Chart = 0,
    /// Table widget: tabular data display.
    Table = 1,
    /// Gauge widget: single-value gauge or meter.
    Gauge = 2,
    /// Log widget: streaming log viewer.
    Log = 3,
    /// Alert widget: active alert display.
    Alert = 4,
}

impl WidgetType {
    /// Whether this widget type requires streaming data updates.
    ///
    /// Streaming widgets (Log, Alert) push data continuously rather
    /// than polling on a refresh interval.
    #[must_use]
    pub const fn is_streaming(&self) -> bool {
        matches!(self, Self::Log | Self::Alert)
    }

    /// Human-readable name for the widget type.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Chart => "chart",
            Self::Table => "table",
            Self::Gauge => "gauge",
            Self::Log => "log",
            Self::Alert => "alert",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nil_widget_id() {
        let id = DashboardWidgetId::nil();
        assert!(id.is_nil());
        assert_eq!(id.raw(), 0);
    }

    #[test]
    fn new_widget_id() {
        let id = DashboardWidgetId::new(42);
        assert!(!id.is_nil());
        assert_eq!(id.raw(), 42);
    }

    #[test]
    fn widget_id_copy() {
        let a = DashboardWidgetId::new(777);
        let b = a;
        assert_eq!(a, b);
        assert_eq!(b.raw(), 777);
    }

    #[test]
    fn nil_constant_is_zero() {
        assert_eq!(DashboardWidgetId::NIL.raw(), 0);
        assert!(DashboardWidgetId::NIL.is_nil());
    }

    #[test]
    fn widget_type_repr_values() {
        assert_eq!(WidgetType::Chart as u8, 0);
        assert_eq!(WidgetType::Table as u8, 1);
        assert_eq!(WidgetType::Gauge as u8, 2);
        assert_eq!(WidgetType::Log as u8, 3);
        assert_eq!(WidgetType::Alert as u8, 4);
    }

    #[test]
    fn streaming_widgets() {
        assert!(WidgetType::Log.is_streaming());
        assert!(WidgetType::Alert.is_streaming());
        assert!(!WidgetType::Chart.is_streaming());
        assert!(!WidgetType::Table.is_streaming());
        assert!(!WidgetType::Gauge.is_streaming());
    }

    #[test]
    fn widget_type_names() {
        assert_eq!(WidgetType::Chart.as_str(), "chart");
        assert_eq!(WidgetType::Table.as_str(), "table");
        assert_eq!(WidgetType::Gauge.as_str(), "gauge");
        assert_eq!(WidgetType::Log.as_str(), "log");
        assert_eq!(WidgetType::Alert.as_str(), "alert");
    }

    #[test]
    fn const_comparison_works() {
        const A: DashboardWidgetId = DashboardWidgetId::new(1);
        const B: DashboardWidgetId = DashboardWidgetId::new(2);
        assert!(A.raw() < B.raw());
    }
}
