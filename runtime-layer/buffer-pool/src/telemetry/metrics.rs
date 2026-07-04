//! Telemetry export framework (Layer 15).
//!
//! [`MetricsExporter`] is the trait every backend implements. The default
//! [`NoopExporter`] discards all data; feature-gated backends live in the
//! [`prometheus`](crate::telemetry::prometheus) and
//! [`otel`](crate::telemetry::otel) submodules.

use crate::stats::StatsSnapshot;

/// Publishes pool statistics to an external system.
///
/// Implementations are called with a fresh [`StatsSnapshot`] periodically (by
/// the maintenance worker) or on demand. The export must be non-blocking —
/// heavy work should be batched or deferred.
pub trait MetricsExporter: Send + Sync {
    /// Publishes a snapshot of the pool's current statistics.
    fn export(&self, snapshot: &StatsSnapshot);
}

/// A no-op exporter that discards every snapshot.
///
/// This is the default when no telemetry feature is enabled.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopExporter;

impl MetricsExporter for NoopExporter {
    fn export(&self, _snapshot: &StatsSnapshot) {
        // intentionally empty
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::StatsSnapshot;

    #[test]
    fn noop_exporter_does_not_panic() {
        let exporter = NoopExporter;
        let snapshot = StatsSnapshot::default();
        exporter.export(&snapshot);
    }
}
