//! [`MethodDispatcher`] — RPC method routing facade (typed skeleton).
//!
//! This is a **Phase 0 skeleton**. The full implementation lands in
//! Phase 1+.
//!
//! See `DESIGN.md` Part 11 (dispatcher).

use crate::config::RpcConfig;
use crate::error::RpcError;
use crate::handle::StreamHandle;
use crate::health::HealthReport;
use crate::method::RpcMethodId;
use crate::stats::RpcStats;
use std::fmt;

/// RPC method dispatcher.
///
/// In Phase 0, this is a typed skeleton. Phase 1+ will include:
/// - Method registry lookup
/// - Request routing to handlers
/// - Load balancing across handlers
/// - Dispatcher-level statistics
pub struct MethodDispatcher {
    /// Dispatcher configuration.
    #[allow(dead_code)]
    cfg: RpcConfig,
    /// Dispatcher statistics.
    #[allow(dead_code)]
    stats: RpcStats,
}

impl fmt::Debug for MethodDispatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MethodDispatcher")
            .field("cfg", &self.cfg)
            .field("stats", &"<stats>")
            .finish()
    }
}

impl MethodDispatcher {
    /// Create a new method dispatcher with the given config.
    #[must_use]
    pub fn new(cfg: RpcConfig) -> Self {
        Self {
            cfg,
            stats: RpcStats::new(),
        }
    }

    /// Dispatch a request to the registered handler.
    ///
    /// # Errors
    /// Returns [`RpcError::ShuttingDown`] if the dispatcher is shutting down.
    ///
    /// # Phase 0
    /// This is a skeleton implementation.
    pub fn dispatch(&self, _id: RpcMethodId, _handle: StreamHandle) -> Result<(), RpcError> {
        Err(RpcError::ShuttingDown)
    }

    /// Route a method ID to its handler.
    ///
    /// # Errors
    /// Returns [`RpcError::MethodNotFound`] if the method is not registered.
    ///
    /// # Phase 0
    /// This is a skeleton implementation.
    pub fn route(&self, id: RpcMethodId) -> Result<(), RpcError> {
        Err(RpcError::MethodNotFound(id))
    }

    /// Get the current health report.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        HealthReport::derive(&self.stats.snapshot())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatcher_creation() {
        let cfg = RpcConfig::new();
        let dispatcher = MethodDispatcher::new(cfg);
        assert_eq!(dispatcher.stats.snapshot().calls_total, 0);
    }

    #[test]
    fn dispatch_returns_shutting_down() {
        let dispatcher = MethodDispatcher::new(RpcConfig::new());
        let result = dispatcher.dispatch(RpcMethodId::new(1), StreamHandle::new(1));
        assert!(matches!(result, Err(RpcError::ShuttingDown)));
    }

    #[test]
    fn route_returns_method_not_found() {
        let dispatcher = MethodDispatcher::new(RpcConfig::new());
        let id = RpcMethodId::new(42);
        let result = dispatcher.route(id);
        assert!(matches!(result, Err(RpcError::MethodNotFound(found)) if found == id));
    }

    #[test]
    fn health_report_initially_healthy() {
        let dispatcher = MethodDispatcher::new(RpcConfig::new());
        let health = dispatcher.health();
        assert!(health.is_healthy());
        assert_eq!(health.calls_total, 0);
    }
}
