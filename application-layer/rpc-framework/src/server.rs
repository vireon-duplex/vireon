//! [`RpcServer`] — server-side RPC facade (typed skeleton).
//!
//! This is a **Phase 0 skeleton**. The full implementation lands in
//! Phase 1+.
//!
//! See `DESIGN.md` Part 9 (server).

use crate::config::RpcConfig;
use crate::error::RpcError;
use crate::health::HealthReport;
use crate::method::{RpcMethodId, RpcMethodType};
use crate::stats::RpcStats;
use std::fmt;

/// Server-side RPC facade.
///
/// In Phase 0, this is a typed skeleton. Phase 1+ will include:
/// - Method registration table
/// - Request handler spawning
/// - Stream lifecycle management
/// - Per-method statistics
pub struct RpcServer {
    /// Server configuration.
    #[allow(dead_code)]
    cfg: RpcConfig,
    /// RPC statistics.
    #[allow(dead_code)]
    stats: RpcStats,
}

impl fmt::Debug for RpcServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RpcServer")
            .field("cfg", &self.cfg)
            .field("stats", &"<stats>")
            .finish()
    }
}

impl RpcServer {
    /// Create a new RPC server with the given config.
    #[must_use]
    pub fn new(cfg: RpcConfig) -> Self {
        Self {
            cfg,
            stats: RpcStats::new(),
        }
    }

    /// Register an RPC method.
    ///
    /// # Errors
    /// Returns [`RpcError::ShuttingDown`] if the server is shutting down.
    ///
    /// # Phase 0
    /// This is a skeleton implementation.
    pub fn register_method(
        &mut self,
        _id: RpcMethodId,
        _mtype: RpcMethodType,
    ) -> Result<(), RpcError> {
        Err(RpcError::ShuttingDown)
    }

    /// Handle an incoming RPC request.
    ///
    /// # Errors
    /// Returns [`RpcError::ShuttingDown`] if the server is shutting down.
    ///
    /// # Phase 0
    /// This is a skeleton implementation.
    pub fn handle(&self, _request: &[u8]) -> Result<Vec<u8>, RpcError> {
        Err(RpcError::ShuttingDown)
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
    fn server_creation() {
        let cfg = RpcConfig::new();
        let server = RpcServer::new(cfg);
        assert_eq!(server.stats.snapshot().calls_total, 0);
    }

    #[test]
    fn register_method_returns_shutting_down() {
        let mut server = RpcServer::new(RpcConfig::new());
        let result = server.register_method(RpcMethodId::new(1), RpcMethodType::Unary);
        assert!(matches!(result, Err(RpcError::ShuttingDown)));
    }

    #[test]
    fn handle_returns_shutting_down() {
        let server = RpcServer::new(RpcConfig::new());
        let result = server.handle(&[]);
        assert!(matches!(result, Err(RpcError::ShuttingDown)));
    }

    #[test]
    fn health_report_initially_healthy() {
        let server = RpcServer::new(RpcConfig::new());
        let health = server.health();
        assert!(health.is_healthy());
        assert_eq!(health.calls_total, 0);
    }
}
