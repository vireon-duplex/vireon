//! [`RpcClient`] — client-side RPC facade (typed skeleton).
//!
//! This is a **Phase 0 skeleton**. The full implementation lands in
//! Phase 1+.
//!
//! See `DESIGN.md` Part 10 (client).

use crate::config::RpcConfig;
use crate::error::RpcError;
use crate::handle::StreamHandle;
use crate::health::HealthReport;
use crate::method::{RpcMethodId, RpcMethodType};
use crate::stats::RpcStats;
use std::fmt;

/// Client-side RPC facade.
///
/// In Phase 0, this is a typed skeleton. Phase 1+ will include:
/// - Connection pooling
/// - Unary call support
/// - Streaming call support
/// - Timeout and cancellation handling
/// - Retry logic for retryable errors
pub struct RpcClient {
    /// Client configuration.
    #[allow(dead_code)]
    cfg: RpcConfig,
    /// RPC statistics.
    #[allow(dead_code)]
    stats: RpcStats,
}

impl fmt::Debug for RpcClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RpcClient")
            .field("cfg", &self.cfg)
            .field("stats", &"<stats>")
            .finish()
    }
}

impl RpcClient {
    /// Create a new RPC client with the given config.
    #[must_use]
    pub fn new(cfg: RpcConfig) -> Self {
        Self {
            cfg,
            stats: RpcStats::new(),
        }
    }

    /// Invoke a unary RPC method.
    ///
    /// # Errors
    /// Returns [`RpcError::ShuttingDown`] if the client is shutting down.
    ///
    /// # Phase 0
    /// This is a skeleton implementation.
    pub fn call(&self, _id: RpcMethodId, _request: &[u8]) -> Result<Vec<u8>, RpcError> {
        Err(RpcError::ShuttingDown)
    }

    /// Open a streaming RPC stream.
    ///
    /// # Errors
    /// Returns [`RpcError::ShuttingDown`] if the client is shutting down.
    ///
    /// # Phase 0
    /// This is a skeleton implementation.
    pub fn open_stream(
        &self,
        _id: RpcMethodId,
        _mtype: RpcMethodType,
    ) -> Result<StreamHandle, RpcError> {
        Err(RpcError::ShuttingDown)
    }

    /// Cancel an active RPC stream.
    ///
    /// # Errors
    /// Returns [`RpcError::ShuttingDown`] if the client is shutting down.
    ///
    /// # Phase 0
    /// This is a skeleton implementation.
    pub fn cancel(&self, _handle: StreamHandle) -> Result<(), RpcError> {
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
    fn client_creation() {
        let cfg = RpcConfig::new();
        let client = RpcClient::new(cfg);
        assert_eq!(client.stats.snapshot().calls_total, 0);
    }

    #[test]
    fn call_returns_shutting_down() {
        let client = RpcClient::new(RpcConfig::new());
        let result = client.call(RpcMethodId::new(1), &[]);
        assert!(matches!(result, Err(RpcError::ShuttingDown)));
    }

    #[test]
    fn open_stream_returns_shutting_down() {
        let client = RpcClient::new(RpcConfig::new());
        let result = client.open_stream(RpcMethodId::new(1), RpcMethodType::Unary);
        assert!(matches!(result, Err(RpcError::ShuttingDown)));
    }

    #[test]
    fn cancel_returns_shutting_down() {
        let client = RpcClient::new(RpcConfig::new());
        let result = client.cancel(StreamHandle::new(1));
        assert!(matches!(result, Err(RpcError::ShuttingDown)));
    }

    #[test]
    fn health_report_initially_healthy() {
        let client = RpcClient::new(RpcConfig::new());
        let health = client.health();
        assert!(health.is_healthy());
        assert_eq!(health.calls_total, 0);
    }
}
