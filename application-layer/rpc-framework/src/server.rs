//! [`RpcServer`] — server-side RPC facade.
//!
//! Phase 1: in-memory method registry. `handle()` echoes the request
//! payload back as the response (real handler dispatch lands later).

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;

use crate::config::RpcConfig;
use crate::error::RpcError;
use crate::health::HealthReport;
use crate::method::{RpcMethodId, RpcMethodType};
use crate::stats::RpcStats;

/// Server-side RPC facade.
///
/// Owns the in-memory method registry and atomic statistics.
pub struct RpcServer {
    /// Server configuration.
    #[allow(dead_code)]
    cfg: RpcConfig,
    /// RPC statistics.
    stats: RpcStats,
    /// Registered methods (method id → type).
    methods: HashMap<RpcMethodId, RpcMethodType>,
}

impl fmt::Debug for RpcServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RpcServer")
            .field("cfg", &self.cfg)
            .field("stats", &"<stats>")
            .field("registered_methods", &self.methods.len())
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
            methods: HashMap::new(),
        }
    }

    /// Register an RPC method.
    ///
    /// Re-registering an existing id updates its method type (idempotent).
    pub fn register_method(
        &mut self,
        id: RpcMethodId,
        mtype: RpcMethodType,
    ) -> Result<(), RpcError> {
        self.methods.insert(id, mtype);
        Ok(())
    }

    /// Handle an incoming RPC request.
    ///
    /// Phase 1 behaviour: bumps the call counter and echoes the request
    /// payload back as the response. Real handler dispatch is a later
    /// phase; this is enough to exercise the full request/response flow
    /// end-to-end.
    ///
    /// The echo case returns [`Cow::Borrowed`] — zero-allocation, zero-copy.
    /// Future real handlers that compute a response will return
    /// [`Cow::Owned`], which converts to `bytes::Bytes` without copying.
    pub fn handle<'a>(&self, request: &'a [u8]) -> Result<Cow<'a, [u8]>, RpcError> {
        self.stats.record_bytes_received(request.len() as u64);
        self.stats.record_bytes_sent(request.len() as u64);
        self.stats.record_call_success();
        Ok(Cow::Borrowed(request))
    }

    /// Returns the number of registered methods.
    #[must_use]
    pub fn method_count(&self) -> usize {
        self.methods.len()
    }

    /// Returns `true` if a method with the given id is registered.
    #[must_use]
    pub fn has_method(&self, id: RpcMethodId) -> bool {
        self.methods.contains_key(&id)
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
        let server = RpcServer::new(RpcConfig::new());
        assert_eq!(server.stats.snapshot().calls_total, 0);
        assert_eq!(server.method_count(), 0);
    }

    #[test]
    fn register_method_succeeds() {
        let mut server = RpcServer::new(RpcConfig::new());
        server
            .register_method(RpcMethodId::new(1), RpcMethodType::Unary)
            .unwrap();
        assert!(server.has_method(RpcMethodId::new(1)));
        assert_eq!(server.method_count(), 1);
    }

    #[test]
    fn register_method_overwrites() {
        let mut server = RpcServer::new(RpcConfig::new());
        server
            .register_method(RpcMethodId::new(1), RpcMethodType::Unary)
            .unwrap();
        server
            .register_method(RpcMethodId::new(1), RpcMethodType::ServerStreaming)
            .unwrap();
        assert_eq!(server.method_count(), 1);
    }

    #[test]
    fn handle_echoes_request_and_counts() {
        let server = RpcServer::new(RpcConfig::new());
        let req = b"ping";
        let resp = server.handle(req).unwrap();
        assert_eq!(&*resp, req);
        let snap = server.stats.snapshot();
        assert_eq!(snap.calls_total, 1);
        assert_eq!(snap.calls_succeeded, 1);
        assert_eq!(snap.bytes_received, 4);
        assert_eq!(snap.bytes_sent, 4);
    }

    #[test]
    fn health_report_initially_healthy() {
        let server = RpcServer::new(RpcConfig::new());
        let health = server.health();
        assert!(health.is_healthy());
        assert_eq!(health.calls_total, 0);
    }
}
