//! [`MethodDispatcher`] — RPC method routing with handler dispatch.
//!
//! Stores a registry of `RpcMethodId → RpcHandler` pairs and dispatches
//! incoming requests to the matching handler. Unregistered methods fall
//! through to an optional fallback handler (defaults to echo).
//!
//! ```text
//! register(1, echo_handler)   → id=1 maps to echo_handler
//! register(2, upper_handler)  → id=2 maps to upper_handler
//! dispatch(2, b"hello")       → upper_handler(b"hello") = Ok(b"HELLO")
//! dispatch(3, b"hello")       → Err(MethodNotFound(3))
//! ```

use crate::config::RpcConfig;
use crate::error::RpcError;
use crate::health::HealthReport;
use crate::method::RpcMethodId;
use crate::stats::RpcStats;
use rustc_hash::FxHashMap;
use std::fmt;
use std::sync::Arc;

/// Handler function type: receives the request payload, returns response bytes.
pub type RpcHandler = Arc<dyn Fn(&[u8]) -> Result<Vec<u8>, RpcError> + Send + Sync>;

/// RPC method dispatcher.
///
/// Owns the method→handler registry and dispatches incoming requests.
/// Thread-safe via `Arc` handler storage.
pub struct MethodDispatcher {
    #[allow(dead_code)]
    cfg: RpcConfig,
    stats: RpcStats,
    handlers: FxHashMap<RpcMethodId, RpcHandler>,
}

impl fmt::Debug for MethodDispatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MethodDispatcher")
            .field("cfg", &self.cfg)
            .field("registered_handlers", &self.handlers.len())
            .finish()
    }
}

impl MethodDispatcher {
    /// Creates a new method dispatcher with the given config.
    #[must_use]
    pub fn new(cfg: RpcConfig) -> Self {
        Self {
            cfg,
            stats: RpcStats::new(),
            handlers: FxHashMap::default(),
        }
    }

    /// Registers a handler for the given method ID.
    ///
    /// Re-registering an existing ID replaces the handler (idempotent).
    pub fn register(&mut self, id: RpcMethodId, handler: RpcHandler) {
        self.handlers.insert(id, handler);
    }

    /// Returns `true` if a handler is registered for the given method ID.
    #[must_use]
    pub fn has_handler(&self, id: RpcMethodId) -> bool {
        self.handlers.contains_key(&id)
    }

    /// Dispatches a request to the registered handler.
    ///
    /// Records call statistics (success/failure, bytes in/out).
    ///
    /// # Errors
    /// - [`RpcError::MethodNotFound`] — no handler registered for `id`.
    /// - Any error from the handler itself.
    pub fn dispatch(&self, id: RpcMethodId, request: &[u8]) -> Result<Vec<u8>, RpcError> {
        let handler = self.handlers.get(&id).ok_or(RpcError::MethodNotFound(id))?;

        self.stats.record_bytes_received(request.len() as u64);
        match handler(request) {
            Ok(response) => {
                self.stats.record_bytes_sent(response.len() as u64);
                self.stats.record_call_success();
                Ok(response)
            }
            Err(e) => {
                self.stats.record_call_failure();
                Err(e)
            }
        }
    }

    /// Returns the number of registered handlers.
    #[must_use]
    pub fn handler_count(&self) -> usize {
        self.handlers.len()
    }

    /// Get the current health report.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        HealthReport::derive(&self.stats.snapshot())
    }
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatcher_creation() {
        let dispatcher = MethodDispatcher::new(RpcConfig::new());
        assert_eq!(dispatcher.handler_count(), 0);
    }

    #[test]
    fn register_and_dispatch() {
        let mut dispatcher = MethodDispatcher::new(RpcConfig::new());
        let handler: RpcHandler = Arc::new(|req: &[u8]| Ok(req.to_vec()));
        dispatcher.register(RpcMethodId::new(1), handler);

        let resp = dispatcher.dispatch(RpcMethodId::new(1), b"hello").unwrap();
        assert_eq!(resp, b"hello");

        let snap = dispatcher.stats.snapshot();
        assert_eq!(snap.calls_total, 1);
        assert_eq!(snap.calls_succeeded, 1);
    }

    #[test]
    fn dispatch_unregistered_returns_method_not_found() {
        let dispatcher = MethodDispatcher::new(RpcConfig::new());
        let result = dispatcher.dispatch(RpcMethodId::new(42), b"hello");
        assert!(matches!(result, Err(RpcError::MethodNotFound(id)) if id == RpcMethodId::new(42)));
    }

    #[test]
    fn handler_error_propagates() {
        let mut dispatcher = MethodDispatcher::new(RpcConfig::new());
        let handler: RpcHandler = Arc::new(|_| Err(RpcError::InvalidRequest("bad")));
        dispatcher.register(RpcMethodId::new(1), handler);

        let result = dispatcher.dispatch(RpcMethodId::new(1), b"hello");
        assert!(matches!(result, Err(RpcError::InvalidRequest(_))));

        let snap = dispatcher.stats.snapshot();
        assert_eq!(snap.calls_failed, 1);
    }

    #[test]
    fn replace_handler() {
        let mut dispatcher = MethodDispatcher::new(RpcConfig::new());
        let h1: RpcHandler = Arc::new(|_| Ok(b"first".to_vec()));
        let h2: RpcHandler = Arc::new(|_| Ok(b"second".to_vec()));
        dispatcher.register(RpcMethodId::new(1), h1);
        dispatcher.register(RpcMethodId::new(1), h2);

        let resp = dispatcher.dispatch(RpcMethodId::new(1), b"hello").unwrap();
        assert_eq!(resp, b"second");
        assert_eq!(dispatcher.handler_count(), 1);
    }

    #[test]
    fn multiple_methods() {
        let mut dispatcher = MethodDispatcher::new(RpcConfig::new());
        let echo: RpcHandler = Arc::new(|req: &[u8]| Ok(req.to_vec()));
        let upper: RpcHandler =
            Arc::new(|req: &[u8]| Ok(req.iter().map(|b| b.to_ascii_uppercase()).collect()));
        dispatcher.register(RpcMethodId::new(1), echo);
        dispatcher.register(RpcMethodId::new(2), upper);

        assert_eq!(
            dispatcher.dispatch(RpcMethodId::new(1), b"hello").unwrap(),
            b"hello"
        );
        assert_eq!(
            dispatcher.dispatch(RpcMethodId::new(2), b"hello").unwrap(),
            b"HELLO"
        );
    }

    #[test]
    fn health_report_initially_healthy() {
        let dispatcher = MethodDispatcher::new(RpcConfig::new());
        let health = dispatcher.health();
        assert!(health.is_healthy());
    }
}
