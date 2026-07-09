//! [`MiddlewareContext`] — request context for middleware execution.
//!
//! See `DESIGN.md` Part 7 (middleware context).

// ── MiddlewareContext ─────────────────────────────────────────────

/// Request context passed through the middleware chain.
///
/// Context carries request metadata and execution state through
/// the middleware pipeline.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MiddlewareContext {
    /// Current execution stage.
    pub stage: crate::MiddlewareStage,
    /// Hash of session identifier (if authenticated).
    pub session_id_hash: u64,
    /// Hash of request method (GET, POST, etc.).
    pub method_hash: u64,
    /// Hash of request metadata (headers, params).
    pub metadata_hash: u64,
    /// Whether to continue executing the chain.
    pub continue_chain: bool,
    /// Error code (if error occurred).
    pub error_code: u16,
    /// Execution timestamp (ns).
    pub timestamp_ns: u64,
}

impl MiddlewareContext {
    /// Construct a new context for the given stage.
    #[must_use]
    pub const fn new(stage: crate::MiddlewareStage) -> Self {
        Self {
            stage,
            session_id_hash: 0,
            method_hash: 0,
            metadata_hash: 0,
            continue_chain: true,
            error_code: 0,
            timestamp_ns: 0,
        }
    }

    /// Set the error code.
    #[must_use]
    pub const fn with_error_code(mut self, code: u16) -> Self {
        self.error_code = code;
        self.continue_chain = false;
        self
    }

    /// Abort the chain (short-circuit).
    #[must_use]
    pub const fn with_abort(mut self) -> Self {
        self.continue_chain = false;
        self
    }

    /// Set the session id hash.
    #[must_use]
    pub const fn with_session_id(mut self, hash: u64) -> Self {
        self.session_id_hash = hash;
        self
    }

    /// Set the method hash.
    #[must_use]
    pub const fn with_method(mut self, hash: u64) -> Self {
        self.method_hash = hash;
        self
    }

    /// Set the metadata hash.
    #[must_use]
    pub const fn with_metadata(mut self, hash: u64) -> Self {
        self.metadata_hash = hash;
        self
    }

    /// Set the timestamp.
    #[must_use]
    pub const fn with_timestamp(mut self, ns: u64) -> Self {
        self.timestamp_ns = ns;
        self
    }

    /// Set an error and abort.
    pub fn set_error(&mut self, code: u16) {
        self.error_code = code;
        self.continue_chain = false;
    }

    /// Abort the chain.
    pub fn abort(&mut self) {
        self.continue_chain = false;
    }

    /// Whether the chain should continue.
    #[must_use]
    pub const fn is_aborted(&self) -> bool {
        !self.continue_chain
    }

    /// Whether an error is set.
    #[must_use]
    pub const fn has_error(&self) -> bool {
        self.error_code != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MiddlewareStage;

    #[test]
    fn new_context_has_default_values() {
        let ctx = MiddlewareContext::new(MiddlewareStage::PreAuth);
        assert_eq!(ctx.stage, MiddlewareStage::PreAuth);
        assert!(ctx.continue_chain);
        assert_eq!(ctx.error_code, 0);
        assert_eq!(ctx.session_id_hash, 0);
    }

    #[test]
    fn with_error_code_sets_error_and_aborts() {
        let ctx = MiddlewareContext::new(MiddlewareStage::AuthN).with_error_code(401);
        assert_eq!(ctx.error_code, 401);
        assert!(ctx.is_aborted());
    }

    #[test]
    fn with_abort_stops_chain() {
        let ctx = MiddlewareContext::new(MiddlewareStage::PreDispatch).with_abort();
        assert!(ctx.is_aborted());
        assert_eq!(ctx.error_code, 0); // No error code set
    }

    #[test]
    fn with_session_id_sets_hash() {
        let ctx = MiddlewareContext::new(MiddlewareStage::AuthZ).with_session_id(12345);
        assert_eq!(ctx.session_id_hash, 12345);
    }

    #[test]
    fn with_method_sets_hash() {
        let ctx = MiddlewareContext::new(MiddlewareStage::PreDispatch).with_method(67890);
        assert_eq!(ctx.method_hash, 67890);
    }

    #[test]
    fn with_metadata_sets_hash() {
        let ctx = MiddlewareContext::new(MiddlewareStage::Response).with_metadata(999);
        assert_eq!(ctx.metadata_hash, 999);
    }

    #[test]
    fn with_timestamp_sets_ns() {
        let ctx = MiddlewareContext::new(MiddlewareStage::Error).with_timestamp(1_000_000);
        assert_eq!(ctx.timestamp_ns, 1_000_000);
    }

    #[test]
    fn set_error_mutates_context() {
        let mut ctx = MiddlewareContext::new(MiddlewareStage::AuthN);
        ctx.set_error(403);
        assert_eq!(ctx.error_code, 403);
        assert!(ctx.is_aborted());
    }

    #[test]
    fn abort_mutates_context() {
        let mut ctx = MiddlewareContext::new(MiddlewareStage::PreDispatch);
        ctx.abort();
        assert!(ctx.is_aborted());
    }

    #[test]
    fn is_aborted_returns_continue_negated() {
        let mut ctx = MiddlewareContext::new(MiddlewareStage::AuthZ);
        assert!(!ctx.is_aborted());
        ctx.abort();
        assert!(ctx.is_aborted());
    }

    #[test]
    fn has_error_checks_error_code() {
        let mut ctx = MiddlewareContext::new(MiddlewareStage::Response);
        assert!(!ctx.has_error());
        ctx.set_error(500);
        assert!(ctx.has_error());
    }

    #[test]
    fn context_is_copy() {
        let ctx1 = MiddlewareContext::new(MiddlewareStage::AuthN).with_session_id(123);
        let ctx2 = ctx1;
        assert_eq!(ctx1.session_id_hash, 123);
        assert_eq!(ctx2.session_id_hash, 123);
    }

    #[test]
    fn builder_pattern_chains() {
        let ctx = MiddlewareContext::new(MiddlewareStage::PreDispatch)
            .with_session_id(111)
            .with_method(222)
            .with_metadata(333)
            .with_timestamp(444);
        assert_eq!(ctx.session_id_hash, 111);
        assert_eq!(ctx.method_hash, 222);
        assert_eq!(ctx.metadata_hash, 333);
        assert_eq!(ctx.timestamp_ns, 444);
    }
}
