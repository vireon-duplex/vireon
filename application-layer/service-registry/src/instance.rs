//! [`ServiceInstance`] — runtime state for a single service instance.
//!
//! See `DESIGN.md` Part 4 (instance model).

use crate::endpoint::ServiceEndpoint;

// ── ServiceInstance ─────────────────────────────────────────────────

/// Runtime state for a single service instance.
///
/// Tracks endpoint metadata, active request count, health score,
/// and last health check timestamp for load balancing decisions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ServiceInstance {
    /// Network endpoint for this instance.
    pub endpoint: ServiceEndpoint,
    /// Number of active requests being served.
    pub active_requests: u32,
    /// Nanoseconds since epoch of the last health check.
    pub last_health_ns: u64,
    /// Health score (0..=100, higher is better).
    pub health_score: u8,
}

impl ServiceInstance {
    /// Construct a new instance from an endpoint.
    #[must_use]
    pub const fn new(endpoint: ServiceEndpoint) -> Self {
        Self {
            endpoint,
            active_requests: 0,
            last_health_ns: 0,
            health_score: 100, // Start with perfect health
        }
    }

    /// Builder-style: set active request count.
    #[must_use]
    pub const fn with_active_requests(mut self, count: u32) -> Self {
        self.active_requests = count;
        self
    }

    /// Builder-style: set health check timestamp.
    #[must_use]
    pub const fn with_last_health_ns(mut self, ns: u64) -> Self {
        self.last_health_ns = ns;
        self
    }

    /// Builder-style: set health score.
    #[must_use]
    pub const fn with_health_score(mut self, score: u8) -> Self {
        // Can't use min() in const, so we check manually
        // If score > 100, set to 100; otherwise use score
        self.health_score = if score > 100 { 100 } else { score };
        self
    }

    /// Whether this instance is healthy (score > 50).
    #[must_use]
    pub const fn is_healthy(&self) -> bool {
        self.health_score > 50 && self.endpoint.is_healthy
    }

    /// Load factor (0..=10000, higher = more loaded).
    ///
    /// Computed as: (active_requests * 10000) / max_requests_per_instance.
    /// For now, assume max_requests_per_instance = 100.
    #[must_use]
    pub const fn load_factor(&self) -> u16 {
        if self.active_requests >= 100 {
            return 10_000;
        }
        ((self.active_requests as u64) * 10000 / 100) as u16
    }

    /// Record that a new request is being served.
    pub fn record_request(&mut self) {
        self.active_requests = self.active_requests.saturating_add(1);
    }

    /// Record that a request has completed.
    pub fn record_completion(&mut self) {
        self.active_requests = self.active_requests.saturating_sub(1);
    }

    /// Update health score based on latest check.
    pub fn update_health(&mut self, score: u8, now_ns: u64) {
        self.health_score = if score > 100 { 100 } else { score };
        self.last_health_ns = now_ns;
    }

    /// Get the endpoint.
    #[must_use]
    pub const fn endpoint(&self) -> ServiceEndpoint {
        self.endpoint
    }

    /// Get the active request count.
    #[must_use]
    pub const fn active_requests(&self) -> u32 {
        self.active_requests
    }

    /// Get the health score.
    #[must_use]
    pub const fn health_score(&self) -> u8 {
        self.health_score
    }

    /// Get the last health check timestamp.
    #[must_use]
    pub const fn last_health_ns(&self) -> u64 {
        self.last_health_ns
    }
}

impl Default for ServiceInstance {
    fn default() -> Self {
        Self::new(ServiceEndpoint::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use session_registry::node::NodeId;

    #[test]
    fn instance_new_defaults() {
        let endpoint = ServiceEndpoint::new(NodeId::new(1), [0u8; 16], 443);
        let instance = ServiceInstance::new(endpoint);
        assert_eq!(instance.endpoint, endpoint);
        assert_eq!(instance.active_requests, 0);
        assert_eq!(instance.health_score, 100);
        assert!(instance.is_healthy());
    }

    #[test]
    fn instance_builder_methods() {
        let endpoint = ServiceEndpoint::new(NodeId::new(1), [0u8; 16], 443);
        let instance = ServiceInstance::new(endpoint)
            .with_active_requests(10)
            .with_last_health_ns(1_000_000)
            .with_health_score(80);

        assert_eq!(instance.active_requests, 10);
        assert_eq!(instance.last_health_ns, 1_000_000);
        assert_eq!(instance.health_score, 80);
    }

    #[test]
    fn instance_is_healthy() {
        let endpoint = ServiceEndpoint::new(NodeId::new(1), [0u8; 16], 443);
        let instance = ServiceInstance::new(endpoint).with_health_score(75);
        assert!(instance.is_healthy());

        let instance = ServiceInstance::new(endpoint).with_health_score(50);
        assert!(!instance.is_healthy());

        let endpoint_unhealthy =
            ServiceEndpoint::new(NodeId::new(1), [0u8; 16], 443).with_healthy(false);
        let instance = ServiceInstance::new(endpoint_unhealthy).with_health_score(100);
        assert!(!instance.is_healthy()); // Unhealthy endpoint
    }

    #[test]
    fn instance_load_factor() {
        let endpoint = ServiceEndpoint::new(NodeId::new(1), [0u8; 16], 443);
        let instance = ServiceInstance::new(endpoint).with_active_requests(0);
        assert_eq!(instance.load_factor(), 0);

        let instance = ServiceInstance::new(endpoint).with_active_requests(50);
        assert_eq!(instance.load_factor(), 5000); // 50%

        let instance = ServiceInstance::new(endpoint).with_active_requests(100);
        assert_eq!(instance.load_factor(), 10000); // 100% (capped)
    }

    #[test]
    fn instance_record_request() {
        let endpoint = ServiceEndpoint::new(NodeId::new(1), [0u8; 16], 443);
        let mut instance = ServiceInstance::new(endpoint);
        instance.record_request();
        instance.record_request();
        assert_eq!(instance.active_requests, 2);
    }

    #[test]
    fn instance_record_completion() {
        let endpoint = ServiceEndpoint::new(NodeId::new(1), [0u8; 16], 443);
        let mut instance = ServiceInstance::new(endpoint).with_active_requests(5);
        instance.record_completion();
        assert_eq!(instance.active_requests, 4);
    }

    #[test]
    fn instance_update_health() {
        let endpoint = ServiceEndpoint::new(NodeId::new(1), [0u8; 16], 443);
        let mut instance = ServiceInstance::new(endpoint);
        instance.update_health(90, 5_000_000);
        assert_eq!(instance.health_score, 90);
        assert_eq!(instance.last_health_ns, 5_000_000);
    }

    #[test]
    fn instance_health_score_clamped() {
        let endpoint = ServiceEndpoint::new(NodeId::new(1), [0u8; 16], 443);
        let instance = ServiceInstance::new(endpoint).with_health_score(150);
        assert_eq!(instance.health_score, 100); // Clamped to 100
    }

    #[test]
    fn instance_accessors() {
        let endpoint = ServiceEndpoint::new(NodeId::new(1), [0u8; 16], 443);
        let instance = ServiceInstance::new(endpoint)
            .with_active_requests(5)
            .with_last_health_ns(10_000)
            .with_health_score(85);

        assert_eq!(instance.endpoint(), endpoint);
        assert_eq!(instance.active_requests(), 5);
        assert_eq!(instance.health_score(), 85);
        assert_eq!(instance.last_health_ns(), 10_000);
    }

    #[test]
    fn instance_clone() {
        let endpoint = ServiceEndpoint::new(NodeId::new(1), [0u8; 16], 443);
        let instance = ServiceInstance::new(endpoint)
            .with_active_requests(3)
            .with_health_score(75);
        let cloned = instance;
        assert_eq!(instance, cloned);
    }

    #[test]
    fn instance_default() {
        let instance = ServiceInstance::default();
        assert_eq!(instance.endpoint, ServiceEndpoint::default());
        assert_eq!(instance.active_requests, 0);
        assert_eq!(instance.health_score, 100);
    }

    #[test]
    fn instance_new_works() {
        let endpoint = ServiceEndpoint::new(NodeId::new(1), [0u8; 16], 443);
        let instance = ServiceInstance::new(endpoint);
        assert_eq!(instance.active_requests(), 0);
    }

    #[test]
    fn instance_equality() {
        let endpoint = ServiceEndpoint::new(NodeId::new(1), [0u8; 16], 443);
        let instance1 = ServiceInstance::new(endpoint);
        let instance2 = ServiceInstance::new(endpoint);
        assert_eq!(instance1, instance2);

        let instance3 = ServiceInstance::new(endpoint).with_active_requests(1);
        assert_ne!(instance1, instance3);
    }
}
