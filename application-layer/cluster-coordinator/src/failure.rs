//! [`FailureDetector`] — heartbeat-based liveness tracking for cluster peers.
//!
//! Each node broadcasts an [`InterNodeMessage::heartbeat`] every
//! `heartbeat_interval`. This detector records the last-seen time per
//! peer and, on each `check_expired` tick, returns peers whose last
//! heartbeat is older than `heartbeat_timeout`.
//!
//! ## v1: simple timeout
//!
//! The first version uses a flat timeout — declare a node down after
//! `heartbeat_timeout` with no heartbeat. This is easy to reason about
//! and sufficient for clusters of <50 nodes. If false-positive rates
//! become a problem on large clusters, upgrade to phi-accrual (Cassandra/
//! Akka style) which tracks inter-arrival variance.

use std::time::{Duration, Instant};

use rustc_hash::FxHashMap;
use session_registry::NodeId;

/// Per-node liveness state tracked by [`FailureDetector`].
#[derive(Debug, Clone)]
pub struct NodeHealth {
    /// Last time a heartbeat was received from this node.
    pub last_seen: Instant,
}

/// Heartbeat-based liveness tracker. Single-threaded (owned by the
/// coordinator, which is per-reactor-worker).
#[derive(Debug)]
pub struct FailureDetector {
    /// `node_id → last heartbeat time`.
    nodes: FxHashMap<NodeId, NodeHealth>,
}

impl FailureDetector {
    /// Construct an empty detector.
    #[must_use]
    pub fn new() -> Self {
        Self {
            nodes: FxHashMap::default(),
        }
    }

    /// Register or refresh a heartbeat from `node` at time `now`.
    pub fn record_heartbeat(&mut self, node: NodeId, now: Instant) {
        self.nodes.entry(node).or_insert(NodeHealth { last_seen: now }).last_seen = now;
    }

    /// Register a node as alive (e.g. from initial membership). Uses
    /// `now` as the seed last-seen time.
    pub fn seed_alive(&mut self, node: NodeId, now: Instant) {
        self.nodes.entry(node).or_insert(NodeHealth { last_seen: now });
    }

    /// Forget a node entirely (after it's been marked down and removed
    /// from the membership).
    pub fn forget(&mut self, node: NodeId) {
        self.nodes.remove(&node);
    }

    /// Returns the IDs of nodes whose last heartbeat is older than
    /// `now - timeout` and that have **not** already been reported as
    /// expired since their last heartbeat. Each expired node is removed
    /// from tracking (so it won't be reported again until a new
    /// heartbeat arrives).
    #[must_use]
    pub fn check_expired(&mut self, now: Instant, timeout: Duration) -> Vec<NodeId> {
        let cutoff = now - timeout;
        let expired: Vec<NodeId> = self
            .nodes
            .iter()
            .filter_map(|(&id, h)| {
                if h.last_seen < cutoff {
                    Some(id)
                } else {
                    None
                }
            })
            .collect();
        for id in &expired {
            self.nodes.remove(id);
        }
        expired
    }

    /// Number of nodes currently being tracked (i.e. that have
    /// heartbeated at least once and haven't timed out).
    #[must_use]
    pub fn tracked_count(&self) -> usize {
        self.nodes.len()
    }

    /// Whether `node` has a recent heartbeat on record.
    #[must_use]
    pub fn is_tracked(&self, node: NodeId) -> bool {
        self.nodes.contains_key(&node)
    }
}

impl Default for FailureDetector {
    fn default() -> Self {
        Self::new()
    }
}

// Re-export for diagnostics.
impl FailureDetector {
    /// Borrow the underlying map (for diagnostics / introspection).
    #[must_use]
    pub fn nodes(&self) -> &FxHashMap<NodeId, NodeHealth> {
        &self.nodes
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn record_and_check_not_expired() {
        let mut det = FailureDetector::new();
        let t0 = Instant::now();
        det.record_heartbeat(NodeId::new(2), t0);
        // 1s later — within a 5s timeout.
        let expired = det.check_expired(t0 + Duration::from_secs(1), Duration::from_secs(5));
        assert!(expired.is_empty());
        assert_eq!(det.tracked_count(), 1);
    }

    #[test]
    fn check_expired_after_timeout() {
        let mut det = FailureDetector::new();
        let t0 = Instant::now();
        det.record_heartbeat(NodeId::new(2), t0);
        det.record_heartbeat(NodeId::new(3), t0 + Duration::from_secs(2));
        // 6s from t0: node 2 (last seen t0) expired, node 3 (last seen t0+2s) is fine.
        let mut expired = det.check_expired(t0 + Duration::from_secs(6), Duration::from_secs(5));
        expired.sort_by_key(|n| n.raw());
        assert_eq!(expired, vec![NodeId::new(2)]);
        // Node 2 removed from tracking.
        assert!(!det.is_tracked(NodeId::new(2)));
        assert!(det.is_tracked(NodeId::new(3)));
    }

    #[test]
    fn expired_node_not_reported_twice() {
        let mut det = FailureDetector::new();
        let t0 = Instant::now();
        det.record_heartbeat(NodeId::new(2), t0);
        let expired1 = det.check_expired(t0 + Duration::from_secs(10), Duration::from_secs(5));
        assert_eq!(expired1, vec![NodeId::new(2)]);
        // Second check — node 2 is no longer tracked, so not reported.
        let expired2 = det.check_expired(t0 + Duration::from_secs(20), Duration::from_secs(5));
        assert!(expired2.is_empty());
    }

    #[test]
    fn refresh_prevents_expiry() {
        let mut det = FailureDetector::new();
        let t0 = Instant::now();
        det.record_heartbeat(NodeId::new(2), t0);
        // Heartbeat at t0 + 3s.
        det.record_heartbeat(NodeId::new(2), t0 + Duration::from_secs(3));
        // Check at t0 + 7s with 5s timeout → last seen was t0+3, 4s ago → OK.
        let expired = det.check_expired(t0 + Duration::from_secs(7), Duration::from_secs(5));
        assert!(expired.is_empty());
    }

    #[test]
    fn seed_alive_then_forget() {
        let mut det = FailureDetector::new();
        let t0 = Instant::now();
        det.seed_alive(NodeId::new(5), t0);
        assert!(det.is_tracked(NodeId::new(5)));
        det.forget(NodeId::new(5));
        assert!(!det.is_tracked(NodeId::new(5)));
    }

    #[test]
    fn empty_detector_reports_nothing() {
        let mut det = FailureDetector::new();
        let expired = det.check_expired(Instant::now(), Duration::from_secs(1));
        assert!(expired.is_empty());
    }
}
