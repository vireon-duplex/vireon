//! [`RouterEvent`] — domain events emitted by the router.
//!
//! See `DESIGN.md` Part 11 (observability).
//!
//! # Role
//!
//! The router publishes events for every meaningful state
//! transition: cache hits/misses, route resolutions, failures,
//! and health changes. Subscribers (metrics pipeline, tracing,
//! circuit breaker) react to these events without polling.
//!
//! # Phase 0
//!
//! Type only — no event bus. Phase 1+ adds a broadcast channel.

use crate::decision::RoutingDecision;
use crate::error::RouterError;
use session_manager::SessionId;
use session_registry::NodeId;

/// A domain event emitted by the router.
///
/// Events are the **only** push-based notification channel the
/// router offers. All other APIs are pull-based.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub enum RouterEvent {
    /// A routing cache hit occurred.
    CacheHit {
        /// The session that was looked up.
        session: SessionId,
    },
    /// A routing cache miss occurred.
    CacheMiss {
        /// The session that was looked up.
        session: SessionId,
    },
    /// A route was successfully resolved.
    RouteResolved {
        /// The session that was routed.
        session: SessionId,
        /// The routing decision.
        decision: RoutingDecision,
    },
    /// A route resolution failed.
    RouteFailed {
        /// The session that could not be routed.
        session: SessionId,
        /// The error that caused the failure.
        error: RouterError,
    },
    /// A node's health status changed.
    NodeHealthChanged {
        /// The node whose health changed.
        node: NodeId,
        /// Whether the node is now considered healthy.
        healthy: bool,
    },
}

impl RouterEvent {
    /// Human-readable category name for the event.
    #[must_use]
    pub const fn category(&self) -> &'static str {
        match self {
            Self::CacheHit { .. } | Self::CacheMiss { .. } => "cache",
            Self::RouteResolved { .. } | Self::RouteFailed { .. } => "routing",
            Self::NodeHealthChanged { .. } => "health",
        }
    }

    /// Whether the event indicates a cache operation.
    #[must_use]
    pub const fn is_cache_event(&self) -> bool {
        matches!(self, Self::CacheHit { .. } | Self::CacheMiss { .. })
    }

    /// Whether the event indicates a failure.
    #[must_use]
    pub const fn is_failure(&self) -> bool {
        matches!(self, Self::RouteFailed { .. })
    }

    /// Whether the event is informational (no action required).
    #[must_use]
    pub const fn is_informational(&self) -> bool {
        matches!(
            self,
            Self::CacheHit { .. } | Self::RouteResolved { .. } | Self::NodeHealthChanged { .. }
        )
    }

    /// The session associated with this event, if any.
    #[must_use]
    pub const fn session(&self) -> Option<SessionId> {
        match self {
            Self::CacheHit { session }
            | Self::CacheMiss { session }
            | Self::RouteResolved { session, .. }
            | Self::RouteFailed { session, .. } => Some(*session),
            Self::NodeHealthChanged { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn category_cache() {
        let e = RouterEvent::CacheHit {
            session: SessionId::nil(),
        };
        assert_eq!(e.category(), "cache");
    }

    #[test]
    fn category_routing() {
        let e = RouterEvent::RouteResolved {
            session: SessionId::nil(),
            decision: RoutingDecision::Local,
        };
        assert_eq!(e.category(), "routing");
    }

    #[test]
    fn category_health() {
        let e = RouterEvent::NodeHealthChanged {
            node: NodeId::new(1),
            healthy: true,
        };
        assert_eq!(e.category(), "health");
    }

    #[test]
    fn is_failure_for_route_failed() {
        let e = RouterEvent::RouteFailed {
            session: SessionId::nil(),
            error: RouterError::Timeout,
        };
        assert!(e.is_failure());
    }

    #[test]
    fn session_extraction() {
        let sid = SessionId::from_pair(42, 99);
        let e = RouterEvent::CacheMiss { session: sid };
        assert_eq!(e.session(), Some(sid));

        let e = RouterEvent::NodeHealthChanged {
            node: NodeId::new(1),
            healthy: false,
        };
        assert_eq!(e.session(), None);
    }
}
