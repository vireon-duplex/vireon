//! [`WatchHandle`] — subscription handle for service change notifications.
//!
//! See `DESIGN.md` Part 10 (watch system).
//!
//! # Role
//!
//! `WatchHandle` represents a subscription to service changes:
//!
//! - Service registration / unregistration
//! - Endpoint additions / removals
//! - Health state changes
//!
//! # Phase 0
//!
//! Phase 0 ships only a typed skeleton. Every method returns a
//! stub result so that callers can code against the API contract
//! today. Phase 1 fills in the real logic.

use crate::error::ServiceRegistryError;

/// Handle for watching service changes.
///
/// Represents a subscription to service registry events.
/// Phase 0 ships a typed skeleton; Phase 1+ populates real logic.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WatchHandle {
    /// Unique watch identifier.
    #[allow(dead_code)]
    id: u64,
}

impl WatchHandle {
    /// Construct a new watch handle.
    #[must_use]
    pub const fn new(id: u64) -> Self {
        Self { id }
    }

    /// Cancel this watch subscription.
    ///
    /// # Errors
    /// Returns [`ServiceRegistryError::ShuttingDown`] in Phase 0.
    pub fn cancel(&mut self) -> Result<(), ServiceRegistryError> {
        // TODO(phase-1): remove subscription + update stats.
        Err(ServiceRegistryError::ShuttingDown)
    }

    /// Whether this watch is still active (not canceled).
    #[must_use]
    pub fn is_active(&self) -> bool {
        // TODO(phase-1): check subscription status.
        false
    }

    /// Get the watch ID.
    #[must_use]
    pub const fn id(&self) -> u64 {
        self.id
    }
}

impl Default for WatchHandle {
    fn default() -> Self {
        Self::new(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watch_handle_new() {
        let handle = WatchHandle::new(42);
        assert_eq!(handle.id(), 42);
    }

    #[test]
    fn watch_handle_default() {
        let handle = WatchHandle::default();
        assert_eq!(handle.id(), 0);
    }

    #[test]
    fn watch_handle_cancel_phase0_stub() {
        let mut handle = WatchHandle::new(1);
        assert!(handle.cancel().is_err());
    }

    #[test]
    fn watch_handle_is_active_phase0_stub() {
        let handle = WatchHandle::new(1);
        assert!(!handle.is_active());
    }

    #[test]
    fn watch_handle_clone() {
        let handle = WatchHandle::new(42);
        let cloned = handle;
        assert_eq!(handle, cloned);
    }

    #[test]
    fn watch_handle_equality() {
        let handle1 = WatchHandle::new(1);
        let handle2 = WatchHandle::new(1);
        let handle3 = WatchHandle::new(2);

        assert_eq!(handle1, handle2);
        assert_ne!(handle1, handle3);
    }

    #[test]
    fn watch_handle_const_new() {
        const HANDLE: WatchHandle = WatchHandle::new(100);
        assert_eq!(HANDLE.id(), 100);
    }

    #[test]
    fn watch_handle_id_accessor() {
        let handle = WatchHandle::new(999);
        assert_eq!(handle.id(), 999);
    }
}
