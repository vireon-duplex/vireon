//! [`Migration`] — connection migration + multipath hooks.
//!
//! Phase 12 — Path Manager (migration half). See `DESIGN.md` §4.12.
//!
//! Connection migration (RFC 9000 §9):
//! - A client may migrate to a new 4-tuple at any time
//! - The server treats a packet from a new tuple as a candidate
//! - Server MUST validate the new path (PATH_CHALLENGE) before
//!   sending non-probing frames
//! - Anti-amplification limit (3×) applies until validation
//!
//! Vireon implementation hooks:
//! - When a packet arrives from a new tuple, the [`PathManager`]
//!   (Phase 12) is queried
//! - If the path is new, a `MigrationEvent` is emitted so the
//!   engine can update the connection's socket bindings
//! - If multipath is enabled (RFC 9464), the new path is added
//!   concurrently; otherwise it replaces the old path

use std::net::SocketAddr;

/// Phase-0 skeleton. Phase 12 populates the migration state machine.
#[derive(Default)]
pub struct Migration {
    /// Whether multipath is enabled (RFC 9464).
    pub multipath_enabled: bool,
}

impl Migration {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            multipath_enabled: false,
        }
    }

    /// TODO(phase-12): `fn on_path_change(&mut self, new: SocketAddr)`
    /// `fn validate(&mut self, challenge: u64) -> bool`
    pub fn _placeholder(&self) {}
}

/// Phase-12 event fired when a peer migrates.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct MigrationEvent {
    /// Previous remote address (None if first packet on connection).
    pub from: Option<SocketAddr>,
    /// New remote address.
    pub to: SocketAddr,
    /// Whether the change was NAT rebinding (same IP, new port).
    pub nat_rebinding: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder() {
        let m = Migration::new();
        assert!(!m.multipath_enabled);
        // TODO(phase-12): migration state machine, multipath hooks,
        // integration with PathManager.
    }
}
