//! [`PathManager`] — PATH_CHALLENGE / PATH_RESPONSE, NAT rebinding,
//! multipath hooks.
//!
//! Phase 12 — Path Manager. See `DESIGN.md` §4.12.
//!
//! RFC 9000 §9: a connection may migrate from one 4-tuple to
//! another (mobile client moves between Wi-Fi and cellular). The
//! server MUST validate the new path before sending non-probing
//! frames to it (anti-amplification).
//!
//! RFC 9464 (Simple Multipath QUIC): allows a single connection to
//! use multiple paths concurrently. Hook points designed in Phase
//! 0; implementation deferred to a later sub-phase of Phase 12.
//!
//! Responsibilities:
//!
//! - Track the active 4-tuple(s) per connection
//! - Send / process PATH_CHALLENGE / PATH_RESPONSE (RFC 9000 §19.23)
//! - Validate new paths before sending bulk data
//! - Per-path RTT and bandwidth estimation (Phase 5 hook)
//! - NAT rebinding detection (peer port change without explicit
//!   migration)
//! - Path failover (multipath)

use std::net::SocketAddr;

/// Phase-0 skeleton. Phase 12 populates the per-path RTT table and
/// the path-validation state machine.
#[derive(Clone, Copy, Debug, Default)]
pub struct PathManager {
    /// Number of currently-active paths (1 unless multipath is on).
    pub active_paths: u8,
}

impl PathManager {
    #[must_use]
    pub const fn new() -> Self {
        Self { active_paths: 1 }
    }

    /// TODO(phase-12): `fn on_peer_addr_change(&mut self, new: SocketAddr)`
    /// `fn validate_path(&mut self, challenge: u64) -> bool`
    pub fn _placeholder(&self) {}
}

/// Phase-12 path-validation state (RFC 9000 §8.2).
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PathValidationState {
    /// Path is validated (RFC 9000 §8.2.4).
    Validated,
    /// Challenge sent, awaiting response.
    Pending,
    /// Validation failed; do not use this path.
    Failed,
}

/// Phase-12 per-path descriptor.
#[derive(Clone, Copy, Debug)]
pub struct Path {
    /// Local transport address.
    pub local: SocketAddr,
    /// Remote transport address.
    pub peer: SocketAddr,
    /// Smoothed RTT on this path (microseconds).
    pub srtt_us: u32,
    /// Validation state.
    pub state: PathValidationState,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder() {
        let p = PathManager::new();
        assert_eq!(p.active_paths, 1);
        // TODO(phase-12): PATH_CHALLENGE/RESPONSE, NAT rebinding,
        // multipath RTT/bandwidth estimation.
    }
}
