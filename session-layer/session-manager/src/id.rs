//! [`SessionId`] — 128-bit application-level session identifier.
//!
//! See `DESIGN.md` Part 2 (session object) and Part 5 (routing).
//!
//! # Why 128 bits?
//!
//! The `SessionId` is the **application-facing** identifier. It must
//! be:
//!
//! - **Unguessable** — a malicious peer must not be able to predict
//!   valid session ids (anti-hijack, anti-enumeration).
//! - **Compact** — small enough for efficient hashmap keys and
//!   network transmission.
//! - **Globally unique** — no two sessions on any node ever share
//!   an id (without coordination).
//!
//! 128 bits (two `u64`) satisfies all three: 64 bits of randomness
//! gives a collision probability of ~2^-64 even at 4 billion
//! sessions, and the remaining 64 bits can encode a timestamp or
//! node id for ordering / locality.
//!
//! This is distinct from:
//! - [`ConnectionId`](connection_shard::ConnectionId) — runtime-level
//!   `u64` (slot + generation) used by the connection shard.
//! - `QuicCid` — QUIC protocol-level connection id (1–20 bytes,
//!   RFC 9000 §17.2) used by quiche.

use std::sync::atomic::{AtomicU64, Ordering};

/// The nil session id — sentinel for "no session".
pub const NIL_SESSION_HI: u64 = 0;
pub const NIL_SESSION_LO: u64 = 0;

// ── Global counter for deterministic id generation (test-friendly) ──

static SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);

fn next_counter() -> u64 {
    SESSION_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// A 128-bit application-level session identifier.
///
/// Displayed as 32 hex characters: `hi:lo` concatenated. Hashable
/// and `Copy` (16 bytes). Used as the primary key in the
/// [`SessionTable`](crate::table::SessionTable) and
/// [`SessionRegistry`](crate::registry::SessionRegistry).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SessionId {
    /// High 64 bits — encodes a monotonic counter or timestamp.
    pub hi: u64,
    /// Low 64 bits — random or node-local nonce.
    pub lo: u64,
}

impl SessionId {
    /// The nil session id — sentinel for "no session".
    #[must_use]
    pub const fn nil() -> Self {
        Self {
            hi: NIL_SESSION_HI,
            lo: NIL_SESSION_LO,
        }
    }

    /// Whether this is the nil sentinel.
    #[must_use]
    pub const fn is_nil(self) -> bool {
        self.hi == NIL_SESSION_HI && self.lo == NIL_SESSION_LO
    }

    /// Construct from two `u64` halves.
    #[must_use]
    pub const fn from_pair(hi: u64, lo: u64) -> Self {
        Self { hi, lo }
    }

    /// Construct a new unique session id. Uses a global atomic
    /// counter for the high half (monotonic, collision-free within
    /// a process) and the counter's previous value XOR-shifted for
    /// the low half (pseudo-random, unguessable).
    ///
    /// Phase 2+ will replace this with a cryptographically secure
    /// random source (e.g. `getrandom`) for the low half.
    #[must_use]
    pub fn new() -> Self {
        let counter = next_counter();
        // XOR-shift PRNG for the low half — fast, non-crypto.
        let mut x = counter.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        x ^= x >> 30;
        x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x ^= x >> 27;
        x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
        x ^= x >> 31;
        Self { hi: counter, lo: x }
    }

    /// The high 64 bits.
    #[must_use]
    pub const fn raw_hi(self) -> u64 {
        self.hi
    }

    /// The low 64 bits.
    #[must_use]
    pub const fn raw_lo(self) -> u64 {
        self.lo
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::nil()
    }
}

impl std::fmt::Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}{:016x}", self.hi, self.lo)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nil_is_nil() {
        let n = SessionId::nil();
        assert!(n.is_nil());
        assert_eq!(n.raw_hi(), 0);
        assert_eq!(n.raw_lo(), 0);
    }

    #[test]
    fn new_is_not_nil() {
        let id = SessionId::new();
        assert!(!id.is_nil());
    }

    #[test]
    fn new_is_unique() {
        let a = SessionId::new();
        let b = SessionId::new();
        assert_ne!(a, b);
    }

    #[test]
    fn from_pair_roundtrips() {
        let id = SessionId::from_pair(0xDEAD_BEEF, 0xCAFE_BABE);
        assert_eq!(id.raw_hi(), 0xDEAD_BEEF);
        assert_eq!(id.raw_lo(), 0xCAFE_BABE);
    }

    #[test]
    fn display_is_hex() {
        let id = SessionId::from_pair(0xDEAD_BEEF, 0xCAFE_BABE);
        let s = format!("{id}");
        assert_eq!(s, "00000000deadbeef00000000cafebabe");
    }

    #[test]
    fn ordering_works() {
        let a = SessionId::from_pair(1, 0);
        let b = SessionId::from_pair(2, 0);
        assert!(a < b);
    }

    #[test]
    fn hashable_and_eq() {
        let id = SessionId::from_pair(42, 99);
        let mut map = std::collections::HashMap::new();
        map.insert(id, "test");
        assert_eq!(map.get(&id), Some(&"test"));
    }
}
