//! [`CryptoContext`] — TLS 1.3 secrets handoff, keylog, 0-RTT,
//! key update.
//!
//! Phase 3 — Crypto Engine. See `DESIGN.md` §4.3.
//!
//! Vireon **delegates all crypto** to `quiche` (which uses
//! BoringSSL by default). This module's job is to:
//!
//! - Configure the TLS context (certificate chain, ALPN, SNI)
//! - Export keying material for observability (`SSLKEYLOGFILE` /
//!   Wireshark decoding)
//! - Surface handshake progress events (EarlyData, KeyUpdate,
//!   HandshakeDone) so the engine can drive state transitions
//! - Hook 0-RTT acceptance (anti-replay window policy)
//! - Future: hand off application secrets to kernel TLS (kTLS) or
//!   hardware offload — `DESIGN.md` §4.3 "Hardware acceleration"
//!
//! RFC 9001 §5 defines the four key spaces: Initial, Handshake Early
//! Data (0-RTT), Application. Key update (RFC 9001 §6) is driven by
//! quiche; this module observes and reports.

/// Phase-0 skeleton. Phase 3 will own:
/// - `TlsConfig` (cert chain, key, ALPN list, SNI)
/// - `KeyLog` (sink for `SSLKEYLOGFILE` — file or tracing span)
/// - `ZeroRttPolicy` (accept / reject / bounded anti-replay)
#[derive(Default)]
pub struct CryptoContext {
    /// Whether 0-RTT is currently enabled for new connections.
    zero_rtt_enabled: bool,
}

impl CryptoContext {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            zero_rtt_enabled: false,
        }
    }

    /// TODO(phase-3): `fn wrap_quiche_config(&self, cfg: &mut quiche::Config)`
    /// to apply TLS settings into a freshly-built quiche config.
    pub fn _placeholder_apply(&self) {}
}

/// Phase-3 hook fired by quiche when new traffic secrets are derived.
#[derive(Debug)]
#[non_exhaustive]
pub enum SecretEvent {
    /// Initial secrets derived (quiche derives these internally; we
    /// observe for stats only).
    Initial,
    /// Handshake secrets derived — endpoint can send 1-RTT.
    HandshakeDone,
    /// Application secrets derived — handshake complete.
    Application,
    /// Key update initiated (RFC 9001 §6).
    KeyUpdate,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholder() {
        let c = CryptoContext::new();
        c._placeholder_apply();
        // TODO(phase-3): cert loading, ALPN negotiation, keylog sink,
        // 0-RTT anti-replay.
    }
}
