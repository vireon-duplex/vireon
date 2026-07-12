//! Crypto / TLS configuration — builds `quiche::Config` from `QuicConfig`.
//!
//! Wraps quiche's TLS 1.3 stack (BoringSSL) configuration into a single
//! entry point: [`build_quiche_config`]. The caller provides certificate
//! and private-key PEM paths plus a [`QuicConfig`]; the function returns
//! a ready-to-use `quiche::Config`.
//!
//! See `DESIGN.md` §4.3 for the full crypto architecture.

use std::time::Duration;

use crate::config::{CongestionAlgorithm, QuicConfig};
use crate::error::QuicError;

/// Builds a [`quiche::Config`] from the engine's [`QuicConfig`] plus
/// certificate / key file paths.
///
/// # Errors
///
/// Returns [`QuicError`] if:
/// - `quiche::Config::new` fails (unsupported version)
/// - cert / key loading fails (file not found, malformed PEM)
/// - ALPN or congestion-control configuration fails
pub fn build_quiche_config(
    quic_cfg: &QuicConfig,
    cert_path: &str,
    key_path: &str,
) -> Result<quiche::Config, QuicError> {
    let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION)?;

    // TLS identity.
    config.load_cert_chain_from_pem_file(cert_path)?;
    config.load_priv_key_from_pem_file(key_path)?;

    // ALPN — advertise common protocols.
    config.set_application_protos(&[b"hq-interop", b"http/1.1", b"h3-29"])?;

    // Flow control (RFC 9000 §18.2).
    config.set_max_idle_timeout(quic_cfg.idle_timeout.as_millis() as u64);
    config.set_initial_max_data(quic_cfg.initial_max_data);
    config.set_initial_max_stream_data_bidi_local(quic_cfg.initial_max_stream_data);
    config.set_initial_max_stream_data_bidi_remote(quic_cfg.initial_max_stream_data);
    config.set_initial_max_stream_data_uni(quic_cfg.initial_max_stream_data);
    config.set_initial_max_streams_bidi(quic_cfg.max_streams_bidi);
    config.set_initial_max_streams_uni(quic_cfg.max_streams_uni);

    // Congestion control.
    let cc_algo = match quic_cfg.congestion {
        CongestionAlgorithm::Reno => quiche::CongestionControlAlgorithm::Reno,
        CongestionAlgorithm::Cubic => quiche::CongestionControlAlgorithm::CUBIC,
        CongestionAlgorithm::Bbr => quiche::CongestionControlAlgorithm::BBR,
        CongestionAlgorithm::Bbr2 => quiche::CongestionControlAlgorithm::BBR2,
    };
    config.set_cc_algorithm(cc_algo);

    // DATAGRAM extension (RFC 9221).
    if quic_cfg.extensions.datagram {
        config.enable_dgram(true, 64, 64);
    }

    // Disable active migration unless explicitly enabled.
    config.set_disable_active_migration(!quic_cfg.extensions.multipath);

    // Pacing helps reduce burstiness on high-throughput links.
    config.enable_pacing(true);

    // UDP payload size — allow jumbo packets on loopback / jumbo-frame
    // links. Without these, quiche defaults to 1200-byte sends, which
    // cripples throughput. 65507 = IPv4 max UDP payload (65535 - 20 - 8).
    config.set_max_send_udp_payload_size(65507);
    config.set_max_recv_udp_payload_size(65507);

    // Disable PMTU discovery — use the full payload size from the first
    // packet. On loopback (benchmark) the MTU is 65536, so probing only
    // adds latency before reaching full throughput.
    config.discover_pmtu(false);

    // Initial congestion window — start with a large window so the first
    // flight fills the pipe immediately (critical for short transfers).
    config
        .set_initial_congestion_window_packets(quic_cfg.initial_congestion_window_packets as usize);

    Ok(config)
}

/// Default idle-check interval derived from the config's idle timeout.
///
/// The engine calls `conn.timeout()` on each tick; if `None`, no timer
/// is needed. This helper returns a reasonable poll interval for the
/// timer-wheel.
#[must_use]
pub fn idle_poll_interval(idle_timeout: Duration) -> Duration {
    // Poll at 1/4 the idle timeout, clamped to [100ms, 5s].
    let quarter = idle_timeout
        .checked_div(4)
        .unwrap_or(Duration::from_millis(500));
    quarter.clamp(Duration::from_millis(100), Duration::from_secs(5))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_poll_clamps() {
        // Very short timeout → 100ms floor.
        let d = idle_poll_interval(Duration::from_millis(10));
        assert_eq!(d, Duration::from_millis(100));

        // Normal timeout (30s) → 7.5s → clamped to 5s.
        let d = idle_poll_interval(Duration::from_secs(30));
        assert_eq!(d, Duration::from_secs(5));

        // 2s timeout → 500ms.
        let d = idle_poll_interval(Duration::from_secs(2));
        assert_eq!(d, Duration::from_millis(500));
    }
}
