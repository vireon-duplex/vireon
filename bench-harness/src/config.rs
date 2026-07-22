//! Quiche client config for the load harness.
//!
//! ALPN `h3-29` matches the server's advertised protocols
//! (`["hq-interop", "http/1.1", "h3-29"]`).

use anyhow::Result;

/// ALPN — must match the server's.
const ALPN: &[u8] = b"h3-29";

/// Builds a quiche client config tuned for load generation: ALPN `h3-29`,
/// generous flow-control windows (10 MiB connection / per-stream), peer
/// verification off.
pub fn build_client_config() -> Result<quiche::Config> {
    let mut config = quiche::Config::new(quiche::PROTOCOL_VERSION)?;
    config.set_application_protos(&[ALPN])?;
    config.set_max_idle_timeout(60_000);
    // 65507 = IPv4 UDP max payload (65535 - 20 IP - 8 UDP).
    config.set_max_recv_udp_payload_size(65507);
    config.set_max_send_udp_payload_size(65507);
    config.set_initial_max_data(100 * 1024 * 1024);
    config.set_initial_max_stream_data_bidi_local(10 * 1024 * 1024);
    config.set_initial_max_stream_data_bidi_remote(10 * 1024 * 1024);
    config.set_initial_max_stream_data_uni(10 * 1024 * 1024);
    config.set_initial_max_streams_bidi(1024);
    config.set_initial_max_streams_uni(1024);
    config.set_disable_active_migration(true);
    // Skip incremental PMTU probing so the full payload size is used from the
    // first 1-RTT packet, maximising per-datagram throughput.
    config.discover_pmtu(false);
    // CUBIC outperforms BBR on loopback: BBR paces to BDP (bandwidth ×
    // min_RTT), and loopback's ~20µs RTT yields a tiny BDP that throttles
    // throughput. CUBIC is loss-based — no loss on loopback means the
    // window grows unbounded.
    config.set_cc_algorithm(quiche::CongestionControlAlgorithm::CUBIC);
    config.set_initial_congestion_window_packets(100);
    Ok(config)
}
