//! [`QuicConfig`] + [`Preset`] + capacity / timeout constants.
//!
//! Phase 0 surface — sufficient fields to drive Phase 1+
//! implementation. Defaults follow RFC 9000 §18.2 recommendations
//! and Cloudflare's published quiche defaults; per-preset tuning
//! (Cloud / BareMetal / HundredGbe / LowLatency / MaxThroughput)
//! matches the policy established by
//! [`connection_shard::config::Preset`](../../connection-shard/src/config.rs).
//!
//! See `DESIGN.md` §2 (Architecture Overview) for the full rationale
//! behind each field, and §4 per-subsystem sections for the RFC
//! section each field implements.

use std::time::Duration;

// ── Capacity / size constants ────────────────────────────────────

/// Default maximum concurrent connections per shard (per Reactor
/// core). The workspace-wide limit is therefore
/// `DEFAULT_MAX_CONNECTIONS × num_cores`. Matches
/// `connection_shard::config::DEFAULT_CAPACITY`.
pub const DEFAULT_MAX_CONNECTIONS: usize = 65_536;

/// Floor on `QuicConfig::max_connections`. Below this, the engine
/// cannot keep enough state to amortize handshake cost.
pub const MIN_MAX_CONNECTIONS: usize = 256;

/// Ceiling on `QuicConfig::max_connections`. One slot costs
/// roughly 4 KiB resident (quiche connection + Vireon bookkeeping),
/// so 1 M connections ≈ 4 GiB resident per shard.
pub const MAX_MAX_CONNECTIONS: usize = 1 << 20; // 1 M

// ── Timeout constants ────────────────────────────────────────────

/// Default idle timeout (RFC 9000 §10.1). Connections with no packet
/// exchange for this long are torn down.
pub const DEFAULT_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// Floor on idle timeout. RFC 9000 §18.2-10.1 permits 0 to disable
/// the idle timer entirely, but we forbid that in production
/// configurations to prevent connection leaks.
pub const MIN_IDLE_TIMEOUT: Duration = Duration::from_millis(100);

/// Maximum handshake duration before the connection is abandoned
/// (RFC 9000 §4.4 implicit). 30 s is generous; production TLS
/// handshakes complete in <100 ms on a healthy network.
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

// ── Flow control defaults ────────────────────────────────────────

/// Initial per-connection flow-control window (MAX_DATA,
/// RFC 9000 §18.2). 100 MiB — large enough for 15 streams × 10 MiB each.
pub const DEFAULT_INITIAL_MAX_DATA: u64 = 100 * 1024 * 1024;

/// Initial per-stream flow-control window (MAX_STREAM_DATA).
/// 10 MiB — matches the benchmark client's window, preventing per-stream
/// flow-control stalls during bulk transfer.
pub const DEFAULT_INITIAL_MAX_STREAM_DATA: u64 = 10 * 1024 * 1024;

/// Initial bidirectional stream count (MAX_STREAMS_BIDIR).
///
/// 1024 + 1024 (uni) = 2048 concurrent streams per connection — aligns
/// with the design vision's "thousands of independent logical streams"
/// claim. Operators with tight per-connection memory budgets can lower
/// this via config; quiche's per-stream state is ~1 KiB so the default
/// costs ~2 MiB per connection.
pub const DEFAULT_INITIAL_MAX_STREAMS_BIDI: u64 = 1024;

/// Initial unidirectional stream count (MAX_STREAMS_UNI).
pub const DEFAULT_INITIAL_MAX_STREAMS_UNI: u64 = 1024;

// ── Congestion control ───────────────────────────────────────────

/// Pluggable congestion control algorithm. Wired through to
/// `quiche` via `Config::set_cc_algorithm_name`. See `DESIGN.md`
/// §5 for the full comparison (CUBIC vs BBR vs BBRv2).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum CongestionAlgorithm {
    /// RFC 9002 §7 default. Window-based, conservative.
    Reno,
    /// High-speed window-based; Linux default since 2.6.19.
    Cubic,
    /// Google's BBRv1 — bandwidth estimation, model-based.
    #[default]
    Bbr,
    /// BBRv2 draft — fixes BBRv1's fairness issues with CUBIC.
    Bbr2,
}

impl CongestionAlgorithm {
    /// Returns the lower-case string quiche expects.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Reno => "reno",
            Self::Cubic => "cubic",
            Self::Bbr => "bbr",
            Self::Bbr2 => "bbr2",
        }
    }
}

// ── Extension toggles ────────────────────────────────────────────

/// Toggles for QUIC extensions. Hook points are designed in Phase 0;
/// implementations land in their respective phases per the roadmap
/// in `DESIGN.md` §12.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ExtensionFlags {
    /// RFC 9221 DATAGRAM frame support.
    pub datagram: bool,
    /// draft-ietf-quic-ack-frequency — reduce ACK rate on high-pps.
    pub ack_frequency: bool,
    /// RFC 9464 Simple Multipath QUIC.
    pub multipath: bool,
    /// RFC 9000 §9 Connection Migration — lets a client change its
    /// 4-tuple (WiFi switch, NAT rebinding) without dropping the
    /// connection. quiche handles PATH_CHALLENGE/PATH_RESPONSE
    /// validation internally. On by default (standard QUIC feature).
    pub migration: bool,
    /// draft-ietf-quic-bbrv2-congestion-control (only meaningful
    /// when `congestion = Bbr2`).
    pub bbr2: bool,
    /// RFC 9287 Grease-QUIC-Bit (always on in practice; this flag
    /// exists only to allow A/B benchmarking).
    pub grease_bit: bool,
}

impl ExtensionFlags {
    /// Production default: DATAGRAM + ACK Frequency + Migration + Grease
    /// Bit on, Multipath / BBRv2 off (still draft).
    #[must_use]
    pub const fn production() -> Self {
        Self {
            datagram: true,
            ack_frequency: true,
            multipath: false,
            migration: true,
            bbr2: false,
            grease_bit: true,
        }
    }
}

// ── Main config type ─────────────────────────────────────────────

/// Engine-wide configuration. One [`QuicConfig`] is shared by all
/// connections on a shard; per-connection overrides happen via
/// quiche's `set_*` methods on individual connections.
#[derive(Clone, Debug)]
pub struct QuicConfig {
    /// Maximum concurrent connections per shard.
    pub max_connections: usize,
    /// Maximum concurrent bidirectional streams per connection.
    pub max_streams_bidi: u64,
    /// Maximum concurrent unidirectional streams per connection.
    pub max_streams_uni: u64,
    /// Connection-level flow-control initial window.
    pub initial_max_data: u64,
    /// Stream-level flow-control initial window.
    pub initial_max_stream_data: u64,
    /// Idle timeout — connections go silent → torn down.
    pub idle_timeout: Duration,
    /// Handshake timeout — abandons stuck handshakes.
    pub handshake_timeout: Duration,
    /// Congestion control algorithm.
    pub congestion: CongestionAlgorithm,
    /// Initial congestion window in packets (RFC 9002 §7.2 —
    /// default `10 * max_datagram_size`).
    pub initial_congestion_window_packets: u32,
    /// Extension toggles.
    pub extensions: ExtensionFlags,
    /// Whether the engine is shutting down. Set by the runtime; the
    /// engine rejects all new connections while true.
    pub shutting_down: bool,
    /// Skip CRC32C verification on received frames. Use only in trusted
    /// environments where the transport layer guarantees integrity.
    pub skip_crc: bool,
    /// Optional PEM-encoded CA bundle used to verify client certificates
    /// (mutual TLS). When set, the server enables `verify_peer(true)`
    /// and rejects any client that does not present a certificate
    /// signed by this CA. When `None`, client certs are not required.
    pub client_ca: Option<String>,
    /// Maximum new QUIC handshakes per second from a single source IP.
    /// `0` disables the limiter (default). When non-zero, an
    /// [`IpRateLimiter`](crate::rate_limiter::IpRateLimiter) tracks
    /// per-IP handshake arrivals and rejects Initial packets beyond
    /// the cap before they allocate connection state. Defends against
    /// handshake storms from a misbehaving or hostile peer without
    /// affecting already-connected clients on the same IP.
    pub max_handshakes_per_ip_per_sec: u32,
}

impl QuicConfig {
    /// Validate the configuration; returns the first violation as
    /// a [`crate::QuicError::InvalidConfig`] or `Ok(())`.
    ///
    /// Called by [`crate::builder::QuicEngineBuilder::build`] — never
    /// needs to be invoked directly by the application.
    pub fn validate(&self) -> Result<(), crate::QuicError> {
        use crate::QuicError::InvalidConfig;

        if self.max_connections < MIN_MAX_CONNECTIONS || self.max_connections > MAX_MAX_CONNECTIONS
        {
            return Err(InvalidConfig(format!(
                "max_connections must be in [{MIN_MAX_CONNECTIONS}, {MAX_MAX_CONNECTIONS}] (is {})",
                self.max_connections
            )));
        }
        if self.idle_timeout < MIN_IDLE_TIMEOUT {
            return Err(InvalidConfig(format!(
                "idle_timeout must be >= {:?} (is {:?})",
                MIN_IDLE_TIMEOUT, self.idle_timeout
            )));
        }
        if self.initial_max_data == 0 {
            return Err(InvalidConfig("initial_max_data must be > 0".into()));
        }
        if self.initial_max_stream_data == 0 {
            return Err(InvalidConfig("initial_max_stream_data must be > 0".into()));
        }
        if self.max_streams_bidi == 0 {
            return Err(InvalidConfig("max_streams_bidi must be > 0".into()));
        }
        if self.max_streams_uni == 0 {
            return Err(InvalidConfig("max_streams_uni must be > 0".into()));
        }
        if self.initial_congestion_window_packets == 0 {
            return Err(InvalidConfig(
                "initial_congestion_window_packets must be > 0".into(),
            ));
        }
        if self.extensions.bbr2 && !matches!(self.congestion, CongestionAlgorithm::Bbr2) {
            return Err(InvalidConfig(
                "extensions.bbr2 is set but congestion != Bbr2".into(),
            ));
        }
        Ok(())
    }
}

impl Default for QuicConfig {
    fn default() -> Self {
        Self {
            max_connections: DEFAULT_MAX_CONNECTIONS,
            max_streams_bidi: DEFAULT_INITIAL_MAX_STREAMS_BIDI,
            max_streams_uni: DEFAULT_INITIAL_MAX_STREAMS_UNI,
            initial_max_data: DEFAULT_INITIAL_MAX_DATA,
            initial_max_stream_data: DEFAULT_INITIAL_MAX_STREAM_DATA,
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            congestion: CongestionAlgorithm::Cubic,
            initial_congestion_window_packets: 100,
            extensions: ExtensionFlags::production(),
            shutting_down: false,
            skip_crc: false,
            client_ca: None,
            max_handshakes_per_ip_per_sec: 0,
        }
    }
}

// ── Presets ──────────────────────────────────────────────────────

/// Deployment-class presets. Each preset populates a [`QuicConfig`]
/// with values tuned for that environment.
///
/// Matches the preset style of
/// [`connection_shard::config::Preset`](../../connection-shard/src/config.rs).
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub enum Preset {
    /// Multi-tenant cloud / shared-tenant workload. Conservative
    /// limits, fast teardown on idle.
    Cloud,
    /// Bare-metal server with dedicated NIC. Default-configured for
    /// general production.
    BareMetal,
    /// 100 GbE+ NIC. Aggressive flow-control windows, BBR pacing.
    HundredGbe,
    /// Sub-ms tail-latency target (RPC, gaming). Smaller windows,
    /// shorter idle / handshake timeouts.
    LowLatency,
    /// Maximize throughput for bulk-transfer workloads (storage,
    /// ML checkpoints). Larger windows, relaxed timeouts.
    MaxThroughput,
}

impl Preset {
    /// Materialize a preset to a [`QuicConfig`]. Callers may then
    /// mutate individual fields before handing the config to the
    /// builder.
    #[must_use]
    pub fn to_config(self) -> QuicConfig {
        let mut cfg = QuicConfig::default();
        match self {
            Self::Cloud => {
                cfg.max_connections = 32_768;
                cfg.idle_timeout = Duration::from_secs(15);
                cfg.handshake_timeout = Duration::from_secs(10);
                cfg.congestion = CongestionAlgorithm::Bbr;
            }
            Self::BareMetal => {
                // Defaults already target this class.
                //
                // Flow-control windows (`initial_max_data` 100 MiB,
                // `initial_max_stream_data` 10 MiB) were experimentally
                // bumped to combat the 100-conn throughput anomaly.
                // The bump did not improve the anomaly (root cause is
                // client-side thread oversubscription + loopback
                // contention) and slightly hurt high-scale throughput
                // (500/1000 conns) due to higher per-conn buffer
                // pressure. Reverted to defaults.
            }
            Self::HundredGbe => {
                cfg.max_connections = 131_072;
                cfg.initial_max_data = 16 * 1024 * 1024;
                cfg.initial_max_stream_data = 4 * 1024 * 1024;
                cfg.max_streams_bidi = 512;
                cfg.max_streams_uni = 512;
                cfg.congestion = CongestionAlgorithm::Bbr;
                cfg.initial_congestion_window_packets = 32;
            }
            Self::LowLatency => {
                cfg.max_connections = 32_768;
                cfg.initial_max_data = 256 * 1024;
                cfg.initial_max_stream_data = 64 * 1024;
                cfg.idle_timeout = Duration::from_secs(5);
                cfg.handshake_timeout = Duration::from_secs(5);
                cfg.congestion = CongestionAlgorithm::Bbr;
                cfg.initial_congestion_window_packets = 5;
            }
            Self::MaxThroughput => {
                cfg.max_connections = 16_384;
                cfg.initial_max_data = 64 * 1024 * 1024;
                cfg.initial_max_stream_data = 16 * 1024 * 1024;
                cfg.max_streams_bidi = 1024;
                cfg.max_streams_uni = 1024;
                cfg.congestion = CongestionAlgorithm::Bbr;
                cfg.initial_congestion_window_packets = 64;
            }
        }
        cfg
    }
}

impl Default for Preset {
    fn default() -> Self {
        Self::BareMetal
    }
}

// ── Tests ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_passes_validate() {
        QuicConfig::default()
            .validate()
            .expect("default config valid");
    }

    #[test]
    fn all_presets_pass_validate() {
        let presets = [
            Preset::Cloud,
            Preset::BareMetal,
            Preset::HundredGbe,
            Preset::LowLatency,
            Preset::MaxThroughput,
        ];
        for p in presets {
            p.to_config()
                .validate()
                .unwrap_or_else(|e| panic!("preset {p:?}: {e}"));
        }
    }

    #[test]
    fn max_connections_below_min_rejected() {
        let c = QuicConfig {
            max_connections: MIN_MAX_CONNECTIONS / 2,
            ..QuicConfig::default()
        };
        assert!(c.validate().is_err());
    }

    #[test]
    fn bbr2_flag_without_bbr2_algo_rejected() {
        let c = QuicConfig {
            extensions: ExtensionFlags {
                bbr2: true,
                ..ExtensionFlags::production()
            },
            congestion: CongestionAlgorithm::Bbr,
            ..QuicConfig::default()
        };
        assert!(c.validate().is_err());
    }

    #[test]
    fn congestion_str_roundtrip() {
        assert_eq!(CongestionAlgorithm::Reno.as_str(), "reno");
        assert_eq!(CongestionAlgorithm::Cubic.as_str(), "cubic");
        assert_eq!(CongestionAlgorithm::Bbr.as_str(), "bbr");
        assert_eq!(CongestionAlgorithm::Bbr2.as_str(), "bbr2");
    }

    #[test]
    fn production_flags_sensible() {
        let f = ExtensionFlags::production();
        assert!(f.datagram);
        assert!(f.ack_frequency);
        assert!(f.grease_bit);
        assert!(f.migration); // standard QUIC feature, on by default
        assert!(!f.multipath); // still draft
        assert!(!f.bbr2); // still draft
    }
}
