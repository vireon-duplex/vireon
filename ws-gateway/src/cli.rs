//! CLI argument parsing via clap.

use clap::Parser;

/// Vireon WebSocket-to-QUIC gateway.
///
/// Terminates WebSocket connections on one side and bridges them to a Vireon
/// QUIC server on the other. The upstream QUIC connection is established via
/// the `vireon-sdk` crate; the browser-side protocol is a compact binary
/// format documented in `src/protocol.rs`.
#[derive(Parser, Debug, Clone)]
#[command(
    name = "ws-gateway",
    version,
    about = "WebSocket-to-QUIC gateway for browser SDK clients"
)]
pub struct Cli {
    /// TCP bind address for the WebSocket listener.
    #[arg(long, default_value = "127.0.0.1:8443")]
    pub listen: String,

    /// Vireon QUIC server address (`host:port` or bare `host`).
    #[arg(long, default_value = "127.0.0.1:4433")]
    pub server: String,

    /// TLS SNI for the upstream QUIC connection. Defaults to the host part
    /// of `--server`.
    #[arg(long)]
    pub sni: Option<String>,

    /// How to verify the upstream QUIC server's TLS certificate.
    #[arg(long, default_value = "tofu")]
    pub tls_verify: TlsVerifyArg,
}

/// CLI selector for the upstream TLS verification mode. Mirrors
/// `vireon_sdk::TlsVerify` with a parser-friendly representation.
#[derive(Clone, Debug)]
pub enum TlsVerifyArg {
    /// Trust-on-first-use (default).
    Tofu,
    /// Disable certificate validation entirely. **Development only.**
    DangerAcceptInvalid,
    /// Validate against a CA bundle on disk.
    Strict { ca: String },
}

impl std::str::FromStr for TlsVerifyArg {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(ca) = s.strip_prefix("strict:") {
            if ca.is_empty() {
                return Err("strict:<path> requires a non-empty path".into());
            }
            return Ok(Self::Strict { ca: ca.into() });
        }
        match s {
            "tofu" => Ok(Self::Tofu),
            "danger-accept-invalid" | "danger_accept_invalid" => Ok(Self::DangerAcceptInvalid),
            other => Err(format!(
                "unknown tls-verify mode: {other} (expected tofu, danger-accept-invalid, strict:<path>)"
            )),
        }
    }
}
