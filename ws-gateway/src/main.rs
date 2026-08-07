//! ws-gateway: WebSocket-to-QUIC gateway for browser SDK clients.
//!
//! Each browser WebSocket connection is bridged to an upstream Vireon QUIC
//! server via the `vireon-sdk` crate. The browser-side wire format is
//! documented in `src/protocol.rs`; per-session state lives in
//! `src/session.rs`.

mod cli;
mod protocol;
mod session;

use std::sync::Arc;

use clap::Parser;
use tokio::net::TcpListener;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use crate::cli::Cli;
use crate::session::run_session;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Cli::parse();
    let listen = args.listen.clone();
    let server = args.server.clone();
    let sni = args.sni.clone();
    let tls = args.tls_verify.clone();

    info!(%listen, %server, "[gateway] listening for WebSocket connections");

    let listener = TcpListener::bind(&listen)
        .await
        .map_err(|e| anyhow::anyhow!("bind {listen} failed: {e}"))?;

    // Shared immutable config cloned per accept — cheap (Arc-free, all
    // small owned strings).
    let cfg = Arc::new((server, sni, tls));

    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                error!(error = %format!("{e:?}"), "[gateway] accept failed");
                continue;
            }
        };
        let cfg = Arc::clone(&cfg);
        tokio::spawn(async move {
            info!(%peer, "[gateway] WS connect from peer");
            let ws = match tokio_tungstenite::accept_async(stream).await {
                Ok(ws) => ws,
                Err(e) => {
                    error!(%peer, error = %format!("{e:?}"), "[gateway] WS handshake failed");
                    return;
                }
            };
            let (server, sni, tls) = (&cfg.0, &cfg.1, &cfg.2);
            if let Err(e) = run_session(ws, server.clone(), sni.clone(), tls.clone()).await {
                info!(%peer, error = %e, "[gateway] session ended");
            } else {
                info!(%peer, "[gateway] session ended cleanly");
            }
        });
    }
}
