//! Vireon QUIC Server — production-grade QUIC server on io_uring + quiche.

mod cli;
mod config;
mod connection;
mod distributed;
mod error;
mod eviction;
mod layers;
mod server;
mod stack;
mod worker;

use clap::Parser;

use crate::cli::Cli;
use crate::config::ServerConfig;
use crate::error::ServerError;
use crate::server::QuicServer;

fn main() -> Result<(), ServerError> {
    let cli = Cli::parse();
    init_tracing(&cli);

    tracing::info!("vireon quic-server starting");
    tracing::debug!(cli = ?cli, "parsed CLI");

    let config = ServerConfig::from_cli(&cli).map_err(|e| ServerError::Config(format!("{e:#}")))?;

    print_banner(&config);

    let server = QuicServer::new(config);
    server.run()
}

fn init_tracing(cli: &Cli) {
    let filter = if cli.verbose {
        "quic_server=debug,quic_engine=debug,io=debug,quiche=trace,warn"
    } else {
        "quic_server=info,quic_engine=info,warn"
    };
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .try_init();
}

fn print_banner(config: &ServerConfig) {
    tracing::info!("============================================================");
    tracing::info!("  Vireon QUIC Server");
    tracing::info!("  Mode:     {:?}", config.mode);
    tracing::info!("  Preset:   {:?}", config.preset);
    tracing::info!("  Bind:     {}:{}", config.bind_addr, config.port);
    tracing::info!("  Workers:  {}", config.num_workers());
    tracing::info!("  Cert:     {}", config.cert_path);
    tracing::info!("  Max conns/shard: {}", config.quic_config.max_connections);
    tracing::info!("  CC:       {:?}", config.quic_config.congestion);
    if config.echo {
        tracing::info!("  Echo:     ENABLED (benchmark mode)");
    }
    tracing::info!("============================================================");
}
