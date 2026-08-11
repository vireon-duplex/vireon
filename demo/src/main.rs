//! Vireon Demo Suite — multi-scenario showcase of Vireon's QUIC pub/sub runtime.
//!
//! Each subcommand tells a different part of the Vireon story:
//!
//!   `showcase` — 3 dedicated streams (Audio + Video + AI) on 1 QUIC connection
//!                 with per-stream delivery policies and automatic reconnect.
//!   `fanout`   — M publishers → N subscribers (broadcast: everyone gets all).
//!   `group`    — Consumer group: 1 publisher → N members (load-balanced).
//!   `cluster`  — N-node cluster: cross-node fan-out across the mesh.
//!
//! Global `--workers N` controls server thread count (single vs multi-worker).
//!
//! Usage:
//!   cargo run -p demo --release                          # defaults to showcase
//!   cargo run -p demo --release -- fanout
//!   cargo run -p demo --release -- fanout --publishers 5 --subscribers 20
//!   cargo run -p demo --release -- --workers 4 group --members 5
//!   cargo run -p demo --release -- cluster --nodes 3

#![allow(clippy::print_stdout)]

mod cluster;
mod common;
mod fanout;
mod group;
mod showcase;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "vireon-showcase",
    version,
    about = "Vireon multi-scenario demo suite",
    long_about = "Each subcommand demonstrates a different Vireon capability.\n\
                  Run without a subcommand to see the default showcase.",
)]
struct Cli {
    /// Number of server worker threads (1 = single, 4+ = multi-worker).
    #[arg(short, long, global = true, default_value = "1")]
    workers: usize,

    #[command(subcommand)]
    command: Option<Scenario>,
}

#[derive(Subcommand)]
enum Scenario {
    /// 3-stream hero demo: Audio + Video + AI + auto-reconnect. [default]
    Showcase,

    /// M publishers → N subscribers (broadcast fan-out).
    Fanout {
        /// Number of publisher connections.
        #[arg(long, default_value = "1")]
        publishers: usize,
        /// Number of subscriber connections.
        #[arg(long, default_value = "10")]
        subscribers: usize,
    },

    /// Consumer group: 1 publisher → N members (load-balanced delivery).
    Group {
        /// Number of group members (workers).
        #[arg(long, default_value = "5")]
        members: usize,
    },

    /// N-node cluster: cross-node fan-out across the mesh.
    Cluster {
        /// Number of server nodes in the cluster.
        #[arg(long, default_value = "3")]
        nodes: usize,
    },
}

#[tokio::main]
async fn main() {
    common::init_tracing();

    let cli = Cli::parse();

    match cli.command.unwrap_or(Scenario::Showcase) {
        Scenario::Showcase => showcase::run(cli.workers).await,
        Scenario::Fanout {
            publishers,
            subscribers,
        } => fanout::run(cli.workers, publishers, subscribers).await,
        Scenario::Group { members } => group::run(cli.workers, members).await,
        Scenario::Cluster { nodes } => cluster::run(cli.workers, nodes).await,
    }
}
