//! CLI argument parsing via clap.

use clap::Parser;

/// Vireon QUIC Server — production-grade QUIC server built on io_uring + quiche.
#[derive(Parser, Debug)]
#[command(
    name = "quic-server",
    version,
    about = "High-performance QUIC server (io_uring + quiche)"
)]
pub struct Cli {
    /// UDP port to listen on.
    #[arg(short, long, default_value = "4433")]
    pub port: u16,

    /// PEM certificate chain path.
    #[arg(long)]
    pub cert: String,

    /// PEM private key path.
    #[arg(long)]
    pub key: String,

    /// Number of worker threads. 0 = auto-detect (one per CPU core).
    #[arg(short, long, default_value = "0")]
    pub workers: usize,

    /// Worker mode: `single` (one thread) or `multi` (thread-per-core).
    #[arg(long, default_value = "single")]
    pub mode: WorkerModeArg,

    /// Configuration preset.
    #[arg(long, default_value = "bare-metal")]
    pub preset: PresetArg,

    /// Bind address (IP only; port comes from --port).
    #[arg(long, default_value = "0.0.0.0")]
    pub bind: String,

    /// io_uring queue depth (0 = default).
    #[arg(long, default_value = "0")]
    pub queue_depth: u32,

    /// Enable verbose logging.
    #[arg(short, long)]
    pub verbose: bool,
}

/// CLI worker-mode selector.
#[derive(Clone, Debug, clap::ValueEnum)]
pub enum WorkerModeArg {
    /// Single worker — one reactor on the main thread.
    Single,
    /// Multi worker — N reactors, one per CPU core (thread-per-core).
    Multi,
    /// Distributed — multi-worker + inter-node coordination (stub).
    Distributed,
}

/// CLI preset selector.
#[derive(Clone, Debug, clap::ValueEnum)]
pub enum PresetArg {
    /// Multi-tenant cloud workload.
    Cloud,
    /// Bare-metal server, general production.
    #[value(name = "bare-metal")]
    BareMetal,
    /// 100 GbE+ high-throughput.
    #[value(name = "hundred-gbe")]
    HundredGbe,
    /// Sub-ms tail latency (RPC, gaming).
    #[value(name = "low-latency")]
    LowLatency,
    /// Bulk-transfer max throughput.
    #[value(name = "max-throughput")]
    MaxThroughput,
}
