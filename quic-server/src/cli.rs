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

    /// Optional PEM-encoded CA bundle for mutual TLS (mTLS). When set,
    /// the server requires every client to present a certificate signed
    /// by this CA during the TLS handshake. Clients without a valid
    /// certificate are rejected at the TLS layer before any application
    /// frame is processed.
    #[arg(long)]
    pub client_ca: Option<String>,

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

    /// Echo mode — echo every received frame back on the same stream.
    /// Used for load-testing (RR / pipeline benchmark modes).
    #[arg(long)]
    pub echo: bool,

    /// Skip CRC32C verification on received frames.
    /// Use ONLY in trusted environments where QUIC's AEAD guarantees
    /// integrity. Saves ~50% of decode time for small frames.
    #[arg(long)]
    pub skip_crc: bool,

    /// Enable SQPOLL kernel thread (kernel ≥ 5.11, requires CAP_SYS_NICE).
    /// The kernel thread polls the SQ ring, eliminating submit syscalls.
    /// Falls back to standard mode if the kernel denies SQPOLL.
    #[arg(long, default_value_t = true)]
    pub sqpoll: bool,

    /// Dump profiler report to this file on shutdown.
    /// Requires building with --features profiling.
    /// Formats: .json, .csv, .ctrace (Chrome trace), .folded (flamegraph),
    /// .dot (Graphviz), anything else = text report.
    #[arg(long)]
    pub profile_output: Option<String>,

    /// Path to a TOML ACL rules file. When set, the server loads
    /// tenant-isolation rules from this file and replaces the default
    /// permissive `allow("*.*", None, ALL)` table. When omitted the
    /// permissive default is kept (single-tenant mode).
    ///
    /// See `acl_loader::AclRulesFile` for the file schema.
    #[arg(long)]
    pub acl_rules_path: Option<String>,

    /// Directory root for the write-ahead log (WAL). When set, every
    /// accepted publish is appended to a per-worker `FileStore` under
    /// `<root>/worker_<core_id>/` before fan-out, and subscribers can
    /// use `Fetch(offset)` to replay missed messages. When omitted the
    /// WAL is disabled (publish is fire-and-forget, Fetch returns an
    /// error ack).
    #[arg(long)]
    pub wal_root: Option<String>,

    /// Cluster self node id (P6). When set, the server joins a
    /// multi-node cluster and routes publishes to the owning node via
    /// UDP-based inter-node transport. Must be >0; must also appear in
    /// `--cluster-peers`.
    #[arg(long)]
    pub cluster_node_id: Option<u32>,

    /// Cluster peer list, formatted as
    /// `"id=host:port,id=host:port,..."`. Every node in the cluster
    /// (including self) must be listed. Example:
    /// `--cluster-peers "1=10.0.0.1:4433,2=10.0.0.2:4433,3=10.0.0.3:4433"`.
    /// Requires `--cluster-node-id`.
    #[arg(long)]
    pub cluster_peers: Option<String>,
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
