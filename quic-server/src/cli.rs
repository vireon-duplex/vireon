//! CLI argument parsing via clap.
//!
//! Args that previously had `default_value` are now `Option<T>` so that
//! we can detect whether the user explicitly passed them. Resolution
//! (CLI > config file > hardcoded default) happens in [`crate::config`].

use clap::Parser;

/// Vireon QUIC Server — production-grade QUIC server built on io_uring + quiche.
#[derive(Parser, Debug)]
#[command(
    name = "quic-server",
    version,
    about = "High-performance QUIC server (io_uring + quiche)"
)]
pub struct Cli {
    /// Path to a TOML config file. Settings in the file are overridden by
    /// explicit CLI flags. See `vireon.conf.example` for the schema.
    #[arg(long)]
    pub config: Option<String>,

    /// UDP port to listen on.
    #[arg(short, long)]
    pub port: Option<u16>,

    /// PEM certificate chain path (or set `cert` in config file).
    #[arg(long)]
    pub cert: Option<String>,

    /// PEM private key path (or set `key` in config file).
    #[arg(long)]
    pub key: Option<String>,

    /// Optional PEM-encoded CA bundle for mutual TLS (mTLS). When set,
    /// the server requires every client to present a certificate signed
    /// by this CA during the TLS handshake. Clients without a valid
    /// certificate are rejected at the TLS layer before any application
    /// frame is processed.
    #[arg(long)]
    pub client_ca: Option<String>,

    /// Number of worker threads. 0 = auto-detect (one per CPU core).
    #[arg(short, long)]
    pub workers: Option<usize>,

    /// Worker mode: `single` (one thread) or `multi` (thread-per-core).
    #[arg(long)]
    pub mode: Option<WorkerModeArg>,

    /// Configuration preset.
    #[arg(long)]
    pub preset: Option<PresetArg>,

    /// Bind address (IP only; port comes from --port).
    #[arg(long)]
    pub bind: Option<String>,

    /// io_uring queue depth (0 = default).
    #[arg(long)]
    pub queue_depth: Option<u32>,

    /// Enable verbose logging.
    #[arg(short, long)]
    pub verbose: bool,

    /// Echo mode — echo every received frame back on the same stream.
    /// Used for load-testing (RR / pipeline benchmark modes).
    #[arg(long)]
    pub echo: bool,

    /// Skip CRC32C verification on received frames (default: ON).
    ///
    /// QUIC's AEAD (AES-GCM) already guarantees frame integrity end-to-end;
    /// the per-frame CRC32C trailer is redundant in every standard
    /// deployment. Skipping verification saves ~5–8% of decode CPU
    /// (flamegraph-verified, 2026-07-26) without changing wire shape —
    /// the trailer is still written by senders for backwards compat.
    ///
    /// Pass `--enforce-crc` to opt back into verification (e.g. when
    /// debugging an upstream frame codec bug).
    #[arg(long)]
    pub skip_crc: Option<bool>,

    /// Force CRC32C verification (overrides the default `--skip-crc`).
    ///
    /// Paranoid mode for debugging suspected frame-codec corruption.
    /// No effect on the wire — senders still compute the trailer.
    #[arg(long)]
    pub enforce_crc: bool,

    /// Enable SQPOLL kernel thread (kernel ≥ 5.11, requires CAP_SYS_NICE).
    /// The kernel thread polls the SQ ring, eliminating submit syscalls.
    /// Falls back to standard mode if the kernel denies SQPOLL.
    #[arg(long)]
    pub sqpoll: Option<bool>,

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

    /// WAL retention by age: entries older than this many seconds are
    /// truncated by the periodic sweeper (runs once per 60 s per
    /// worker). `0` (default) disables age-based retention. Only
    /// effective when `--wal-root` is also set.
    #[arg(long)]
    pub wal_retention_secs: Option<u64>,

    /// WAL retention by total bytes: per-topic cap. When a topic
    /// exceeds this, oldest entries are truncated until under the
    /// cap. `0` (default) disables byte-based retention. Only
    /// effective when `--wal-root` is also set.
    #[arg(long)]
    pub wal_max_bytes: Option<u64>,

    /// Maximum number of distinct topics tracked in the catalog.
    /// `TopicCreate` requests beyond this cap are rejected with a
    /// `quota_exceeded` ack. `0` (default) = unlimited.
    #[arg(long)]
    pub max_topics: Option<usize>,

    /// Maximum subscribers per distinct subscription pattern.
    /// `Subscribe` requests whose pattern already has this many
    /// subscribers are rejected with a `quota_exceeded` ack.
    /// `0` (default) = unlimited.
    #[arg(long)]
    pub max_subscribers_per_topic: Option<usize>,

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

    /// Optional HTTP port for the Prometheus `/metrics` endpoint. When
    /// set, the server exposes counters (connections, publishes,
    /// deliveries, errors, bytes) in Prometheus text format. A scrape
    /// request completes in <1 ms. When omitted, no metrics endpoint
    /// is started.
    #[arg(long)]
    pub metrics_port: Option<u16>,

    /// Maximum new QUIC handshakes per second accepted from a single
    /// source IP. `0` disables the limit (default — opt-in for
    /// multi-tenant deployments). When non-zero, an over-limit IP has
    /// its Initial packets dropped before connection state is
    /// allocated, defending against handshake storms (DoS, retry
    /// loops, misbehaving clients). Already-connected clients on the
    /// same IP are unaffected.
    #[arg(long)]
    pub max_handshakes_per_ip_per_sec: Option<u32>,
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
