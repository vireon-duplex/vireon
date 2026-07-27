//! Server configuration — resolves CLI args into typed configuration.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use quic_engine::{QuicConfig, QuicPreset};

use crate::cli::{Cli, PresetArg, WorkerModeArg};

/// Worker mode — resolved from CLI.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerMode {
    /// One reactor on the main thread.
    Single,
    /// N reactors, one per CPU core.
    Multi,
    /// Multi-worker + inter-node coordination (stub).
    Distributed,
}

impl From<WorkerModeArg> for WorkerMode {
    fn from(arg: WorkerModeArg) -> Self {
        match arg {
            WorkerModeArg::Single => Self::Single,
            WorkerModeArg::Multi => Self::Multi,
            WorkerModeArg::Distributed => Self::Distributed,
        }
    }
}

/// Resolved server configuration.
#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub port: u16,
    pub bind_addr: IpAddr,
    pub cert_path: String,
    pub key_path: String,
    pub workers: usize,
    pub mode: WorkerMode,
    pub preset: QuicPreset,
    pub quic_config: QuicConfig,
    pub queue_depth: u32,
    pub echo: bool,
    pub sqpoll: bool,
    pub skip_crc: bool,
    /// Optional operator-provided ACL rules file path. When set, the
    /// server loads tenant-isolation rules from this path at startup
    /// (one `AclTable` parsed in `from_cli`, shared across workers
    /// via `Clone`). When `None`, the permissive default
    /// `allow("*.*", None, ALL)` is kept.
    #[allow(dead_code)]
    pub acl_rules_path: Option<String>,
    /// Parsed ACL table, ready to apply. `None` keeps the dispatcher's
    /// built-in permissive default. Parsed once in `from_cli`; cloned
    /// per-worker at stack construction (cheap: `Vec<AclRule>` clone).
    pub acl_table: Option<topic_acl::AclTable>,
    /// Optional WAL root directory. When set, each worker opens its own
    /// `FileStore` rooted at `<root>/worker_<core_id>/` and appends
    /// every accepted publish before fan-out. `None` disables WAL.
    #[allow(dead_code)]
    pub wal_root: Option<std::path::PathBuf>,
    /// WAL retention policy applied per-topic by the periodic sweeper
    /// (runs once per 60 s per worker). Both fields `None` = keep
    /// forever (default). Configured via `--wal-retention-secs` and
    /// `--wal-max-bytes`.
    pub wal_retention: Option<wal_store::RetentionPolicy>,
    /// Cap on total topics in the catalog. `None` = unlimited.
    pub max_topics: Option<usize>,
    /// Cap on subscribers per distinct subscription pattern.
    /// `None` = unlimited.
    pub max_subscribers_per_topic: Option<usize>,

    /// Optional cluster transport config (P6). When set, the server
    /// joins a multi-node cluster and routes publishes via UDP-based
    /// inter-node transport. `None` = single-node mode (all topics
    /// owned locally).
    #[allow(dead_code)]
    pub cluster: Option<crate::cluster::ClusterTransportConfig>,

    /// Optional Prometheus `/metrics` endpoint port. When `Some`, the
    /// server starts an HTTP listener on this port.
    pub metrics_port: Option<u16>,
    /// Shared thread-safe metrics store, cloned per worker. `None` when
    /// metrics are disabled; `Some(Arc<...>)` when the endpoint is
    /// active.
    pub metrics: Option<Arc<crate::metrics::ServerMetrics>>,
}

/// Default queue depth if the CLI passes 0.
///
/// Matches `io::QUEUE_DEPTH` (1024) — generous enough for the 1 M-connection
/// per-core target once provided-buffer rings absorb per-recv buffering.
/// The previous value (256) bottlenecked recv batching under bursty loads
/// (e.g. 100-connection handshake storms) because the SQ filled before the
/// kernel could drain provided-buffer completions.
const DEFAULT_QUEUE_DEPTH: u32 = 1024;

impl ServerConfig {
    /// Parse CLI args into a resolved configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the bind address is invalid.
    pub fn from_cli(cli: &Cli) -> anyhow::Result<Self> {
        let bind_addr: IpAddr = cli
            .bind
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid bind address '{}': {e}", cli.bind))?;

        let preset = preset_from_arg(&cli.preset);
        let mut quic_config = preset.to_config();
        // `--enforce-crc` wins over the default `--skip-crc=true`. Both
        // flags can be present without error; the explicit "verify"
        // semantic takes precedence so operators can flip back without
        // editing the launch line.
        quic_config.skip_crc = cli.skip_crc && !cli.enforce_crc;
        let preset_skip_crc = quic_config.skip_crc;
        quic_config.client_ca = cli.client_ca.clone();
        quic_config.max_handshakes_per_ip_per_sec = cli.max_handshakes_per_ip_per_sec;

        let workers = resolve_workers(cli.workers, &cli.mode);

        // Parse the ACL rules file once at startup so worker threads
        // just `Clone` the resulting table. A parse failure surfaces as
        // a `Config` error — the server refuses to start rather than
        // silently falling back to permissive defaults.
        let acl_table = match cli.acl_rules_path.as_deref() {
            Some(path) => Some(
                crate::acl_loader::load(std::path::Path::new(path))
                    .map_err(|e| anyhow::anyhow!("ACL rules file `{path}`: {e:#}"))?,
            ),
            None => None,
        };

        // P6: parse cluster configuration if --cluster-node-id is set.
        let cluster = match (cli.cluster_node_id, cli.cluster_peers.as_deref()) {
            (Some(id), Some(peers_str)) => {
                let self_id = session_registry::NodeId::new(id);
                let peers = crate::cluster::parse_cluster_peers(peers_str)
                    .map_err(|e| anyhow::anyhow!("--cluster-peers: {e}"))?;
                let cfg = crate::cluster::build_cluster_transport_config(self_id, &peers)
                    .map_err(|e| anyhow::anyhow!("--cluster-peers: {e}"))?;
                Some(cfg)
            }
            (Some(_), None) => {
                return Err(anyhow::anyhow!(
                    "--cluster-node-id requires --cluster-peers"
                ));
            }
            (None, Some(_)) => {
                return Err(anyhow::anyhow!(
                    "--cluster-peers requires --cluster-node-id"
                ));
            }
            (None, None) => None,
        };

        Ok(Self {
            port: cli.port,
            bind_addr,
            cert_path: cli.cert.clone(),
            key_path: cli.key.clone(),
            workers,
            mode: WorkerMode::from(cli.mode.clone()),
            preset,
            quic_config,
            queue_depth: if cli.queue_depth == 0 {
                DEFAULT_QUEUE_DEPTH
            } else {
                cli.queue_depth
            },
            echo: cli.echo,
            sqpoll: cli.sqpoll,
            skip_crc: preset_skip_crc,
            acl_rules_path: cli.acl_rules_path.clone(),
            acl_table,
            wal_root: cli.wal_root.as_deref().map(std::path::PathBuf::from),
            wal_retention: build_wal_retention(cli.wal_retention_secs, cli.wal_max_bytes),
            max_topics: (cli.max_topics > 0).then_some(cli.max_topics),
            max_subscribers_per_topic: (cli.max_subscribers_per_topic > 0)
                .then_some(cli.max_subscribers_per_topic),
            cluster,
            metrics_port: cli.metrics_port,
            metrics: cli
                .metrics_port
                .map(|_| Arc::new(crate::metrics::ServerMetrics::new())),
        })
    }

    /// Returns the number of workers to spawn.
    #[must_use]
    pub fn num_workers(&self) -> usize {
        match self.mode {
            WorkerMode::Single => 1,
            WorkerMode::Multi | WorkerMode::Distributed => self.workers.max(1),
        }
    }

    /// Returns the shutdown grace period.
    #[must_use]
    #[allow(dead_code)]
    pub const fn shutdown_grace(&self) -> Duration {
        Duration::from_secs(5)
    }
}

fn preset_from_arg(arg: &PresetArg) -> QuicPreset {
    match arg {
        PresetArg::Cloud => QuicPreset::Cloud,
        PresetArg::BareMetal => QuicPreset::BareMetal,
        PresetArg::HundredGbe => QuicPreset::HundredGbe,
        PresetArg::LowLatency => QuicPreset::LowLatency,
        PresetArg::MaxThroughput => QuicPreset::MaxThroughput,
    }
}

fn resolve_workers(requested: usize, mode: &WorkerModeArg) -> usize {
    match mode {
        WorkerModeArg::Single => 1,
        WorkerModeArg::Multi | WorkerModeArg::Distributed => {
            if requested > 0 {
                requested
            } else {
                num_cpus_on_system()
            }
        }
    }
}

/// Build the WAL [`wal_store::RetentionPolicy`] from CLI flags.
///
/// Both `0` = "no limit on that dimension". When both are zero the
/// result is `None` (keep-forever, also the default when WAL is
/// disabled).
fn build_wal_retention(retention_secs: u64, max_bytes: u64) -> Option<wal_store::RetentionPolicy> {
    let max_age_ns = (retention_secs > 0).then(|| retention_secs.saturating_mul(1_000_000_000));
    let max_bytes = (max_bytes > 0).then_some(max_bytes);
    if max_age_ns.is_none() && max_bytes.is_none() {
        return None;
    }
    Some(wal_store::RetentionPolicy {
        max_bytes,
        max_age_ns,
    })
}

/// Detects the number of logical CPUs via `kernel-nic::CpuTopology`.
///
/// Falls back to `libc::sysconf` if sysfs is unavailable (containers,
/// chroot without `/sys`).
fn num_cpus_on_system() -> usize {
    // [kernel-layer] Try kernel-nic topology discovery (reads sysfs).
    if let Ok(topo) = kernel_nic::CpuTopology::discover() {
        let n = topo.cpus().len();
        if n > 0 {
            tracing::debug!(
                cpus = n,
                numa_nodes = topo.numa_nodes().len(),
                "[kernel-nic] CPU topology discovered"
            );
            return n;
        }
    }
    tracing::debug!("[kernel-nic] sysfs unavailable, falling back to sysconf");

    // Fallback: libc sysconf.
    let n = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) };
    if n > 0 { n as usize } else { 1 }
}
