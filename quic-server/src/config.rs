//! Server configuration — resolves CLI args + config file into typed configuration.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use quic_engine::{QuicConfig, QuicPreset};

use crate::cli::{Cli, PresetArg, WorkerModeArg};
use crate::conf::{LayerTuning, ServerConf};

/// Worker mode — resolved from CLI or config file.
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

    /// Resolved layer-tuning knobs (buffer pool, backpressure, reactor
    /// budget, send batching, eviction, session/stream limits, task
    /// queue, timer wheel, stream scheduler).
    pub tuning: crate::conf::LayerTuning,
}

/// Default queue depth if the CLI passes 0.
///
/// Matches `io::QUEUE_DEPTH` (1024) — generous enough for the 1 M-connection
/// per-core target once provided-buffer rings absorb per-recv buffering.
/// The previous value (256) bottlenecked recv batching under bursty loads
/// (e.g. 100-connection handshake storms) because the SQ filled before the
/// kernel could drain provided-buffer completions.
const DEFAULT_QUEUE_DEPTH: u32 = 1024;

/// Hardcoded defaults for fields without an explicit CLI flag or config key.
const DEFAULT_PORT: u16 = 4433;
const DEFAULT_BIND: &str = "0.0.0.0";

impl ServerConfig {
    /// Resolve CLI args + optional config file into a typed configuration.
    ///
    /// # Precedence
    ///
    /// ```text
    /// CLI flag (explicit)  >  config file value  >  hardcoded default
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the bind address is invalid, the config file
    /// fails to load/parse, or a required field (`cert`/`key`) is missing
    /// from both CLI and config file.
    pub fn from_cli(cli: &Cli) -> anyhow::Result<Self> {
        // ── Load config file (if any) ───────────────────────────────
        let conf: ServerConf = match cli.config.as_deref() {
            Some(path) => ServerConf::load(std::path::Path::new(path))?,
            None => ServerConf::default(),
        };

        // ── Resolve simple scalars via CLI > conf > default ─────────
        let port = cli.port.or(conf.port).unwrap_or(DEFAULT_PORT);
        let bind_str = cli
            .bind
            .clone()
            .or(conf.bind.clone())
            .unwrap_or_else(|| DEFAULT_BIND.to_string());
        let bind_addr: IpAddr = bind_str
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid bind address '{bind_str}': {e}"))?;

        let cert_path = cli.cert.clone().or(conf.cert.clone()).ok_or_else(|| {
            anyhow::anyhow!("cert is required (set --cert or `cert=` in config file)")
        })?;
        let key_path = cli.key.clone().or(conf.key.clone()).ok_or_else(|| {
            anyhow::anyhow!("key is required (set --key or `key=` in config file)")
        })?;

        // Worker mode: CLI enum > config-file string > default Single.
        let mode_arg = cli
            .mode
            .clone()
            .or_else(|| conf.mode.as_deref().and_then(parse_worker_mode_arg))
            .unwrap_or(WorkerModeArg::Single);

        // Preset: CLI enum > config-file string > default BareMetal.
        let preset_arg = cli
            .preset
            .clone()
            .or_else(|| conf.preset.as_deref().and_then(parse_preset_arg))
            .unwrap_or(PresetArg::BareMetal);

        let preset = preset_from_arg(&preset_arg);
        let mut quic_config = preset.to_config();

        // ── Apply [quic] overrides on top of preset ─────────────────
        if let Some(q) = &conf.quic {
            if let Some(v) = q.max_connections {
                quic_config.max_connections = v;
            }
            if let Some(v) = q.max_streams_bidi {
                quic_config.max_streams_bidi = v;
            }
            if let Some(v) = q.max_streams_uni {
                quic_config.max_streams_uni = v;
            }
            if let Some(v) = q.initial_max_data {
                quic_config.initial_max_data = v.0;
            }
            if let Some(v) = q.initial_max_stream_data {
                quic_config.initial_max_stream_data = v.0;
            }
            if let Some(v) = q.idle_timeout {
                quic_config.idle_timeout = v.0;
            }
            if let Some(v) = q.handshake_timeout {
                quic_config.handshake_timeout = v.0;
            }
            if let Some(v) = q.initial_cwnd_packets {
                quic_config.initial_congestion_window_packets = v;
            }
            // Congestion requires validation against the allow-list.
            if let Some(v) = q.congestion_str() {
                quic_config.congestion = parse_congestion(v);
            }
        }

        // ── CRC resolution ──────────────────────────────────────────
        // `--enforce-crc` wins over everything. Otherwise CLI > conf >
        // default(true). The default is "skip CRC verification" since
        // QUIC AEAD already guarantees integrity.
        let skip_crc = if cli.enforce_crc {
            false
        } else {
            cli.skip_crc.or(conf.skip_crc).unwrap_or(true)
        };
        quic_config.skip_crc = skip_crc;

        quic_config.client_ca = cli
            .client_ca
            .clone()
            .or(conf.client_ca.clone())
            .filter(|s| !s.is_empty());
        quic_config.max_handshakes_per_ip_per_sec = cli
            .max_handshakes_per_ip_per_sec
            .or(conf.max_handshakes_per_ip_per_sec)
            .unwrap_or(0);

        // ── Workers (mode-aware) ────────────────────────────────────
        let workers_requested = cli.workers.or(conf.workers).unwrap_or(0);
        let workers = resolve_workers(workers_requested, &mode_arg);

        // ── Queue depth ─────────────────────────────────────────────
        let queue_depth_raw = cli.queue_depth.or(conf.queue_depth).unwrap_or(0);
        let queue_depth = if queue_depth_raw == 0 {
            DEFAULT_QUEUE_DEPTH
        } else {
            queue_depth_raw
        };

        // ── SQPOLL ──────────────────────────────────────────────────
        let sqpoll = cli.sqpoll.or(conf.sqpoll).unwrap_or(false);

        // ── Echo / verbose (CLI-only flags; conf has them too for parity) ──
        let echo = cli.echo || conf.echo.unwrap_or(false);

        // ── ACL ─────────────────────────────────────────────────────
        let acl_rules_path = cli
            .acl_rules_path
            .clone()
            .or(conf.acl_rules_path.clone())
            .filter(|s| !s.is_empty());
        let acl_table = match acl_rules_path.as_deref() {
            Some(path) => Some(
                crate::acl_loader::load(std::path::Path::new(path))
                    .map_err(|e| anyhow::anyhow!("ACL rules file `{path}`: {e:#}"))?,
            ),
            None => None,
        };

        // ── WAL ─────────────────────────────────────────────────────
        let wal_root = cli
            .wal_root
            .clone()
            .or(conf.wal_root.clone())
            .filter(|s| !s.is_empty())
            .map(std::path::PathBuf::from);
        let wal_retention_secs = cli.wal_retention_secs.or(conf.wal_retention_secs).unwrap_or(0);
        let wal_max_bytes = cli.wal_max_bytes.or(conf.wal_max_bytes).unwrap_or(0);
        let wal_retention = build_wal_retention(wal_retention_secs, wal_max_bytes);

        // ── Quotas ──────────────────────────────────────────────────
        let max_topics_raw = cli.max_topics.or(conf.max_topics).unwrap_or(0);
        let max_subscribers_raw = cli
            .max_subscribers_per_topic
            .or(conf.max_subscribers_per_topic)
            .unwrap_or(0);
        let max_topics = (max_topics_raw > 0).then_some(max_topics_raw);
        let max_subscribers_per_topic = (max_subscribers_raw > 0).then_some(max_subscribers_raw);

        // ── Cluster (P6) ────────────────────────────────────────────
        // 0 = not set (single-node mode). Filter so Some(0) doesn't
        // trigger the cluster-peers requirement.
        let cluster_node_id = cli
            .cluster_node_id
            .or(conf.cluster_node_id)
            .filter(|&id| id != 0);
        let cluster_peers = cli
            .cluster_peers
            .clone()
            .or(conf.cluster_peers.clone())
            .filter(|s| !s.is_empty());
        let cluster = match (cluster_node_id, cluster_peers.as_deref()) {
            (Some(id), Some(peers_str)) => {
                let self_id = session_registry::NodeId::new(id);
                let peers = crate::cluster::parse_cluster_peers(peers_str)
                    .map_err(|e| anyhow::anyhow!("cluster-peers: {e}"))?;
                let cfg = crate::cluster::build_cluster_transport_config(self_id, &peers)
                    .map_err(|e| anyhow::anyhow!("cluster-peers: {e}"))?;
                Some(cfg)
            }
            (Some(_), None) => {
                return Err(anyhow::anyhow!(
                    "cluster-node-id requires cluster-peers (set via CLI or config file)"
                ));
            }
            (None, Some(_)) => {
                return Err(anyhow::anyhow!(
                    "cluster-peers requires cluster-node-id (set via CLI or config file)"
                ));
            }
            (None, None) => None,
        };

        // ── Metrics ─────────────────────────────────────────────────
        // 0 = disabled. Filter so Some(0) doesn't start a listener.
        let metrics_port = cli
            .metrics_port
            .or(conf.metrics_port)
            .filter(|&p| p != 0);
        let metrics = metrics_port
            .map(|_| Arc::new(crate::metrics::ServerMetrics::new()));

        // ── Layer tuning ────────────────────────────────────────────
        let tuning = resolve_tuning(&conf);

        Ok(Self {
            port,
            bind_addr,
            cert_path,
            key_path,
            workers,
            mode: WorkerMode::from(mode_arg),
            preset,
            quic_config,
            queue_depth,
            echo,
            sqpoll,
            skip_crc,
            acl_rules_path,
            acl_table,
            wal_root,
            wal_retention,
            max_topics,
            max_subscribers_per_topic,
            cluster,
            metrics_port,
            metrics,
            tuning,
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

/// Parse a worker-mode string from the config file into the CLI enum.
/// Accepts `single`, `multi`, `distributed` (case-insensitive).
fn parse_worker_mode_arg(s: &str) -> Option<WorkerModeArg> {
    match s.trim().to_ascii_lowercase().as_str() {
        "single" => Some(WorkerModeArg::Single),
        "multi" => Some(WorkerModeArg::Multi),
        "distributed" => Some(WorkerModeArg::Distributed),
        _ => None,
    }
}

/// Parse a preset string from the config file into the CLI enum.
/// Accepts `cloud`, `bare-metal`, `hundred-gbe`, `low-latency`,
/// `max-throughput` (case-insensitive).
fn parse_preset_arg(s: &str) -> Option<PresetArg> {
    match s.trim().to_ascii_lowercase().as_str() {
        "cloud" => Some(PresetArg::Cloud),
        "bare-metal" => Some(PresetArg::BareMetal),
        "hundred-gbe" => Some(PresetArg::HundredGbe),
        "low-latency" => Some(PresetArg::LowLatency),
        "max-throughput" => Some(PresetArg::MaxThroughput),
        _ => None,
    }
}

/// Parse a validated congestion string into the quic-engine enum.
fn parse_congestion(s: &str) -> quic_engine::config::CongestionAlgorithm {
    match s.to_ascii_lowercase().as_str() {
        "reno" => quic_engine::config::CongestionAlgorithm::Reno,
        "cubic" => quic_engine::config::CongestionAlgorithm::Cubic,
        "bbr" => quic_engine::config::CongestionAlgorithm::Bbr,
        "bbr2" => quic_engine::config::CongestionAlgorithm::Bbr2,
        // Unreachable: `congestion_str()` already validated the input.
        _ => quic_engine::config::CongestionAlgorithm::Bbr,
    }
}

/// Build the WAL [`wal_store::RetentionPolicy`] from resolved settings.
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

/// Resolve all layer-tuning knobs from the config file, falling back
/// to [`LayerTuning::default()`] for any field not present.
fn resolve_tuning(conf: &ServerConf) -> LayerTuning {
    let mut t = LayerTuning::default();

    // ── Buffer pool ──
    if let Some(bp) = &conf.buffer_pool {
        if let Some(v) = bp.huge_threshold { t.buffer_huge_threshold = v.0 as usize; }
        if let Some(v) = bp.cgroup_fraction { t.buffer_cgroup_fraction = v; }
    }

    // ── Backpressure / connection shard ──
    if let Some(bp) = &conf.backpressure {
        if let Some(v) = bp.connection_capacity { t.connection_capacity = v; }
        if let Some(v) = bp.soft_pct { t.soft_bp_pct = v; }
        if let Some(v) = bp.hard_pct { t.hard_bp_pct = v; }
    }

    // ── Reactor tick budget ──
    if let Some(r) = &conf.reactor {
        if let Some(v) = r.max_cqes { t.reactor_max_cqes = v; }
        if let Some(v) = r.max_tasks { t.reactor_max_tasks = v; }
        if let Some(v) = r.max_send_flush { t.reactor_max_send_flush = v; }
        if let Some(v) = r.tick_deadline_us { t.reactor_tick_deadline_ns = v * 1_000; }
    }

    // ── Send queue / batching ──
    if let Some(s) = &conf.send_batch {
        if let Some(v) = s.max_packets { t.send_batch_max_packets = v; }
        if let Some(v) = s.max_bytes { t.send_batch_max_bytes = v.0 as usize; }
        if let Some(v) = s.max_age_us { t.send_batch_max_age_ns = v * 1_000; }
        if let Some(v) = s.high_watermark_pct { t.send_high_watermark_pct = v; }
        if let Some(v) = s.low_watermark_pct { t.send_low_watermark_pct = v; }
    }

    // ── Eviction ──
    if let Some(e) = &conf.eviction {
        if let Some(v) = e.idle_threshold_secs {
            t.eviction_idle_threshold_ns = v * 1_000_000_000;
        }
        if let Some(v) = e.high_evict_fraction { t.eviction_high_fraction = v as f32; }
        if let Some(v) = e.critical_evict_fraction { t.eviction_critical_fraction = v as f32; }
        if let Some(v) = e.min_survivors { t.eviction_min_survivors = v; }
    }

    // ── WAL sweep interval ──
    if let Some(v) = conf.wal_sweep_interval_secs {
        t.wal_sweep_interval = Duration::from_secs(v);
    }

    // ── Session ──
    if let Some(s) = &conf.session {
        if let Some(v) = s.max_sessions { t.session_max_sessions = v; }
        if let Some(v) = s.max_per_tenant { t.session_max_per_tenant = v; }
        if let Some(v) = s.idle_timeout { t.session_idle_timeout_ns = v.0.as_nanos() as u64; }
        if let Some(v) = s.handshake_timeout { t.session_handshake_timeout_ns = v.0.as_nanos() as u64; }
        if let Some(v) = s.max_streams_per_session { t.session_max_streams_per_session = v; }
        if let Some(v) = s.max_memory_per_session { t.session_max_memory_per_session = v.0; }
    }

    // ── Stream ──
    if let Some(s) = &conf.stream {
        if let Some(v) = s.max_streams_bidi { t.stream_max_bidi = v; }
        if let Some(v) = s.max_streams_uni { t.stream_max_uni = v; }
    }

    // ── Task queue ──
    if let Some(q) = &conf.task_queue {
        if let Some(v) = q.capacity { t.task_queue_capacity = v; }
        if let Some(v) = q.batch_max_tasks { t.task_batch_max_tasks = v; }
        if let Some(v) = q.batch_max_time_us { t.task_batch_max_time_ns = v * 1_000; }
        if let Some(v) = q.shutdown_grace_secs { t.task_shutdown_grace_ns = v * 1_000_000_000; }
    }

    // ── Timer wheel ──
    if let Some(tw) = &conf.timer_wheel {
        if let Some(v) = tw.tick_us { t.timer_tick_ns = v * 1_000; }
        if let Some(v) = tw.capacity_per_shard { t.timer_capacity_per_shard = v; }
    }

    // ── Stream scheduler ──
    if let Some(ss) = &conf.stream_scheduler {
        if let Some(v) = ss.max_streams_per_tick { t.scheduler_max_streams_per_tick = v; }
        if let Some(v) = ss.max_bytes_per_tick { t.scheduler_max_bytes_per_tick = v.0 as usize; }
        if let Some(v) = ss.starvation_threshold_us { t.scheduler_starvation_threshold_us = v; }
    }

    t
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_worker_mode_variants() {
        assert!(matches!(parse_worker_mode_arg("single"), Some(WorkerModeArg::Single)));
        assert!(matches!(parse_worker_mode_arg("MULTI"), Some(WorkerModeArg::Multi)));
        assert!(matches!(parse_worker_mode_arg("Distributed"), Some(WorkerModeArg::Distributed)));
        assert!(parse_worker_mode_arg("bogus").is_none());
    }

    #[test]
    fn parse_preset_variants() {
        assert!(matches!(parse_preset_arg("cloud"), Some(PresetArg::Cloud)));
        assert!(matches!(parse_preset_arg("BARE-METAL"), Some(PresetArg::BareMetal)));
        assert!(matches!(parse_preset_arg("hundred-gbe"), Some(PresetArg::HundredGbe)));
        assert!(matches!(parse_preset_arg("low-latency"), Some(PresetArg::LowLatency)));
        assert!(matches!(parse_preset_arg("max-throughput"), Some(PresetArg::MaxThroughput)));
        assert!(parse_preset_arg("nope").is_none());
    }
}
