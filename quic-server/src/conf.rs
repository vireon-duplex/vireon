//! TOML config file support.
//!
//! All fields are `Option<T>` — missing keys in the file fall back to
//! hardcoded defaults during resolution in [`config::ServerConfig::from_cli`].
//!
//! # Precedence
//!
//! ```text
//! CLI flag (explicit)  >  config file value  >  hardcoded default
//! ```
//!
//! # Example
//!
//! ```toml
//! port = 4433
//! bind = "0.0.0.0"
//! cert = "cert.pem"
//! key = "key.pem"
//! mode = "multi"
//! workers = 4
//!
//! [quic]
//! max_connections = 131072
//! congestion = "bbr"
//! ```

use std::str::FromStr;
use std::time::Duration;

use serde::Deserialize;

// ── Human-readable helpers ──────────────────────────────────────

/// Parses byte sizes: `"100MiB"`, `"1GiB"`, `"256KiB"`, or raw integers.
#[derive(Debug, Clone, Copy)]
pub struct HumanBytes(pub u64);

impl<'de> serde::Deserialize<'de> for HumanBytes {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Str(String),
            Int(u64),
        }
        match Raw::deserialize(d)? {
            Raw::Int(n) => Ok(Self(n)),
            Raw::Str(s) => parse_human_bytes(&s).map(Self).ok_or_else(|| {
                serde::de::Error::custom(format!(
                    "invalid byte size '{s}' — expected e.g. 100MiB, 1GiB, 256KiB, or raw integer"
                ))
            }),
        }
    }
}

/// Parses durations: `"30s"`, `"5m"`, `"1h"`, or raw integers (seconds).
#[derive(Debug, Clone, Copy)]
pub struct HumanDuration(pub Duration);

impl<'de> serde::Deserialize<'de> for HumanDuration {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Str(String),
            Int(u64),
        }
        match Raw::deserialize(d)? {
            Raw::Int(secs) => Ok(Self(Duration::from_secs(secs))),
            Raw::Str(s) => parse_human_duration(&s)
                .map(Self)
                .ok_or_else(|| {
                    serde::de::Error::custom(format!(
                        "invalid duration '{s}' — expected e.g. 30s, 5m, 1h, or raw integer (seconds)"
                    ))
                }),
        }
    }
}

/// Parse `"100MiB"` / `"1G"` / `"256K"` / `"100"` → bytes.
/// Accepts B, K/KiB, M/MiB, G/GiB, T/TiB (case-insensitive).
/// Bare integers are interpreted as raw bytes.
fn parse_human_bytes(s: &str) -> Option<u64> {
    let s = s.trim();
    // All-numeric → raw bytes.
    if let Ok(n) = s.parse::<u64>() {
        return Some(n);
    }
    // Find the split point between digits and suffix.
    let split = s.find(|c: char| c.is_alphabetic())?;
    let (num_str, suffix) = s.split_at(split);
    let num: u64 = num_str.trim().parse().ok()?;
    let mult = match suffix.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024 * 1024,
        "g" | "gb" | "gib" => 1024 * 1024 * 1024,
        "t" | "tb" | "tib" => 1024_u64 * 1024 * 1024 * 1024,
        _ => return None,
    };
    num.checked_mul(mult)
}

/// Parse `"30s"` / `"5m"` / `"1h"` / `"500ms"` / `"30"` → Duration.
/// Bare integers are interpreted as seconds.
fn parse_human_duration(s: &str) -> Option<Duration> {
    let s = s.trim();
    // All-numeric → seconds.
    if let Ok(n) = s.parse::<u64>() {
        return Some(Duration::from_secs(n));
    }
    let split = s.find(|c: char| c.is_alphabetic())?;
    let (num_str, suffix) = s.split_at(split);
    let num: u64 = num_str.trim().parse().ok()?;
    let suffix = suffix.trim().to_ascii_lowercase();
    match suffix.as_str() {
        "s" => Some(Duration::from_secs(num)),
        "ms" => Some(Duration::from_millis(num)),
        "us" | "µs" => Some(Duration::from_micros(num)),
        "m" => Some(Duration::from_secs(num * 60)),
        "h" => Some(Duration::from_secs(num * 3600)),
        "d" => Some(Duration::from_secs(num * 86400)),
        _ => None,
    }
}

// ── Config structs ──────────────────────────────────────────────

/// Top-level config file. Every field is optional — missing keys
/// resolve to hardcoded defaults.
#[derive(Deserialize, Default)]
#[serde(default)]
pub struct ServerConf {
    // ── Network ──
    pub port: Option<u16>,
    pub bind: Option<String>,
    pub cert: Option<String>,
    pub key: Option<String>,
    pub client_ca: Option<String>,

    // ── Workers ──
    pub mode: Option<String>,
    pub workers: Option<usize>,
    pub queue_depth: Option<u32>,

    // ── Preset ──
    pub preset: Option<String>,

    // ── CRC ──
    pub skip_crc: Option<bool>,
    pub enforce_crc: Option<bool>,

    // ── Performance ──
    pub sqpoll: Option<bool>,
    pub echo: Option<bool>,
    pub verbose: Option<bool>,
    pub profile_output: Option<String>,

    // ── WAL ──
    pub wal_root: Option<String>,
    pub wal_retention_secs: Option<u64>,
    pub wal_max_bytes: Option<u64>,
    pub wal_sweep_interval_secs: Option<u64>,
    /// fsync policy for the WAL. See `[wal_sync]` section in
    /// `vireon.conf.example`. `None` = default (group-commit 100 ms).
    pub wal_sync: Option<WalSyncConf>,

    // ── Limits ──
    pub max_topics: Option<usize>,
    pub max_subscribers_per_topic: Option<usize>,

    // ── Security ──
    pub max_handshakes_per_ip_per_sec: Option<u32>,

    // ── Metrics ──
    pub metrics_port: Option<u16>,

    // ── Cluster ──
    pub cluster_node_id: Option<u32>,
    pub cluster_peers: Option<String>,

    // ── ACL ──
    pub acl_rules_path: Option<String>,

    // ── QUIC engine overrides ──
    pub quic: Option<QuicConf>,

    // ── Layer tuning sections ──
    pub buffer_pool: Option<BufferPoolConf>,
    pub backpressure: Option<BackpressureConf>,
    pub reactor: Option<ReactorConf>,
    pub send_batch: Option<SendBatchConf>,
    pub eviction: Option<EvictionConf>,
    pub session: Option<SessionConf>,
    pub stream: Option<StreamConf>,
    pub task_queue: Option<TaskQueueConf>,
    pub timer_wheel: Option<TimerWheelConf>,
    pub stream_scheduler: Option<StreamSchedulerConf>,
}

/// `[quic]` section — optional overrides applied on top of the preset.
#[derive(Deserialize, Default)]
#[serde(default)]
pub struct QuicConf {
    pub max_connections: Option<usize>,
    pub max_streams_bidi: Option<u64>,
    pub max_streams_uni: Option<u64>,
    pub initial_max_data: Option<HumanBytes>,
    pub initial_max_stream_data: Option<HumanBytes>,
    pub idle_timeout: Option<HumanDuration>,
    pub handshake_timeout: Option<HumanDuration>,
    pub congestion: Option<String>,
    pub initial_cwnd_packets: Option<u32>,
}

/// `[buffer_pool]` — per-worker buffer allocation pool tuning.
#[derive(Deserialize, Default)]
#[serde(default)]
pub struct BufferPoolConf {
    /// Maximum total memory the pool will hold cached buffers for.
    /// Accepts human-readable: "256MiB", "1GiB".
    pub max_memory: Option<HumanBytes>,
    /// Allocations above this size bypass the pool (allocated via
    /// system allocator, freed on drop). Default: 1 MiB.
    pub huge_threshold: Option<HumanBytes>,
    /// Fraction of cgroup memory limit to budget for the pool
    /// (0.0–1.0). Only used when cgroup limit is detectable.
    pub cgroup_fraction: Option<f64>,
}

/// `[backpressure]` — connection-shard pressure thresholds.
#[derive(Deserialize, Default)]
#[serde(default)]
pub struct BackpressureConf {
    /// Connection slots per shard (per reactor/core).
    pub connection_capacity: Option<usize>,
    /// Soft backpressure percentage (0–100). Above this, the shard
    /// starts slowing down acceptance.
    pub soft_pct: Option<u8>,
    /// Hard backpressure percentage (0–100). Above this, the shard
    /// rejects new connections.
    pub hard_pct: Option<u8>,
}

/// `[reactor]` — per-tick processing budget for the io_uring reactor.
#[derive(Deserialize, Default)]
#[serde(default)]
pub struct ReactorConf {
    /// Max completion queue events drained per tick.
    pub max_cqes: Option<usize>,
    /// Max reactor-local tasks processed per tick.
    pub max_tasks: Option<usize>,
    /// Max send-flush operations per tick.
    pub max_send_flush: Option<usize>,
    /// Soft time deadline per tick in microseconds.
    pub tick_deadline_us: Option<u64>,
}

/// `[send_batch]` — outbound packet coalescing / GSO batching.
#[derive(Deserialize, Default)]
#[serde(default)]
pub struct SendBatchConf {
    /// Max packets coalesced into a single GSO/sendmsg call.
    pub max_packets: Option<usize>,
    /// Max bytes accumulated before a flush is forced.
    pub max_bytes: Option<HumanBytes>,
    /// Max microseconds to wait before flushing a partial batch.
    pub max_age_us: Option<u64>,
    /// High watermark percentage (0–100). Above this, backpressure
    /// is signalled upstream.
    pub high_watermark_pct: Option<u8>,
    /// Low watermark percentage (0–100). Backpressure clears below this.
    pub low_watermark_pct: Option<u8>,
}

/// `[eviction]` — connection eviction under memory pressure.
#[derive(Deserialize, Default)]
#[serde(default)]
pub struct EvictionConf {
    /// Idle threshold in seconds. Connections idle longer than this
    /// are candidates for eviction under pressure.
    pub idle_threshold_secs: Option<u64>,
    /// Fraction of connections evicted at high (soft) pressure (0.0–1.0).
    pub high_evict_fraction: Option<f64>,
    /// Fraction evicted at critical (hard) pressure (0.0–1.0).
    pub critical_evict_fraction: Option<f64>,
    /// Minimum connections that survive any eviction pass.
    pub min_survivors: Option<usize>,
}

/// `[session]` — session-layer limits and timeouts.
#[derive(Deserialize, Default)]
#[serde(default)]
pub struct SessionConf {
    /// Maximum concurrent sessions across all tenants.
    pub max_sessions: Option<u64>,
    /// Maximum sessions per tenant.
    pub max_per_tenant: Option<u64>,
    /// Session idle timeout.
    pub idle_timeout: Option<HumanDuration>,
    /// Handshake timeout.
    pub handshake_timeout: Option<HumanDuration>,
    /// Max concurrent streams per session.
    pub max_streams_per_session: Option<u32>,
    /// Max memory budget per session.
    pub max_memory_per_session: Option<HumanBytes>,
}

/// `[stream]` — stream-layer limits and timeouts.
#[derive(Deserialize, Default)]
#[serde(default)]
pub struct StreamConf {
    /// Stream idle timeout.
    pub idle_timeout: Option<HumanDuration>,
    /// Max concurrent bidirectional streams per connection.
    pub max_streams_bidi: Option<u32>,
    /// Max concurrent unidirectional streams per connection.
    pub max_streams_uni: Option<u32>,
}

/// `[task_queue]` — reactor-local deferred-task queue.
#[derive(Deserialize, Default)]
#[serde(default)]
pub struct TaskQueueConf {
    /// Queue capacity (pending tasks).
    pub capacity: Option<usize>,
    /// Max tasks processed per drain batch.
    pub batch_max_tasks: Option<u16>,
    /// Max microseconds to wait before draining a partial batch.
    pub batch_max_time_us: Option<u64>,
    /// Grace period for shutdown drain.
    pub shutdown_grace_secs: Option<u64>,
}

/// `[timer_wheel]` — hierarchical timer wheel configuration.
#[derive(Deserialize, Default)]
#[serde(default)]
pub struct TimerWheelConf {
    /// Timer tick resolution in microseconds.
    pub tick_us: Option<u64>,
    /// Max timers tracked per shard.
    pub capacity_per_shard: Option<usize>,
}

/// `[stream_scheduler]` — fair stream scheduling per tick.
#[derive(Deserialize, Default)]
#[serde(default)]
pub struct StreamSchedulerConf {
    /// Max streams polled per tick.
    pub max_streams_per_tick: Option<u32>,
    /// Max bytes sent per tick before yielding.
    pub max_bytes_per_tick: Option<HumanBytes>,
    /// Starvation threshold: a stream is considered starved if it
    /// waits longer than this.
    pub starvation_threshold_us: Option<u64>,
}

/// fsync policy for the WAL.
#[derive(Deserialize, Default)]
#[serde(default)]
pub struct WalSyncConf {
    /// Strategy: `"never"` | `"every_flush"` | `"group_commit"`.
    /// Default `group_commit` (when the section is present but the
    /// field is omitted).
    pub mode: String,
    /// Used only by `group_commit`: max milliseconds between fsyncs.
    /// Default 100 ms.
    pub group_commit_ms: Option<u64>,
}

impl ServerConf {
    /// Load and parse a TOML config file.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or contains invalid TOML.
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("failed to read config file '{}': {e}", path.display()))?;
        let conf: Self = toml::from_str(&contents)
            .map_err(|e| anyhow::anyhow!("failed to parse config file '{}': {e}", path.display()))?;
        Ok(conf)
    }
}

impl QuicConf {
    /// Parse the congestion string into an algorithm name quiche accepts.
    ///
    /// Returns the lower-cased string ("reno", "cubic", "bbr", "bbr2")
    /// or `None` if the value doesn't match.
    #[must_use]
    pub fn congestion_str(&self) -> Option<&str> {
        self.congestion.as_deref().and_then(|s| match s.to_ascii_lowercase().as_str() {
            "reno" | "cubic" | "bbr" | "bbr2" => Some(s),
            _ => None,
        })
    }
}

// ── Resolved layer-tuning struct ────────────────────────────────

/// All resolved per-layer tuning values. Populated by
/// [`crate::config::ServerConfig::from_cli`] from the `[buffer_pool]`,
/// `[backpressure]`, `[reactor]`, etc. TOML sections. Every field has
/// a hardcoded default.
#[derive(Clone, Debug)]
pub struct LayerTuning {
    // ── Buffer pool ──
    pub buffer_huge_threshold: usize,
    pub buffer_cgroup_fraction: f64,
    // ── Connection shard / backpressure ──
    pub connection_capacity: usize,
    pub soft_bp_pct: u8,
    pub hard_bp_pct: u8,
    // ── Reactor tick budget ──
    pub reactor_max_cqes: usize,
    pub reactor_max_tasks: usize,
    pub reactor_max_send_flush: usize,
    pub reactor_tick_deadline_ns: u64,
    // ── Send queue / batching ──
    pub send_queue_capacity: usize,
    pub send_batch_max_packets: usize,
    pub send_batch_max_bytes: usize,
    pub send_batch_max_age_ns: u64,
    pub send_high_watermark_pct: u8,
    pub send_low_watermark_pct: u8,
    // ── Task queue ──
    pub task_queue_capacity: usize,
    pub task_batch_max_tasks: u16,
    pub task_batch_max_time_ns: u64,
    pub task_shutdown_grace_ns: u64,
    // ── Timer wheel ──
    pub timer_tick_ns: u64,
    pub timer_capacity_per_shard: usize,
    // ── CQE processor ──
    pub cqe_batch_max: usize,
    // ── Eviction ──
    pub eviction_idle_threshold_ns: u64,
    pub eviction_high_fraction: f32,
    pub eviction_critical_fraction: f32,
    pub eviction_min_survivors: usize,
    // ── WAL sweep ──
    pub wal_sweep_interval: std::time::Duration,
    // ── Session ──
    pub session_max_sessions: u64,
    pub session_max_per_tenant: u64,
    pub session_idle_timeout_ns: u64,
    pub session_handshake_timeout_ns: u64,
    pub session_max_streams_per_session: u32,
    pub session_max_memory_per_session: u64,
    // ── Stream ──
    pub stream_max_bidi: u32,
    pub stream_max_uni: u32,
    // ── Stream scheduler ──
    pub scheduler_max_streams_per_tick: u32,
    pub scheduler_max_bytes_per_tick: usize,
    pub scheduler_starvation_threshold_us: u64,
}

impl Default for LayerTuning {
    fn default() -> Self {
        Self {
            buffer_huge_threshold: 1024 * 1024,
            buffer_cgroup_fraction: 0.50,
            connection_capacity: 65_536,
            soft_bp_pct: 80,
            hard_bp_pct: 95,
            reactor_max_cqes: 256,
            reactor_max_tasks: 64,
            reactor_max_send_flush: 128,
            reactor_tick_deadline_ns: 200_000,
            send_queue_capacity: 4096,
            send_batch_max_packets: 64,
            send_batch_max_bytes: 65_536,
            send_batch_max_age_ns: 50_000,
            send_high_watermark_pct: 90,
            send_low_watermark_pct: 75,
            task_queue_capacity: 4096,
            task_batch_max_tasks: 128,
            task_batch_max_time_ns: 100_000,
            task_shutdown_grace_ns: 5_000_000_000,
            timer_tick_ns: 1_000_000,
            timer_capacity_per_shard: 65_536,
            cqe_batch_max: 256,
            eviction_idle_threshold_ns: 5_000_000_000,
            eviction_high_fraction: 0.05,
            eviction_critical_fraction: 0.10,
            eviction_min_survivors: 2,
            wal_sweep_interval: std::time::Duration::from_secs(60),
            session_max_sessions: 100_000,
            session_max_per_tenant: 10_000,
            session_idle_timeout_ns: 300_000_000_000,
            session_handshake_timeout_ns: 30_000_000_000,
            session_max_streams_per_session: 100,
            session_max_memory_per_session: 1024 * 1024,
            stream_max_bidi: 100,
            stream_max_uni: 100,
            scheduler_max_streams_per_tick: 64,
            scheduler_max_bytes_per_tick: 65_536,
            scheduler_starvation_threshold_us: 10_000,
        }
    }
}

// Keep FromStr impls for potential reuse in tests.
impl FromStr for HumanBytes {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_human_bytes(s).map(Self).ok_or_else(|| format!("invalid byte size: {s}"))
    }
}

impl FromStr for HumanDuration {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_human_duration(s).map(Self).ok_or_else(|| format!("invalid duration: {s}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_empty_file() {
        let conf: ServerConf = toml::from_str("").unwrap();
        assert!(conf.port.is_none());
        assert!(conf.cert.is_none());
    }

    #[test]
    fn parse_basic_config() {
        let toml_str = r#"
port = 4433
bind = "0.0.0.0"
cert = "cert.pem"
key = "key.pem"
mode = "multi"
workers = 4
"#;
        let conf: ServerConf = toml::from_str(toml_str).unwrap();
        assert_eq!(conf.port, Some(4433));
        assert_eq!(conf.cert.as_deref(), Some("cert.pem"));
        assert_eq!(conf.mode.as_deref(), Some("multi"));
        assert_eq!(conf.workers, Some(4));
    }

    #[test]
    fn parse_quic_overrides() {
        let toml_str = r#"
[quic]
max_connections = 131072
initial_max_data = "100MiB"
idle_timeout = "30s"
congestion = "bbr"
"#;
        let conf: ServerConf = toml::from_str(toml_str).unwrap();
        let quic = conf.quic.unwrap();
        assert_eq!(quic.max_connections, Some(131072));
        assert_eq!(quic.initial_max_data.unwrap().0, 100 * 1024 * 1024);
        assert_eq!(quic.idle_timeout.unwrap().0, Duration::from_secs(30));
        assert_eq!(quic.congestion.as_deref(), Some("bbr"));
    }

    #[test]
    fn parse_human_bytes_variants() {
        assert_eq!(parse_human_bytes("100"), Some(100));
        assert_eq!(parse_human_bytes("1KiB"), Some(1024));
        assert_eq!(parse_human_bytes("1MiB"), Some(1024 * 1024));
        assert_eq!(parse_human_bytes("1GiB"), Some(1024 * 1024 * 1024));
        assert_eq!(parse_human_bytes("2G"), Some(2 * 1024 * 1024 * 1024));
        assert_eq!(parse_human_bytes("256KB"), Some(256 * 1024));
    }

    #[test]
    fn parse_human_duration_variants() {
        assert_eq!(parse_human_duration("30"), Some(Duration::from_secs(30)));
        assert_eq!(parse_human_duration("30s"), Some(Duration::from_secs(30)));
        assert_eq!(parse_human_duration("5m"), Some(Duration::from_secs(300)));
        assert_eq!(parse_human_duration("1h"), Some(Duration::from_secs(3600)));
        assert_eq!(parse_human_duration("500ms"), Some(Duration::from_millis(500)));
    }

    #[test]
    fn congestion_str_resolution() {
        let q = QuicConf {
            congestion: Some("bbr".into()),
            ..Default::default()
        };
        assert_eq!(q.congestion_str(), Some("bbr"));

        let q = QuicConf {
            congestion: Some("invalid".into()),
            ..Default::default()
        };
        assert_eq!(q.congestion_str(), None);
    }
}
