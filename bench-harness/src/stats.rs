//! Per-worker results and aggregate stats.
//!
//! Each worker records its RTTs in a `Vec<Duration>` plus frame/byte counters.
//! After all workers join, the main thread merges every RTT into one flat
//! vector, sorts it, and indexes by percentile. No histogram dependency —
//! sorted-vec is O(n log n) once and gives exact percentiles.

use std::time::Duration;

use crate::workload::WorkloadEntry;

/// What one worker thread reports back.
pub struct WorkerResult {
    /// Round-trip latencies for every completed batch ping-pong.
    pub rtts: Vec<Duration>,
    /// Total frames handed to `stream_send` (iterations x 15).
    pub frames_sent: u64,
    /// Total wire bytes handed to `stream_send`.
    pub bytes_sent: u64,
    /// Total frames received back (echo).
    pub frames_recv: u64,
    /// Total wire bytes received back (echo).
    pub bytes_recv: u64,
    /// Peak number of frames in-flight (unacknowledged) observed this run.
    pub frames_in_flight_peak: u64,
    /// Sum of per-iteration in-flight frame counts.
    pub queue_depth_sum: u64,
    /// Number of iterations sampled for `queue_depth_sum`.
    pub queue_depth_samples: u64,
    /// Number of times `stream_send` returned `quiche::Error::Done`.
    pub blocked_writes: u64,
    /// Number of times the worker spun waiting on flow-control credit.
    pub flow_control_waits: u64,
    /// Total `stream_send` / `conn.send` calls.
    pub write_syscalls: u64,
    /// Total `socket.recv` calls.
    pub read_syscalls: u64,
    /// Time from `quiche::connect` to `is_established`.
    pub handshake_time: Duration,
    /// Set when the worker failed.
    pub error: Option<String>,
}

impl WorkerResult {
    /// Constructs a successful result.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn ok(
        rtts: Vec<Duration>,
        frames_sent: u64,
        bytes_sent: u64,
        frames_recv: u64,
        bytes_recv: u64,
        frames_in_flight_peak: u64,
        queue_depth_sum: u64,
        queue_depth_samples: u64,
        blocked_writes: u64,
        flow_control_waits: u64,
        write_syscalls: u64,
        read_syscalls: u64,
        handshake_time: Duration,
    ) -> Self {
        Self {
            rtts,
            frames_sent,
            bytes_sent,
            frames_recv,
            bytes_recv,
            frames_in_flight_peak,
            queue_depth_sum,
            queue_depth_samples,
            blocked_writes,
            flow_control_waits,
            write_syscalls,
            read_syscalls,
            handshake_time,
            error: None,
        }
    }

    pub(crate) fn failed(msg: String, handshake_time: Duration) -> Self {
        Self {
            rtts: Vec::new(),
            frames_sent: 0,
            bytes_sent: 0,
            frames_recv: 0,
            bytes_recv: 0,
            frames_in_flight_peak: 0,
            queue_depth_sum: 0,
            queue_depth_samples: 0,
            blocked_writes: 0,
            flow_control_waits: 0,
            write_syscalls: 0,
            read_syscalls: 0,
            handshake_time,
            error: Some(msg),
        }
    }

    /// Returns the failure reason, or `"ok"` if this result is successful.
    #[allow(dead_code)]
    /// Used for pre-barrier failure logging in `reactor::run`.
    pub fn reason(&self) -> &str {
        self.error.as_deref().unwrap_or("ok")
    }
}

/// Aggregated summary across all workers.
pub struct Summary {
    pub target: String,
    pub mode_label: String,
    pub connections_requested: usize,
    pub connections_ok: usize,
    pub connections_failed: usize,
    pub batch_bytes: usize,
    pub frames_per_batch: usize,
    pub duration: Duration,
    pub total_requests: u64,
    pub total_frames: u64,
    pub total_bytes: u64,
    pub req_per_sec: f64,
    pub frames_per_sec: f64,
    pub latency_min: Duration,
    pub latency_p50: Duration,
    pub latency_p90: Duration,
    pub latency_p99: Duration,
    pub latency_p999: Duration,
    pub latency_max: Duration,
    pub throughput_mibps: f64,
    pub handshake_p50: Duration,
    pub handshake_p99: Duration,
    pub handshake_max: Duration,
    pub workload: Box<[WorkloadEntry]>,

    pub tx_frames_per_sec: f64,
    pub rx_frames_per_sec: f64,
    pub tx_mibps: f64,
    pub rx_mibps: f64,

    pub frames_in_flight_peak: u64,
    pub avg_queue_depth: f64,

    pub blocked_writes: u64,
    pub flow_control_waits: u64,

    pub write_syscalls_per_sec: f64,
    pub read_syscalls_per_sec: f64,

    // ── Resource efficiency metrics ────────────────────────────────
    /// CPU cores utilized during measurement (cpu_time_delta / wall_time).
    pub cpu_cores: f64,
    /// CPU cores per GiB/s of throughput — lower is better.
    pub cpu_per_gibps: f64,
    /// Peak process RSS (KiB) sampled during the run — true working set.
    pub rss_kib: u64,
    /// Peak RSS (KiB) per successful connection.
    pub rss_per_conn_kib: f64,
}

/// Merges per-worker results into a [`Summary`].
#[allow(clippy::too_many_lines)]
pub fn aggregate(
    results: Vec<WorkerResult>,
    target: &str,
    batch_bytes: usize,
    frames_per_batch: usize,
    duration: Duration,
    workload: &[WorkloadEntry],
    mode_label: String,
) -> Summary {
    let connections_requested = results.len();
    let connections_ok = results.iter().filter(|r| r.error.is_none()).count();
    let connections_failed = connections_requested - connections_ok;

    let mut all_rtts: Vec<Duration> = results
        .iter()
        .flat_map(|r| r.rtts.iter().copied())
        .collect();
    all_rtts.sort_unstable();

    let total_requests = all_rtts.len() as u64;
    let total_frames: u64 = results.iter().map(|r| r.frames_sent).sum();
    let total_bytes: u64 = results.iter().map(|r| r.bytes_sent).sum();
    let secs = duration.as_secs_f64().max(0.001);
    let req_per_sec = total_requests as f64 / secs;
    let frames_per_sec = total_frames as f64 / secs;
    let throughput_mibps = total_bytes as f64 / secs / (1024.0 * 1024.0);

    let total_frames_recv: u64 = results.iter().map(|r| r.frames_recv).sum();
    let total_bytes_recv: u64 = results.iter().map(|r| r.bytes_recv).sum();
    let tx_frames_per_sec = total_frames as f64 / secs;
    let rx_frames_per_sec = total_frames_recv as f64 / secs;
    let tx_mibps = total_bytes as f64 / secs / (1024.0 * 1024.0);
    let rx_mibps = total_bytes_recv as f64 / secs / (1024.0 * 1024.0);

    let frames_in_flight_peak = results
        .iter()
        .map(|r| r.frames_in_flight_peak)
        .max()
        .unwrap_or(0);
    let total_qd_sum: u64 = results.iter().map(|r| r.queue_depth_sum).sum();
    let total_qd_samples: u64 = results.iter().map(|r| r.queue_depth_samples).sum();
    let avg_queue_depth = if total_qd_samples > 0 {
        total_qd_sum as f64 / total_qd_samples as f64
    } else {
        0.0
    };

    let blocked_writes: u64 = results.iter().map(|r| r.blocked_writes).sum();
    let flow_control_waits: u64 = results.iter().map(|r| r.flow_control_waits).sum();

    let total_write_syscalls: u64 = results.iter().map(|r| r.write_syscalls).sum();
    let total_read_syscalls: u64 = results.iter().map(|r| r.read_syscalls).sum();
    let write_syscalls_per_sec = total_write_syscalls as f64 / secs;
    let read_syscalls_per_sec = total_read_syscalls as f64 / secs;

    let mut hs: Vec<Duration> = results.iter().map(|r| r.handshake_time).collect();
    hs.sort_unstable();

    Summary {
        target: target.to_string(),
        mode_label,
        connections_requested,
        connections_ok,
        connections_failed,
        batch_bytes,
        frames_per_batch,
        duration,
        total_requests,
        total_frames,
        total_bytes,
        req_per_sec,
        frames_per_sec,
        latency_min: pct(&all_rtts, 0.0),
        latency_p50: pct(&all_rtts, 50.0),
        latency_p90: pct(&all_rtts, 90.0),
        latency_p99: pct(&all_rtts, 99.0),
        latency_p999: pct(&all_rtts, 99.9),
        latency_max: pct(&all_rtts, 100.0),
        throughput_mibps,
        handshake_p50: pct(&hs, 50.0),
        handshake_p99: pct(&hs, 99.0),
        handshake_max: pct(&hs, 100.0),
        workload: workload.to_vec().into_boxed_slice(),
        tx_frames_per_sec,
        rx_frames_per_sec,
        tx_mibps,
        rx_mibps,
        frames_in_flight_peak,
        avg_queue_depth,
        blocked_writes,
        flow_control_waits,
        write_syscalls_per_sec,
        read_syscalls_per_sec,
        cpu_cores: 0.0,
        cpu_per_gibps: 0.0,
        rss_kib: 0,
        rss_per_conn_kib: 0.0,
    }
}

/// Returns the `p`-th percentile of a **pre-sorted** slice.
fn pct(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = (p / 100.0 * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

impl Summary {
    /// Attaches resource-efficiency metrics after aggregation.
    ///
    /// - `cpu_delta_secs`: total CPU time (all threads) consumed during the run.
    /// - `wall_secs`: wall-clock duration of the measurement window.
    /// - `rss_peak_kib`: peak process RSS (KiB) sampled during the run.
    pub fn attach_resources(&mut self, cpu_delta_secs: f64, wall_secs: f64, rss_peak_kib: u64) {
        self.cpu_cores = if wall_secs > 0.0 {
            cpu_delta_secs / wall_secs
        } else {
            0.0
        };
        let gibps = self.throughput_mibps / 1024.0;
        self.cpu_per_gibps = if gibps > 0.01 {
            self.cpu_cores / gibps
        } else {
            0.0
        };
        self.rss_kib = rss_peak_kib;
        self.rss_per_conn_kib = if self.connections_ok > 0 {
            rss_peak_kib as f64 / self.connections_ok as f64
        } else {
            0.0
        };
    }
}

/// One row in a sweep comparison table.
pub struct SweepPoint {
    /// Label for this row (e.g. "1", "100", "1 KB", "1 MB").
    pub label: String,
    /// Connections used in this run.
    pub conns: usize,
    /// Throughput in MiB/s.
    pub throughput_mibps: f64,
    pub latency_p50: Duration,
    pub latency_p99: Duration,
    pub latency_max: Duration,
    /// CPU cores per GiB/s.
    pub cpu_per_gibps: f64,
    /// Peak RSS (KiB) per connection — true per-conn memory cost.
    pub rss_per_conn_kib: f64,
}

impl SweepPoint {
    /// Builds a sweep point from a summary with the given row label.
    #[must_use]
    pub fn from_summary(label: impl Into<String>, s: &Summary) -> Self {
        Self {
            label: label.into(),
            conns: s.connections_ok,
            throughput_mibps: s.throughput_mibps,
            latency_p50: s.latency_p50,
            latency_p99: s.latency_p99,
            latency_max: s.latency_max,
            cpu_per_gibps: s.cpu_per_gibps,
            rss_per_conn_kib: s.rss_per_conn_kib,
        }
    }
}

/// Prints the resource-efficiency lines (CPU + memory).
fn print_resource_stats(s: &Summary) {
    println!(
        "  CPU Cores Used      {:>10.2}  (over measurement window)",
        s.cpu_cores
    );
    println!(
        "  CPU per GiB/s       {:>10.2}  (cores per GiB/s — lower is better)",
        s.cpu_per_gibps
    );
    println!("  RSS (peak)          {:>10.0} KiB", s.rss_kib);
    println!("  RSS/conn (peak)     {:>10.0} KiB", s.rss_per_conn_kib);
}

/// Prints the connections-sweep comparison table.
pub fn print_conn_sweep(points: &[SweepPoint]) {
    let us = |d: Duration| d.as_secs_f64() * 1_000_000.0;
    println!();
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║              Throughput vs. Connections Sweep                     ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");
    println!();
    println!("┌────────┬───────────┬──────────┬──────────┬──────────┬──────────────┐");
    println!("│ Conns  │ MiB/s     │ p50 (us) │ p99 (us) │ CPU/GiB  │ KiB/conn     │");
    println!("├────────┼───────────┼──────────┼──────────┼──────────┼──────────────┤");
    for p in points {
        println!(
            "│ {:>6} │ {:>9.1} │ {:>8.1} │ {:>8.1} │ {:>8.2} │ {:>12.0} │",
            p.conns,
            p.throughput_mibps,
            us(p.latency_p50),
            us(p.latency_p99),
            p.cpu_per_gibps,
            p.rss_per_conn_kib,
        );
    }
    println!("└────────┴───────────┴──────────┴──────────┴──────────┴──────────────┘");
    println!("encryption: TLS 1.3 (BoringSSL via quiche) — ACTIVE");
    println!();
}

/// Prints the payload-size-sweep comparison table.
pub fn print_payload_sweep(points: &[SweepPoint]) {
    let us = |d: Duration| d.as_secs_f64() * 1_000_000.0;
    println!();
    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║            Latency vs. Payload Size Sweep (rr mode)              ║");
    println!("╚══════════════════════════════════════════════════════════════════╝");
    println!();
    println!("┌──────────┬───────────┬──────────┬──────────┐");
    println!("│ Payload  │ p50 (us)  │ p99 (us) │ Max (us) │");
    println!("├──────────┼───────────┼──────────┼──────────┤");
    for p in points {
        println!(
            "│ {:<8} │ {:>9.1} │ {:>8.1} │ {:>8.1} │",
            p.label,
            us(p.latency_p50),
            us(p.latency_p99),
            us(p.latency_max),
        );
    }
    println!("└──────────┴───────────┴──────────┴──────────┘");
    println!("encryption: TLS 1.3 (BoringSSL via quiche) — ACTIVE");
    println!();
}

/// Prints the summary to stdout in a human-readable table.
#[allow(clippy::too_many_lines)]
pub fn print_summary(s: &Summary) {
    let us = |d: Duration| d.as_secs_f64() * 1_000_000.0;

    println!();
    println!("╔══════════════════════════════════════════════════════════════╗");
    println!("║                   Vireon Load Test — Full Stack              ║");
    println!("╚══════════════════════════════════════════════════════════════╝");
    println!();
    println!("Server architecture under test:");
    println!("  ┌─ Application Layer ─────────────────────────────────────┐");
    println!("  │ PubSub Engine · Auth · TopicACL · WAL Store · Observ.  │");
    println!("  │ Resource Quotas                                       │");
    println!("  ├─ Session Layer ────────────────────────────────────────┤");
    println!("  │ SessionManager                                         │");
    println!("  ├─ Stream Layer ─────────────────────────────────────────┤");
    println!("  │ StreamManager · SendPolicy (4 delivery semantics)      │");
    println!("  ├─ Message Layer ────────────────────────────────────────┤");
    println!("  │ Frame Codec (22B header) · Message Classifier          │");
    println!("  ├─ Transport Layer ──────────────────────────────────────┤");
    println!("  │ QUIC Engine (quiche: TLS 1.3, CC, Loss Recovery)       │");
    println!("  ├─ Runtime Layer ────────────────────────────────────────┤");
    println!("  │ ConnectionShard · TimerWheel · BufferPool · SendQueue  │");
    println!("  │ TaskQueue · CqeProcessor · Reactor facade              │");
    println!("  ├─ IO Layer ─────────────────────────────────────────────┤");
    println!("  │ io_uring Reactor (SEND_ZC, multishot, PBUF_RING)       │");
    println!("  ├─ Kernel Layer ─────────────────────────────────────────┤");
    println!("  │ kernel-nic (CPU topology, NIC discovery)               │");
    println!("  └────────────────────────────────────────────────────────┘");
    println!();
    println!("target:       {}", s.target);
    println!("mode:         {}", s.mode_label);
    println!(
        "connections:  {} ({} ok, {} failed)",
        s.connections_requested, s.connections_ok, s.connections_failed
    );
    println!(
        "logical streams: {} ({} users x {} types)",
        s.connections_ok * s.frames_per_batch,
        s.connections_ok,
        s.frames_per_batch,
    );
    println!(
        "batch:        {} bytes/req ({} frames x 22B hdr + {}B payload)",
        s.batch_bytes,
        s.frames_per_batch,
        s.batch_bytes.saturating_sub(s.frames_per_batch * 22),
    );
    println!("duration:     {:.1}s", s.duration.as_secs_f64());
    println!("encryption:  TLS 1.3 (BoringSSL via quiche) — ACTIVE");
    println!();
    println!("requests:     {}", s.total_requests);
    println!("frames:       {}", s.total_frames);
    println!(
        "bytes:        {:.2} MiB",
        s.total_bytes as f64 / (1024.0 * 1024.0)
    );
    println!("errors:       {}", s.connections_failed);
    println!("req/sec:      {:.1}", s.req_per_sec);
    println!("frames/sec:   {:.1}", s.frames_per_sec);
    println!();

    if !all_rtts_empty(s) {
        println!("latency (us):");
        println!("  min     {:>8.1}", us(s.latency_min));
        println!("  p50     {:>8.1}", us(s.latency_p50));
        println!("  p90     {:>8.1}", us(s.latency_p90));
        println!("  p99     {:>8.1}", us(s.latency_p99));
        println!("  p99.9   {:>8.1}", us(s.latency_p999));
        println!("  max     {:>8.1}", us(s.latency_max));
    } else {
        println!("latency:      (no RTT samples — stream mode does not measure per-batch latency)");
    }
    println!();
    println!("throughput:   {:.1} MiB/s send", s.throughput_mibps);
    println!(
        "handshake:    p50={:.1}ms  p99={:.1}ms  max={:.1}ms",
        s.handshake_p50.as_secs_f64() * 1000.0,
        s.handshake_p99.as_secs_f64() * 1000.0,
        s.handshake_max.as_secs_f64() * 1000.0,
    );
    println!();

    println!("extended stats:");
    println!("  TX Frames/s         {:>10.1}", s.tx_frames_per_sec);
    println!("  RX Frames/s         {:>10.1}", s.rx_frames_per_sec);
    println!("  TX MiB/s            {:>10.3}", s.tx_mibps);
    println!("  RX MiB/s            {:>10.3}", s.rx_mibps);
    println!(
        "  Frames In Flight    {:>10}  (peak)",
        s.frames_in_flight_peak
    );
    println!(
        "  Avg Queue Depth     {:>10.2}  (frames/iter)",
        s.avg_queue_depth
    );
    println!(
        "  Blocked Writes      {:>10}  (stream_send -> Error::Done)",
        s.blocked_writes
    );
    println!(
        "  Flow Control Waits  {:>10}  (drain loops entered)",
        s.flow_control_waits
    );
    println!("  Write Syscalls/s    {:>10.1}", s.write_syscalls_per_sec);
    println!("  Read Syscalls/s     {:>10.1}", s.read_syscalls_per_sec);
    println!();
    println!("resource efficiency:");
    print_resource_stats(s);
    println!();

    println!("workload breakdown (per request):");
    println!(
        "  {:<14} {:>4}  {:>8}  {:>6}",
        "type", "hex", "bytes", "share"
    );
    println!("  {}", "-".repeat(36));
    for e in s.workload.iter() {
        let share = e.payload_len as f64 / s.batch_bytes as f64 * 100.0;
        println!(
            "  {:<14} 0x{:02X} {:>8}  {:>5.1}%",
            e.name,
            e.msg_type.as_byte(),
            e.payload_len,
            share
        );
    }
    println!();
}

/// Returns `true` when all latency percentiles are zero (no RTT samples).
fn all_rtts_empty(s: &Summary) -> bool {
    s.total_requests == 0
}
