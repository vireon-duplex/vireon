//! `loadtest` — network load-test harness for Vireon `quic-server`.
//!
//! Opens N concurrent QUIC connections to a running server. Each connection
//! sends the full 25-frame realistic workload (one frame per `MessageType`,
//! covering text / audio / video / image / tool / state / control planes).
//!
//! ```text
//! cargo run -p bench-harness -- --target 127.0.0.1:4433 --conns 4 --duration 10
//! ```
//!
//! A load-test tool's job is to print results, so `print_stdout` is allowed
//! at the crate root. All other workspace deny lints still apply.

#![allow(clippy::print_stdout)]

// Global allocator: mimalloc replaces glibc malloc to eliminate arena
// explosion under 1000-thread burst allocation. Per-thread heaps mean
// no cross-thread contention and efficient memory return to the OS.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

mod config;
mod conn_state;
mod gso;
mod layers;
mod reactor;
mod resource;
mod stats;
mod workload;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Result;
use tracing_subscriber::EnvFilter;

use conn_state::{BenchMode, EgressPolicy};
use stats::{Summary, SweepPoint, WorkerResult, aggregate, print_summary};
use workload::Workload;

struct Args {
    target: SocketAddr,
    conns: usize,
    duration: Duration,
    mode: BenchMode,
    pipeline_depth: usize,
    sweep: SweepMode,
    egress_policy: EgressPolicy,
    /// Max reactor threads. `None` = auto (available_parallelism).
    /// Used with `taskset` to leave cores for the server when both
    /// run on the same machine.
    reactors: Option<usize>,
}

/// Selects single-run vs. sweep mode.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum SweepMode {
    /// Default single-run benchmark.
    None,
    /// Sweep across connection counts [1, 10, 100, 500, 1000].
    Conn,
    /// Sweep across payload sizes [1KB, 4KB, 64KB, 1MB] in rr mode.
    Payload,
}

fn main() -> Result<()> {
    init_tracing();
    let args = parse_args()?;

    let sni = args.target.ip().to_string();
    let shutdown = Arc::new(AtomicBool::new(false));
    install_signal_handlers(Arc::clone(&shutdown));

    match args.sweep {
        SweepMode::None => {
            let workload = workload::build()?;
            let summary = run_benchmark(
                args.target,
                &sni,
                args.conns,
                args.duration,
                args.mode,
                args.pipeline_depth,
                args.egress_policy,
                &workload,
                &shutdown,
                args.reactors,
            )?;
            print_summary(&summary);
        }
        SweepMode::Conn => {
            let workload = workload::build()?;
            let mut points = Vec::new();
            for &c in &[1_usize, 10, 100, 500, 1000] {
                if shutdown.load(Ordering::Relaxed) {
                    tracing::info!("shutdown requested — aborting sweep");
                    break;
                }
                tracing::info!("=== connections sweep: {c} conns ===");
                resource::malloc_trim();
                let summary = run_benchmark(
                    args.target,
                    &sni,
                    c,
                    args.duration,
                    args.mode,
                    args.pipeline_depth,
                    args.egress_policy,
                    &workload,
                    &shutdown,
                    args.reactors,
                )?;
                print_summary(&summary);
                points.push(SweepPoint::from_summary(c.to_string(), &summary));
            }
            if !points.is_empty() {
                stats::print_conn_sweep(&points);
            }
        }
        SweepMode::Payload => {
            let sizes: [(usize, &str); 4] = [
                (1024, "1 KB"),
                (4096, "4 KB"),
                (65_536, "64 KB"),
                (1_048_576, "1 MB"),
            ];
            let mut points = Vec::new();
            for &(sz, label) in &sizes {
                if shutdown.load(Ordering::Relaxed) {
                    tracing::info!("shutdown requested — aborting sweep");
                    break;
                }
                tracing::info!("=== payload sweep: {label} ===");
                resource::malloc_trim();
                let workload = workload::build_fixed_size(sz)?;
                let summary = run_benchmark(
                    args.target,
                    &sni,
                    1,
                    args.duration,
                    BenchMode::Rr,
                    args.pipeline_depth,
                    args.egress_policy,
                    &workload,
                    &shutdown,
                    args.reactors,
                )?;
                print_summary(&summary);
                points.push(SweepPoint::from_summary(label, &summary));
            }
            if !points.is_empty() {
                stats::print_payload_sweep(&points);
            }
        }
    }

    Ok(())
}

/// Runs one benchmark pass: spawns reactor threads, waits for
/// handshakes, measures CPU/RSS, then aggregates results.
///
/// Returns a [`Summary`] with resource-efficiency metrics attached.
#[allow(clippy::too_many_arguments)]
fn run_benchmark(
    target: SocketAddr,
    sni: &str,
    conns: usize,
    duration: Duration,
    mode: BenchMode,
    pipeline_depth: usize,
    egress_policy: EgressPolicy,
    workload: &Workload,
    shutdown: &Arc<AtomicBool>,
    max_reactors: Option<usize>,
) -> Result<Summary> {
    let num_reactors = max_reactors
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        })
        .min(conns)
        .max(1);

    tracing::info!(
        target = %target,
        conns,
        reactors = num_reactors,
        logical_streams = conns * workload.entries.len(),
        batch_bytes = workload.batch_wire_bytes,
        frames_per_batch = workload.entries.len(),
        payload_bytes = workload.total_payload_bytes,
        duration_secs = duration.as_secs(),
        mode = ?mode,
        "starting benchmark run",
    );

    let barrier = Barrier::new(num_reactors + 1);
    let conns_per_reactor = conns.div_ceil(num_reactors);

    let cpu_start = resource::cpu_time_secs();

    // Peak-RSS sampler: a background thread polls VmRSS every 50ms during
    // the run and tracks the maximum.
    let rss_baseline = resource::rss_kib();
    let peak_rss = Arc::new(AtomicU64::new(rss_baseline));
    let sampler_stop = Arc::new(AtomicBool::new(false));
    let sampler_peak = Arc::clone(&peak_rss);
    let sampler_stop_ref = Arc::clone(&sampler_stop);
    let sampler_handle = thread::spawn(move || {
        while !sampler_stop_ref.load(Ordering::Relaxed) {
            let current = resource::rss_kib();
            let prev = sampler_peak.load(Ordering::Relaxed);
            if current > prev {
                sampler_peak.store(current, Ordering::Relaxed);
            }
            thread::sleep(Duration::from_millis(50));
        }
    });

    let coalesce = workload.coalesce;

    let results = thread::scope(|s| {
        let shutdown_ref = shutdown;
        let barrier_ref = &barrier;
        let sni_ref = sni;
        let entries_ref: &[workload::WorkloadEntry] = &workload.entries;

        let handles: Vec<_> = (0..num_reactors)
            .map(|r| {
                let conn_start = r * conns_per_reactor;
                let conn_count = conns_per_reactor.min(conns.saturating_sub(conn_start));

                thread::Builder::new()
                    .spawn_scoped(s, move || {
                        reactor::run(
                            target,
                            sni_ref,
                            entries_ref,
                            conn_start,
                            conn_count,
                            mode,
                            pipeline_depth,
                            egress_policy,
                            coalesce,
                            duration,
                            shutdown_ref,
                            barrier_ref,
                        )
                    })
                    .unwrap_or_else(|e| {
                        tracing::error!("reactor {r} spawn failed: {e}");
                        std::process::exit(1);
                    })
            })
            .collect();

        tracing::info!(
            spawned = handles.len(),
            "waiting for handshakes to complete..."
        );

        // Watchdog: if handshakes take longer than 30s, dump diagnostics.
        let watchdog_shutdown = Arc::new(AtomicBool::new(false));
        let watchdog_handle = {
            let ws = Arc::clone(&watchdog_shutdown);
            thread::spawn(move || {
                let deadline = Duration::from_secs(30);
                let poll_interval = Duration::from_millis(100);
                let start = Instant::now();
                while !ws.load(Ordering::Relaxed) {
                    if start.elapsed() >= deadline {
                        tracing::error!(
                            elapsed_secs = start.elapsed().as_secs(),
                            "handshake watchdog fired — barrier did not release in 30s. \
                             Check: (1) server is running, (2) ephemeral port range, \
                             (3) per-process fd limit."
                        );
                        std::process::exit(2);
                    }
                    thread::sleep(poll_interval);
                }
            })
        };

        barrier.wait();
        watchdog_shutdown.store(true, Ordering::Relaxed);
        let _ = watchdog_handle.join();

        tracing::info!("all connections ready — measurement started");

        handles
            .into_iter()
            .flat_map(|h| match h.join() {
                Ok(results) => results,
                Err(_) => vec![WorkerResult::failed(
                    "reactor panicked".into(),
                    Duration::ZERO,
                )],
            })
            .collect::<Vec<_>>()
    });

    // Stop the sampler and collect peak.
    sampler_stop.store(true, Ordering::Relaxed);
    sampler_handle.join().ok();
    let rss_peak = peak_rss.load(Ordering::Relaxed);

    let cpu_end = resource::cpu_time_secs();

    let mut summary = aggregate(
        results,
        &target.to_string(),
        workload.batch_wire_bytes,
        workload.entries.len(),
        duration,
        &workload.entries,
        mode_label(mode, pipeline_depth),
    );

    let wall_secs = duration.as_secs_f64().max(0.001);
    let cpu_delta = (cpu_end - cpu_start).max(0.0);
    summary.attach_resources(cpu_delta, wall_secs, rss_peak);

    Ok(summary)
}

fn mode_label(mode: BenchMode, depth: usize) -> String {
    match mode {
        BenchMode::Rr => "rr (request-response, 1 stream)".into(),
        BenchMode::MultiRr => "multi-rr (15 streams, concurrent)".into(),
        BenchMode::Stream => "stream (full-duplex, no wait)".into(),
        BenchMode::Pipeline => format!("pipeline (depth={depth}, 15 streams)"),
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

fn install_signal_handlers(shutdown: Arc<AtomicBool>) {
    // First Ctrl-C: set the shutdown flag so workers and sweep loops
    // wind down gracefully. Second Ctrl-C: force-exit immediately.
    let _ = ctrlc::set_handler(move || {
        if shutdown.swap(true, Ordering::Relaxed) {
            // Already set — this is the second interrupt, force exit now.
            tracing::warn!("interrupted — forcing exit");
            std::process::exit(130); // 130 = standard exit code for SIGINT
        }
        tracing::info!("stopping... (press Ctrl-C again to force exit)");
    });
}

fn parse_args() -> Result<Args> {
    let mut target_str = "127.0.0.1:4433".to_string();
    let mut conns: usize = 1;
    let mut duration_secs: u64 = 10;
    let mut mode = BenchMode::Stream;
    let mut pipeline_depth: usize = 4;
    let mut sweep = SweepMode::None;
    let mut egress_policy = EgressPolicy::None;
    let mut reactors: Option<usize> = None;

    let argv: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < argv.len() {
        match argv[i].as_str() {
            "--target" | "-t" => {
                i += 1;
                if i < argv.len() {
                    target_str = argv[i].clone();
                }
            }
            "--conns" | "-c" => {
                i += 1;
                if i < argv.len() {
                    conns = argv[i].parse().unwrap_or(conns);
                }
            }
            "--duration" | "-d" => {
                i += 1;
                if i < argv.len() {
                    duration_secs = argv[i].parse().unwrap_or(duration_secs);
                }
            }
            "--mode" | "-m" => {
                i += 1;
                if i < argv.len() {
                    mode = match argv[i].as_str() {
                        "rr" => BenchMode::Rr,
                        "multi-rr" => BenchMode::MultiRr,
                        "stream" => BenchMode::Stream,
                        "pipeline" => BenchMode::Pipeline,
                        other => anyhow::bail!(
                            "unknown --mode '{other}'; valid: rr, multi-rr, stream, pipeline"
                        ),
                    };
                }
            }
            "--pipeline-depth" | "-p" => {
                i += 1;
                if i < argv.len() {
                    pipeline_depth = argv[i].parse().unwrap_or(pipeline_depth);
                }
            }
            "--reactors" => {
                i += 1;
                if i < argv.len() {
                    match argv[i].parse() {
                        Ok(v) if v > 0 => reactors = Some(v),
                        _ => anyhow::bail!("--reactors must be >= 1"),
                    }
                }
            }
            "--sweep" => {
                i += 1;
                if i < argv.len() {
                    sweep = match argv[i].as_str() {
                        "conn" | "connections" => SweepMode::Conn,
                        "payload" => SweepMode::Payload,
                        "none" => SweepMode::None,
                        other => {
                            anyhow::bail!("unknown --sweep '{other}'; valid: conn, payload, none")
                        }
                    };
                }
            }
            "--egress-policy" => {
                i += 1;
                if i < argv.len() {
                    egress_policy = match argv[i].as_str() {
                        "none" => EgressPolicy::None,
                        "reliable_ordered" => EgressPolicy::ReliableOrdered,
                        "reliable_unordered" => EgressPolicy::ReliableUnordered,
                        "realtime_drop_old" => EgressPolicy::RealtimeDropOld,
                        "latest_only" => EgressPolicy::LatestOnly,
                        other => anyhow::bail!(
                            "unknown --egress-policy '{other}'; valid: none, reliable_ordered, reliable_unordered, realtime_drop_old, latest_only"
                        ),
                    };
                }
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            _ => {}
        }
        i += 1;
    }

    let target: SocketAddr = target_str
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid --target '{target_str}': {e}"))?;

    if conns == 0 {
        anyhow::bail!("--conns must be >= 1");
    }

    Ok(Args {
        target,
        conns,
        duration: Duration::from_secs(duration_secs),
        mode,
        pipeline_depth,
        sweep,
        egress_policy,
        reactors,
    })
}

fn print_usage() {
    println!("Usage: loadtest [OPTIONS]");
    println!();
    println!("Options:");
    println!("  -t, --target HOST:PORT       Server address (default: 127.0.0.1:4433)");
    println!("  -c, --conns N                Concurrent connections (default: 1)");
    println!("  -d, --duration SECS          Measurement window in seconds (default: 10)");
    println!("  -m, --mode MODE              Benchmark mode (default: stream)");
    println!("  -p, --pipeline-depth N       In-flight batches for pipeline mode (default: 4)");
    println!("      --sweep MODE             Sweep mode: conn, payload, none (default: none)");
    println!("      --egress-policy POLICY   Client send policy: none, reliable_ordered,");
    println!("                               reliable_unordered, realtime_drop_old, latest_only");
    println!("                               (default: none — legacy pass-through)");
    println!("      --reactors N             Max reactor threads (default: num_cpus).");
    println!("                               Lower this when client+server share a machine");
    println!("                               so the server gets CPU too. Use with taskset:");
    println!("                               taskset -c 8-13 loadtest --reactors 6 ...");
    println!("  -h, --help                   Show this help");
    println!();
    println!("Modes:");
    println!("  stream      Full-duplex: fire continuously, drain in background. (default)");
    println!("              Maximises throughput; works without server echo.");
    println!("  rr          Request-response on 1 stream. Requires server echo.");
    println!("  multi-rr    15 streams fired together, wait all echoes. Requires server echo.");
    println!("  pipeline    N batches in-flight (--pipeline-depth). Requires server echo.");
    println!();
    println!("Workload covers all 25 MessageType variants:");
    println!("  TextToken(0x01)   → Text / content payload");
    println!("  AudioFrame(0x02)  → Audio media chunk");
    println!("  VideoFrame(0x03)  → Video media chunk");
    println!("  ImageFrame(0x04)  → Image media chunk");
    println!("  ToolCall(0x05)    → Tool invocation request");
    println!("  ToolResult(0x06)  → Tool invocation result");
    println!("  StateUpdate(0x07) → State synchronization delta");
    println!("  Error(0x08)       → Error frame");
    println!("  Heartbeat(0x09)   → Keep-alive heartbeat");
    println!("  StreamOpen(0x0A)  → Stream lifecycle (open)");
    println!("  StreamClose(0x0B) → Stream lifecycle (close)");
    println!("  StreamReset(0x0C) → Stream lifecycle (reset)");
    println!("  Handshake(0x0D)   → Connection handshake");
    println!("  Fragment(0x0E)    → Stream continuation fragment");
    println!("  Ack(0x0F)         → Delivery acknowledgment");
    println!("  Subscribe(0x10)   → PubSub subscription");
    println!("  Unsubscribe(0x11) → PubSub unsubscription");
    println!("  Publish(0x12)     → PubSub message publish");
    println!("  TopicCreate(0x13) → Topic management (create)");
    println!("  TopicDelete(0x14) → Topic management (delete)");
    println!("  Fetch(0x15)       → WAL replay request");
    println!("  FetchReply(0x16)  → WAL replay response");
    println!();
    println!("Example:");
    println!("  loadtest -c 4 -d 10 -m stream          # throughput test, 4 users");
    println!("  loadtest -c 4 -d 30 -m pipeline -p 8   # pipelined, depth 8");
    println!("  loadtest -c 1 -d 10 -m rr              # latency baseline");
    println!("  loadtest --sweep conn -d 5             # throughput vs connections sweep");
    println!("  loadtest --sweep payload -d 5          # latency vs payload size sweep");
    println!();
    println!("Server start:");
    println!("  quic-server --cert cert.pem --key key.pem --port 4433 --mode single");
    println!();
    println!("  For rr / multi-rr / pipeline modes, add --echo:");
    println!("  quic-server --cert cert.pem --key key.pem --port 4433 --mode single --echo");
}
