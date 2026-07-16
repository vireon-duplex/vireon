//! `loadtest` — network load-test harness for Vireon `quic-server`.
//!
//! Opens N concurrent QUIC connections to a running server. Each connection
//! sends the full 15-frame realistic workload (one frame per `MessageType`,
//! covering text / audio / video / image / tool / state / control planes).
//!
//! ```text
//! cargo run -p bench-harness -- --target 127.0.0.1:4433 --conns 4 --duration 10
//! ```
//!
//! A load-test tool's job is to print results, so `print_stdout` is allowed
//! at the crate root. All other workspace deny lints still apply.

#![allow(clippy::print_stdout)]

mod config;
mod gso;
mod layers;
mod resource;
mod stats;
mod worker;
mod workload;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use anyhow::Result;
use tracing_subscriber::EnvFilter;

use stats::{Summary, SweepPoint, WorkerResult, aggregate, print_summary};
use worker::{BenchMode, worker};
use workload::Workload;

struct Args {
    target: SocketAddr,
    conns: usize,
    duration: Duration,
    mode: BenchMode,
    pipeline_depth: usize,
    sweep: SweepMode,
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
                &workload,
                &shutdown,
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
                    &workload,
                    &shutdown,
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
                    &workload,
                    &shutdown,
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

/// Runs one benchmark pass: spawns `conns` worker threads, waits for
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
    workload: &Workload,
    shutdown: &Arc<AtomicBool>,
) -> Result<Summary> {
    tracing::info!(
        target = %target,
        conns,
        logical_streams = conns * workload.entries.len(),
        batch_bytes = workload.batch_wire_bytes,
        frames_per_batch = workload.entries.len(),
        payload_bytes = workload.total_payload_bytes,
        duration_secs = duration.as_secs(),
        mode = ?mode,
        "starting benchmark run",
    );

    let barrier = Barrier::new(conns + 1);

    let cpu_start = resource::cpu_time_secs();

    // Peak-RSS sampler: a background thread polls VmRSS every 50ms during
    // the run and tracks the maximum. This captures the true working set
    // when all connections are active, regardless of cleanup that happens
    // after threads exit (munmap, MADV_FREE reclamation, arena trimming).
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

        let handles: Vec<_> = (0..conns)
            .map(|idx| {
                s.spawn(move || {
                    worker(
                        idx,
                        target,
                        sni_ref,
                        entries_ref,
                        duration,
                        mode,
                        pipeline_depth,
                        coalesce,
                        shutdown_ref,
                        barrier_ref,
                    )
                })
            })
            .collect();

        tracing::info!("waiting for handshakes to complete...");
        barrier.wait();
        tracing::info!("all connections ready — measurement started");

        handles
            .into_iter()
            .map(|h| match h.join() {
                Ok(result) => result,
                Err(_) => WorkerResult::failed("thread panicked".into(), Duration::ZERO),
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
            eprintln!("\nInterrupted — forcing exit.");
            std::process::exit(130); // 130 = standard exit code for SIGINT
        }
        eprintln!("Stopping... (press Ctrl-C again to force exit)");
    });
}

fn parse_args() -> Result<Args> {
    let mut target_str = "127.0.0.1:4433".to_string();
    let mut conns: usize = 1;
    let mut duration_secs: u64 = 10;
    let mut mode = BenchMode::Stream;
    let mut pipeline_depth: usize = 4;
    let mut sweep = SweepMode::None;

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
    println!("  -h, --help                   Show this help");
    println!();
    println!("Modes:");
    println!("  stream      Full-duplex: fire continuously, drain in background. (default)");
    println!("              Maximises throughput; works without server echo.");
    println!("  rr          Request-response on 1 stream. Requires server echo.");
    println!("  multi-rr    15 streams fired together, wait all echoes. Requires server echo.");
    println!("  pipeline    N batches in-flight (--pipeline-depth). Requires server echo.");
    println!();
    println!("Workload covers all 15 MessageType variants:");
    println!("  TextToken(0x01)   → AI Streaming pipeline");
    println!("  AudioFrame(0x02)  → Media Streaming (audio track)");
    println!("  VideoFrame(0x03)  → Media Streaming (video track)");
    println!("  ImageFrame(0x04)  → Media Streaming (data track)");
    println!("  ToolCall(0x05)    → RPC Framework (echo response)");
    println!("  ToolResult(0x06)  → RPC Framework (log only)");
    println!("  StateUpdate(0x07) → Event Bus + PubSub Engine");
    println!("  Error(0x08)       → Application log");
    println!("  Heartbeat(0x09)   → Application heartbeat");
    println!("  StreamOpen(0x0A)  → Stream layer (lifecycle)");
    println!("  StreamClose(0x0B) → Stream layer (lifecycle)");
    println!("  StreamReset(0x0C) → Stream layer (lifecycle)");
    println!("  Handshake(0x0D)   → Stream layer (control)");
    println!("  Fragment(0x0E)    → Stream layer (continuation)");
    println!("  Ack(0x0F)         → Stream layer (control)");
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
