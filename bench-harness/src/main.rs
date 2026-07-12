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
mod stats;
mod worker;
mod workload;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use anyhow::Result;
use tracing_subscriber::EnvFilter;

use stats::{WorkerResult, aggregate, print_summary};
use worker::{BenchMode, worker};
use workload::WorkloadEntry;

struct Args {
    target: SocketAddr,
    conns: usize,
    duration: Duration,
    mode: BenchMode,
    pipeline_depth: usize,
}

fn main() -> Result<()> {
    init_tracing();
    let args = parse_args()?;

    let workload = workload::build()?;
    let sni = args.target.ip().to_string();

    tracing::info!(
        target = %args.target,
        conns = args.conns,
        logical_streams = args.conns * workload.entries.len(),
        batch_bytes = workload.batch_wire_bytes,
        frames_per_batch = workload.entries.len(),
        payload_bytes = workload.total_payload_bytes,
        duration_secs = args.duration.as_secs(),
        mode = ?args.mode,
        "starting load test",
    );

    let shutdown = Arc::new(AtomicBool::new(false));
    install_signal_handlers(Arc::clone(&shutdown));

    let barrier = Barrier::new(args.conns + 1);

    let results = thread::scope(|s| {
        let shutdown_ref = &shutdown;
        let barrier_ref = &barrier;
        let sni_ref = &sni;
        let entries_ref: &[WorkloadEntry] = &workload.entries;
        let mode = args.mode;
        let pipeline_depth = args.pipeline_depth;

        let handles: Vec<_> = (0..args.conns)
            .map(|idx| {
                s.spawn(move || {
                    worker(
                        idx,
                        args.target,
                        sni_ref,
                        entries_ref,
                        args.duration,
                        mode,
                        pipeline_depth,
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

    let summary = aggregate(
        results,
        &args.target.to_string(),
        workload.batch_wire_bytes,
        workload.entries.len(),
        args.duration,
        &workload.entries,
        mode_label(args.mode, args.pipeline_depth),
    );
    print_summary(&summary);

    Ok(())
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
    // Best-effort: if the handler fails (e.g. already installed), the load
    // test still runs to completion — the duration timer acts as a fallback.
    let _ = ctrlc::set_handler(move || {
        tracing::info!("received SIGINT/SIGTERM, stopping load test");
        shutdown.store(true, Ordering::Relaxed);
    });
}

fn parse_args() -> Result<Args> {
    let mut target_str = "127.0.0.1:4433".to_string();
    let mut conns: usize = 1;
    let mut duration_secs: u64 = 10;
    let mut mode = BenchMode::Stream;
    let mut pipeline_depth: usize = 4;

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
    println!();
    println!("Server start:");
    println!("  quic-server --cert cert.pem --key key.pem --port 4433 --mode single");
    println!();
    println!("  For rr / multi-rr / pipeline modes, add --echo:");
    println!("  quic-server --cert cert.pem --key key.pem --port 4433 --mode single --echo");
}
