//! Long-running stability stress test.
//!
//! Not a Criterion benchmark — this is a standalone binary that runs the
//! timer wheel under sustained load for a configurable duration, reporting
//! periodic telemetry and a final leak / latency report.
//!
//! ## Usage
//!
//! ```bash
//! # 60-second run with 4 producer threads at 100k ops/s (defaults)
//! cargo bench -p timer-wheel --bench stress
//!
//! # 5-minute run with 8 threads at 500k ops/s
//! cargo bench -p timer-wheel --bench stress -- 300 8 500000
//!
//! # 1-hour soak test
//! cargo bench -p timer-wheel --bench stress -- 3600 4 50000
//! ```
//!
//! ## What it checks
//!
//! - **Latency drift**: p99 should not climb over time.
//! - **Memory growth**: `VmPeak` / `VmRSS` should stabilise, not grow unbounded.
//! - **Leaks**: `outstanding` must return to 0 after a final drain window.
//! - **Throughput stability**: ops/sec should stay within ±10% of the target.

#![allow(
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::expect_used,
    clippy::unwrap_used
)]

#[path = "common.rs"]
mod common;

use std::collections::BinaryHeap;
use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use timer_wheel::{TimerHandle, TimerWheel, TimerWheelBuilder};

/// Caps the per-op latency samples we hold in memory (1M samples = ~16MB).
const MAX_SAMPLES: usize = 1_000_000;

struct Config {
    duration_secs: u64,
    threads: usize,
    target_hz: u64,
}

impl Config {
    fn parse() -> Self {
        let args: Vec<String> = env::args().skip(1).collect();
        Self {
            duration_secs: args.first().and_then(|s| s.parse().ok()).unwrap_or(60),
            threads: args.get(1).and_then(|s| s.parse().ok()).unwrap_or(4),
            target_hz: args.get(2).and_then(|s| s.parse().ok()).unwrap_or(100_000),
        }
    }
}

fn main() {
    let cfg = Config::parse();
    println!("=== Timer Wheel Stress Test ===");
    println!(
        "Duration: {}s  Threads: {}  Target: {} ops/s",
        cfg.duration_secs, cfg.threads, cfg.target_hz
    );
    println!();

    let wheel: Arc<TimerWheel> = Arc::new(
        TimerWheelBuilder::new()
            .shards(cfg.threads as u32)
            .tick_duration(1_000_000)
            .capacity_per_shard(1 << 20) // 1M timers per shard
            .cmd_queue_capacity(8192)
            .spawn()
            .expect("wheel spawn"),
    );

    let stop = Arc::new(AtomicBool::new(false));
    let ops_counter = Arc::new(AtomicU64::new(0));
    let start_counter = Arc::new(AtomicU64::new(0));
    let cancel_counter = Arc::new(AtomicU64::new(0));
    let samples: Arc<std::sync::Mutex<Vec<Duration>>> =
        Arc::new(std::sync::Mutex::new(Vec::with_capacity(MAX_SAMPLES)));

    let start = Instant::now();

    // Producer threads.
    let mut producers = Vec::new();
    let per_thread_hz = cfg.target_hz / cfg.threads as u64;
    let interval = if per_thread_hz == 0 {
        Duration::ZERO
    } else {
        Duration::from_nanos(1_000_000_000 / per_thread_hz)
    };
    for tid in 0..cfg.threads {
        let wheel = Arc::clone(&wheel);
        let stop = Arc::clone(&stop);
        let ops = Arc::clone(&ops_counter);
        let starts = Arc::clone(&start_counter);
        let cancels = Arc::clone(&cancel_counter);
        let samples = Arc::clone(&samples);
        producers.push(thread::spawn(move || {
            let mut id_base = (tid as u64) << 48; // disjoint id ranges per thread
            let mut last_handles: Vec<TimerHandle> = Vec::new();
            let mut next_cancel_at = 256u64;
            let mut since_cancel = 0u64;
            // Deadline pool: kept under the post-run drain window (5s) so the
            // leak check is meaningful — every started timer should be able to
            // retire within the drain window. To stress long-deadline memory
            // pressure, raise the drain window or accept that "outstanding"
            // will be non-zero at shutdown.
            let deadline_pool = [
                Duration::from_millis(1),
                Duration::from_millis(10),
                Duration::from_millis(100),
                Duration::from_secs(1),
            ];
            while !stop.load(Ordering::Relaxed) {
                let t0 = Instant::now();
                id_base = id_base.wrapping_add(1);
                let dur = deadline_pool[(id_base as usize) & 0b11];
                match wheel.start(id_base, dur) {
                    Ok(h) => {
                        last_handles.push(h);
                        since_cancel += 1;
                        if since_cancel >= next_cancel_at && !last_handles.is_empty() {
                            // Cancel a random earlier handle (FIFO-ish).
                            let victim = last_handles.remove(0);
                            let _ = wheel.cancel(victim);
                            cancels.fetch_add(1, Ordering::Relaxed);
                            since_cancel = 0;
                            next_cancel_at = 128 + (id_base & 0x1ff); // 128..639
                        }
                        starts.fetch_add(1, Ordering::Relaxed);
                        ops.fetch_add(1, Ordering::Relaxed);
                        let elapsed = t0.elapsed();
                        let mut s = samples.lock().expect("samples lock");
                        if s.len() < MAX_SAMPLES {
                            s.push(elapsed);
                        }
                    }
                    Err(timer_wheel::TimerError::WheelFull { .. })
                    | Err(timer_wheel::TimerError::ShardFull { .. }) => {
                        // Back off briefly; the wheel is saturated.
                        thread::yield_now();
                    }
                    Err(e) => {
                        eprintln!("producer error: {e:?}");
                        break;
                    }
                }
                if interval > Duration::ZERO {
                    thread::sleep(interval);
                }
            }
        }));
    }

    // Consumer thread — continuous drain.
    let consumer = {
        let wheel = Arc::clone(&wheel);
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            let mut drained_total = 0u64;
            while !stop.load(Ordering::Relaxed) {
                drained_total = drained_total.wrapping_add(wheel.drain_expired().len() as u64);
                thread::sleep(Duration::from_millis(5));
            }
            drained_total
        })
    };

    // Periodic reporter.
    let reporter = {
        let stop = Arc::clone(&stop);
        let ops = Arc::clone(&ops_counter);
        let starts = Arc::clone(&start_counter);
        let cancels = Arc::clone(&cancel_counter);
        let samples = Arc::clone(&samples);
        let wheel = Arc::clone(&wheel);
        thread::spawn(move || {
            let mut last_ops = 0u64;
            let mut last_ts = Instant::now();
            while !stop.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_secs(5));
                let now_ops = ops.load(Ordering::Relaxed);
                let delta_ops = now_ops.saturating_sub(last_ops);
                let delta_t = last_ts.elapsed().as_secs_f64().max(0.001);
                last_ops = now_ops;
                last_ts = Instant::now();
                let s = samples.lock().expect("samples lock");
                let (p50, p99, max) = percentiles(&s);
                let mem = common::proc_vm_stats();
                drop(s);
                println!(
                    "  t={:>5.1}s  ops={:>10}  rate={:>10.0}/s  starts={:>9}  cancels={:>8}  outstanding≈{}  p50={:>7.2?}  p99={:>7.2?}  max={:>7.2?}  mem={}",
                    start.elapsed().as_secs_f64(),
                    now_ops,
                    delta_ops as f64 / delta_t,
                    starts.load(Ordering::Relaxed),
                    cancels.load(Ordering::Relaxed),
                    wheel.outstanding(),
                    p50,
                    p99,
                    max,
                    format_mem(mem),
                );
            }
        })
    };

    thread::sleep(Duration::from_secs(cfg.duration_secs));
    stop.store(true, Ordering::Release);

    // Drain window so in-flight timers can retire.
    println!("\nDraining for 5s...");
    thread::sleep(Duration::from_secs(5));

    let drained_total = consumer.join().expect("consumer");
    for h in producers {
        let _ = h.join();
    }
    let _ = reporter.join();

    let total_ops = ops_counter.load(Ordering::Relaxed);
    let elapsed = start.elapsed();
    let s = samples.lock().expect("samples lock");
    let (p50, p90, p99, max) = full_percentiles(&s);
    let mem = common::proc_vm_stats();
    drop(s);

    println!();
    println!("=== Final Report ===");
    println!("Wall-clock elapsed: {:.2}s", elapsed.as_secs_f64());
    println!(
        "Total ops: {}  ({:.0} ops/s overall)",
        total_ops,
        total_ops as f64 / elapsed.as_secs_f64().max(0.001)
    );
    println!(
        "Starts: {}  Cancels: {}  Drained-expired: {}",
        start_counter.load(Ordering::Relaxed),
        cancel_counter.load(Ordering::Relaxed),
        drained_total,
    );
    println!("Latency: p50={p50:.2?}  p90={p90:.2?}  p99={p99:.2?}  max={max:.2?}");
    println!("Memory: {}", format_mem(mem));
    let outstanding = wheel.outstanding();
    if outstanding > 0 {
        println!(
            "LEAK CHECK: FAILED — {} timers still outstanding",
            outstanding
        );
    } else {
        println!("LEAK CHECK: PASS (outstanding == 0 after drain window)");
    }
    // Workers shut down via Drop when the Arc is released. If any thread is
    // still holding a clone, `into_inner` returns None — we leak the wheel
    // (acceptable: process is about to exit).
    if let Some(mut owned) = Arc::into_inner(wheel) {
        owned.shutdown();
    }
}

/// Returns (p50, p99, max) for the most recent samples (compact form for the
/// periodic reporter).
fn percentiles(samples: &[Duration]) -> (Duration, Duration, Duration) {
    if samples.is_empty() {
        return (Duration::ZERO, Duration::ZERO, Duration::ZERO);
    }
    let mut copy: Vec<u128> = samples.iter().map(|d| d.as_nanos()).collect();
    copy.sort_unstable();
    let len = copy.len();
    let p50 = copy[len / 2];
    let p99 = copy[(len - 1).min((len as f64 * 0.99) as usize)];
    let max = copy[len - 1];
    (
        Duration::from_nanos(p50 as u64),
        Duration::from_nanos(p99 as u64),
        Duration::from_nanos(max as u64),
    )
}

/// Returns (p50, p90, p99, max) for the full sample set (final report).
fn full_percentiles(samples: &[Duration]) -> (Duration, Duration, Duration, Duration) {
    if samples.is_empty() {
        return (
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
        );
    }
    let mut copy: Vec<u128> = samples.iter().map(|d| d.as_nanos()).collect();
    copy.sort_unstable();
    let len = copy.len();
    let p50 = copy[len / 2];
    let p90 = copy[(len - 1).min((len as f64 * 0.90) as usize)];
    let p99 = copy[(len - 1).min((len as f64 * 0.99) as usize)];
    let max = copy[len - 1];
    (
        Duration::from_nanos(p50 as u64),
        Duration::from_nanos(p90 as u64),
        Duration::from_nanos(p99 as u64),
        Duration::from_nanos(max as u64),
    )
}

fn format_mem(mem: Option<(u64, u64)>) -> String {
    match mem {
        Some((peak_kb, rss_kb)) => format!(
            "VmPeak={:.1} MiB  VmRSS={:.1} MiB",
            peak_kb as f64 / 1024.0,
            rss_kb as f64 / 1024.0
        ),
        None => "(unavailable on this platform)".into(),
    }
}

// Allows the consumer thread to use BinaryHeap without warnings below.
#[allow(dead_code)]
fn _unused() -> BinaryHeap<u64> {
    BinaryHeap::new()
}
