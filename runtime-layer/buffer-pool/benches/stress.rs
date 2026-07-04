//! Long-running stability stress test (Category 20).
//!
//! Not a Criterion benchmark — this is a standalone binary that runs the
//! pool under sustained load for a configurable duration, reporting periodic
//! telemetry and a final leak / fragmentation report.
//!
//! ## Usage
//!
//! ```bash
//! # 60-second run with 4 threads (defaults)
//! cargo bench -p buffer-pool --bench stress
//!
//! # 5-minute run with 8 threads
//! cargo bench -p buffer-pool --bench stress -- 300 8
//!
//! # 1-hour soak test
//! cargo bench -p buffer-pool --bench stress -- 3600 4
//! ```
//!
//! ## What it checks
//!
//! - **Memory growth**: `arena_bytes` and `cached_bytes` should stabilise,
//!   not grow unbounded.
//! - **Leaks**: `outstanding` must be 0 after all threads finish.
//! - **Reuse efficiency**: `hit_rate` + `reuse_rate` should stay high.
//! - **Latency stability**: `p99_ns` should not drift upward over time.
//! - **Fragmentation**: `peak_outstanding` vs steady-state `outstanding`.

#![allow(
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::expect_used,
    clippy::unwrap_used
)]

#[path = "common.rs"]
mod common;

use std::env;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use buffer_pool::BufferPool;

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    let duration_secs: u64 = args.first().and_then(|s| s.parse().ok()).unwrap_or(60);
    let num_threads: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(4);

    let duration = Duration::from_secs(duration_secs);
    let pool = BufferPool::default();

    println!("=== Buffer Pool Stress Test ===");
    println!("Duration: {duration_secs}s  Threads: {num_threads}");
    println!(
        "Config: local_cache={} global_cache={} shards={} batch={}",
        pool.config().local_cache,
        pool.config().global_cache,
        pool.config().shards,
        pool.config().batch,
    );
    println!();

    let stop = Arc::new(AtomicBool::new(false));
    let ops_counter = Arc::new(AtomicU64::new(0));
    let bytes_counter = Arc::new(AtomicU64::new(0));

    let start = Instant::now();

    // Background reporter.
    let reporter = {
        let pool = pool.clone();
        let stop = Arc::clone(&stop);
        let ops = Arc::clone(&ops_counter);
        let bytes = Arc::clone(&bytes_counter);
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_secs(5));
                let elapsed = start.elapsed().as_secs_f64();
                let stats = pool.stats();
                let o = ops.load(Ordering::Relaxed);
                let b = bytes.load(Ordering::Relaxed);
                println!(
                    "[{:>7.1}s] ops={:>12}  {:.1} GiB/s  hit={:5.1}%  reuse={:5.1}%  out={:>6}  arena={:6.1}MiB  cached={:6.1}MiB  p99={:>6}ns",
                    elapsed,
                    o,
                    b as f64 / elapsed / 1_073_741_824.0,
                    stats.hit_rate() * 100.0,
                    stats.reuse_rate() * 100.0,
                    stats.counters.outstanding,
                    stats.arena_bytes as f64 / 1_048_576.0,
                    stats.cached_bytes as f64 / 1_048_576.0,
                    stats.latency.percentiles.p99_ns,
                );
            }
        })
    };

    // Worker threads + timer inside the same scope.
    // The timer sets `stop` after `duration`; workers exit when they see it.
    thread::scope(|s| {
        // Timer: signals stop after the configured duration.
        let stop_timer = Arc::clone(&stop);
        s.spawn(move || {
            thread::sleep(duration);
            stop_timer.store(true, Ordering::Relaxed);
        });

        for tid in 0..num_threads {
            let pool_ref = &pool;
            let stop_ref = &stop;
            let ops_ref = &ops_counter;
            let bytes_ref = &bytes_counter;
            s.spawn(move || {
                let mut rng = common::XorShift64::new(0x1234_5678_9ABC_DEF0 ^ (tid as u64));
                while !stop_ref.load(Ordering::Relaxed) {
                    let &(size, _) = rng.pick(common::WORKLOAD);
                    let mut buf = pool_ref.get(size).expect("get");
                    buf[0] = tid as u8;
                    black_box(buf.as_ptr());
                    black_box(buf.len());
                    drop(buf);
                    ops_ref.fetch_add(1, Ordering::Relaxed);
                    bytes_ref.fetch_add(size as u64, Ordering::Relaxed);
                }
                pool_ref.flush();
            });
        }
    });

    let _ = reporter.join();

    // Final report.
    thread::sleep(Duration::from_millis(200)); // let releases settle
    let stats = pool.stats();
    let elapsed = start.elapsed().as_secs_f64();
    let total_ops = ops_counter.load(Ordering::Relaxed);
    let total_bytes = bytes_counter.load(Ordering::Relaxed);

    println!();
    println!("=== Final Report ===");
    println!("Elapsed:       {elapsed:.2}s");
    println!("Total ops:     {total_ops}");
    println!(
        "Total bytes:   {:.2} GiB",
        total_bytes as f64 / 1_073_741_824.0
    );
    println!("Ops/sec:       {:.0}", total_ops as f64 / elapsed);
    println!(
        "Throughput:    {:.2} GiB/s",
        total_bytes as f64 / elapsed / 1_073_741_824.0
    );
    println!();
    println!("--- Cache efficiency ---");
    println!("Hit rate:      {:.1}%", stats.hit_rate() * 100.0);
    println!("Reuse rate:    {:.1}%", stats.reuse_rate() * 100.0);
    println!("Hits:          {}", stats.counters.hits);
    println!("Misses:        {}", stats.counters.misses);
    println!("Reuses:        {}", stats.counters.reuses);
    println!("Allocations:   {}", stats.counters.allocations);
    println!("Frees:         {}", stats.counters.frees);
    println!();
    println!("--- Memory ---");
    println!("Outstanding:   {}", stats.counters.outstanding);
    println!("Peak outstanding: {}", stats.counters.peak_outstanding);
    println!(
        "Arena memory:  {:.2} MiB",
        stats.arena_bytes as f64 / 1_048_576.0
    );
    println!(
        "Cached memory: {:.2} MiB",
        stats.cached_bytes as f64 / 1_048_576.0
    );
    println!();
    println!("--- Latency ---");
    println!("p50: {} ns", stats.latency.percentiles.p50_ns);
    println!("p90: {} ns", stats.latency.percentiles.p90_ns);
    println!("p99: {} ns", stats.latency.percentiles.p99_ns);
    println!("avg: {} ns", stats.latency.average_ns);
    println!();

    // Leak check.
    if stats.counters.outstanding == 0 {
        println!("LEAK CHECK:    PASS (outstanding = 0)");
    } else {
        println!(
            "LEAK CHECK:    *** FAIL *** (outstanding = {})",
            stats.counters.outstanding
        );
    }
}
