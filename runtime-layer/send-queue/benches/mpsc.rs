//! Benchmark: MPSC scaling, thread-local scaling, and false sharing.
//!
//! Three concerns:
//!
//! 1. **Thread-local scaling** — the production design is one queue
//!    per CPU core (no cross-thread sharing). N threads each run an
//!    independent submit → flush → reclaim cycle. Aggregate throughput
//!    should scale linearly with N.
//!
//! 2. **Shared-queue contention** — the anti-pattern: N producers share
//!    one `Arc<Mutex<SendQueue>>`. This measures the Mutex serialisation
//!    penalty and justifies the thread-local design.
//!
//! 3. **False sharing** — the ring uses `CachePadded<AtomicU64>` for its
//!    head/tail counters. This micro-bench proves the value of that
//!    padding by comparing padded vs unpadded counters under two-thread
//!    contention.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "common.rs"]
mod common;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

use send_queue::{PacketId, PacketMeta};

use common::{NullSink, dest_addr, fresh_queue};

// ══════════════════════════════════════════════════════════════════
// 1. Thread-local scaling
// ══════════════════════════════════════════════════════════════════

/// Each of `n_threads` threads owns a private `SendQueue` and runs
/// `pkts_per` complete lifecycles. No cross-thread sharing.
fn run_thread_local(n_threads: usize, pkts_per: usize) -> Duration {
    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..n_threads {
            s.spawn(move || {
                let mut sq = fresh_queue(4096);
                let dest = dest_addr();
                let mut sink = NullSink;
                for _ in 0..pkts_per {
                    let id = sq.submit(0, 64, dest, PacketMeta::new()).expect("submit");
                    sq.flush(&mut sink, 0).expect("flush");
                    sq.on_send_cqe(id.raw());
                    sq.on_reclaim_cqe(id.raw());
                }
            });
        }
    });
    start.elapsed()
}

fn bench_thread_local_scale(c: &mut Criterion) {
    let pkts_per = 5_000;
    let mut g = c.benchmark_group("thread_local_scale");
    for &n in common::PRODUCER_COUNTS {
        g.throughput(Throughput::Elements((pkts_per * n) as u64));
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    total += run_thread_local(n, pkts_per);
                }
                total
            })
        });
    }
    g.finish();
}

// ══════════════════════════════════════════════════════════════════
// 2. Shared-queue Mutex contention
// ══════════════════════════════════════════════════════════════════

/// N producers + 1 consumer, all sharing `Arc<Mutex<SendQueue>>`.
/// Producers submit and hand the PacketId to the consumer via a shared
/// `Mutex<Vec>`. The consumer flushes + completes each packet.
fn run_shared_mutex(n_producers: usize, pkts_per: usize) -> Duration {
    let sq = Arc::new(Mutex::new(fresh_queue(8192)));
    let pending: Arc<Mutex<Vec<PacketId>>> = Arc::new(Mutex::new(Vec::new()));
    let total_pkts = n_producers * pkts_per;

    let start = Instant::now();
    std::thread::scope(|s| {
        // Consumer.
        {
            let sq = Arc::clone(&sq);
            let pending = Arc::clone(&pending);
            s.spawn(move || {
                let mut sink = NullSink;
                let mut completed = 0usize;
                while completed < total_pkts {
                    let batch: Vec<PacketId> = {
                        let mut p = pending.lock().expect("pending");
                        if p.is_empty() {
                            drop(p);
                            std::hint::spin_loop();
                            continue;
                        }
                        std::mem::take(&mut *p)
                    };
                    let mut q = sq.lock().expect("queue");
                    let _ = q.flush(&mut sink, 0);
                    for id in &batch {
                        q.on_send_cqe(id.raw());
                        q.on_reclaim_cqe(id.raw());
                        completed += 1;
                    }
                    drop(q);
                }
            });
        }

        // Producers.
        let dest = dest_addr();
        for _ in 0..n_producers {
            let sq = Arc::clone(&sq);
            let pending = Arc::clone(&pending);
            s.spawn(move || {
                for _ in 0..pkts_per {
                    let id = loop {
                        let mut q = sq.lock().expect("queue");
                        match q.submit(0, 64, dest, PacketMeta::new()) {
                            Ok(id) => break id,
                            Err(_) => {
                                drop(q);
                                std::hint::spin_loop();
                            }
                        }
                    };
                    pending.lock().expect("pending").push(id);
                }
            });
        }
    });
    start.elapsed()
}

fn bench_shared_mutex_mpsc(c: &mut Criterion) {
    let pkts_per = 2_000;
    let mut g = c.benchmark_group("shared_mutex_mpsc");
    for &n in common::PRODUCER_COUNTS {
        g.throughput(Throughput::Elements((pkts_per * n) as u64));
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    total += run_shared_mutex(n, pkts_per);
                }
                total
            })
        });
    }
    g.finish();
}

// ══════════════════════════════════════════════════════════════════
// 3. False sharing micro-benchmark
// ══════════════════════════════════════════════════════════════════

/// Cache-line-aligned wrapper — each instance occupies its own line.
#[repr(C, align(64))]
struct PaddedU64(AtomicU64);

/// Two counters on **separate** cache lines — no false sharing.
#[repr(C, align(64))]
struct PaddedPair {
    a: PaddedU64, // occupies cache line 0
    b: PaddedU64, // occupies cache line 1
}

/// Two counters on the **same** cache line — false sharing.
#[repr(C, align(64))]
struct UnpaddedPair {
    a: AtomicU64, // offset 0
    b: AtomicU64, // offset 8 — same cache line
}

const FALSE_SHARING_ITERS: u64 = 1_000_000;

fn bench_false_sharing(c: &mut Criterion) {
    let mut g = c.benchmark_group("false_sharing");
    g.throughput(Throughput::Elements(FALSE_SHARING_ITERS * 2));

    // Padded: each thread hammers its own cache line.
    g.bench_function("padded", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let pair = Arc::new(PaddedPair {
                    a: PaddedU64(AtomicU64::new(0)),
                    b: PaddedU64(AtomicU64::new(0)),
                });
                let a_cloned = Arc::clone(&pair);
                let start = Instant::now();
                let h = std::thread::spawn(move || {
                    for _ in 0..FALSE_SHARING_ITERS {
                        a_cloned.a.0.fetch_add(1, Ordering::Relaxed);
                    }
                });
                for _ in 0..FALSE_SHARING_ITERS {
                    pair.b.0.fetch_add(1, Ordering::Relaxed);
                }
                h.join().expect("thread");
                total += start.elapsed();
            }
            total
        })
    });

    // Unpadded: both threads invalidate each other's cache line.
    g.bench_function("unpadded", |b| {
        b.iter_custom(|iters| {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let pair = Arc::new(UnpaddedPair {
                    a: AtomicU64::new(0),
                    b: AtomicU64::new(0),
                });
                let a_cloned = Arc::clone(&pair);
                let start = Instant::now();
                let h = std::thread::spawn(move || {
                    for _ in 0..FALSE_SHARING_ITERS {
                        a_cloned.a.fetch_add(1, Ordering::Relaxed);
                    }
                });
                for _ in 0..FALSE_SHARING_ITERS {
                    pair.b.fetch_add(1, Ordering::Relaxed);
                }
                h.join().expect("thread");
                total += start.elapsed();
            }
            total
        })
    });

    g.finish();
}

criterion_group!(
    benches,
    bench_thread_local_scale,
    bench_shared_mutex_mpsc,
    bench_false_sharing,
);
criterion_main!(benches);
