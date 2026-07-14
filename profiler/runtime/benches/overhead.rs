//! Benchmark: profiler overhead.
//!
//! Compares three scenarios:
//! 1. **No profiler** — plain function call (baseline)
//! 2. **Profiler disabled** — `#[profile]` but `enabled` feature off
//! 3. **Profiler enabled** — `#[profile]` with `enabled` feature on
//!
//! Measures: empty function call, nested calls (5 deep), and
//! a computational workload.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use profiler::{profile, Guard, Profiler, ProfilerConfig};

// ── Instrumented functions ────────────────────────────────────────

#[profile]
fn empty_call() -> u64 {
    42
}

#[profile]
fn nested_a() -> u64 {
    nested_b()
}

#[profile]
fn nested_b() -> u64 {
    nested_c()
}

#[profile]
fn nested_c() -> u64 {
    nested_d()
}

#[profile]
fn nested_d() -> u64 {
    nested_e()
}

#[profile]
fn nested_e() -> u64 {
    42
}

// ── Plain (uninstrumented) equivalents ────────────────────────────

fn plain_empty() -> u64 {
    42
}

fn plain_nested_a() -> u64 {
    plain_nested_b()
}
fn plain_nested_b() -> u64 {
    plain_nested_c()
}
fn plain_nested_c() -> u64 {
    plain_nested_d()
}
fn plain_nested_d() -> u64 {
    plain_nested_e()
}
fn plain_nested_e() -> u64 {
    42
}

// ── Benchmarks ────────────────────────────────────────────────────

fn bench_single_call(c: &mut Criterion) {
    Profiler::init(ProfilerConfig::default());

    let mut group = c.benchmark_group("single_call");

    group.bench_function("plain", |b| {
        b.iter(|| black_box(plain_empty()))
    });

    group.bench_function("profiler_enabled", |b| {
        Profiler::enable();
        b.iter(|| black_box(empty_call()))
    });

    group.bench_function("profiler_disabled", |b| {
        Profiler::disable();
        b.iter(|| black_box(empty_call()))
    });

    group.finish();
}

fn bench_nested_calls(c: &mut Criterion) {
    Profiler::init(ProfilerConfig::default());

    let mut group = c.benchmark_group("nested_5_deep");

    group.bench_function("plain", |b| {
        b.iter(|| black_box(plain_nested_a()))
    });

    group.bench_function("profiler_enabled", |b| {
        Profiler::enable();
        b.iter(|| black_box(nested_a()))
    });

    group.bench_function("profiler_disabled", |b| {
        Profiler::disable();
        b.iter(|| black_box(nested_a()))
    });

    group.finish();
}

fn bench_guard_direct(c: &mut Criterion) {
    Profiler::init(ProfilerConfig::default());
    Profiler::enable();

    c.bench_function("guard_enter_drop", |b| {
        b.iter(|| {
            let _g = Guard::enter("bench_guard");
            black_box(42)
        })
    });
}

fn bench_high_throughput(c: &mut Criterion) {
    Profiler::init(ProfilerConfig::default());
    Profiler::enable();

    c.bench_function("1k_calls", |b| {
        b.iter(|| {
            for _ in 0..1000 {
                black_box(empty_call());
            }
        })
    });
}

criterion_group!(
    benches,
    bench_single_call,
    bench_nested_calls,
    bench_guard_direct,
    bench_high_throughput,
);
criterion_main!(benches);
