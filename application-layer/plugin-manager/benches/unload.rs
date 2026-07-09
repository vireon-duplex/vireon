//! Benchmark: plugin unloading throughput.
//!
//! Measures the performance of unloading plugins in sequence.

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};

fn bench_plugin_unload(c: &mut Criterion) {
    let mut group = c.benchmark_group("unload");

    // Benchmark unloading a single plugin.
    group.bench_function("single", |b| {
        b.iter(|| {
            // TODO(phase-1): real plugin unload.
            black_box(42)
        });
    });

    // Benchmark unloading multiple plugins in sequence.
    for count in [1, 4, 16, 64].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(count), count, |b, &n| {
            b.iter(|| {
                for _ in 0..n {
                    black_box(42);
                }
            });
        });
    }

    group.finish();
}

criterion_group!(unload, bench_plugin_unload);
criterion_main!(unload);
