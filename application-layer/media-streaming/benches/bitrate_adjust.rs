//! Benchmark for bitrate adjustment operations.

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use media_streaming::{AdaptiveBitrateConfig, BitrateLadder};

fn bench_bitrate_ladder_add_layer(c: &mut Criterion) {
    let mut group = c.benchmark_group("bitrate_ladder_add_layer");

    for size in [1, 4, 8].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let mut ladder = BitrateLadder::new();
            for i in 0..size {
                ladder.add_layer(i, 1_000_000 * (i + 1) as u32).unwrap();
            }
            b.iter(|| {
                let mut l = ladder;
                l.add_layer(black_box(size), black_box(100_000))
            });
        });
    }

    group.finish();
}

fn bench_bitrate_best_for_bandwidth(c: &mut Criterion) {
    let mut ladder = BitrateLadder::new();
    for i in 0..8 {
        ladder.add_layer(i, 1_000_000 * (i + 1) as u32).unwrap();
    }

    c.bench_function("bitrate_best_for_bandwidth", |b| {
        b.iter(|| {
            ladder.best_for_bandwidth(black_box(3_500_000));
        });
    });
}

fn bench_abr_config_validation(c: &mut Criterion) {
    c.bench_function("abr_config_validation", |b| {
        b.iter(|| {
            AdaptiveBitrateConfig::new(
                black_box(300_000),
                black_box(5_000_000),
                black_box(1_500_000),
                black_box(500_000),
                black_box(1_000_000),
            )
        });
    });
}

criterion_group!(
    benches,
    bench_bitrate_ladder_add_layer,
    bench_bitrate_best_for_bandwidth,
    bench_abr_config_validation
);
criterion_main!(benches);
