//! Benchmark for simulcast layer switch operations.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use media_streaming::{BitrateLadder, MediaStreamingStats};

fn bench_layer_switch_record(c: &mut Criterion) {
    let stats = MediaStreamingStats::new();

    c.bench_function("record_layer_switch", |b| {
        b.iter(|| {
            stats.record_layer_switch();
        });
    });
}

fn bench_bitrate_adjustment_record(c: &mut Criterion) {
    let stats = MediaStreamingStats::new();

    c.bench_function("record_bitrate_adjustment", |b| {
        b.iter(|| {
            stats.record_bitrate_adjustment();
        });
    });
}

fn bench_ladder_highest_lowest(c: &mut Criterion) {
    let mut ladder = BitrateLadder::new();
    for i in 0..8 {
        ladder.add_layer(i, 1_000_000 * (i + 1) as u32).unwrap();
    }

    c.bench_function("ladder_highest", |b| {
        b.iter(|| {
            ladder.highest();
        });
    });

    c.bench_function("ladder_lowest", |b| {
        b.iter(|| {
            ladder.lowest();
        });
    });
}

criterion_group!(
    benches,
    bench_layer_switch_record,
    bench_bitrate_adjustment_record,
    bench_ladder_highest_lowest
);
criterion_main!(benches);
