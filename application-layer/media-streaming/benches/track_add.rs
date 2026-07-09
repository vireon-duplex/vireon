//! Benchmark for track addition operations.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use media_streaming::{MediaStreamingConfig, MediaStreamingStats, MediaTrackId};

fn bench_track_id_creation(c: &mut Criterion) {
    c.bench_function("track_id_new", |b| {
        b.iter(|| MediaTrackId::new(black_box(42)));
    });
}

fn bench_record_track_created(c: &mut Criterion) {
    let stats = MediaStreamingStats::new();

    c.bench_function("record_track_created", |b| {
        b.iter(|| {
            stats.record_track_created();
        });
    });
}

fn bench_config_validation(c: &mut Criterion) {
    let cfg = MediaStreamingConfig::new();

    c.bench_function("config_validate", |b| {
        b.iter(|| {
            cfg.validate();
        });
    });
}

criterion_group!(
    benches,
    bench_track_id_creation,
    bench_record_track_created,
    bench_config_validation
);
criterion_main!(benches);
