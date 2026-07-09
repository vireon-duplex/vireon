//! Benchmark for RTP packetization operations.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use media_streaming::MediaStreamingStats;

fn bench_record_packet_sent(c: &mut Criterion) {
    let stats = MediaStreamingStats::new();

    c.bench_function("record_packet_sent", |b| {
        b.iter(|| {
            stats.record_packet_sent(black_box(1200));
        });
    });
}

fn bench_record_packet_received(c: &mut Criterion) {
    let stats = MediaStreamingStats::new();

    c.bench_function("record_packet_received", |b| {
        b.iter(|| {
            stats.record_packet_received(black_box(1200));
        });
    });
}

fn bench_stats_snapshot(c: &mut Criterion) {
    let stats = MediaStreamingStats::new();

    c.bench_function("stats_snapshot", |b| {
        b.iter(|| {
            stats.snapshot();
        });
    });
}

criterion_group!(
    benches,
    bench_record_packet_sent,
    bench_record_packet_received,
    bench_stats_snapshot
);
criterion_main!(benches);
