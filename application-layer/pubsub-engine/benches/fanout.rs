//! Bench: fanout — measure publish cost as the number of matching
//! subscriptions grows (1, 10, 100, 1000).
//!
//! Exercises the publish hot path the way a real broker does: one
//! `publish()` call fans out to N subscribers. The runtime is expected
//! to scale linearly with N (each matching pattern triggers a stat
//! increment), so the bench surfaces any accidental superlinear work.

#![allow(missing_docs)]

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use pubsub_engine::{PubSubConfig, PubSubEngine, PubSubMessage, TopicPattern};

const FANOUTS: &[usize] = &[1, 10, 100, 1000];

/// Build an engine with `n` subscriptions, all matching `sensor.temp`
/// via the `sensor.*` wildcard. Each subscription is a distinct entry
/// in the engine's map, so `publish()` performs `n` matcher calls.
fn build_engine_with_fans(n: usize) -> PubSubEngine {
    let mut engine = PubSubEngine::new(PubSubConfig::new());
    let pattern = TopicPattern::new("sensor.*").unwrap();
    for _ in 0..n {
        engine.subscribe(pattern.clone()).unwrap();
    }
    engine
}

fn bench_fanout(c: &mut Criterion) {
    let msg = PubSubMessage::new("sensor.temp", vec![0xAB; 64]);
    let mut group = c.benchmark_group("fanout");
    for &n in FANOUTS {
        let mut engine = build_engine_with_fans(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &_| {
            b.iter(|| {
                let _ = black_box(engine.publish(black_box(msg.clone())));
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_fanout);
criterion_main!(benches);
