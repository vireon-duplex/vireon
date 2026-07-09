//! Benchmark: Health check performance.
//!
//! Measures the cost of performing health checks on service instances.

#![allow(clippy::unwrap_used)]

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use service_registry::stats::ServiceRegistryStats;
use service_registry::{ServiceEndpoint, ServiceInstance};
use session_registry::node::NodeId;

fn bench_health_check(c: &mut Criterion) {
    let mut group = c.benchmark_group("health_check");

    // Benchmark checking if a single instance is healthy
    group.bench_function("single_instance", |b| {
        let endpoint = ServiceEndpoint::new(
            NodeId::new(1),
            [127, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            443,
        );
        let instance = ServiceInstance::new(endpoint).with_health_score(100);
        b.iter(|| black_box(&instance).is_healthy());
    });

    // Benchmark checking health with different scores
    for score in [0, 50, 75, 100].iter() {
        group.bench_with_input(BenchmarkId::new("by_score", score), score, |b, &score| {
            let endpoint = ServiceEndpoint::new(NodeId::new(1), [0u8; 16], 443);
            let instance = ServiceInstance::new(endpoint).with_health_score(score);
            b.iter(|| black_box(&instance).is_healthy());
        });
    }

    group.finish();
}

fn bench_update_health(c: &mut Criterion) {
    let mut group = c.benchmark_group("update_health");

    group.bench_function("update_single", |b| {
        let mut stats = ServiceRegistryStats::new();
        let endpoint = ServiceEndpoint::new(NodeId::new(1), [0u8; 16], 443);
        let mut instance = ServiceInstance::new(endpoint);
        b.iter(|| {
            black_box(&mut instance).update_health(black_box(85), black_box(1_000_000));
            black_box(&stats).record_endpoint_health_change(true);
        });
    });

    // Benchmark batch health updates
    for count in [10, 100, 1000].iter() {
        group.bench_with_input(BenchmarkId::new("batch", count), count, |b, &count| {
            let stats = ServiceRegistryStats::new();
            let mut instances: Vec<_> = (0..count)
                .map(|i| {
                    let endpoint = ServiceEndpoint::new(NodeId::new(i as u32), [0u8; 16], 443);
                    ServiceInstance::new(endpoint)
                })
                .collect();
            b.iter(|| {
                for (i, instance) in instances.iter_mut().enumerate() {
                    instance.update_health(75, 1_000_000);
                    let _ = black_box(&stats).record_endpoint_health_change(true);
                }
            });
        });
    }

    group.finish();
}

fn bench_load_factor(c: &mut Criterion) {
    c.bench_function("compute_load_factor", |b| {
        let endpoint = ServiceEndpoint::new(NodeId::new(1), [0u8; 16], 443);
        let instance = ServiceInstance::new(endpoint).with_active_requests(50);
        b.iter(|| black_box(&instance).load_factor());
    });
}

criterion_group!(
    benches,
    bench_health_check,
    bench_update_health,
    bench_load_factor
);
criterion_main!(benches);
