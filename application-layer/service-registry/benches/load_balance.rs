//! Benchmark: Load balancing performance.
//!
//! Measures the cost of selecting instances using different strategies.

#![allow(clippy::unwrap_used)]

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use service_registry::config::Preset;
use service_registry::{LoadBalancer, ServiceEndpoint, ServiceInstance};
use session_registry::node::NodeId;

fn bench_load_balance_select(c: &mut Criterion) {
    let mut group = c.benchmark_group("load_balance_select");

    // Benchmark selecting from different numbers of instances
    for instance_count in [1, 10, 100].iter() {
        group.bench_with_input(
            BenchmarkId::new("select", instance_count),
            instance_count,
            |b, &instance_count| {
                let cfg = Preset::Local.to_config();
                let lb = LoadBalancer::new(cfg);
                let instances: Vec<ServiceInstance> = (0..instance_count)
                    .map(|i| {
                        let endpoint = ServiceEndpoint::new(
                            NodeId::new(i as u32),
                            [127, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                            8080 + i as u16,
                        );
                        ServiceInstance::new(endpoint).with_active_requests(i as u32)
                    })
                    .collect();
                b.iter(|| black_box(&lb).select(black_box(&instances)));
            },
        );
    }

    group.finish();
}

fn bench_round_robin(c: &mut Criterion) {
    let mut group = c.benchmark_group("round_robin");

    // Benchmark round-robin with different endpoint counts
    for endpoint_count in [1, 10, 100].iter() {
        group.bench_with_input(
            BenchmarkId::new("endpoints", endpoint_count),
            endpoint_count,
            |b, &endpoint_count| {
                let cfg = Preset::Local.to_config();
                let lb = LoadBalancer::new(cfg);
                let endpoints: Vec<ServiceEndpoint> = (0..endpoint_count)
                    .map(|i| {
                        ServiceEndpoint::new(
                            NodeId::new(i as u32),
                            [127, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                            8080 + i as u16,
                        )
                    })
                    .collect();
                b.iter(|| black_box(&lb).round_robin(black_box(&endpoints)));
            },
        );
    }

    group.finish();
}

fn bench_least_loaded(c: &mut Criterion) {
    let mut group = c.benchmark_group("least_loaded");

    // Benchmark least-loaded selection with different instance counts
    for instance_count in [1, 10, 100].iter() {
        group.bench_with_input(
            BenchmarkId::new("instances", instance_count),
            instance_count,
            |b, &instance_count| {
                let cfg = Preset::Local.to_config();
                let lb = LoadBalancer::new(cfg);
                let instances: Vec<ServiceInstance> = (0..instance_count)
                    .map(|i| {
                        let endpoint = ServiceEndpoint::new(
                            NodeId::new(i as u32),
                            [127, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                            8080 + i as u16,
                        );
                        ServiceInstance::new(endpoint).with_active_requests((i % 50) as u32)
                    })
                    .collect();
                b.iter(|| black_box(&lb).least_loaded(black_box(&instances)));
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_load_balance_select,
    bench_round_robin,
    bench_least_loaded
);
criterion_main!(benches);
