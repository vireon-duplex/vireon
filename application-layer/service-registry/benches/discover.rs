//! Benchmark: Service discovery performance.
//!
//! Measures the cost of discovering services and their endpoints.

#![allow(clippy::unwrap_used)]

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use service_registry::config::Preset;
use service_registry::{ServiceEndpoint, ServiceId, ServiceRegistry, ServiceRegistryBuilder};
use session_registry::node::NodeId;

fn bench_discover(c: &mut Criterion) {
    let mut group = c.benchmark_group("discover");

    // Benchmark discovering a non-existent service
    group.bench_function("not_found", |b| {
        let cfg = Preset::Local.to_config();
        let registry = ServiceRegistry::new(cfg).unwrap();
        b.iter(|| black_box(&registry).discover(black_box(ServiceId::new(999))));
    });

    // Benchmark discovering with different endpoint counts
    for endpoint_count in [1, 10, 100].iter() {
        group.bench_with_input(
            BenchmarkId::new("with_endpoints", endpoint_count),
            endpoint_count,
            |b, &endpoint_count| {
                b.iter(|| {
                    let cfg = Preset::Cluster.to_config();
                    let mut registry = ServiceRegistry::new(cfg).unwrap();
                    let service_id = ServiceId::new(1);
                    for i in 0..endpoint_count {
                        let endpoint = ServiceEndpoint::new(
                            NodeId::new(i as u32),
                            [127, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                            8080 + i as u16,
                        );
                        let _ = black_box(&mut registry)
                            .add_endpoint(black_box(service_id), black_box(endpoint));
                    }
                    black_box(&registry).discover(black_box(service_id))
                });
            },
        );
    }

    group.finish();
}

fn bench_discover_concurrent(c: &mut Criterion) {
    c.bench_function("discover_multiple_services", |b| {
        b.iter(|| {
            let cfg = Preset::Cluster.to_config();
            let registry = ServiceRegistry::new(cfg).unwrap();
            // Simulate multiple services
            for i in 0..10 {
                let _ = black_box(&registry).discover(black_box(ServiceId::new(i)));
            }
        });
    });
}

criterion_group!(benches, bench_discover, bench_discover_concurrent);
criterion_main!(benches);
