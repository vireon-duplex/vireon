//! Benchmark: Service registration performance.
//!
//! Measures the cost of registering services in the registry.

#![allow(clippy::unwrap_used)]

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use service_registry::config::Preset;
use service_registry::{ServiceId, ServiceInfo, ServiceRegistry, ServiceRegistryBuilder};

fn bench_register(c: &mut Criterion) {
    let mut group = c.benchmark_group("register");

    // Benchmark registering a single service
    group.bench_function("single_service", |b| {
        let cfg = Preset::Local.to_config();
        let mut registry = ServiceRegistry::new(cfg).unwrap();
        b.iter(|| {
            let info = ServiceInfo::new(ServiceId::new(1)).with_name("test-service");
            black_box(&mut registry).register(black_box(info))
        });
    });

    // Benchmark registering multiple services
    for count in [10, 100, 1000].iter() {
        group.bench_with_input(BenchmarkId::new("batch", count), count, |b, &count| {
            b.iter(|| {
                let cfg = Preset::Cluster.to_config();
                let mut registry = ServiceRegistry::new(cfg).unwrap();
                for i in 0..count {
                    let info = ServiceInfo::new(ServiceId::new(i as u64))
                        .with_name(&format!("service-{}", i));
                    let _ = black_box(&mut registry).register(black_box(info));
                }
            });
        });
    }

    group.finish();
}

fn bench_register_with_builder(c: &mut Criterion) {
    c.bench_function("register_with_builder", |b| {
        b.iter(|| {
            let cfg = ServiceRegistryBuilder::from_preset(Preset::Local)
                .build()
                .unwrap();
            let mut registry = ServiceRegistry::new(cfg).unwrap();
            let info = ServiceInfo::new(ServiceId::new(1)).with_name("test-service");
            black_box(&mut registry).register(black_box(info))
        });
    });
}

criterion_group!(benches, bench_register, bench_register_with_builder);
criterion_main!(benches);
