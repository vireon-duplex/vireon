//! Benchmark: Role checking performance.

#![allow(clippy::unwrap_used)]

use auth_framework::config::Preset;
use auth_framework::{AuthorizationEngine, PrincipalId};
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};

fn bench_role_check(c: &mut Criterion) {
    let mut group = c.benchmark_group("role_check");

    group.bench_function("single_role", |b| {
        let cfg = Preset::Standard.to_config();
        let mut authz = AuthorizationEngine::new(cfg);
        b.iter(|| black_box(&mut authz).check_role(black_box(PrincipalId::new(1)), black_box(1)));
    });

    for count in [10, 100, 1000].iter() {
        group.bench_with_input(BenchmarkId::new("batch", count), count, |b, &count| {
            b.iter(|| {
                let cfg = Preset::Standard.to_config();
                let mut authz = AuthorizationEngine::new(cfg);
                for i in 0..count {
                    let _ = black_box(&mut authz)
                        .check_role(black_box(PrincipalId::new(i as u64)), black_box(i % 10));
                }
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_role_check);
criterion_main!(benches);
