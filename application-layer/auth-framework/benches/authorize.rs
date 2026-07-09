//! Benchmark: Authorization performance.

#![allow(clippy::unwrap_used)]

use auth_framework::config::Preset;
use auth_framework::{AuthorizationEngine, Permission, PrincipalId};
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};

fn bench_authorize(c: &mut Criterion) {
    let mut group = c.benchmark_group("authorize");

    group.bench_function("single_permission", |b| {
        let cfg = Preset::Standard.to_config();
        let mut authz = AuthorizationEngine::new(cfg);
        let perm = Permission::new(1, 2);
        b.iter(|| black_box(&mut authz).authorize(black_box(PrincipalId::new(1)), black_box(perm)));
    });

    for checks in [10, 100, 1000].iter() {
        group.bench_with_input(BenchmarkId::new("batch", checks), checks, |b, &checks| {
            b.iter(|| {
                let cfg = Preset::Standard.to_config();
                let mut authz = AuthorizationEngine::new(cfg);
                for i in 0..checks {
                    let perm = Permission::new(i, i);
                    let _ = black_box(&mut authz)
                        .authorize(black_box(PrincipalId::new(i as u64)), black_box(perm));
                }
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_authorize);
criterion_main!(benches);
