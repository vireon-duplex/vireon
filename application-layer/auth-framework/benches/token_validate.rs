//! Benchmark: Token validation performance.

#![allow(clippy::unwrap_used)]

use auth_framework::config::Preset;
use auth_framework::{PrincipalId, TokenManager};
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};

fn bench_token_validate(c: &mut Criterion) {
    let mut group = c.benchmark_group("token_validate");

    group.bench_function("single_token", |b| {
        let cfg = Preset::Standard.to_config();
        let mut tm = TokenManager::new(cfg);
        b.iter(|| black_box(&mut tm).validate(black_box(123)));
    });

    for count in [10, 100, 1000].iter() {
        group.bench_with_input(BenchmarkId::new("batch", count), count, |b, &count| {
            b.iter(|| {
                let cfg = Preset::Standard.to_config();
                let mut tm = TokenManager::new(cfg);
                for i in 0..count {
                    let _ = black_box(&mut tm).validate(black_box(i));
                }
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_token_validate);
criterion_main!(benches);
