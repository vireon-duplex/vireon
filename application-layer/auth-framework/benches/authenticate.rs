//! Benchmark: Authentication performance.

#![allow(clippy::unwrap_used)]

use auth_framework::config::Preset;
use auth_framework::{AuthenticationPipeline, Credential, CredentialType, PrincipalId};
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};

fn bench_authenticate(c: &mut Criterion) {
    let mut group = c.benchmark_group("authenticate");

    group.bench_function("single_credential", |b| {
        let cfg = Preset::Standard.to_config();
        let mut auth = AuthenticationPipeline::new(cfg).unwrap();
        let cred = Credential::new(CredentialType::Password, 123, 0);
        b.iter(|| {
            black_box(&mut auth).authenticate(black_box(PrincipalId::new(1)), black_box(cred))
        });
    });

    for attempts in [10, 100, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::new("batch", attempts),
            attempts,
            |b, &attempts| {
                b.iter(|| {
                    let cfg = Preset::Standard.to_config();
                    let mut auth = AuthenticationPipeline::new(cfg).unwrap();
                    for i in 0..attempts {
                        let cred = Credential::new(CredentialType::Password, i, 0);
                        let _ = black_box(&mut auth)
                            .authenticate(black_box(PrincipalId::new(i as u64)), black_box(cred));
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_authenticate);
criterion_main!(benches);
