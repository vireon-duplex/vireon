//! `get_value` bench entry point — `ConfigurationManager::get` cost.

use criterion::{Criterion, criterion_group, criterion_main};

fn bench_get_value(_c: &mut Criterion) {
    // TODO(phase-1): ConfigKey → ConfigValue lookup cost.
}

criterion_group!(benches, bench_get_value);
criterion_main!(benches);
