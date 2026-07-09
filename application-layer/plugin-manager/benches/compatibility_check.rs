//! Benchmark: API compatibility checking.
//!
//! Measures the performance of checking plugin API compatibility.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use plugin_manager::config::Preset;
use plugin_manager::{ApiVersion, PluginDescriptor, PluginId, PluginLoader, PluginVersion};

fn bench_api_check(c: &mut Criterion) {
    let cfg = Preset::Strict.to_config();
    let loader = PluginLoader::new(cfg, 42);

    // Compatible plugin (same API version).
    let compatible_desc = PluginDescriptor::new(PluginId::nil(), PluginVersion::new(1, 0, 0));

    // Incompatible plugin (higher minor version).
    let incompatible_desc = PluginDescriptor {
        api_version: ApiVersion::new(1, 5),
        ..compatible_desc
    };

    let mut group = c.benchmark_group("compatibility_check");

    group.bench_function("compatible", |b| {
        b.iter(|| black_box(loader.check_api(&compatible_desc)));
    });

    group.bench_function("incompatible", |b| {
        b.iter(|| black_box(loader.check_api(&incompatible_desc)));
    });

    group.finish();
}

criterion_group!(compatibility_check, bench_api_check);
criterion_main!(compatibility_check);
