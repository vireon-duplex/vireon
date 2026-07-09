//! Benchmark: plugin discovery throughput.
//!
//! Measures the performance of discovering plugins in a scan path.

use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use plugin_manager::PluginLoader;
use plugin_manager::config::Preset;

fn bench_plugin_discover(c: &mut Criterion) {
    let mut group = c.benchmark_group("discover");

    // Benchmark discovering a single plugin.
    group.bench_function("single", |b| {
        let cfg = Preset::Strict.to_config();
        let mut loader = PluginLoader::new(cfg, 42);
        b.iter(|| {
            // TODO(phase-1): real plugin discovery.
            black_box(loader.discover())
        });
    });

    // Benchmark discovering multiple plugin sets.
    for count in [1, 4, 16, 64].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(count), count, |b, &n| {
            let cfg = Preset::Strict.to_config();
            let mut loader = PluginLoader::new(cfg, 42);
            b.iter(|| {
                for _ in 0..n {
                    let _ = black_box(loader.discover());
                }
            });
        });
    }

    group.finish();
}

criterion_group!(discover, bench_plugin_discover);
criterion_main!(discover);
