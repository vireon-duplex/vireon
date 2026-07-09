#![allow(missing_docs)]

//! Benchmark for widget refresh operations.
//!
//! Measures the throughput of widget refresh and health check operations.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use ops_dashboard::{
    DashboardWidgetId, HealthReport, OpsDashboardConfig, OpsDashboardService, OpsDashboardStats,
};

fn bench_refresh_widget(c: &mut Criterion) {
    let cfg = OpsDashboardConfig::new();
    let stats = OpsDashboardStats::new();
    let svc = OpsDashboardService::new(cfg, stats);
    let widget_id = DashboardWidgetId::new(1);

    c.bench_function("refresh_widget", |b| {
        b.iter(|| {
            let _ = svc.refresh_widget(black_box(widget_id));
        });
    });
}

fn bench_health_derive(c: &mut Criterion) {
    let stats = OpsDashboardStats::new();
    let snapshot = stats.snapshot();

    c.bench_function("health_derive", |b| {
        b.iter(|| {
            black_box(HealthReport::derive(black_box(&snapshot)));
        });
    });
}

fn bench_service_health(c: &mut Criterion) {
    let cfg = OpsDashboardConfig::new();
    let stats = OpsDashboardStats::new();
    let svc = OpsDashboardService::new(cfg, stats);

    c.bench_function("service_health", |b| {
        b.iter(|| {
            black_box(svc.health());
        });
    });
}

criterion_group!(
    benches,
    bench_refresh_widget,
    bench_health_derive,
    bench_service_health
);
criterion_main!(benches);
