#![allow(missing_docs)]

//! Benchmark for rendering widgets.
//!
//! Measures the throughput of widget rendering operations.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use ops_dashboard::{
    DashboardWidgetId, OpsDashboardConfig, OpsDashboardService, OpsDashboardStats,
};

fn bench_widget_id_new(c: &mut Criterion) {
    c.bench_function("widget_id_new", |b| {
        b.iter(|| {
            let id = DashboardWidgetId::new(black_box(42));
            black_box(id);
        });
    });
}

fn bench_widget_id_nil(c: &mut Criterion) {
    c.bench_function("widget_id_nil", |b| {
        b.iter(|| {
            let id = DashboardWidgetId::nil();
            black_box(id.is_nil());
        });
    });
}

fn bench_render_widget(c: &mut Criterion) {
    let cfg = OpsDashboardConfig::new();
    let stats = OpsDashboardStats::new();
    let svc = OpsDashboardService::new(cfg, stats);
    let widget_id = DashboardWidgetId::new(1);

    c.bench_function("render_widget", |b| {
        b.iter(|| {
            let _ = svc.render_widget(black_box(widget_id));
        });
    });
}

criterion_group!(
    benches,
    bench_widget_id_new,
    bench_widget_id_nil,
    bench_render_widget
);
criterion_main!(benches);
