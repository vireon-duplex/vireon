//! Export formats: JSON, CSV, Chrome Trace.
//!
//! All functions take a `&[FunctionSnapshot]` slice and return a
//! `String`. JSON and Chrome Trace use `serde_json` when the `enabled`
//! feature is active; CSV is always available (manual formatting).

use crate::stats::FunctionSnapshot;

/// Exports snapshots as a JSON array.
///
/// Each object has: `name`, `calls`, `total_ns`, `avg_ns`, `min_ns`,
/// `max_ns`, `p50`, `p95`, `p99`, `last_ns`.
#[cfg(feature = "enabled")]
#[must_use]
pub fn export_json(snapshots: &[FunctionSnapshot]) -> String {
    #[derive(serde::Serialize)]
    struct JsonEntry {
        name: &'static str,
        calls: u64,
        total_ns: u64,
        avg_ns: u64,
        min_ns: u64,
        max_ns: u64,
        p50: u64,
        p95: u64,
        p99: u64,
        last_ns: u64,
    }

    let entries: Vec<JsonEntry> = snapshots
        .iter()
        .map(|s| JsonEntry {
            name: s.name,
            calls: s.calls,
            total_ns: s.total_ns,
            avg_ns: s.avg_ns,
            min_ns: s.min_ns,
            max_ns: s.max_ns,
            p50: s.p50,
            p95: s.p95,
            p99: s.p99,
            last_ns: s.last_ns,
        })
        .collect();

    serde_json::to_string_pretty(&entries).unwrap_or_else(|_| "[]".to_string())
}

/// Exports snapshots as CSV.
#[must_use]
pub fn export_csv(snapshots: &[FunctionSnapshot]) -> String {
    let mut out = String::with_capacity(snapshots.len() * 128);
    out.push_str("function,calls,total_ns,avg_ns,min_ns,max_ns,p50,p95,p99,last_ns\n");
    for s in snapshots {
        out.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{}\n",
            s.name,
            s.calls,
            s.total_ns,
            s.avg_ns,
            s.min_ns,
            s.max_ns,
            s.p50,
            s.p95,
            s.p99,
            s.last_ns,
        ));
    }
    out
}

/// Exports snapshots in Chrome Trace format (`chrome://tracing`).
///
/// Produces a JSON object with a `traceEvents` array. Each function is
/// represented as a metadata event (type "M") with summary statistics.
#[cfg(feature = "enabled")]
#[must_use]
pub fn export_chrome_trace(snapshots: &[FunctionSnapshot]) -> String {
    #[derive(serde::Serialize)]
    struct TraceEvent {
        name: &'static str,
        cat: &'static str,
        ph: &'static str,
        pid: i32,
        tid: i32,
        args: TraceArgs,
    }

    #[derive(serde::Serialize)]
    struct TraceArgs {
        calls: u64,
        total_ns: u64,
        avg_ns: u64,
        min_ns: u64,
        max_ns: u64,
        p50: u64,
        p95: u64,
        p99: u64,
    }

    let events: Vec<TraceEvent> = snapshots
        .iter()
        .map(|s| TraceEvent {
            name: s.name,
            cat: "profiler",
            ph: "M",
            pid: 1,
            tid: 1,
            args: TraceArgs {
                calls: s.calls,
                total_ns: s.total_ns,
                avg_ns: s.avg_ns,
                min_ns: s.min_ns,
                max_ns: s.max_ns,
                p50: s.p50,
                p95: s.p95,
                p99: s.p99,
            },
        })
        .collect();

    serde_json::to_string_pretty(&serde_json::json!({
        "traceEvents": events,
    }))
    .unwrap_or_else(|_| r#"{"traceEvents":[]}"#.to_string())
}

/// Fallback JSON export without serde (when `enabled` feature is off).
///
/// Produces a minimal JSON array with function names and call counts.
#[cfg(not(feature = "enabled"))]
#[must_use]
pub fn export_json(snapshots: &[FunctionSnapshot]) -> String {
    let mut out = String::from("[");
    for (i, s) in snapshots.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&format!(
            r#"{{"name":"{}","calls":{}}}"#,
            s.name, s.calls
        ));
    }
    out.push(']');
    out
}

/// Fallback Chrome Trace export without serde.
#[cfg(not(feature = "enabled"))]
#[must_use]
pub fn export_chrome_trace(snapshots: &[FunctionSnapshot]) -> String {
    let mut events = String::from("[");
    for (i, s) in snapshots.iter().enumerate() {
        if i > 0 {
            events.push(',');
        }
        events.push_str(&format!(
            r#"{{"name":"{}","ph":"M","args":{{"calls":{}}}}}"#,
            s.name, s.calls
        ));
    }
    events.push(']');
    format!(r#"{{"traceEvents":{events}}}"#)
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    fn sample(name: &'static str, calls: u64) -> FunctionSnapshot {
        FunctionSnapshot {
            name,
            calls,
            total_ns: calls * 100,
            avg_ns: 100,
            min_ns: 50,
            max_ns: 200,
            p50: 100,
            p95: 150,
            p99: 200,
            last_ns: 100,
        }
    }

    #[test]
    fn csv_has_header() {
        let csv = export_csv(&[sample("foo", 10)]);
        assert!(csv.starts_with("function,calls"));
        assert!(csv.contains("foo,10"));
    }

    #[test]
    fn csv_empty_snapshots() {
        let csv = export_csv(&[]);
        assert!(csv.starts_with("function,calls"));
        // Just the header line + newline
        assert_eq!(csv.matches('\n').count(), 1);
    }

    #[test]
    fn json_valid_array() {
        let json = export_json(&[sample("foo", 10), sample("bar", 20)]);
        assert!(json.starts_with('['));
        assert!(json.ends_with(']'));
        assert!(json.contains("foo"));
        assert!(json.contains("bar"));
    }

    #[test]
    fn json_empty_array() {
        let json = export_json(&[]);
        assert!(json.starts_with('['));
    }

    #[test]
    fn chrome_trace_has_events() {
        let trace = export_chrome_trace(&[sample("foo", 10)]);
        assert!(trace.contains("traceEvents"));
        assert!(trace.contains("foo"));
    }
}
