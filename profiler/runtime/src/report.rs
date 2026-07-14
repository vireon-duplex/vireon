//! Text report generation.
//!
//! Produces a formatted table of all profiled functions sorted by total
//! time descending. Each row shows call count, average, min, max, P50,
//! P95, P99, and last duration.

use crate::stats::FunctionSnapshot;

/// Generates a formatted text report from snapshots.
#[must_use]
pub fn generate(snapshots: &[FunctionSnapshot]) -> String {
    if snapshots.is_empty() {
        return "No profiling data.\n".to_string();
    }

    let mut out = String::with_capacity(4096);
    out.push_str("\n");
    out.push_str("═══════════════════════════════════════════════════════════════════════════════\n");
    out.push_str("                               PROFILER REPORT\n");
    out.push_str("═══════════════════════════════════════════════════════════════════════════════\n");
    out.push_str("\n");

    // Table header
    out.push_str(&format!(
        "{:<40} {:>10} {:>12} {:>12} {:>10} {:>10} {:>10} {:>10} {:>10}\n",
        "Function", "Calls", "Total", "Avg", "Min", "Max", "P50", "P95", "P99"
    ));
    out.push_str(&"─".repeat(134));
    out.push('\n');

    for snap in snapshots {
        out.push_str(&format!(
            "{:<40} {:>10} {:>12} {:>12} {:>10} {:>10} {:>10} {:>10} {:>10}\n",
            truncate(snap.name, 40),
            snap.calls,
            FunctionSnapshot::fmt_duration(snap.total_ns),
            FunctionSnapshot::fmt_duration(snap.avg_ns),
            FunctionSnapshot::fmt_duration(snap.min_ns),
            FunctionSnapshot::fmt_duration(snap.max_ns),
            FunctionSnapshot::fmt_duration(snap.p50),
            FunctionSnapshot::fmt_duration(snap.p95),
            FunctionSnapshot::fmt_duration(snap.p99),
        ));
    }

    out.push_str(&"─".repeat(134));
    out.push('\n');

    // Summary line
    let total_calls: u64 = snapshots.iter().map(|s| s.calls).sum();
    let total_ns: u64 = snapshots.iter().map(|s| s.total_ns).sum();
    out.push_str(&format!(
        "\nTotal: {} functions, {} calls, {}\n\n",
        snapshots.len(),
        total_calls,
        FunctionSnapshot::fmt_duration(total_ns),
    ));

    out
}

/// Truncates `s` to `max` bytes, appending "…" if truncated.
fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        // Leave room for "…" (3 bytes in UTF-8).
        let cut = max.saturating_sub(3);
        // Find a valid UTF-8 boundary.
        let mut end = cut;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        let mut t = s[..end].to_string();
        t.push('…');
        t
    }
}

// ── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    fn sample_snapshot(name: &'static str, calls: u64, avg: u64) -> FunctionSnapshot {
        FunctionSnapshot {
            name,
            calls,
            total_ns: calls * avg,
            avg_ns: avg,
            min_ns: avg / 2,
            max_ns: avg * 2,
            p50: avg,
            p95: avg * 3 / 2,
            p99: avg * 2,
            last_ns: avg,
        }
    }

    #[test]
    fn empty_report() {
        let report = generate(&[]);
        assert!(report.contains("No profiling data"));
    }

    #[test]
    fn report_has_header() {
        let report = generate(&[sample_snapshot("foo", 10, 100)]);
        assert!(report.contains("PROFILER REPORT"));
    }

    #[test]
    fn report_contains_function_name() {
        let report = generate(&[sample_snapshot("my_function", 10, 100)]);
        assert!(report.contains("my_function"));
    }

    #[test]
    fn report_contains_call_count() {
        let report = generate(&[sample_snapshot("foo", 42, 100)]);
        assert!(report.contains("42"));
    }

    #[test]
    fn report_shows_summary() {
        let report = generate(&[
            sample_snapshot("foo", 10, 100),
            sample_snapshot("bar", 20, 200),
        ]);
        assert!(report.contains("Total:"));
        assert!(report.contains("2 functions"));
        assert!(report.contains("30 calls"));
    }

    #[test]
    fn truncate_long_name() {
        assert_eq!(truncate("short", 10), "short");
        let long = "a_really_long_function_name_that_exceeds";
        let t = truncate(long, 20);
        assert!(t.ends_with('…'));
        assert!(t.len() <= 20);
    }
}
