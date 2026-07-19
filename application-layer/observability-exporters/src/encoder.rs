//! `encode_prometheus` — turn [`Metric`] families into the Prometheus
//! text exposition format (`text/plain; version=0.0.4`).

use crate::escape::{escape_help, escape_label_value};
use crate::metric::{Metric, Sample};

use std::fmt::Write as _;

/// Encode the given metrics to Prometheus text format.
///
/// Output format per metric family:
/// ```text
/// # HELP <name> <help-escaped>
/// # TYPE <name> <counter|gauge|histogram>
/// <name>[{<k>="<v>",...}] <value>
/// ...
/// ```
///
/// Empty sample lists are legal — `# HELP` and `# TYPE` lines are
/// still emitted so downstream scrapers recognise the metric as
/// registered but currently empty.
///
/// Float formatting follows the Prometheus convention: Rust's `{}`
/// formatter for finite values; `NaN` → `NaN`, `+inf` → `+Inf`,
/// `-inf` → `-Inf`.
#[must_use]
pub fn encode_prometheus(metrics: &[Metric]) -> String {
    let mut out = String::new();
    for m in metrics {
        // HELP line.
        // SAFETY(knowledge): write! into a String cannot fail.
        let _ = writeln!(out, "# HELP {} {}", m.name, escape_help(&m.help));
        let _ = writeln!(out, "# TYPE {} {}", m.name, m.kind.as_str());

        for s in &m.samples {
            write_sample(&mut out, &m.name, s);
        }
    }
    out
}

/// Append a single sample line (including trailing newline).
fn write_sample(out: &mut String, name: &str, s: &Sample) {
    if s.labels.is_empty() {
        let _ = writeln!(out, "{} {}", name, format_float(s.value));
    } else {
        // Format: name{k1="v1",k2="v2",...} value
        let _ = write!(out, "{}{{", name);
        for (i, l) in s.labels.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            let _ = write!(out, "{}=\"{}\"", l.key, escape_label_value(&l.value));
        }
        let _ = writeln!(out, "}} {}", format_float(s.value));
    }
}

/// Format an `f64` Prometheus-style: `NaN`, `+Inf`, `-Inf` use the
/// conventional spellings; finite values use Rust's default formatter.
fn format_float(v: f64) -> String {
    if v.is_nan() {
        return "NaN".to_owned();
    }
    if v.is_infinite() {
        return if v.is_sign_positive() {
            "+Inf".to_owned()
        } else {
            "-Inf".to_owned()
        };
    }
    // For integer-valued floats, prefer the canonical "5.0" rather
    // than "5" so scrapers that expect a decimal point don't
    // mis-parse. Rust's `{}` on f64 already produces this.
    format!("{}", v)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use crate::metric::{Label, MetricKind, Sample};

    fn metric(name: &str, help: &str, kind: MetricKind, samples: Vec<Sample>) -> Metric {
        Metric {
            name: name.to_owned(),
            help: help.to_owned(),
            kind,
            samples,
        }
    }

    // ── Single counter, no labels ────────────────────────────────

    #[test]
    fn single_unlabeled_counter() {
        let m = metric(
            "a_total",
            "count of a",
            MetricKind::Counter,
            vec![Sample::unlabeled(5.0)],
        );
        let out = encode_prometheus(&[m]);
        assert_eq!(
            out,
            "# HELP a_total count of a\n# TYPE a_total counter\na_total 5\n"
        );
    }

    #[test]
    fn float_value_uses_decimal() {
        let m = metric(
            "a_total",
            "x",
            MetricKind::Counter,
            vec![Sample::unlabeled(7.0)],
        );
        assert!(encode_prometheus(&[m]).contains("a_total 7\n"));
    }

    #[test]
    fn non_integer_value() {
        let m = metric(
            "a_total",
            "x",
            MetricKind::Gauge,
            vec![Sample::unlabeled(3.5)],
        );
        assert!(encode_prometheus(&[m]).contains("a_total 3.5\n"));
    }

    // ── Labeled samples ──────────────────────────────────────────

    #[test]
    fn single_labeled_gauge() {
        let m = metric(
            "g",
            "x",
            MetricKind::Gauge,
            vec![Sample {
                labels: vec![Label::new_unchecked("a", "1")],
                value: 10.0,
            }],
        );
        let out = encode_prometheus(&[m]);
        assert_eq!(out, "# HELP g x\n# TYPE g gauge\ng{a=\"1\"} 10\n");
    }

    #[test]
    fn multiple_labeled_samples_preserve_order() {
        let m = metric(
            "rq_total",
            "requests",
            MetricKind::Counter,
            vec![
                Sample {
                    labels: vec![
                        Label::new_unchecked("method", "GET"),
                        Label::new_unchecked("code", "200"),
                    ],
                    value: 100.0,
                },
                Sample {
                    labels: vec![
                        Label::new_unchecked("method", "POST"),
                        Label::new_unchecked("code", "500"),
                    ],
                    value: 3.0,
                },
            ],
        );
        let out = encode_prometheus(&[m]);
        assert_eq!(
            out,
            "# HELP rq_total requests\n# TYPE rq_total counter\n\
             rq_total{method=\"GET\",code=\"200\"} 100\n\
             rq_total{method=\"POST\",code=\"500\"} 3\n"
        );
    }

    // ── Escaping ─────────────────────────────────────────────────

    #[test]
    fn help_with_backslash_and_newline_is_escaped() {
        let m = metric("a", "with \\ and \n inside", MetricKind::Counter, vec![]);
        let out = encode_prometheus(&[m]);
        assert!(out.contains("# HELP a with \\\\ and \\n inside"));
    }

    #[test]
    fn label_value_with_quote_and_backslash_escaped() {
        let m = metric(
            "a",
            "x",
            MetricKind::Counter,
            vec![Sample {
                labels: vec![Label::new_unchecked("k", "has \" and \\")],
                value: 1.0,
            }],
        );
        let out = encode_prometheus(&[m]);
        // Expect: a{k="has \" and \\"} 1
        assert!(out.contains(r#"a{k="has \" and \\"} 1"#));
    }

    #[test]
    fn label_value_with_newline_escaped() {
        let m = metric(
            "a",
            "x",
            MetricKind::Counter,
            vec![Sample {
                labels: vec![Label::new_unchecked("k", "line1\nline2")],
                value: 1.0,
            }],
        );
        let out = encode_prometheus(&[m]);
        assert!(out.contains(r#"a{k="line1\nline2"} 1"#));
    }

    // ── NaN / Inf ────────────────────────────────────────────────

    #[test]
    fn nan_value_spelled_prometheus() {
        let m = metric(
            "g",
            "x",
            MetricKind::Gauge,
            vec![Sample::unlabeled(f64::NAN)],
        );
        assert!(encode_prometheus(&[m]).contains("g NaN\n"));
    }

    #[test]
    fn positive_inf_value_spelled_prometheus() {
        let m = metric(
            "g",
            "x",
            MetricKind::Gauge,
            vec![Sample::unlabeled(f64::INFINITY)],
        );
        assert!(encode_prometheus(&[m]).contains("g +Inf\n"));
    }

    #[test]
    fn negative_inf_value_spelled_prometheus() {
        let m = metric(
            "g",
            "x",
            MetricKind::Gauge,
            vec![Sample::unlabeled(f64::NEG_INFINITY)],
        );
        assert!(encode_prometheus(&[m]).contains("g -Inf\n"));
    }

    #[test]
    fn zero_value() {
        let m = metric("a", "x", MetricKind::Counter, vec![Sample::unlabeled(0.0)]);
        assert!(encode_prometheus(&[m]).contains("a 0\n"));
    }

    #[test]
    fn negative_value_emitted() {
        // Technically invalid for a counter but the encoder must not
        // panic or omit the sample.
        let m = metric("a", "x", MetricKind::Counter, vec![Sample::unlabeled(-3.5)]);
        assert!(encode_prometheus(&[m]).contains("a -3.5\n"));
    }

    // ── Empty samples ────────────────────────────────────────────

    #[test]
    fn empty_samples_emits_help_and_type_only() {
        let m = metric("a", "x", MetricKind::Counter, vec![]);
        let out = encode_prometheus(&[m]);
        assert_eq!(out, "# HELP a x\n# TYPE a counter\n");
    }

    // ── Multiple metrics ─────────────────────────────────────────

    #[test]
    fn multiple_metrics_concatenated() {
        let m1 = metric(
            "a_total",
            "count",
            MetricKind::Counter,
            vec![Sample::unlabeled(1.0)],
        );
        let m2 = metric(
            "b_gauge",
            "level",
            MetricKind::Gauge,
            vec![Sample::unlabeled(2.0)],
        );
        let out = encode_prometheus(&[m1, m2]);
        assert_eq!(
            out,
            "# HELP a_total count\n# TYPE a_total counter\na_total 1\n\
             # HELP b_gauge level\n# TYPE b_gauge gauge\nb_gauge 2\n"
        );
    }

    // ── Empty input ──────────────────────────────────────────────

    #[test]
    fn empty_input_yields_empty_string() {
        assert_eq!(encode_prometheus(&[]), "");
    }

    // ── Round-trip golden ────────────────────────────────────────

    #[test]
    fn golden_round_trip() {
        let metrics = vec![
            metric(
                "vireon_pubsub_messages_published_total",
                "Total messages published.",
                MetricKind::Counter,
                vec![
                    Sample {
                        labels: vec![Label::new_unchecked("topic", "sensor.temp")],
                        value: 1234.0,
                    },
                    Sample {
                        labels: vec![Label::new_unchecked("topic", "sensor.humidity")],
                        value: 5678.0,
                    },
                ],
            ),
            metric(
                "vireon_connections_active",
                "Currently-active connections.",
                MetricKind::Gauge,
                vec![Sample::unlabeled(42.0)],
            ),
        ];
        let out = encode_prometheus(&metrics);
        let expected = "# HELP vireon_pubsub_messages_published_total Total messages published.\n\
                        # TYPE vireon_pubsub_messages_published_total counter\n\
                        vireon_pubsub_messages_published_total{topic=\"sensor.temp\"} 1234\n\
                        vireon_pubsub_messages_published_total{topic=\"sensor.humidity\"} 5678\n\
                        # HELP vireon_connections_active Currently-active connections.\n\
                        # TYPE vireon_connections_active gauge\n\
                        vireon_connections_active 42\n";
        assert_eq!(out, expected);
    }

    // ── Histogram type line ──────────────────────────────────────

    #[test]
    fn histogram_emits_correct_type() {
        let m = metric(
            "latency_seconds",
            "RPC latency",
            MetricKind::Histogram,
            vec![Sample::unlabeled(0.5)],
        );
        let out = encode_prometheus(&[m]);
        assert!(out.contains("# TYPE latency_seconds histogram"));
    }

    // ── Integer formatting ───────────────────────────────────────

    #[test]
    fn large_integer_value() {
        let m = metric(
            "big_total",
            "x",
            MetricKind::Counter,
            vec![Sample::unlabeled(1_000_000.0)],
        );
        let out = encode_prometheus(&[m]);
        // Rust formats f64 1_000_000.0 as "1000000" (no scientific
        // notation for this magnitude).
        assert!(out.contains("big_total 1000000\n"));
    }
}
