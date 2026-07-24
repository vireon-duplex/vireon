//! Name / label-key validation + Prometheus text-format escaping.
//!
//! Rules (from the [Prometheus exposition format][1]):
//! - **Metric name**: `[a-zA-Z_:][a-zA-Z0-9_:]*`
//! - **Label key**:   `[a-zA-Z_][a-zA-Z0-9_]*` (no `:`)
//! - **Help**:        `\` → `\\`; newline → `\n`
//! - **Label value**: `\` → `\\`; `"` → `\"`; newline → `\n`
//!
//! [1]: https://prometheus.io/docs/instrumenting/exposition_formats/

use crate::error::ExporterError;

/// Max bytes for a metric name or label key. Generous cap that catches
/// accidental mis-use without rejecting legitimate names.
pub const MAX_NAME_LEN: usize = 255;

/// Validate a Prometheus metric name: `[a-zA-Z_:][a-zA-Z0-9_:]*`.
///
/// # Errors
/// Returns [`ExporterError::EmptyName`] if empty, [`ExporterError::OverlongName`]
/// if longer than [`MAX_NAME_LEN`], or [`ExporterError::InvalidChar`] on the
/// first offending character.
pub fn validate_metric_name(name: &str) -> Result<(), ExporterError> {
    validate_with(name, true)
}

/// Validate a Prometheus label key: `[a-zA-Z_][a-zA-Z0-9_]*` (no `:`).
///
/// # Errors
/// Same error set as [`validate_metric_name`]; `:` is rejected as invalid.
pub fn validate_label_key(key: &str) -> Result<(), ExporterError> {
    validate_with(key, false)
}

fn validate_with(s: &str, allow_colon: bool) -> Result<(), ExporterError> {
    if s.is_empty() {
        return Err(ExporterError::EmptyName);
    }
    if s.len() > MAX_NAME_LEN {
        return Err(ExporterError::OverlongName(MAX_NAME_LEN, s.to_owned()));
    }
    for (i, c) in s.chars().enumerate() {
        let ok = if i == 0 {
            c.is_ascii_alphabetic() || c == '_' || (allow_colon && c == ':')
        } else {
            c.is_ascii_alphanumeric() || c == '_' || (allow_colon && c == ':')
        };
        if !ok {
            return Err(ExporterError::InvalidChar(c, s.to_owned()));
        }
    }
    Ok(())
}

/// Escape the HELP line for Prometheus text format:
/// `\` → `\\`; newline → `\n`.
#[must_use]
pub fn escape_help(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}

/// Escape a label value for Prometheus text format:
/// `\` → `\\`; `"` → `\"`; newline → `\n`.
#[must_use]
pub fn escape_label_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    // ── Name validation ───────────────────────────────────────────

    #[test]
    fn valid_metric_names() {
        for ok in &[
            "a", "_a", ":a", "abc", "abc_def", "abc:def", "a:b:c", "_", ":", "A_B",
        ] {
            assert!(validate_metric_name(ok).is_ok(), "should accept {ok:?}");
        }
    }

    #[test]
    fn invalid_metric_names() {
        // Empty.
        assert_eq!(validate_metric_name(""), Err(ExporterError::EmptyName));
        // Leading digit.
        assert!(matches!(
            validate_metric_name("1abc"),
            Err(ExporterError::InvalidChar('1', _))
        ));
        // Dash.
        assert!(matches!(
            validate_metric_name("my-metric"),
            Err(ExporterError::InvalidChar('-', _))
        ));
        // Space.
        assert!(matches!(
            validate_metric_name("with space"),
            Err(ExporterError::InvalidChar(' ', _))
        ));
    }

    #[test]
    fn overlong_name_rejected() {
        let long = "a".repeat(MAX_NAME_LEN + 1);
        assert!(matches!(
            validate_metric_name(&long),
            Err(ExporterError::OverlongName(_, _))
        ));
    }

    #[test]
    fn label_key_rejects_colon() {
        // Colon is OK for metric names but NOT label keys.
        assert!(matches!(
            validate_label_key("a:b"),
            Err(ExporterError::InvalidChar(':', _))
        ));
        // Otherwise label keys follow the same rules.
        assert!(validate_label_key("valid_key").is_ok());
        assert!(validate_label_key("_underscore_leading").is_ok());
        assert!(validate_label_key("").is_err());
    }

    // ── Help escaping ─────────────────────────────────────────────

    #[test]
    fn escape_help_passes_through_plain() {
        assert_eq!(escape_help("hello world"), "hello world");
    }

    #[test]
    fn escape_help_escapes_backslash_and_newline() {
        assert_eq!(escape_help(r"a\b"), r"a\\b");
        assert_eq!(escape_help("a\nb"), r"a\nb");
        assert_eq!(escape_help("a\\b\nc"), r"a\\b\nc");
    }

    #[test]
    fn escape_help_preserves_quote() {
        // Quotes are legal in HELP (no escaping needed there).
        assert_eq!(escape_help(r#"a"b"#), r#"a"b"#);
    }

    #[test]
    fn escape_help_empty_stays_empty() {
        assert_eq!(escape_help(""), "");
    }

    // ── Label-value escaping ──────────────────────────────────────

    #[test]
    fn escape_label_value_plain() {
        assert_eq!(escape_label_value("hello"), "hello");
    }

    #[test]
    fn escape_label_value_escapes_quote() {
        assert_eq!(escape_label_value(r#"a"b"#), r#"a\"b"#);
    }

    #[test]
    fn escape_label_value_escapes_backslash() {
        assert_eq!(escape_label_value(r"a\b"), r"a\\b");
    }

    #[test]
    fn escape_label_value_escapes_newline() {
        assert_eq!(escape_label_value("a\nb"), r"a\nb");
    }

    #[test]
    fn escape_label_value_all_three() {
        assert_eq!(
            escape_label_value(
                r#"a"b\c
d"#
            ),
            r#"a\"b\\c\nd"#
        );
    }

    #[test]
    fn escape_label_value_empty() {
        assert_eq!(escape_label_value(""), "");
    }
}
