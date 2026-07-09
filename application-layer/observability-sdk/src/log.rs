//! [`LogLevel`], [`LogRecord`] — structured logging.
//!
//! See `DESIGN.md` Part 7 (logging).
//!
//! # Design
//!
//! - `LogLevel`: Severity levels (Error > Warn > Info > Debug > Trace).
//! - `LogRecord`: A single log entry with level, message hash, trace context,
//!   and timestamp.

use crate::trace::{SpanId, TraceId};

// ── LogLevel ───────────────────────────────────────────────────

/// Severity level for log records.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum LogLevel {
    /// Error: critical failures requiring immediate attention.
    Error = 0,
    /// Warning: potential issues that should be investigated.
    Warn = 1,
    /// Info: normal informational messages.
    #[default]
    Info = 2,
    /// Debug: detailed information for debugging.
    Debug = 3,
    /// Trace: very detailed tracing information.
    Trace = 4,
}

impl LogLevel {
    /// Human-readable name for the log level.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Error => "ERROR",
            Self::Warn => "WARN",
            Self::Info => "INFO",
            Self::Debug => "DEBUG",
            Self::Trace => "TRACE",
        }
    }

    /// Severity as a u8 (lower = more severe).
    #[must_use]
    pub const fn as_severity_u8(&self) -> u8 {
        *self as u8
    }

    /// Whether a log at this level should be emitted given a threshold.
    ///
    /// Returns true if this level is at least as severe as the threshold.
    #[must_use]
    pub fn should_log(&self, threshold: LogLevel) -> bool {
        self.as_severity_u8() <= threshold.as_severity_u8()
    }
}

// ── LogRecord ──────────────────────────────────────────────────

/// A single log record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogRecord {
    /// Severity level.
    pub level: LogLevel,
    /// Hash of the log message (for deduplication/aggregation).
    pub message_hash: u64,
    /// Associated trace ID (nil if no trace context).
    pub trace_id: TraceId,
    /// Associated span ID (nil if no span context).
    pub span_id: SpanId,
    /// Timestamp in nanoseconds since Unix epoch.
    pub timestamp_ns: u64,
}

impl LogRecord {
    /// Create a new log record.
    #[must_use]
    pub const fn new(
        level: LogLevel,
        message_hash: u64,
        trace_id: TraceId,
        span_id: SpanId,
        timestamp_ns: u64,
    ) -> Self {
        Self {
            level,
            message_hash,
            trace_id,
            span_id,
            timestamp_ns,
        }
    }

    /// Create a log record without trace context.
    #[must_use]
    pub const fn without_trace(level: LogLevel, message_hash: u64, timestamp_ns: u64) -> Self {
        Self {
            level,
            message_hash,
            trace_id: TraceId::nil(),
            span_id: SpanId::nil(),
            timestamp_ns,
        }
    }

    /// Whether this record has trace context.
    #[must_use]
    pub const fn has_trace_context(&self) -> bool {
        !self.trace_id.is_nil()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_level_names() {
        assert_eq!(LogLevel::Error.as_str(), "ERROR");
        assert_eq!(LogLevel::Warn.as_str(), "WARN");
        assert_eq!(LogLevel::Info.as_str(), "INFO");
        assert_eq!(LogLevel::Debug.as_str(), "DEBUG");
        assert_eq!(LogLevel::Trace.as_str(), "TRACE");
    }

    #[test]
    fn log_level_severity() {
        assert_eq!(LogLevel::Error.as_severity_u8(), 0);
        assert_eq!(LogLevel::Warn.as_severity_u8(), 1);
        assert_eq!(LogLevel::Info.as_severity_u8(), 2);
        assert_eq!(LogLevel::Debug.as_severity_u8(), 3);
        assert_eq!(LogLevel::Trace.as_severity_u8(), 4);
    }

    #[test]
    fn should_log_with_threshold() {
        // Error should log at all thresholds.
        assert!(LogLevel::Error.should_log(LogLevel::Error));
        assert!(LogLevel::Error.should_log(LogLevel::Info));
        assert!(LogLevel::Error.should_log(LogLevel::Trace));

        // Info should not log at Error/Warn thresholds.
        assert!(!LogLevel::Info.should_log(LogLevel::Error));
        assert!(!LogLevel::Info.should_log(LogLevel::Warn));
        assert!(LogLevel::Info.should_log(LogLevel::Info));
        assert!(LogLevel::Info.should_log(LogLevel::Debug));
    }

    #[test]
    fn log_record_creation() {
        let rec = LogRecord::new(
            LogLevel::Warn,
            12345,
            TraceId::new(999),
            SpanId::new(777),
            1_600_000_000_000_000_000,
        );
        assert_eq!(rec.level, LogLevel::Warn);
        assert_eq!(rec.message_hash, 12345);
        assert_eq!(rec.trace_id, TraceId::new(999));
        assert_eq!(rec.span_id, SpanId::new(777));
    }

    #[test]
    fn log_record_without_trace() {
        let rec = LogRecord::without_trace(LogLevel::Info, 42, 1000);
        assert!(rec.trace_id.is_nil());
        assert!(rec.span_id.is_nil());
        assert!(!rec.has_trace_context());
    }

    #[test]
    fn log_record_with_trace_context() {
        let rec = LogRecord::new(LogLevel::Error, 1, TraceId::new(100), SpanId::new(50), 0);
        assert!(rec.has_trace_context());
    }

    #[test]
    fn log_level_default() {
        assert_eq!(LogLevel::default(), LogLevel::Info);
    }
}
