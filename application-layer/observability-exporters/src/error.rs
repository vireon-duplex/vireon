//! Errors returned by the encoder / metric constructors.

use thiserror::Error;

/// Errors that arise while constructing or validating metrics.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ExporterError {
    /// Name / label key was empty.
    #[error("name or label key is empty")]
    EmptyName,

    /// Name / label key exceeded the 255-byte cap.
    #[error("name or label key exceeds {0} bytes: {1}")]
    OverlongName(usize, String),

    /// Name / label key contained a character outside the legal set.
    /// The offending byte and the offending string are returned for
    /// diagnostics.
    #[error("invalid character {0:?} in {1:?}")]
    InvalidChar(char, String),
}
