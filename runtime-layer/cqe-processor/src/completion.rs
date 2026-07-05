//! Completion descriptor (Layer 4).
//!
//! [`CompletionDescriptor`] is the owned form of a decoded CQE. We keep it
//! in a pre-allocated scratch across the dispatch call so handlers can
//! inspect raw fields without re-decoding.

use io::completion::Completion;

/// Owned form of a decoded CQE.
#[derive(Clone, Debug)]
pub struct CompletionDescriptor {
    /// Decoded variant (the typed view).
    pub completion: Completion,
    /// Raw `user_data` (high bits = OpKind).
    pub user_data: u64,
    /// Raw `res` field (signed).
    pub res: i32,
    /// Raw `flags` field.
    pub flags: u32,
}

impl CompletionDescriptor {
    /// Construct from raw CQE fields plus the decoded variant.
    #[inline]
    #[must_use]
    pub const fn new(completion: Completion, user_data: u64, res: i32, flags: u32) -> Self {
        Self {
            completion,
            user_data,
            res,
            flags,
        }
    }
}
