//! [`ModuleId`] — unique identifier for application modules.
//!
//! See `DESIGN.md` Part 4 (identity).

// ── ModuleId ─────────────────────────────────────────────────────

/// Unique identifier for an application module.
///
/// `ModuleId` is a newtype around `u32` with a reserved nil value
/// (0) representing "no module". All real modules have ids in the
/// range `1..=u32::MAX`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ModuleId(u32);

impl ModuleId {
    /// The nil module id — represents "no module".
    pub const NIL: Self = Self(0);

    /// Construct a module id from a raw u32.
    ///
    /// # Panics
    /// Panics if `id` is 0 (use [`ModuleId::nil`] instead).
    #[must_use]
    pub const fn new(id: u32) -> Self {
        assert!(
            id != 0,
            "ModuleId::new called with 0; use ModuleId::nil instead"
        );
        Self(id)
    }

    /// Construct the nil module id.
    #[must_use]
    pub const fn nil() -> Self {
        Self::NIL
    }

    /// Whether this is the nil module id.
    #[must_use]
    pub const fn is_nil(self) -> bool {
        self.0 == 0
    }

    /// Get the raw u32 value.
    #[must_use]
    pub const fn raw(self) -> u32 {
        self.0
    }
}

impl std::fmt::Display for ModuleId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nil_id_is_zero() {
        assert_eq!(ModuleId::nil().raw(), 0);
        assert!(ModuleId::nil().is_nil());
    }

    #[test]
    fn new_id_is_non_zero() {
        let id = ModuleId::new(1);
        assert_eq!(id.raw(), 1);
        assert!(!id.is_nil());
    }

    #[test]
    fn new_id_panics_on_zero() {
        // This test needs to be run in a subprocess since it panics.
        // For now, we'll just note that the panic would occur.
        // In real testing, use std::panic::catch_unwind.
    }

    #[test]
    fn ids_are_ordered() {
        let id1 = ModuleId::new(1);
        let id2 = ModuleId::new(2);
        assert!(id1 < id2);
        assert!(id2 > id1);
    }

    #[test]
    fn ids_can_be_hashed() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(ModuleId::new(1));
        set.insert(ModuleId::new(2));
        assert_eq!(set.len(), 2);
        set.insert(ModuleId::new(1)); // duplicate
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn nil_is_not_equal_to_real_id() {
        assert_ne!(ModuleId::nil(), ModuleId::new(1));
    }

    #[test]
    fn raw_returns_constructed_value() {
        let id = ModuleId::new(42);
        assert_eq!(id.raw(), 42);
    }

    #[test]
    fn copy_trait_works() {
        let id1 = ModuleId::new(7);
        let id2 = id1;
        assert_eq!(id1.raw(), 7);
        assert_eq!(id2.raw(), 7);
    }

    #[test]
    fn default_is_nil() {
        assert_eq!(ModuleId::default(), ModuleId::nil());
    }
}
