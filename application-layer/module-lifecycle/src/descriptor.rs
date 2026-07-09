//! [`ModuleDescriptor`] — metadata for an application module.
//!
//! See `DESIGN.md` Part 5 (module descriptor).

use crate::id::ModuleId;

// ── Constants ───────────────────────────────────────────────────

/// Maximum length of a module name in bytes.
pub const MAX_NAME_BYTES: usize = 64;

/// Maximum number of dependencies a module can declare.
pub const MAX_DEPENDENCIES: usize = 16;

// ── ModuleDescriptor ─────────────────────────────────────────────

/// Metadata describing an application module.
///
/// Descriptors are registered with the lifecycle coordinator and
/// contain all information needed to manage the module's lifecycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ModuleDescriptor {
    /// Unique module identifier.
    pub id: ModuleId,
    /// Module name as fixed-size byte array.
    name: [u8; MAX_NAME_BYTES],
    /// Actual length of the name in bytes.
    name_len: u8,
    /// Module version.
    pub version: u32,
    /// Dependency module ids.
    dependencies: [ModuleId; MAX_DEPENDENCIES],
    /// Number of dependencies.
    dependency_count: u8,
    /// Initialization priority (0 = lowest, 255 = highest).
    pub priority: u8,
    /// Whether this module is critical (failure triggers shutdown).
    pub is_critical: bool,
}

impl Default for ModuleDescriptor {
    fn default() -> Self {
        Self {
            id: ModuleId::nil(),
            name: [0; MAX_NAME_BYTES],
            name_len: 0,
            version: 0,
            dependencies: [ModuleId::nil(); MAX_DEPENDENCIES],
            dependency_count: 0,
            priority: 128,
            is_critical: false,
        }
    }
}

impl ModuleDescriptor {
    /// Construct a descriptor with the given module id.
    #[must_use]
    pub const fn new(id: ModuleId) -> Self {
        Self {
            id,
            name: [0; MAX_NAME_BYTES],
            name_len: 0,
            version: 0,
            dependencies: [ModuleId::nil(); MAX_DEPENDENCIES],
            dependency_count: 0,
            priority: 128,
            is_critical: false,
        }
    }

    /// Set the module name.
    ///
    /// If `name` is longer than `MAX_NAME_BYTES`, it will be truncated.
    #[must_use]
    pub fn with_name(mut self, name: &str) -> Self {
        let bytes = name.as_bytes();
        let len = bytes.len().min(MAX_NAME_BYTES);
        let mut arr = [0u8; MAX_NAME_BYTES];
        let mut i = 0;
        while i < len {
            arr[i] = bytes[i];
            i += 1;
        }
        self.name = arr;
        self.name_len = len as u8;
        self
    }

    /// Add a dependency on another module.
    ///
    /// If more than `MAX_DEPENDENCIES` are added, extras are silently ignored.
    #[must_use]
    pub fn add_dependency(mut self, dep: ModuleId) -> Self {
        let idx = self.dependency_count as usize;
        if idx < MAX_DEPENDENCIES {
            self.dependencies[idx] = dep;
            self.dependency_count += 1;
        }
        // Exceeding limit is silently ignored (no panic)
        self
    }

    /// Get the number of dependencies.
    #[must_use]
    pub const fn dependency_count(&self) -> usize {
        self.dependency_count as usize
    }

    /// Get the module name as a string slice.
    #[must_use]
    pub fn name_str(&self) -> &str {
        // SAFETY: We only write valid UTF-8 into name and maintain name_len.
        let bytes = &self.name[..self.name_len as usize];
        unsafe { std::str::from_utf8_unchecked(bytes) }
    }

    /// Get the dependency at the given index.
    ///
    /// # Returns
    /// `Some(dependency)` if index is valid, `None` otherwise.
    #[must_use]
    pub const fn dependency(&self, index: usize) -> Option<ModuleId> {
        if index < self.dependency_count as usize {
            Some(self.dependencies[index])
        } else {
            None
        }
    }

    /// Get the priority value.
    #[must_use]
    pub const fn priority(&self) -> u8 {
        self.priority
    }

    /// Set the priority.
    #[must_use]
    pub const fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority;
        self
    }

    /// Set the version.
    #[must_use]
    pub const fn with_version(mut self, version: u32) -> Self {
        self.version = version;
        self
    }

    /// Set whether this module is critical.
    #[must_use]
    pub const fn with_critical(mut self, critical: bool) -> Self {
        self.is_critical = critical;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_descriptor_has_given_id() {
        let desc = ModuleDescriptor::new(ModuleId::new(1));
        assert_eq!(desc.id, ModuleId::new(1));
        assert_eq!(desc.name_str(), "");
        assert_eq!(desc.dependency_count(), 0);
        assert_eq!(desc.version, 0);
        assert_eq!(desc.priority, 128);
        assert!(!desc.is_critical);
    }

    #[test]
    fn with_name_sets_name() {
        let desc = ModuleDescriptor::new(ModuleId::new(1)).with_name("http-server");
        assert_eq!(desc.name_str(), "http-server");
        assert_eq!(desc.name_len, 11);
    }

    #[test]
    fn with_name_truncates_long_name() {
        let long_name = "a".repeat(65);
        let desc = ModuleDescriptor::new(ModuleId::new(1)).with_name(&long_name);
        assert_eq!(desc.name_str(), &long_name[..64]); // Truncated to 64 bytes
        assert_eq!(desc.name_len, 64);
    }

    #[test]
    fn with_name_accepts_max_length() {
        let max_name = "a".repeat(64);
        let desc = ModuleDescriptor::new(ModuleId::new(1)).with_name(&max_name);
        assert_eq!(desc.name_str(), &max_name);
        assert_eq!(desc.name_len, 64);
    }

    #[test]
    fn add_dependency_increases_count() {
        let desc = ModuleDescriptor::new(ModuleId::new(1))
            .add_dependency(ModuleId::new(2))
            .add_dependency(ModuleId::new(3));
        assert_eq!(desc.dependency_count(), 2);
        assert_eq!(desc.dependency(0), Some(ModuleId::new(2)));
        assert_eq!(desc.dependency(1), Some(ModuleId::new(3)));
        assert_eq!(desc.dependency(2), None);
    }

    #[test]
    fn add_dependency_silently_ignores_overflow() {
        let mut desc = ModuleDescriptor::new(ModuleId::new(1));
        for i in 0..16 {
            desc = desc.add_dependency(ModuleId::new(i + 2));
        }
        assert_eq!(desc.dependency_count(), 16);
        // Adding beyond limit is silently ignored
        desc = desc.add_dependency(ModuleId::new(100));
        assert_eq!(desc.dependency_count(), 16); // Still 16
    }

    #[test]
    fn with_priority_sets_priority() {
        let desc = ModuleDescriptor::new(ModuleId::new(1)).with_priority(200);
        assert_eq!(desc.priority(), 200);
    }

    #[test]
    fn with_version_sets_version() {
        let desc = ModuleDescriptor::new(ModuleId::new(1)).with_version(42);
        assert_eq!(desc.version, 42);
    }

    #[test]
    fn with_critical_sets_flag() {
        let desc = ModuleDescriptor::new(ModuleId::new(1)).with_critical(true);
        assert!(desc.is_critical);
    }

    #[test]
    fn descriptor_is_copy() {
        let desc1 = ModuleDescriptor::new(ModuleId::new(1)).with_name("test");
        let desc2 = desc1;
        assert_eq!(desc1.name_str(), "test");
        assert_eq!(desc2.name_str(), "test");
    }

    #[test]
    fn default_is_nil_descriptor() {
        let desc = ModuleDescriptor::default();
        assert!(desc.id.is_nil());
        assert_eq!(desc.name_str(), "");
        assert_eq!(desc.dependency_count(), 0);
    }

    #[test]
    fn builder_pattern_works() {
        let desc = ModuleDescriptor::new(ModuleId::new(1))
            .with_name("database")
            .with_version(2)
            .with_priority(100)
            .with_critical(true)
            .add_dependency(ModuleId::new(2))
            .add_dependency(ModuleId::new(3));

        assert_eq!(desc.id, ModuleId::new(1));
        assert_eq!(desc.name_str(), "database");
        assert_eq!(desc.version, 2);
        assert_eq!(desc.priority, 100);
        assert!(desc.is_critical);
        assert_eq!(desc.dependency_count(), 2);
    }
}
