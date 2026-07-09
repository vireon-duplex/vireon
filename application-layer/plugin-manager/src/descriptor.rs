//! [`PluginDescriptor`] — metadata for a loaded plugin.
//!
//! See `DESIGN.md` Part 3 (plugin model) and Part 7 (capability flags).
//!
//! # Capability Flags
//!
//! Plugins declare their capabilities as a bitmask of feature flags:
//!
//! | Flag | Value | Meaning |
//! |------|-------|---------|
//! | `CAP_ROUTING` | 0x01 | Packet routing / forwarding |
//! | `CAP_AUTH` | 0x02 | Authentication / authorization |
//! | `CAP_METRICS` | 0x04 | Metrics collection / export |
//! | `CAP_LOGGING` | 0x08 | Structured logging |
//! | `CAP_SCHEDULING` | 0x10 | Task scheduling / timers |

use crate::api_version::ApiVersion;
use crate::id::{PluginId, PluginVersion};

// ── Capability Constants ───────────────────────────────────────────

/// Routing capability: packet forwarding.
pub const CAP_ROUTING: u32 = 0x01;

/// Authentication capability: auth/z logic.
pub const CAP_AUTH: u32 = 0x02;

/// Metrics capability: collection and export.
pub const CAP_METRICS: u32 = 0x04;

/// Logging capability: structured logging.
pub const CAP_LOGGING: u32 = 0x08;

/// Scheduling capability: task scheduling.
pub const CAP_SCHEDULING: u32 = 0x10;

// ── PluginDescriptor ───────────────────────────────────────────────

/// Metadata descriptor for a loaded plugin.
///
/// Encapsulates the plugin's identity, version, API requirements,
/// dependencies, and capability declarations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PluginDescriptor {
    /// Plugin identifier (hash).
    pub id: PluginId,
    /// Plugin semantic version.
    pub version: PluginVersion,
    /// Plugin name (fixed-size buffer with length).
    pub name: [u8; 64],
    pub name_len: u8,
    /// Required API version.
    pub api_version: ApiVersion,
    /// Dependency plugin IDs (fixed-size with count).
    pub dependencies: [PluginId; 8],
    pub dep_count: u8,
    /// Capability bitmask.
    pub capabilities: u32,
}

impl PluginDescriptor {
    /// Create a new descriptor with an ID and version.
    #[must_use]
    pub const fn new(id: PluginId, version: PluginVersion) -> Self {
        Self {
            id,
            version,
            name: [0u8; 64],
            name_len: 0,
            api_version: ApiVersion::new(1, 0),
            dependencies: [PluginId::nil(); 8],
            dep_count: 0,
            capabilities: 0,
        }
    }

    /// Set the plugin name (builder-style).
    #[must_use]
    pub fn with_name(mut self, name: &str) -> Self {
        let bytes = name.as_bytes();
        let len = bytes.len().min(64) as u8;
        self.name_len = len;
        let mut i = 0;
        while (i as usize) < bytes.len() && (i as usize) < 64 {
            self.name[i as usize] = bytes[i as usize];
            i += 1;
        }
        self
    }

    /// Add a dependency (builder-style).
    ///
    /// Silently ignores if the dependency list is full.
    #[must_use]
    pub fn add_dependency(mut self, dep: PluginId) -> Self {
        if self.dep_count < 8 {
            self.dependencies[self.dep_count as usize] = dep;
            self.dep_count += 1;
        }
        self
    }

    /// Check if the plugin has a specific capability.
    #[must_use]
    pub const fn has_capability(&self, flag: u32) -> bool {
        (self.capabilities & flag) != 0
    }

    /// Get the plugin name as a string slice.
    #[must_use]
    pub fn name_str(&self) -> &str {
        // SAFETY: We initialized the name buffer with valid UTF-8
        // (from a &str), and we only read up to name_len bytes.
        let bytes = &self.name[..self.name_len as usize];
        unsafe { std::str::from_utf8_unchecked(bytes) }
    }

    /// Get the list of dependencies.
    #[must_use]
    pub const fn dependencies(&self) -> &[PluginId] {
        // SAFETY: Return a slice of our own dependencies array.
        let ptr = self.dependencies.as_ptr();
        unsafe { std::slice::from_raw_parts(ptr, self.dep_count as usize) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_new_has_no_name() {
        let desc = PluginDescriptor::new(PluginId::nil(), PluginVersion::new(1, 0, 0));
        assert_eq!(desc.name_str(), "");
        assert_eq!(desc.name_len, 0);
    }

    #[test]
    fn descriptor_with_name_sets_name() {
        let desc = PluginDescriptor::new(PluginId::nil(), PluginVersion::new(1, 0, 0))
            .with_name("test-plugin");
        assert_eq!(desc.name_str(), "test-plugin");
        assert_eq!(desc.name_len, 11);
    }

    #[test]
    fn descriptor_with_name_truncates_at_64_bytes() {
        let long_name = "a".repeat(100);
        let desc = PluginDescriptor::new(PluginId::nil(), PluginVersion::new(1, 0, 0))
            .with_name(&long_name);
        assert_eq!(desc.name_len, 64);
        assert_eq!(desc.name_str().len(), 64);
    }

    #[test]
    fn descriptor_add_dependency() {
        let dep = PluginId::from_hash(123);
        let desc =
            PluginDescriptor::new(PluginId::nil(), PluginVersion::new(1, 0, 0)).add_dependency(dep);
        assert_eq!(desc.dep_count, 1);
        assert_eq!(desc.dependencies(), [dep]);
    }

    #[test]
    fn descriptor_add_multiple_dependencies() {
        let dep1 = PluginId::from_hash(1);
        let dep2 = PluginId::from_hash(2);
        let desc = PluginDescriptor::new(PluginId::nil(), PluginVersion::new(1, 0, 0))
            .add_dependency(dep1)
            .add_dependency(dep2);
        assert_eq!(desc.dep_count, 2);
        assert_eq!(desc.dependencies(), [dep1, dep2]);
    }

    #[test]
    fn descriptor_add_dependency_stops_at_8() {
        let mut desc = PluginDescriptor::new(PluginId::nil(), PluginVersion::new(1, 0, 0));
        for i in 0..10u64 {
            desc = desc.add_dependency(PluginId::from_hash(i));
        }
        assert_eq!(desc.dep_count, 8);
        assert_eq!(desc.dependencies().len(), 8);
    }

    #[test]
    fn descriptor_has_capability() {
        let desc = PluginDescriptor::new(PluginId::nil(), PluginVersion::new(1, 0, 0));
        let desc = desc.with_name("test");
        assert!(!desc.has_capability(CAP_ROUTING));

        let desc = PluginDescriptor {
            capabilities: CAP_ROUTING | CAP_AUTH,
            ..desc
        };
        assert!(desc.has_capability(CAP_ROUTING));
        assert!(desc.has_capability(CAP_AUTH));
        assert!(!desc.has_capability(CAP_METRICS));
    }

    #[test]
    fn descriptor_default_api_version() {
        let desc = PluginDescriptor::new(PluginId::nil(), PluginVersion::new(1, 0, 0));
        assert_eq!(desc.api_version, ApiVersion::new(1, 0));
    }

    #[test]
    fn descriptor_equality() {
        let id = PluginId::from_hash(42);
        let version = PluginVersion::new(1, 2, 3);
        let desc1 = PluginDescriptor::new(id, version).with_name("test");
        let desc2 = PluginDescriptor::new(id, version).with_name("test");
        assert_eq!(desc1, desc2);

        let desc3 = PluginDescriptor::new(id, PluginVersion::new(1, 0, 0));
        assert_ne!(desc1, desc3);
    }
}
