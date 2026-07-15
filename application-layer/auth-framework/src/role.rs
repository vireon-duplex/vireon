//! [`Role`], [`RoleAssignment`] — role definitions and assignments.

use crate::permission::{Permission, PermissionSet};
use crate::principal::PrincipalId;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Role {
    pub id: u16,
    pub name_hash: u64,
    pub permissions: PermissionSet,
    pub inherits: [u16; 4],
    pub inherit_count: u8,
}

impl Role {
    #[must_use]
    pub const fn new(id: u16, name_hash: u64) -> Self {
        Self {
            id,
            name_hash,
            permissions: PermissionSet::new(),
            inherits: [0; 4],
            inherit_count: 0,
        }
    }

    pub fn add_permission(&mut self, perm: Permission) {
        let _ = self.permissions.add(perm);
    }

    pub fn add_inherited(&mut self, role_id: u16) {
        if self.inherit_count < 4 {
            self.inherits[self.inherit_count as usize] = role_id;
            self.inherit_count += 1;
        }
    }

    #[must_use]
    pub const fn has_permission(&self, perm: Permission) -> bool {
        self.permissions.contains(perm)
    }

    /// Computes the effective permissions including inherited roles.
    ///
    /// Traverses the inheritance chain (up to 4 levels deep) and
    /// merges permissions from all reachable roles. Cycles are handled
    /// by the limited inheritance depth.
    #[must_use]
    pub fn effective_permissions(&self, roles: &[Role]) -> PermissionSet {
        let mut result = self.permissions;
        self.collect_inherited_permissions(&mut result, roles, 0);
        result
    }

    fn collect_inherited_permissions(&self, result: &mut PermissionSet, roles: &[Role], depth: u8) {
        if depth >= 4 {
            return;
        }
        for i in 0..self.inherit_count as usize {
            let inherited_id = self.inherits[i];
            // Find the inherited role in the slice.
            if let Some(inherited) = roles.iter().find(|r| r.id == inherited_id) {
                for &perm in inherited.permissions.as_slice() {
                    let _ = result.add(perm);
                }
                inherited.collect_inherited_permissions(result, roles, depth + 1);
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RoleAssignment {
    pub principal: PrincipalId,
    pub role_id: u16,
    pub assigned_ns: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_new() {
        let role = Role::new(1, 100);
        assert_eq!(role.id, 1);
        assert!(role.permissions.is_empty());
    }

    #[test]
    fn role_add_permission() {
        let mut role = Role::new(1, 100);
        let perm = Permission::new(1, 2);
        role.add_permission(perm);
        assert!(role.has_permission(perm));
    }

    #[test]
    fn role_add_inherited() {
        let mut role = Role::new(1, 100);
        role.add_inherited(10);
        role.add_inherited(20);
        assert_eq!(role.inherit_count, 2);
    }
}
