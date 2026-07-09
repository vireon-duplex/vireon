//! [`Permission`], [`PermissionSet`] — permission definitions and sets.

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Permission {
    pub resource_hash: u64,
    pub action_hash: u64,
}

impl Permission {
    #[must_use]
    pub const fn new(resource_hash: u64, action_hash: u64) -> Self {
        Self {
            resource_hash,
            action_hash,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PermissionSet {
    permissions: [Permission; 16],
    count: u8,
}

impl PermissionSet {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            permissions: [Permission {
                resource_hash: 0,
                action_hash: 0,
            }; 16],
            count: 0,
        }
    }

    pub fn add(&mut self, perm: Permission) -> Result<(), &'static str> {
        if self.count >= 16 {
            return Err("permission set full");
        }
        self.permissions[self.count as usize] = perm;
        self.count += 1;
        Ok(())
    }

    #[must_use]
    pub const fn contains(&self, perm: Permission) -> bool {
        let mut i = 0;
        while i < self.count as usize {
            if self.permissions[i].resource_hash == perm.resource_hash
                && self.permissions[i].action_hash == perm.action_hash
            {
                return true;
            }
            i += 1;
        }
        false
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.count as usize
    }
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }
}

impl Default for PermissionSet {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permission_set_add_contains() {
        let mut set = PermissionSet::new();
        let perm = Permission::new(1, 2);
        assert!(set.add(perm).is_ok());
        assert!(set.contains(perm));
    }

    #[test]
    fn permission_set_full() {
        let mut set = PermissionSet::new();
        for i in 0..17 {
            let perm = Permission::new(i, i);
            if i < 16 {
                assert!(set.add(perm).is_ok());
            } else {
                assert!(set.add(perm).is_err());
            }
        }
    }
}
