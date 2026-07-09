//! [`Credential`], [`CredentialType`] — credential types and validation.

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum CredentialType {
    #[default]
    Password,
    Token,
    Certificate,
    ApiKey,
    OAuth,
}

impl CredentialType {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Password => "password",
            Self::Token => "token",
            Self::Certificate => "certificate",
            Self::ApiKey => "api_key",
            Self::OAuth => "oauth",
        }
    }
    #[must_use]
    pub const fn is_token_based(self) -> bool {
        matches!(self, Self::Token | Self::ApiKey | Self::OAuth)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Credential {
    pub cred_type: CredentialType,
    pub data_hash: u64,
    pub issued_ns: u64,
}

impl Credential {
    #[must_use]
    pub const fn new(cred_type: CredentialType, data_hash: u64, issued_ns: u64) -> Self {
        Self {
            cred_type,
            data_hash,
            issued_ns,
        }
    }
    #[must_use]
    pub const fn type_of(&self) -> CredentialType {
        self.cred_type
    }
    #[must_use]
    pub const fn data_hash(&self) -> u64 {
        self.data_hash
    }
    #[must_use]
    pub const fn is_expired(&self, ttl_ms: u64, now_ns: u64) -> bool {
        let age_ns = now_ns.saturating_sub(self.issued_ns);
        let ttl_ns = ttl_ms * 1_000_000;
        age_ns > ttl_ns
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credential_type_predicates() {
        assert!(CredentialType::Token.is_token_based());
        assert!(!CredentialType::Password.is_token_based());
    }

    #[test]
    fn credential_expiration() {
        let cred = Credential::new(CredentialType::Token, 123, 0);
        assert!(!cred.is_expired(1_000, 500_000_000));
        assert!(cred.is_expired(1_000, 2_000_000_000));
    }
}
