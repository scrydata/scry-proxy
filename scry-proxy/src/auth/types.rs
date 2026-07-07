//! Authentication types

use thiserror::Error;

/// Authentication error types
#[derive(Error, Debug)]
pub enum AuthError {
    #[error("Authentication failed: invalid username or password")]
    InvalidCredentials,

    #[error("User not found: {0}")]
    UserNotFound(String),

    #[error("Auth file not found: {0}")]
    AuthFileNotFound(String),

    #[error("Failed to read auth file: {0}")]
    AuthFileReadError(#[source] std::io::Error),

    #[error("Invalid auth file format: {0}")]
    InvalidFormat(String),
}

/// Result of authentication attempt
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthResult {
    /// Authentication succeeded
    Success,
    /// Authentication failed
    Failed,
    /// User requires backend authentication (auth_query)
    RequiresBackendAuth,
}

/// User credentials from auth_file
#[derive(Clone)]
pub struct UserCredentials {
    /// Username
    pub username: String,
    /// Password (plain, MD5 hash, or SCRAM-SHA-256)
    pub password: PasswordEntry,
}

// Manual Debug so credentials never render their secret in logs/panics (P1 §4.7).
impl std::fmt::Debug for UserCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UserCredentials")
            .field("username", &self.username)
            .field("password", &self.password)
            .finish()
    }
}

/// Password entry type (how it's stored in auth_file)
#[derive(Clone)]
pub enum PasswordEntry {
    /// Plain text password
    Plain(String),
    /// MD5 hash (32 hex chars, stored as "md5" + hash)
    Md5(String),
    /// SCRAM-SHA-256 verifier
    ScramSha256(String),
}

// Manual Debug: reveal only the storage kind, never the secret value (P1 §4.7).
impl std::fmt::Debug for PasswordEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match self {
            PasswordEntry::Plain(_) => "Plain",
            PasswordEntry::Md5(_) => "Md5",
            PasswordEntry::ScramSha256(_) => "ScramSha256",
        };
        write!(f, "{kind}(<redacted>)")
    }
}

impl PasswordEntry {
    /// Check if this is a plain text password
    pub fn is_plain(&self) -> bool {
        matches!(self, PasswordEntry::Plain(_))
    }

    /// Get the raw password value (for plain text only)
    pub fn as_plain(&self) -> Option<&str> {
        match self {
            PasswordEntry::Plain(p) => Some(p),
            _ => None,
        }
    }
}

#[cfg(test)]
mod redacting_debug_tests {
    use super::*;

    #[test]
    fn password_entry_debug_hides_value_keeps_kind() {
        let entry = PasswordEntry::Plain("hunter2-secret".to_string());
        let dbg = format!("{entry:?}");
        assert!(!dbg.contains("hunter2-secret"), "leaked password: {dbg}");
        assert!(dbg.contains("Plain"), "should keep the storage kind: {dbg}");
        assert!(dbg.contains("<redacted>"));

        assert_eq!(format!("{:?}", PasswordEntry::Md5("md5deadbeef".into())), "Md5(<redacted>)");
        assert_eq!(
            format!("{:?}", PasswordEntry::ScramSha256("SCRAM$secret".into())),
            "ScramSha256(<redacted>)"
        );
    }

    #[test]
    fn user_credentials_debug_hides_password_keeps_username() {
        let creds = UserCredentials {
            username: "alice".to_string(),
            password: PasswordEntry::Md5("md5-secret-hash".to_string()),
        };
        let dbg = format!("{creds:?}");
        assert!(dbg.contains("alice"), "username should be visible: {dbg}");
        assert!(!dbg.contains("md5-secret-hash"), "leaked password hash: {dbg}");
    }
}
