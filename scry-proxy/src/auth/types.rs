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
#[derive(Debug, Clone)]
pub struct UserCredentials {
    /// Username
    pub username: String,
    /// Password (plain, MD5 hash, or SCRAM-SHA-256)
    pub password: PasswordEntry,
}

/// Password entry type (how it's stored in auth_file)
#[derive(Debug, Clone)]
pub enum PasswordEntry {
    /// Plain text password
    Plain(String),
    /// MD5 hash (32 hex chars, stored as "md5" + hash)
    Md5(String),
    /// SCRAM-SHA-256 verifier
    ScramSha256(String),
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
