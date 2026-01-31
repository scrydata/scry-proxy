//! File-based authenticator using PgBouncer userlist.txt format
//!
//! Format: "username" "password"
//! - One entry per line
//! - Lines starting with ; or # are comments
//! - Password can be:
//!   - Plain text: "user" "password123"
//!   - MD5 hash: "user" "md5<32 hex chars>"
//!   - SCRAM-SHA-256: "user" "SCRAM-SHA-256$<iterations>:<salt>$<stored_key>:<server_key>"

use super::types::{AuthError, PasswordEntry, UserCredentials};
use std::collections::HashMap;
use std::path::Path;

/// File-based authenticator
///
/// Loads credentials from a PgBouncer-compatible userlist.txt file
pub struct FileAuthenticator {
    users: HashMap<String, PasswordEntry>,
}

impl FileAuthenticator {
    /// Create a new empty authenticator
    pub fn new() -> Self {
        Self { users: HashMap::new() }
    }

    /// Load credentials from a file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, AuthError> {
        let content = std::fs::read_to_string(path.as_ref()).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                AuthError::AuthFileNotFound(path.as_ref().display().to_string())
            } else {
                AuthError::AuthFileReadError(e)
            }
        })?;
        Self::from_string(&content)
    }

    /// Load credentials from a string (for testing)
    pub fn from_string(content: &str) -> Result<Self, AuthError> {
        let mut users = HashMap::new();

        for (line_num, line) in content.lines().enumerate() {
            let line = line.trim();

            // Skip empty lines and comments
            if line.is_empty() || line.starts_with(';') || line.starts_with('#') {
                continue;
            }

            // Parse: "username" "password"
            if let Some((user, pass)) = Self::parse_line(line) {
                let entry = if pass.starts_with("md5") && pass.len() == 35 {
                    PasswordEntry::Md5(pass[3..].to_string())
                } else if pass.starts_with("SCRAM-SHA-256$") {
                    PasswordEntry::ScramSha256(pass.to_string())
                } else {
                    PasswordEntry::Plain(pass)
                };
                users.insert(user, entry);
            } else {
                return Err(AuthError::InvalidFormat(format!(
                    "Invalid format at line {}: {}",
                    line_num + 1,
                    line
                )));
            }
        }

        Ok(Self { users })
    }

    /// Parse a single line from the auth file
    /// Format: "username" "password"
    fn parse_line(line: &str) -> Option<(String, String)> {
        let mut in_quotes = false;
        let mut parts = Vec::new();
        let mut current = String::new();

        for ch in line.chars() {
            match ch {
                '"' => {
                    if in_quotes {
                        // End of quoted string
                        parts.push(current.clone());
                        current.clear();
                    }
                    in_quotes = !in_quotes;
                }
                _ if in_quotes => {
                    current.push(ch);
                }
                _ => {
                    // Outside quotes, skip whitespace
                }
            }
        }

        if parts.len() >= 2 {
            Some((parts[0].clone(), parts[1].clone()))
        } else {
            None
        }
    }

    /// Check if a username exists
    pub fn has_user(&self, username: &str) -> bool {
        self.users.contains_key(username)
    }

    /// Get credentials for a user
    pub fn get_user(&self, username: &str) -> Option<UserCredentials> {
        self.users.get(username).map(|entry| UserCredentials {
            username: username.to_string(),
            password: entry.clone(),
        })
    }

    /// Verify a plain text password against stored credentials
    pub fn check_password(&self, username: &str, password: &str) -> bool {
        match self.users.get(username) {
            Some(PasswordEntry::Plain(stored)) => stored == password,
            Some(PasswordEntry::Md5(stored_hash)) => {
                // MD5 in PgBouncer format: md5(password + username)
                let computed = Self::compute_md5_password(password, username);
                &computed == stored_hash
            }
            Some(PasswordEntry::ScramSha256(_)) => {
                // SCRAM-SHA-256 requires full SASL exchange, not simple comparison
                false
            }
            None => false,
        }
    }

    /// Compute MD5 password hash (md5(password + username))
    fn compute_md5_password(password: &str, username: &str) -> String {
        let input = format!("{}{}", password, username);
        let digest = md5::compute(input.as_bytes());
        format!("{:x}", digest)
    }

    /// Get the plain text password for a user (for backend forwarding)
    pub fn get_password(&self, username: &str) -> Option<&str> {
        match self.users.get(username) {
            Some(PasswordEntry::Plain(p)) => Some(p),
            _ => None,
        }
    }

    /// Number of users loaded
    pub fn len(&self) -> usize {
        self.users.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.users.is_empty()
    }
}

impl Default for FileAuthenticator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_userlist_plain() {
        let content = r#"
"postgres" "password123"
"admin" "adminpass"
"#;
        let auth = FileAuthenticator::from_string(content).unwrap();

        assert!(auth.check_password("postgres", "password123"));
        assert!(!auth.check_password("postgres", "wrong"));
        assert!(auth.check_password("admin", "adminpass"));
        assert!(!auth.check_password("nonexistent", "password"));
    }

    #[test]
    fn test_parse_userlist_with_comments() {
        let content = r#"
; This is a comment
# This is also a comment
"user1" "pass1"
  ; Indented comment
"user2" "pass2"
"#;
        let auth = FileAuthenticator::from_string(content).unwrap();

        assert_eq!(auth.len(), 2);
        assert!(auth.check_password("user1", "pass1"));
        assert!(auth.check_password("user2", "pass2"));
    }

    #[test]
    fn test_parse_userlist_md5() {
        // MD5 format: "user" "md5<32 hex chars>"
        // MD5 = md5(password + username)
        // For user "postgres" with password "password":
        // md5("passwordpostgres") = "32e12f215ba27cb750c9e093ce4b5127"
        let content = r#"
"postgres" "md532e12f215ba27cb750c9e093ce4b5127"
"#;
        let auth = FileAuthenticator::from_string(content).unwrap();

        assert!(auth.check_password("postgres", "password"));
        assert!(!auth.check_password("postgres", "wrongpassword"));
    }

    #[test]
    fn test_empty_file() {
        let auth = FileAuthenticator::from_string("").unwrap();
        assert!(auth.is_empty());
        assert!(!auth.has_user("anyone"));
    }

    #[test]
    fn test_has_user() {
        let content = r#""testuser" "testpass""#;
        let auth = FileAuthenticator::from_string(content).unwrap();

        assert!(auth.has_user("testuser"));
        assert!(!auth.has_user("nobody"));
    }

    #[test]
    fn test_get_user() {
        let content = r#""testuser" "testpass""#;
        let auth = FileAuthenticator::from_string(content).unwrap();

        let creds = auth.get_user("testuser").unwrap();
        assert_eq!(creds.username, "testuser");
        assert!(creds.password.is_plain());
        assert_eq!(creds.password.as_plain(), Some("testpass"));

        assert!(auth.get_user("nobody").is_none());
    }

    #[test]
    fn test_get_password() {
        let content = r#""testuser" "testpass""#;
        let auth = FileAuthenticator::from_string(content).unwrap();

        assert_eq!(auth.get_password("testuser"), Some("testpass"));
        assert!(auth.get_password("nobody").is_none());
    }

    #[test]
    fn test_invalid_format() {
        let content = r#"invalid line without quotes"#;
        let result = FileAuthenticator::from_string(content);
        assert!(result.is_err());
    }
}
