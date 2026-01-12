//! Authentication module for Scry proxy
//!
//! Provides authentication mechanisms compatible with PgBouncer:
//! - File-based authentication (userlist.txt format)
//! - Trust authentication (no auth required)
//! - MD5 password authentication
//! - SCRAM-SHA-256 authentication
//! - Certificate-based authentication

mod authenticator;
mod file_auth;
mod types;

pub use authenticator::{AuthHandshakeResult, Authenticator};
pub use file_auth::FileAuthenticator;
pub use types::{AuthError, AuthResult, UserCredentials};
