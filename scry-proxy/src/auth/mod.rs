//! Authentication module for Scry proxy
//!
//! Provides authentication mechanisms compatible with PgBouncer:
//! - File-based authentication (userlist.txt format)
//! - Trust authentication (no auth required)
//! - MD5 password authentication
//! - SCRAM-SHA-256 authentication
//! - Certificate-based authentication

mod authenticator;
mod backend;
mod file_auth;
mod scram;
mod types;

pub use authenticator::{AuthHandshakeResult, Authenticator};
pub use backend::BackendAuthenticator;
pub use file_auth::FileAuthenticator;
pub use scram::{ScramClient, ScramError};
pub use types::{AuthError, AuthResult, UserCredentials};
