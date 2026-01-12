//! Client authentication handler for the proxy
//!
//! This module implements the PostgreSQL authentication protocol flow:
//! 1. Parse StartupMessage from client to get username
//! 2. Send authentication challenge (MD5 or cleartext)
//! 3. Receive password from client
//! 4. Verify against FileAuthenticator
//! 5. Return credentials for backend forwarding

use super::FileAuthenticator;
use crate::config::{AuthType, Config};
use crate::protocol::{
    build_auth_md5_password, build_auth_ok, build_error_response, parse_password_message,
    verify_md5_response, StartupMessage,
};
use crate::tls::ClientTransport;
use anyhow::{Context, Result};
use rand::Rng;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, info, warn};

/// Result of the authentication handshake
#[derive(Debug)]
pub struct AuthHandshakeResult {
    /// The parsed startup message from the client
    pub startup: StartupMessage,
    /// The username that was authenticated (or requested in trust mode)
    pub username: String,
    /// The database requested by the client
    pub database: Option<String>,
    /// The startup message to forward to the backend (may be modified)
    pub startup_bytes: Vec<u8>,
}

/// Handles client authentication for incoming connections
pub struct Authenticator {
    config: Arc<Config>,
    file_auth: Option<Arc<FileAuthenticator>>,
}

impl Authenticator {
    /// Create a new Authenticator
    pub fn new(config: Arc<Config>, file_auth: Option<Arc<FileAuthenticator>>) -> Self {
        Self { config, file_auth }
    }

    /// Perform the authentication handshake with a client
    ///
    /// This handles:
    /// 1. Reading/parsing the StartupMessage
    /// 2. Authenticating the client (if auth enabled)
    /// 3. Preparing the startup message to forward to the backend
    ///
    /// Returns the authenticated username and the startup bytes for the backend.
    pub async fn authenticate(
        &self,
        client: &mut ClientTransport,
        startup_data: &[u8],
    ) -> Result<AuthHandshakeResult> {
        // Parse or read startup message
        let (startup, startup_bytes) = if startup_data.is_empty() {
            // TLS case: need to read startup from client
            debug!("Reading startup message from TLS client");
            self.read_startup(client).await?
        } else {
            // Plain TCP case: startup was buffered during SSL handshake
            let startup = StartupMessage::parse(startup_data)
                .context("Failed to parse startup message")?;
            (startup, startup_data.to_vec())
        };

        let username = startup.user()
            .ok_or_else(|| anyhow::anyhow!("Startup message missing user parameter"))?
            .to_string();
        let database = startup.database().map(|s| s.to_string());

        info!(
            username = %username,
            database = ?database,
            "Client startup message parsed"
        );

        // Determine if we need to authenticate
        match self.config.auth.auth_type {
            AuthType::Trust => {
                // Trust mode: no authentication required
                debug!("Trust mode: skipping authentication");

                // Build startup message for backend with backend credentials
                let backend_startup = self.build_backend_startup(&startup);

                Ok(AuthHandshakeResult {
                    startup,
                    username,
                    database,
                    startup_bytes: backend_startup,
                })
            }

            AuthType::Md5 => {
                // MD5 authentication
                self.authenticate_md5(client, &startup, &username, database, startup_bytes).await
            }

            AuthType::ScramSha256 => {
                // SCRAM-SHA-256 not yet implemented
                warn!("SCRAM-SHA-256 authentication not yet implemented, falling back to trust");
                let backend_startup = self.build_backend_startup(&startup);
                Ok(AuthHandshakeResult {
                    startup,
                    username,
                    database,
                    startup_bytes: backend_startup,
                })
            }

            AuthType::Cert => {
                // Certificate auth handled at TLS layer
                debug!("Certificate authentication (validated at TLS layer)");
                let backend_startup = self.build_backend_startup(&startup);
                Ok(AuthHandshakeResult {
                    startup,
                    username,
                    database,
                    startup_bytes: backend_startup,
                })
            }
        }
    }

    /// Read startup message from client
    async fn read_startup(&self, client: &mut ClientTransport) -> Result<(StartupMessage, Vec<u8>)> {
        let mut buf = vec![0u8; 8192];
        let n = client.read(&mut buf).await.context("Failed to read startup message")?;

        if n < 8 {
            anyhow::bail!("Startup message too short: {} bytes", n);
        }

        buf.truncate(n);

        let startup = StartupMessage::parse(&buf)
            .context("Failed to parse startup message")?;

        Ok((startup, buf))
    }

    /// Perform MD5 authentication
    async fn authenticate_md5(
        &self,
        client: &mut ClientTransport,
        startup: &StartupMessage,
        username: &str,
        database: Option<String>,
        _original_startup: Vec<u8>,
    ) -> Result<AuthHandshakeResult> {
        let file_auth = self.file_auth.as_ref()
            .ok_or_else(|| anyhow::anyhow!("MD5 auth enabled but no auth_file configured"))?;

        // Check if user exists in auth file
        if !file_auth.has_user(username) {
            warn!(username = %username, "User not found in auth file");
            self.send_auth_error(client, "password authentication failed").await?;
            anyhow::bail!("User not found: {}", username);
        }

        // Generate random salt
        let mut salt = [0u8; 4];
        rand::thread_rng().fill(&mut salt);

        // Send MD5 authentication request
        debug!(username = %username, "Sending MD5 authentication request");
        let auth_request = build_auth_md5_password(salt);
        client.write_all(&auth_request).await.context("Failed to send auth request")?;

        // Read password response
        let mut buf = vec![0u8; 8192];
        let n = client.read(&mut buf).await.context("Failed to read password")?;
        buf.truncate(n);

        let client_password = parse_password_message(&buf)
            .ok_or_else(|| anyhow::anyhow!("Invalid password message from client"))?;

        debug!(username = %username, "Received password response from client");

        // Get stored password from auth file
        let stored_password = file_auth.get_password(username)
            .ok_or_else(|| anyhow::anyhow!("Cannot verify MD5 auth without plain password in auth_file"))?;

        // Verify MD5 response
        if !verify_md5_response(&client_password, stored_password, username, &salt) {
            warn!(username = %username, "MD5 authentication failed");
            self.send_auth_error(client, "password authentication failed").await?;
            anyhow::bail!("Authentication failed for user: {}", username);
        }

        info!(username = %username, "MD5 authentication successful");

        // Build startup for backend with backend credentials
        let backend_startup = self.build_backend_startup(startup);

        Ok(AuthHandshakeResult {
            startup: startup.clone(),
            username: username.to_string(),
            database,
            startup_bytes: backend_startup,
        })
    }

    /// Build startup message for backend with backend credentials
    fn build_backend_startup(&self, client_startup: &StartupMessage) -> Vec<u8> {
        // Use backend credentials from config
        let backend_user = &self.config.backend.user;
        let backend_database = &self.config.backend.database;

        // Collect extra parameters from client (excluding user/database)
        let extra_params: Vec<(&str, &str)> = client_startup.parameters
            .iter()
            .filter(|(k, _)| k.as_str() != "user" && k.as_str() != "database")
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        StartupMessage::build(backend_user, backend_database, &extra_params)
    }

    /// Send authentication error to client
    async fn send_auth_error(&self, client: &mut ClientTransport, message: &str) -> Result<()> {
        let error = build_error_response("FATAL", "28P01", message);
        client.write_all(&error).await.context("Failed to send auth error")?;
        Ok(())
    }

    /// Send authentication success to client
    pub async fn send_auth_ok(&self, client: &mut ClientTransport) -> Result<()> {
        let auth_ok = build_auth_ok();
        client.write_all(&auth_ok).await.context("Failed to send auth OK")?;
        Ok(())
    }

    /// Check if authentication is required (not trust mode)
    pub fn requires_auth(&self) -> bool {
        self.config.auth.auth_type != AuthType::Trust
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    fn create_test_config() -> Config {
        Config::default()
    }

    #[test]
    fn test_authenticator_trust_mode() {
        let config = Arc::new(create_test_config());
        let auth = Authenticator::new(config, None);
        assert!(!auth.requires_auth());
    }

    #[test]
    fn test_authenticator_md5_mode_requires_auth() {
        let mut config = create_test_config();
        config.auth.auth_type = AuthType::Md5;
        let config = Arc::new(config);
        let auth = Authenticator::new(config, None);
        assert!(auth.requires_auth());
    }

    #[test]
    fn test_build_backend_startup() {
        let mut config = create_test_config();
        config.backend.user = "backend_user".to_string();
        config.backend.database = "backend_db".to_string();
        let config = Arc::new(config);
        let auth = Authenticator::new(config, None);

        // Create a client startup message
        let client_startup_bytes = StartupMessage::build(
            "client_user",
            "client_db",
            &[("application_name", "test_app")],
        );
        let client_startup = StartupMessage::parse(&client_startup_bytes).unwrap();

        // Build backend startup
        let backend_bytes = auth.build_backend_startup(&client_startup);
        let backend_startup = StartupMessage::parse(&backend_bytes).unwrap();

        // Verify backend credentials are used
        assert_eq!(backend_startup.user(), Some("backend_user"));
        assert_eq!(backend_startup.database(), Some("backend_db"));

        // Verify extra params are preserved
        assert_eq!(
            backend_startup.parameters.get("application_name").map(|s| s.as_str()),
            Some("test_app")
        );
    }
}
