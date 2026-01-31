//! Backend authentication handler
//!
//! Handles authenticating the proxy TO the backend PostgreSQL database.
//! Supports Trust, MD5, Cleartext, and SCRAM-SHA-256 authentication.

use super::scram::ScramClient;
use crate::protocol::{
    build_password_message, build_sasl_initial_response, build_sasl_response, compute_md5_response,
    AuthRequest,
};
use anyhow::{Context, Result};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::debug;

/// Authenticates the proxy to a backend PostgreSQL database
pub struct BackendAuthenticator {
    username: String,
    password: String,
}

impl BackendAuthenticator {
    /// Create a new backend authenticator with the given credentials
    pub fn new(username: String, password: String) -> Self {
        Self { username, password }
    }

    /// Perform backend authentication
    ///
    /// Reads authentication requests from the backend and responds appropriately.
    /// Returns when AuthenticationOk is received.
    ///
    /// The buffer should contain any already-read data from the backend.
    /// Any non-auth messages at the end will be left in the returned buffer.
    pub async fn authenticate<S>(&self, stream: &mut S, initial_data: &[u8]) -> Result<Vec<u8>>
    where
        S: AsyncReadExt + AsyncWriteExt + Unpin,
    {
        let mut buffer = vec![0u8; 8192];
        let mut pending_data = initial_data.to_vec();
        let mut scram_client: Option<ScramClient> = None;

        loop {
            // Try to parse auth request from pending data
            let (auth_req, consumed) = if !pending_data.is_empty() {
                match self.try_parse_auth(&pending_data) {
                    Some((req, len)) => (req, len),
                    None => {
                        // Need more data
                        let n = stream
                            .read(&mut buffer)
                            .await
                            .context("Failed to read from backend")?;
                        if n == 0 {
                            anyhow::bail!("Backend closed connection during authentication");
                        }
                        pending_data.extend_from_slice(&buffer[..n]);
                        continue;
                    }
                }
            } else {
                // No pending data, read from stream
                let n = stream.read(&mut buffer).await.context("Failed to read from backend")?;
                if n == 0 {
                    anyhow::bail!("Backend closed connection during authentication");
                }
                pending_data = buffer[..n].to_vec();
                continue;
            };

            // Remove consumed bytes
            pending_data = pending_data[consumed..].to_vec();

            debug!(auth_type = ?auth_req, "Received backend auth request");

            match auth_req {
                AuthRequest::Ok => {
                    debug!("Backend authentication successful");
                    return Ok(pending_data);
                }

                AuthRequest::CleartextPassword => {
                    let msg = build_password_message(&self.password);
                    stream.write_all(&msg).await.context("Failed to send cleartext password")?;
                }

                AuthRequest::Md5 { salt } => {
                    let response = compute_md5_response(&self.password, &self.username, &salt);
                    let msg = build_password_message(&response);
                    stream.write_all(&msg).await.context("Failed to send MD5 password")?;
                }

                AuthRequest::Sasl { mechanisms } => {
                    if !mechanisms.iter().any(|m| m == "SCRAM-SHA-256") {
                        anyhow::bail!(
                            "Backend requires unsupported SASL mechanism: {:?}",
                            mechanisms
                        );
                    }

                    let mut client = ScramClient::new(&self.username, &self.password);
                    let client_first = client.client_first();
                    let msg = build_sasl_initial_response("SCRAM-SHA-256", &client_first);
                    stream.write_all(&msg).await.context("Failed to send SASL initial response")?;
                    scram_client = Some(client);
                }

                AuthRequest::SaslContinue { data } => {
                    let client = scram_client.as_mut().ok_or_else(|| {
                        anyhow::anyhow!("Received SASLContinue without SASL init")
                    })?;

                    let client_final = client
                        .client_final(&data)
                        .context("Failed to process SCRAM server-first")?;
                    let msg = build_sasl_response(&client_final);
                    stream.write_all(&msg).await.context("Failed to send SASL response")?;
                }

                AuthRequest::SaslFinal { data } => {
                    let client = scram_client
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("Received SASLFinal without SASL init"))?;

                    client
                        .verify_server_final(&data)
                        .context("SCRAM server signature verification failed")?;

                    debug!("SCRAM-SHA-256 authentication successful");
                    // AuthenticationOk should follow
                }
            }
        }
    }

    /// Try to parse an auth request from the buffer
    /// Returns (AuthRequest, bytes_consumed) or None if more data needed
    fn try_parse_auth(&self, data: &[u8]) -> Option<(AuthRequest, usize)> {
        if data.len() < 9 {
            return None;
        }

        if data[0] != b'R' {
            return None;
        }

        let length = i32::from_be_bytes([data[1], data[2], data[3], data[4]]) as usize;
        let total_len = 1 + length;

        if data.len() < total_len {
            return None;
        }

        AuthRequest::parse(&data[..total_len]).map(|req| (req, total_len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::build_auth_md5_password;
    use tokio::io::duplex;

    #[tokio::test]
    async fn test_backend_auth_trust() {
        // AuthenticationOk message
        let auth_ok = vec![b'R', 0, 0, 0, 8, 0, 0, 0, 0];

        let (mut client, mut server) = duplex(4096);

        tokio::spawn(async move {
            server.write_all(&auth_ok).await.unwrap();
        });

        let auth = BackendAuthenticator::new("user".to_string(), "pass".to_string());
        let remaining = auth.authenticate(&mut client, &[]).await.unwrap();

        assert!(remaining.is_empty());
    }

    #[tokio::test]
    async fn test_backend_auth_md5() {
        let salt = [0x01, 0x02, 0x03, 0x04];
        let md5_request = build_auth_md5_password(salt);
        let auth_ok = vec![b'R', 0, 0, 0, 8, 0, 0, 0, 0];

        let (mut client, mut server) = duplex(4096);

        let expected_response = compute_md5_response("password", "postgres", &salt);

        tokio::spawn(async move {
            // Send MD5 request
            server.write_all(&md5_request).await.unwrap();

            // Read password response
            let mut buf = [0u8; 256];
            let n = server.read(&mut buf).await.unwrap();

            // Verify it's a password message with correct hash
            assert_eq!(buf[0], b'p');
            let password = crate::protocol::parse_password_message(&buf[..n]).unwrap();
            assert_eq!(password, expected_response);

            // Send AuthOk
            server.write_all(&auth_ok).await.unwrap();
        });

        let auth = BackendAuthenticator::new("postgres".to_string(), "password".to_string());
        let remaining = auth.authenticate(&mut client, &[]).await.unwrap();

        assert!(remaining.is_empty());
    }
}
