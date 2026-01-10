//! Backend SSL/TLS negotiation for PostgreSQL connections
//!
//! This module handles the SSL handshake when connecting to PostgreSQL backends.

use crate::config::TlsSslMode;
use anyhow::{Context, Result};
use rustls::pki_types::ServerName;
use rustls::ClientConfig;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tracing::{debug, info, warn};

use super::transport::BackendTransport;

/// PostgreSQL SSL request message
/// 8 bytes: 4-byte length (8) + 4-byte request code (80877103)
pub const SSL_REQUEST: [u8; 8] = [0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f];

/// Result of SSL negotiation with backend
pub enum BackendSslResult {
    /// Backend accepted SSL, connection upgraded
    Upgraded(BackendTransport),
    /// Backend declined SSL, continuing with plain TCP
    Declined(TcpStream),
    /// Backend doesn't support SSL and sslmode requires it
    Required,
}

/// Negotiate SSL with a PostgreSQL backend
///
/// Sends SSLRequest to the backend and handles the response:
/// - 'S': Backend accepts SSL, upgrade the connection
/// - 'N': Backend declines SSL, continue based on sslmode
///
/// # Arguments
/// * `stream` - TCP connection to backend
/// * `hostname` - Backend hostname for TLS verification
/// * `sslmode` - SSL mode configuration
/// * `tls_config` - TLS configuration (required for non-disable modes)
pub async fn negotiate_backend_ssl(
    mut stream: TcpStream,
    hostname: &str,
    sslmode: &TlsSslMode,
    tls_config: Option<Arc<ClientConfig>>,
) -> Result<BackendSslResult> {
    match sslmode {
        TlsSslMode::Disable => {
            debug!("Backend TLS disabled, using plain TCP");
            Ok(BackendSslResult::Declined(stream))
        }

        TlsSslMode::Allow | TlsSslMode::Require | TlsSslMode::VerifyCa | TlsSslMode::VerifyFull => {
            // Send SSL request to backend
            debug!("Sending SSLRequest to backend");
            stream.write_all(&SSL_REQUEST).await.context("Failed to send SSLRequest to backend")?;

            // Read single-byte response
            let mut response = [0u8; 1];
            stream
                .read_exact(&mut response)
                .await
                .context("Failed to read SSL response from backend")?;

            match response[0] {
                b'S' => {
                    info!("Backend accepted SSL, upgrading connection");

                    let config = tls_config.ok_or_else(|| {
                        anyhow::anyhow!(
                            "TLS config required but not provided for sslmode {:?}",
                            sslmode
                        )
                    })?;

                    let connector = TlsConnector::from(config);

                    // Parse hostname for TLS verification
                    let server_name: ServerName<'static> = hostname
                        .to_string()
                        .try_into()
                        .map_err(|_| anyhow::anyhow!("Invalid hostname for TLS: {}", hostname))?;

                    let tls_stream = connector
                        .connect(server_name, stream)
                        .await
                        .context("TLS handshake with backend failed")?;

                    info!("Backend TLS handshake completed");
                    Ok(BackendSslResult::Upgraded(BackendTransport::Tls(Box::new(tls_stream))))
                }

                b'N' => {
                    debug!("Backend declined SSL");

                    match sslmode {
                        TlsSslMode::Allow => {
                            info!("Backend declined SSL, continuing with plain TCP (allow mode)");
                            Ok(BackendSslResult::Declined(stream))
                        }
                        TlsSslMode::Require | TlsSslMode::VerifyCa | TlsSslMode::VerifyFull => {
                            warn!(
                                "Backend does not support SSL but sslmode={:?} requires it",
                                sslmode
                            );
                            Ok(BackendSslResult::Required)
                        }
                        TlsSslMode::Disable => unreachable!(),
                    }
                }

                other => Err(anyhow::anyhow!(
                    "Unexpected SSL response from backend: {} (expected 'S' or 'N')",
                    other as char
                )),
            }
        }
    }
}

/// Upgrade a TCP stream to TLS for backend connection
///
/// This is a convenience function that wraps negotiate_backend_ssl
/// and returns a BackendTransport directly.
pub async fn upgrade_backend_to_tls(
    stream: TcpStream,
    hostname: &str,
    sslmode: &TlsSslMode,
    tls_config: Option<Arc<ClientConfig>>,
) -> Result<BackendTransport> {
    match negotiate_backend_ssl(stream, hostname, sslmode, tls_config).await? {
        BackendSslResult::Upgraded(transport) => Ok(transport),
        BackendSslResult::Declined(stream) => Ok(BackendTransport::Plain(stream)),
        BackendSslResult::Required => {
            Err(anyhow::anyhow!("Backend does not support SSL but configuration requires it"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ssl_request_bytes() {
        // Verify our SSL request matches PostgreSQL spec
        assert_eq!(SSL_REQUEST.len(), 8);
        // Length field = 8
        assert_eq!(
            i32::from_be_bytes([SSL_REQUEST[0], SSL_REQUEST[1], SSL_REQUEST[2], SSL_REQUEST[3]]),
            8
        );
        // Request code = 80877103
        assert_eq!(
            i32::from_be_bytes([SSL_REQUEST[4], SSL_REQUEST[5], SSL_REQUEST[6], SSL_REQUEST[7]]),
            80877103
        );
    }

    #[test]
    fn test_ssl_request_matches_startup() {
        // Verify SSL_REQUEST here matches the one in startup.rs
        use crate::tls::startup::SSL_REQUEST_CODE;
        let code =
            i32::from_be_bytes([SSL_REQUEST[4], SSL_REQUEST[5], SSL_REQUEST[6], SSL_REQUEST[7]]);
        assert_eq!(code, SSL_REQUEST_CODE);
    }
}
