use crate::config::TlsSslMode;
use crate::protocol::read_startup_message;
use rustls::ServerConfig;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, info, warn};

use super::transport::ClientTransport;

/// PostgreSQL SSL request code (80877103 in decimal, 0x04d2162f in hex)
/// Sent as: length (8) + request code in big-endian
pub const SSL_REQUEST_CODE: i32 = 80877103;

/// Check if a startup message is an SSL request
pub fn is_ssl_request(data: &[u8]) -> bool {
    if data.len() < 8 {
        return false;
    }

    // First 4 bytes are length (should be 8)
    let length = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    if length != 8 {
        return false;
    }

    // Next 4 bytes are the request code
    let code = i32::from_be_bytes([data[4], data[5], data[6], data[7]]);
    code == SSL_REQUEST_CODE
}

/// SSL response bytes
pub const SSL_RESPONSE_YES: u8 = b'S';
pub const SSL_RESPONSE_NO: u8 = b'N';

/// Result of handling the SSL startup phase
pub enum SslStartupResult {
    /// Client requested SSL and was upgraded
    Upgraded(ClientTransport),
    /// Client requested SSL but server doesn't support it (sent 'N')
    Declined(TcpStream, Vec<u8>),
    /// Client didn't request SSL (startup message buffered)
    NoSslRequest(TcpStream, Vec<u8>),
}

/// Handle the PostgreSQL SSL startup handshake
///
/// PostgreSQL clients can send an SSLRequest before the normal StartupMessage.
/// We need to:
/// 1. Read the first message (could be SSLRequest or StartupMessage)
/// 2. If it's an SSLRequest, respond with 'S' (yes) or 'N' (no)
/// 3. If 'S', upgrade the connection to TLS
/// 4. Return the (possibly upgraded) transport and any buffered data
///
/// This implementation properly handles:
/// - Large startup messages (>8192 bytes) via read_startup_message()
/// - TCP fragmentation (loops until complete message received)
/// - Malformed messages (validates length bounds)
pub async fn handle_ssl_startup(
    mut stream: TcpStream,
    sslmode: &TlsSslMode,
    tls_config: Option<Arc<ServerConfig>>,
) -> Result<SslStartupResult, std::io::Error> {
    // Read the first message using proper framing
    // This handles large messages and TCP fragmentation correctly
    let buf = match read_startup_message(&mut stream).await {
        Ok(data) => data,
        Err(e) => {
            warn!(error = %e, "Failed to read initial startup message");
            return Err(std::io::Error::other(format!(
                "Failed to read startup message: {}",
                e
            )));
        }
    };

    // Check if it's an SSL request
    if !is_ssl_request(&buf) {
        debug!("Client sent StartupMessage without SSLRequest");
        return Ok(SslStartupResult::NoSslRequest(stream, buf));
    }

    info!("Client sent SSLRequest");

    // Determine response based on sslmode and whether we have TLS configured
    match (sslmode, &tls_config) {
        (TlsSslMode::Disable, _) => {
            // TLS disabled, always say no
            debug!("TLS disabled, declining SSLRequest");
            stream.write_all(&[SSL_RESPONSE_NO]).await?;

            // Client should send StartupMessage next - read it properly
            let startup_buf = match read_startup_message(&mut stream).await {
                Ok(data) => data,
                Err(e) => {
                    warn!(error = %e, "Failed to read startup message after SSL decline");
                    return Err(std::io::Error::other(format!(
                        "Failed to read startup message: {}",
                        e
                    )));
                }
            };

            Ok(SslStartupResult::Declined(stream, startup_buf))
        }

        (TlsSslMode::Allow, None) => {
            // Allow mode but no TLS config, say no
            debug!("TLS allowed but not configured, declining SSLRequest");
            stream.write_all(&[SSL_RESPONSE_NO]).await?;

            // Read startup message properly
            let startup_buf = match read_startup_message(&mut stream).await {
                Ok(data) => data,
                Err(e) => {
                    warn!(error = %e, "Failed to read startup message after SSL decline");
                    return Err(std::io::Error::other(format!(
                        "Failed to read startup message: {}",
                        e
                    )));
                }
            };

            Ok(SslStartupResult::Declined(stream, startup_buf))
        }

        (_, Some(config)) => {
            // TLS available, accept the request
            info!("Accepting SSLRequest, upgrading to TLS");
            stream.write_all(&[SSL_RESPONSE_YES]).await?;

            // Upgrade to TLS
            let acceptor = TlsAcceptor::from(config.clone());
            let tls_stream = acceptor.accept(stream).await?;

            info!("TLS handshake completed");
            Ok(SslStartupResult::Upgraded(ClientTransport::Tls(Box::new(
                tls_stream,
            ))))
        }

        (TlsSslMode::Require | TlsSslMode::VerifyCa | TlsSslMode::VerifyFull, None) => {
            // TLS required but not configured - this is a config error
            // Should have been caught at startup, but handle gracefully
            warn!("TLS required but not configured, declining (config error)");
            stream.write_all(&[SSL_RESPONSE_NO]).await?;

            Err(std::io::Error::other("TLS required but not configured"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ssl_request_detection() {
        // Valid SSL request
        let ssl_req: [u8; 8] = [0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f];
        assert!(is_ssl_request(&ssl_req));

        // Protocol version 3.0 startup (not SSL)
        let startup: [u8; 8] = [0, 0, 0, 8, 0, 3, 0, 0];
        assert!(!is_ssl_request(&startup));

        // Too short
        let short: [u8; 4] = [0, 0, 0, 8];
        assert!(!is_ssl_request(&short));

        // Wrong length field
        let wrong_len: [u8; 8] = [0, 0, 0, 9, 0x04, 0xd2, 0x16, 0x2f];
        assert!(!is_ssl_request(&wrong_len));
    }
}
