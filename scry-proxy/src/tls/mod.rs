mod config;
pub mod startup;
mod transport;

pub use config::{load_client_tls_config, load_server_tls_config, TlsError};
pub use startup::{handle_ssl_startup, SslStartupResult};
pub use transport::ClientTransport;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{TlsConfig, TlsSslMode};

    #[test]
    fn test_disabled_tls_returns_none() {
        let config = TlsConfig::default();
        let result = load_client_tls_config(&config);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_require_without_cert_fails() {
        let mut config = TlsConfig::default();
        config.client_tls_sslmode = TlsSslMode::Require;
        let result = load_client_tls_config(&config);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_client_transport_plain_tcp() {
        use super::transport::ClientTransport;
        use tokio::net::{TcpListener, TcpStream};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).await.unwrap();
        let transport = ClientTransport::Plain(client);

        assert!(!transport.is_encrypted());
    }

    #[test]
    fn test_ssl_request_message_format() {
        use super::startup::{is_ssl_request, SSL_REQUEST_CODE};

        // PostgreSQL SSLRequest: 8 bytes total
        // - 4 bytes length (8 as i32 big-endian)
        // - 4 bytes SSL request code (80877103 as i32 big-endian)
        let ssl_request: [u8; 8] = [0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f];
        assert!(is_ssl_request(&ssl_request));

        // Verify the SSL request code constant matches
        assert_eq!(SSL_REQUEST_CODE, 80877103);

        // Not an SSL request (regular startup)
        let startup: [u8; 8] = [0, 0, 0, 8, 0, 3, 0, 0];
        assert!(!is_ssl_request(&startup));
    }
}
