mod config;

pub use config::{load_client_tls_config, load_server_tls_config, TlsError};

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
}
