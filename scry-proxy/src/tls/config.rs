use crate::config::{TlsConfig, TlsSslMode};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use rustls_pemfile::{certs, private_key};
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TlsError {
    #[error("TLS certificate file required but not configured")]
    CertificateNotConfigured,

    #[error("TLS key file required but not configured")]
    KeyNotConfigured,

    #[error("Failed to read certificate file: {0}")]
    CertificateReadError(#[source] std::io::Error),

    #[error("Failed to read key file: {0}")]
    KeyReadError(#[source] std::io::Error),

    #[error("No certificates found in file: {0}")]
    NoCertificatesFound(String),

    #[error("No private key found in file: {0}")]
    NoPrivateKeyFound(String),

    #[error("Failed to build TLS config: {0}")]
    ConfigBuildError(String),

    #[error("Failed to read CA file: {0}")]
    CaReadError(#[source] std::io::Error),
}

/// Load certificates from a PEM file
fn load_certs(path: &str) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let file = File::open(path).map_err(TlsError::CertificateReadError)?;
    let mut reader = BufReader::new(file);
    let certs: Vec<_> = certs(&mut reader).filter_map(|cert| cert.ok()).collect();

    if certs.is_empty() {
        return Err(TlsError::NoCertificatesFound(path.to_string()));
    }

    Ok(certs)
}

/// Load private key from a PEM file
fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>, TlsError> {
    let file = File::open(path).map_err(TlsError::KeyReadError)?;
    let mut reader = BufReader::new(file);

    private_key(&mut reader)
        .map_err(|e| {
            TlsError::KeyReadError(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })?
        .ok_or_else(|| TlsError::NoPrivateKeyFound(path.to_string()))
}

/// Load CA certificates into a RootCertStore
fn load_ca_certs(path: &str) -> Result<RootCertStore, TlsError> {
    let file = File::open(path).map_err(TlsError::CaReadError)?;
    let mut reader = BufReader::new(file);
    let certs = certs(&mut reader).filter_map(|cert| cert.ok()).collect::<Vec<_>>();

    let mut root_store = RootCertStore::empty();
    for cert in certs {
        root_store.add(cert).map_err(|e| TlsError::ConfigBuildError(e.to_string()))?;
    }

    Ok(root_store)
}

/// Load TLS configuration for accepting client connections (server-side TLS)
/// Returns None if TLS is disabled
pub fn load_client_tls_config(config: &TlsConfig) -> Result<Option<Arc<ServerConfig>>, TlsError> {
    match config.client_tls_sslmode {
        TlsSslMode::Disable => Ok(None),

        TlsSslMode::Allow | TlsSslMode::Require => {
            let cert_path =
                config.client_tls_cert_file.as_ref().ok_or(TlsError::CertificateNotConfigured)?;
            let key_path = config.client_tls_key_file.as_ref().ok_or(TlsError::KeyNotConfigured)?;

            let certs = load_certs(cert_path)?;
            let key = load_private_key(key_path)?;

            let server_config = ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .map_err(|e| TlsError::ConfigBuildError(e.to_string()))?;

            Ok(Some(Arc::new(server_config)))
        }

        TlsSslMode::VerifyCa | TlsSslMode::VerifyFull => {
            let cert_path =
                config.client_tls_cert_file.as_ref().ok_or(TlsError::CertificateNotConfigured)?;
            let key_path = config.client_tls_key_file.as_ref().ok_or(TlsError::KeyNotConfigured)?;
            let ca_path =
                config.client_tls_ca_file.as_ref().ok_or(TlsError::CertificateNotConfigured)?;

            let certs = load_certs(cert_path)?;
            let key = load_private_key(key_path)?;
            let root_store = load_ca_certs(ca_path)?;

            let client_verifier = WebPkiClientVerifier::builder(Arc::new(root_store))
                .build()
                .map_err(|e| TlsError::ConfigBuildError(e.to_string()))?;

            let server_config = ServerConfig::builder()
                .with_client_cert_verifier(client_verifier)
                .with_single_cert(certs, key)
                .map_err(|e| TlsError::ConfigBuildError(e.to_string()))?;

            Ok(Some(Arc::new(server_config)))
        }
    }
}

/// Load TLS configuration for connecting to backend servers (client-side TLS)
/// Returns None if TLS is disabled
pub fn load_server_tls_config(config: &TlsConfig) -> Result<Option<Arc<ClientConfig>>, TlsError> {
    match config.server_tls_sslmode {
        TlsSslMode::Disable => Ok(None),

        TlsSslMode::Allow | TlsSslMode::Require => {
            // Accept any certificate (dangerous but matches PgBouncer behavior for these modes)
            let client_config = ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(danger::NoCertificateVerification))
                .with_no_client_auth();

            Ok(Some(Arc::new(client_config)))
        }

        TlsSslMode::VerifyCa | TlsSslMode::VerifyFull => {
            let mut root_store = RootCertStore::empty();

            if let Some(ca_path) = &config.server_tls_ca_file {
                root_store = load_ca_certs(ca_path)?;
            } else {
                root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            }

            let builder = ClientConfig::builder().with_root_certificates(root_store);

            let client_config = if let (Some(cert_path), Some(key_path)) =
                (&config.server_tls_cert_file, &config.server_tls_key_file)
            {
                let certs = load_certs(cert_path)?;
                let key = load_private_key(key_path)?;
                builder
                    .with_client_auth_cert(certs, key)
                    .map_err(|e| TlsError::ConfigBuildError(e.to_string()))?
            } else {
                builder.with_no_client_auth()
            };

            Ok(Some(Arc::new(client_config)))
        }
    }
}

/// Dangerous certificate verifier that accepts any certificate
/// Used for sslmode=require where we want encryption but not verification
mod danger {
    use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use rustls::{DigitallySignedStruct, Error, SignatureScheme};

    #[derive(Debug)]
    pub struct NoCertificateVerification;

    impl ServerCertVerifier for NoCertificateVerification {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp_response: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, Error> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, Error> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, Error> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            vec![
                SignatureScheme::RSA_PKCS1_SHA256,
                SignatureScheme::RSA_PKCS1_SHA384,
                SignatureScheme::RSA_PKCS1_SHA512,
                SignatureScheme::ECDSA_NISTP256_SHA256,
                SignatureScheme::ECDSA_NISTP384_SHA384,
                SignatureScheme::ECDSA_NISTP521_SHA512,
                SignatureScheme::RSA_PSS_SHA256,
                SignatureScheme::RSA_PSS_SHA384,
                SignatureScheme::RSA_PSS_SHA512,
                SignatureScheme::ED25519,
            ]
        }
    }
}
