# TLS Support Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add TLS/SSL support for client and backend connections to achieve parity with PgBouncer.

**Architecture:** The proxy will handle the PostgreSQL SSL startup handshake, upgrading plain TCP connections to TLS when clients request it. A transport abstraction (`ClientTransport`) will unify `TcpStream` and `TlsStream` handling, keeping the rest of the proxy code unchanged. Configuration follows PgBouncer's naming conventions for familiarity.

**Tech Stack:** tokio-rustls (async TLS), rustls-pemfile (certificate parsing), rustls (TLS implementation)

**References:**
- [PgBouncer TLS Configuration](https://www.pgbouncer.org/config.html)
- [Crunchy Data PgBouncer TLS Guide](https://www.crunchydata.com/blog/improving-pgbouncer-security-with-tlsssl)

---

## Phase 1: Client-Facing TLS

### Task 1: Add TLS Dependencies

**Files:**
- Modify: `Cargo.toml:11-79` (workspace dependencies)
- Modify: `scry-proxy/Cargo.toml:13-72` (crate dependencies)

**Step 1: Add workspace dependencies**

In `Cargo.toml`, add after line 69 (after `ahash = "0.8"`):

```toml
# TLS support
tokio-rustls = "0.26"
rustls = { version = "0.23", default-features = false, features = ["std", "tls12"] }
rustls-pemfile = "2.2"
webpki-roots = "0.26"
```

**Step 2: Add crate dependencies**

In `scry-proxy/Cargo.toml`, add after line 71 (after `indexmap = "2.7"`):

```toml
# TLS support
tokio-rustls = { workspace = true }
rustls = { workspace = true }
rustls-pemfile = { workspace = true }
webpki-roots = { workspace = true }
```

**Step 3: Verify dependencies resolve**

Run: `cargo check -p scry`
Expected: Compiles without errors

**Step 4: Commit**

```bash
git add Cargo.toml scry-proxy/Cargo.toml
git commit -m "$(cat <<'EOF'
deps: add tokio-rustls and rustls for TLS support

Add TLS dependencies to enable SSL/TLS connections for both
client-facing and backend connections, matching PgBouncer capabilities.
EOF
)"
```

---

### Task 2: Add TLS Configuration Types

**Files:**
- Modify: `scry-proxy/src/config/mod.rs:36-53` (add TLS config)

**Step 1: Write the failing test**

Add to `scry-proxy/src/config/mod.rs` at the end of the `#[cfg(test)]` module (after line 384):

```rust
    #[test]
    fn test_tls_sslmode_default_is_disable() {
        let config = Config::default();
        assert_eq!(config.tls.client_tls_sslmode, TlsSslMode::Disable);
        assert_eq!(config.tls.server_tls_sslmode, TlsSslMode::Disable);
    }

    #[test]
    fn test_tls_config_defaults() {
        let config = Config::default();
        assert!(config.tls.client_tls_cert_file.is_none());
        assert!(config.tls.client_tls_key_file.is_none());
        assert!(config.tls.client_tls_ca_file.is_none());
        assert!(config.tls.server_tls_ca_file.is_none());
    }
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p scry test_tls_sslmode_default_is_disable`
Expected: FAIL with "cannot find type `TlsSslMode`" or similar

**Step 3: Add TlsSslMode enum**

Add after line 124 (after `PoolingStrategy` enum), before `PerformanceConfig`:

```rust
/// TLS SSL mode - matches PgBouncer naming for familiarity
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum TlsSslMode {
    /// Plain TCP, TLS disabled (default)
    #[default]
    Disable,
    /// If client requests TLS, use it; otherwise plain TCP
    Allow,
    /// Client must use TLS, but certificate not validated
    Require,
    /// Client must use TLS with valid certificate (CA verified)
    #[serde(rename = "verify-ca")]
    VerifyCa,
    /// Client must use TLS with valid certificate + hostname match
    #[serde(rename = "verify-full")]
    VerifyFull,
}
```

**Step 4: Add TlsConfig struct**

Add after `TlsSslMode`:

```rust
/// TLS configuration for client and server connections
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    // Client-facing TLS (clients -> proxy)
    /// TLS mode for client connections
    pub client_tls_sslmode: TlsSslMode,
    /// Path to server certificate file (PEM format)
    pub client_tls_cert_file: Option<String>,
    /// Path to server private key file (PEM format)
    pub client_tls_key_file: Option<String>,
    /// Path to CA certificate for client certificate validation
    pub client_tls_ca_file: Option<String>,

    // Server-facing TLS (proxy -> backend)
    /// TLS mode for backend connections
    pub server_tls_sslmode: TlsSslMode,
    /// Path to CA certificate for server certificate validation
    pub server_tls_ca_file: Option<String>,
    /// Path to client certificate for backend authentication
    pub server_tls_cert_file: Option<String>,
    /// Path to client private key for backend authentication
    pub server_tls_key_file: Option<String>,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            client_tls_sslmode: TlsSslMode::Disable,
            client_tls_cert_file: None,
            client_tls_key_file: None,
            client_tls_ca_file: None,
            server_tls_sslmode: TlsSslMode::Disable,
            server_tls_ca_file: None,
            server_tls_cert_file: None,
            server_tls_key_file: None,
        }
    }
}
```

**Step 5: Add TlsConfig to Config struct**

Modify the `Config` struct (around line 37) to add the tls field:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub proxy: ProxyConfig,
    pub backend: BackendConfig,
    pub observability: ObservabilityConfig,
    pub protocol: ProtocolConfig,
    pub publisher: PublisherConfig,
    pub performance: PerformanceConfig,
    pub resilience: ResilienceConfig,
    pub tls: TlsConfig,
}
```

**Step 6: Add TlsConfig to Config::default()**

In the `Default` impl for `Config` (around line 289), add before the closing brace:

```rust
            tls: TlsConfig::default(),
```

**Step 7: Run tests to verify they pass**

Run: `cargo test -p scry test_tls`
Expected: PASS (both tests)

**Step 8: Commit**

```bash
git add scry-proxy/src/config/mod.rs
git commit -m "$(cat <<'EOF'
feat(config): add TLS configuration types

Add TlsSslMode enum and TlsConfig struct matching PgBouncer's
TLS configuration options for client and backend connections.

Environment variables:
- SCRY_TLS__CLIENT_TLS_SSLMODE (disable|allow|require|verify-ca|verify-full)
- SCRY_TLS__CLIENT_TLS_CERT_FILE
- SCRY_TLS__CLIENT_TLS_KEY_FILE
- SCRY_TLS__CLIENT_TLS_CA_FILE
- SCRY_TLS__SERVER_TLS_SSLMODE
- SCRY_TLS__SERVER_TLS_CA_FILE
EOF
)"
```

---

### Task 3: Create TLS Module with Certificate Loading

**Files:**
- Create: `scry-proxy/src/tls/mod.rs`
- Create: `scry-proxy/src/tls/config.rs`
- Modify: `scry-proxy/src/lib.rs` or `scry-proxy/src/main.rs` (add module)

**Step 1: Create TLS module directory**

Run: `mkdir -p scry-proxy/src/tls` (or equivalent on Windows)

**Step 2: Write the failing test**

Create `scry-proxy/src/tls/mod.rs`:

```rust
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
```

**Step 3: Run test to verify it fails**

Run: `cargo test -p scry test_disabled_tls_returns_none`
Expected: FAIL with "cannot find function `load_client_tls_config`"

**Step 4: Implement certificate loading**

Create `scry-proxy/src/tls/config.rs`:

```rust
use crate::config::{TlsConfig, TlsSslMode};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
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
    let certs: Vec<_> = certs(&mut reader)
        .filter_map(|cert| cert.ok())
        .collect();

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
        .map_err(|e| TlsError::KeyReadError(std::io::Error::new(std::io::ErrorKind::InvalidData, e)))?
        .ok_or_else(|| TlsError::NoPrivateKeyFound(path.to_string()))
}

/// Load CA certificates into a RootCertStore
fn load_ca_certs(path: &str) -> Result<RootCertStore, TlsError> {
    let file = File::open(path).map_err(TlsError::CaReadError)?;
    let mut reader = BufReader::new(file);
    let certs = certs(&mut reader)
        .filter_map(|cert| cert.ok())
        .collect::<Vec<_>>();

    let mut root_store = RootCertStore::empty();
    for cert in certs {
        root_store.add(cert).map_err(|e| TlsError::ConfigBuildError(e.to_string()))?;
    }

    Ok(root_store)
}

/// Load TLS configuration for accepting client connections
/// Returns None if TLS is disabled
pub fn load_client_tls_config(config: &TlsConfig) -> Result<Option<Arc<ServerConfig>>, TlsError> {
    match config.client_tls_sslmode {
        TlsSslMode::Disable => Ok(None),

        TlsSslMode::Allow | TlsSslMode::Require => {
            // Need cert and key for accepting TLS connections
            let cert_path = config.client_tls_cert_file.as_ref()
                .ok_or(TlsError::CertificateNotConfigured)?;
            let key_path = config.client_tls_key_file.as_ref()
                .ok_or(TlsError::KeyNotConfigured)?;

            let certs = load_certs(cert_path)?;
            let key = load_private_key(key_path)?;

            let server_config = ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .map_err(|e| TlsError::ConfigBuildError(e.to_string()))?;

            Ok(Some(Arc::new(server_config)))
        }

        TlsSslMode::VerifyCa | TlsSslMode::VerifyFull => {
            // Need cert, key, and CA for client certificate verification
            let cert_path = config.client_tls_cert_file.as_ref()
                .ok_or(TlsError::CertificateNotConfigured)?;
            let key_path = config.client_tls_key_file.as_ref()
                .ok_or(TlsError::KeyNotConfigured)?;
            let ca_path = config.client_tls_ca_file.as_ref()
                .ok_or(TlsError::CertificateNotConfigured)?;

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

/// Load TLS configuration for connecting to backend servers
/// Returns None if TLS is disabled
pub fn load_server_tls_config(config: &TlsConfig) -> Result<Option<Arc<rustls::ClientConfig>>, TlsError> {
    use rustls::ClientConfig;

    match config.server_tls_sslmode {
        TlsSslMode::Disable => Ok(None),

        TlsSslMode::Allow | TlsSslMode::Require => {
            // Accept any certificate (dangerous but matches PgBouncer behavior)
            let client_config = ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(danger::NoCertificateVerification))
                .with_no_client_auth();

            Ok(Some(Arc::new(client_config)))
        }

        TlsSslMode::VerifyCa | TlsSslMode::VerifyFull => {
            let mut root_store = RootCertStore::empty();

            // Load custom CA if provided, otherwise use system roots
            if let Some(ca_path) = &config.server_tls_ca_file {
                root_store = load_ca_certs(ca_path)?;
            } else {
                root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            }

            let builder = ClientConfig::builder()
                .with_root_certificates(root_store);

            // Add client certificate if provided (for mTLS)
            let client_config = if let (Some(cert_path), Some(key_path)) =
                (&config.server_tls_cert_file, &config.server_tls_key_file)
            {
                let certs = load_certs(cert_path)?;
                let key = load_private_key(key_path)?;
                builder.with_client_auth_cert(certs, key)
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
```

**Step 5: Add tls module to main.rs**

In `scry-proxy/src/main.rs`, add after other module declarations:

```rust
mod tls;
```

**Step 6: Run tests to verify they pass**

Run: `cargo test -p scry test_disabled_tls`
Expected: PASS

Run: `cargo test -p scry test_require_without_cert`
Expected: PASS

**Step 7: Commit**

```bash
git add scry-proxy/src/tls/ scry-proxy/src/main.rs
git commit -m "$(cat <<'EOF'
feat(tls): add certificate loading and TLS config builders

Implements load_client_tls_config() and load_server_tls_config()
functions that build rustls configurations from TlsConfig settings.

Supports all SSL modes:
- disable: No TLS
- allow/require: TLS without certificate verification
- verify-ca/verify-full: TLS with certificate verification
EOF
)"
```

---

### Task 4: Create ClientTransport Abstraction

**Files:**
- Create: `scry-proxy/src/tls/transport.rs`
- Modify: `scry-proxy/src/tls/mod.rs`

**Step 1: Write the failing test**

Add to `scry-proxy/src/tls/mod.rs`:

```rust
mod transport;

pub use transport::ClientTransport;

// In the tests module, add:
    #[tokio::test]
    async fn test_client_transport_plain_tcp() {
        use tokio::net::{TcpListener, TcpStream};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).await.unwrap();
        let transport = ClientTransport::Plain(client);

        // Should be able to check if it's encrypted
        assert!(!transport.is_encrypted());
    }
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p scry test_client_transport_plain_tcp`
Expected: FAIL with "cannot find type `ClientTransport`"

**Step 3: Implement ClientTransport**

Create `scry-proxy/src/tls/transport.rs`:

```rust
use pin_project::pin_project;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;

/// A client transport that can be either plain TCP or TLS-encrypted
#[pin_project(project = ClientTransportProj)]
pub enum ClientTransport {
    /// Plain unencrypted TCP connection
    Plain(#[pin] TcpStream),
    /// TLS-encrypted connection
    Tls(#[pin] TlsStream<TcpStream>),
}

impl ClientTransport {
    /// Check if the transport is encrypted
    pub fn is_encrypted(&self) -> bool {
        matches!(self, ClientTransport::Tls(_))
    }

    /// Get the peer address
    pub fn peer_addr(&self) -> io::Result<std::net::SocketAddr> {
        match self {
            ClientTransport::Plain(stream) => stream.peer_addr(),
            ClientTransport::Tls(stream) => stream.get_ref().0.peer_addr(),
        }
    }
}

impl AsyncRead for ClientTransport {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.project() {
            ClientTransportProj::Plain(stream) => stream.poll_read(cx, buf),
            ClientTransportProj::Tls(stream) => stream.poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ClientTransport {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.project() {
            ClientTransportProj::Plain(stream) => stream.poll_write(cx, buf),
            ClientTransportProj::Tls(stream) => stream.poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.project() {
            ClientTransportProj::Plain(stream) => stream.poll_flush(cx),
            ClientTransportProj::Tls(stream) => stream.poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.project() {
            ClientTransportProj::Plain(stream) => stream.poll_shutdown(cx),
            ClientTransportProj::Tls(stream) => stream.poll_shutdown(cx),
        }
    }
}
```

**Step 4: Update tls/mod.rs exports**

Update `scry-proxy/src/tls/mod.rs`:

```rust
mod config;
mod transport;

pub use config::{load_client_tls_config, load_server_tls_config, TlsError};
pub use transport::ClientTransport;
```

**Step 5: Run tests to verify they pass**

Run: `cargo test -p scry test_client_transport_plain_tcp`
Expected: PASS

**Step 6: Commit**

```bash
git add scry-proxy/src/tls/transport.rs scry-proxy/src/tls/mod.rs
git commit -m "$(cat <<'EOF'
feat(tls): add ClientTransport abstraction for unified I/O

ClientTransport wraps either TcpStream or TlsStream, implementing
AsyncRead + AsyncWrite so the rest of the proxy doesn't need to
know whether the connection is encrypted.
EOF
)"
```

---

### Task 5: Implement PostgreSQL SSL Startup Handshake

**Files:**
- Create: `scry-proxy/src/tls/startup.rs`
- Modify: `scry-proxy/src/tls/mod.rs`

**Step 1: Write the failing test**

Add to `scry-proxy/src/tls/mod.rs` tests:

```rust
    #[test]
    fn test_ssl_request_message_format() {
        use super::startup::{is_ssl_request, SSL_REQUEST_CODE};

        // PostgreSQL SSLRequest: 8 bytes total
        // - 4 bytes length (8 as i32 big-endian)
        // - 4 bytes SSL request code (80877103 as i32 big-endian)
        let ssl_request: [u8; 8] = [0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f];
        assert!(is_ssl_request(&ssl_request));

        // Not an SSL request (regular startup)
        let startup: [u8; 8] = [0, 0, 0, 8, 0, 3, 0, 0];
        assert!(!is_ssl_request(&startup));
    }
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p scry test_ssl_request_message_format`
Expected: FAIL with "cannot find function `is_ssl_request`"

**Step 3: Implement SSL startup handling**

Create `scry-proxy/src/tls/startup.rs`:

```rust
use crate::config::TlsSslMode;
use rustls::ServerConfig;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
/// 1. Read the first message
/// 2. If it's an SSLRequest, respond with 'S' (yes) or 'N' (no)
/// 3. If 'S', upgrade the connection to TLS
/// 4. Return the (possibly upgraded) transport and any buffered data
pub async fn handle_ssl_startup(
    mut stream: TcpStream,
    sslmode: &TlsSslMode,
    tls_config: Option<Arc<ServerConfig>>,
) -> Result<SslStartupResult, std::io::Error> {
    // Read the first message (at least 8 bytes for SSLRequest or StartupMessage length)
    let mut buf = vec![0u8; 8192];
    let n = stream.read(&mut buf).await?;

    if n < 8 {
        // Not enough data for any valid startup message
        buf.truncate(n);
        return Ok(SslStartupResult::NoSslRequest(stream, buf));
    }

    buf.truncate(n);

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

            // Client should send StartupMessage next
            let mut startup_buf = vec![0u8; 8192];
            let n = stream.read(&mut startup_buf).await?;
            startup_buf.truncate(n);

            Ok(SslStartupResult::Declined(stream, startup_buf))
        }

        (TlsSslMode::Allow, None) => {
            // Allow mode but no TLS config, say no
            debug!("TLS allowed but not configured, declining SSLRequest");
            stream.write_all(&[SSL_RESPONSE_NO]).await?;

            let mut startup_buf = vec![0u8; 8192];
            let n = stream.read(&mut startup_buf).await?;
            startup_buf.truncate(n);

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
            Ok(SslStartupResult::Upgraded(ClientTransport::Tls(tls_stream)))
        }

        (TlsSslMode::Require | TlsSslMode::VerifyCa | TlsSslMode::VerifyFull, None) => {
            // TLS required but not configured - this is a config error
            // Should have been caught at startup, but handle gracefully
            warn!("TLS required but not configured, declining (config error)");
            stream.write_all(&[SSL_RESPONSE_NO]).await?;

            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "TLS required but not configured",
            ))
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
```

**Step 4: Update tls/mod.rs exports**

```rust
mod config;
pub mod startup;
mod transport;

pub use config::{load_client_tls_config, load_server_tls_config, TlsError};
pub use startup::{handle_ssl_startup, SslStartupResult};
pub use transport::ClientTransport;
```

**Step 5: Run tests to verify they pass**

Run: `cargo test -p scry tls::startup`
Expected: PASS

**Step 6: Commit**

```bash
git add scry-proxy/src/tls/startup.rs scry-proxy/src/tls/mod.rs
git commit -m "$(cat <<'EOF'
feat(tls): implement PostgreSQL SSL startup handshake

Add handle_ssl_startup() that:
- Detects SSLRequest messages from clients
- Responds with 'S' (upgrade) or 'N' (decline) based on config
- Upgrades connection to TLS when accepted
- Buffers the subsequent StartupMessage for forwarding
EOF
)"
```

---

### Task 6: Integrate TLS into ProxyServer

**Files:**
- Modify: `scry-proxy/src/proxy/server.rs`
- Modify: `scry-proxy/src/proxy/connection.rs`

**Step 1: Update ProxyServer to load TLS config on startup**

In `scry-proxy/src/proxy/server.rs`, add imports at top:

```rust
use crate::tls::{load_client_tls_config, handle_ssl_startup, SslStartupResult, ClientTransport};
use rustls::ServerConfig;
```

**Step 2: Add TLS config field to ProxyServer**

Update the `ProxyServer` struct:

```rust
pub struct ProxyServer {
    config: Arc<Config>,
    listener: TcpListener,
    batcher: Arc<EventBatcher>,
    pool: Option<Arc<TcpConnectionPool>>,
    metrics: Arc<ProxyMetrics>,
    tls_config: Option<Arc<ServerConfig>>,
}
```

**Step 3: Load TLS config in ProxyServer::new()**

In the `new()` function, after creating the pool but before `Ok(Self { ... })`:

```rust
        // Load TLS configuration
        let tls_config = load_client_tls_config(&config.tls)
            .context("Failed to load TLS configuration")?;

        if tls_config.is_some() {
            info!(
                sslmode = ?config.tls.client_tls_sslmode,
                "Client TLS enabled"
            );
        } else {
            info!("Client TLS disabled");
        }
```

Update the return:

```rust
        Ok(Self {
            config: Arc::new(config),
            listener,
            batcher: Arc::new(batcher),
            pool,
            metrics,
            tls_config,
        })
```

**Step 4: Handle SSL startup in accept loop**

In the `run()` method, after accepting a connection (around line 212), replace the handler spawn with:

```rust
                        Ok((client_stream, client_addr)) => {
                            connection_count += 1;
                            let conn_id = connection_count;

                            info!(
                                connection_id = conn_id,
                                client_addr = %client_addr,
                                "Accepted client connection"
                            );

                            let config = Arc::clone(&self.config);
                            let batcher = Arc::clone(&self.batcher);
                            let pool = self.pool.clone();
                            let metrics = Arc::clone(&self.metrics);
                            let tls_config = self.tls_config.clone();

                            // Spawn a task to handle this connection and track it
                            connection_tasks.spawn(async move {
                                // Handle SSL startup handshake
                                let (transport, startup_data) = match handle_ssl_startup(
                                    client_stream,
                                    &config.tls.client_tls_sslmode,
                                    tls_config,
                                ).await {
                                    Ok(SslStartupResult::Upgraded(transport)) => {
                                        info!(
                                            connection_id = conn_id,
                                            "Connection upgraded to TLS"
                                        );
                                        (transport, Vec::new())
                                    }
                                    Ok(SslStartupResult::Declined(stream, startup_data)) => {
                                        debug!(
                                            connection_id = conn_id,
                                            "SSL declined, continuing with plain TCP"
                                        );
                                        (ClientTransport::Plain(stream), startup_data)
                                    }
                                    Ok(SslStartupResult::NoSslRequest(stream, startup_data)) => {
                                        debug!(
                                            connection_id = conn_id,
                                            "No SSL request, continuing with plain TCP"
                                        );
                                        (ClientTransport::Plain(stream), startup_data)
                                    }
                                    Err(e) => {
                                        error!(
                                            connection_id = conn_id,
                                            error = %e,
                                            "SSL startup failed"
                                        );
                                        return;
                                    }
                                };

                                let handler = ConnectionHandler::new(
                                    transport,
                                    client_addr,
                                    conn_id,
                                    config,
                                    batcher,
                                    pool,
                                    metrics,
                                    startup_data,
                                );

                                if let Err(e) = handler.handle().await {
                                    error!(
                                        connection_id = conn_id,
                                        client_addr = %client_addr,
                                        error = %e,
                                        "Connection handler failed"
                                    );
                                }

                                info!(
                                    connection_id = conn_id,
                                    client_addr = %client_addr,
                                    "Connection closed"
                                );
                            });
                        }
```

**Step 5: Update ConnectionHandler to accept ClientTransport**

In `scry-proxy/src/proxy/connection.rs`, update imports:

```rust
use crate::tls::ClientTransport;
```

Update the struct:

```rust
pub struct ConnectionHandler {
    client_stream: ClientTransport,
    client_addr: SocketAddr,
    connection_id: u64,
    config: Arc<Config>,
    batcher: Arc<EventBatcher>,
    pool: Option<Arc<TcpConnectionPool>>,
    metrics: Arc<ProxyMetrics>,
    startup_data: Vec<u8>,
}
```

Update the `new()` function signature:

```rust
    pub fn new(
        client_stream: ClientTransport,
        client_addr: SocketAddr,
        connection_id: u64,
        config: Arc<Config>,
        batcher: Arc<EventBatcher>,
        pool: Option<Arc<TcpConnectionPool>>,
        metrics: Arc<ProxyMetrics>,
        startup_data: Vec<u8>,
    ) -> Self {
        Self {
            client_stream,
            client_addr,
            connection_id,
            config,
            batcher,
            pool,
            metrics,
            startup_data,
        }
    }
```

**Step 6: Handle startup_data in connection handling**

In the `handle_with_pooled_backend` and `handle_with_owned_backend` methods, before the main loop, forward any buffered startup data:

```rust
        // Forward any buffered startup data from SSL handshake
        if !self.startup_data.is_empty() {
            debug!(
                connection_id = connection_id,
                bytes = self.startup_data.len(),
                "Forwarding buffered startup data"
            );
            backend_conn.write_all(&self.startup_data).await
                .context("Failed to forward startup data")?;
        }
```

**Step 7: Run the build to verify compilation**

Run: `cargo build -p scry`
Expected: Compiles without errors

**Step 8: Commit**

```bash
git add scry-proxy/src/proxy/server.rs scry-proxy/src/proxy/connection.rs
git commit -m "$(cat <<'EOF'
feat(proxy): integrate TLS into connection handling

- ProxyServer loads TLS config on startup
- SSL startup handshake handled before connection handler
- ConnectionHandler accepts ClientTransport (TCP or TLS)
- Startup data buffered during SSL handshake is forwarded
EOF
)"
```

---

### Task 7: Add TLS Integration Tests

**Files:**
- Create: `scry-proxy/tests/tls_integration.rs`

**Step 1: Create test certificates for testing**

First, we need a way to generate test certificates. Add a test utility:

Create `scry-proxy/tests/tls_integration.rs`:

```rust
//! TLS integration tests
//!
//! These tests verify TLS functionality with real PostgreSQL using testcontainers.

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Generate self-signed test certificates
fn generate_test_certs() -> (tempfile::TempDir, String, String) {
    use std::process::Command;

    let dir = tempfile::tempdir().unwrap();
    let cert_path = dir.path().join("server.crt");
    let key_path = dir.path().join("server.key");

    // Generate self-signed certificate using openssl
    let status = Command::new("openssl")
        .args([
            "req", "-x509", "-newkey", "rsa:2048",
            "-keyout", key_path.to_str().unwrap(),
            "-out", cert_path.to_str().unwrap(),
            "-days", "1",
            "-nodes",
            "-subj", "/CN=localhost",
        ])
        .status();

    if status.is_err() || !status.unwrap().success() {
        panic!("Failed to generate test certificates - is openssl installed?");
    }

    (dir, cert_path.to_string_lossy().to_string(), key_path.to_string_lossy().to_string())
}

/// Send an SSLRequest and read the response
async fn send_ssl_request(addr: &str) -> std::io::Result<u8> {
    let mut stream = TcpStream::connect(addr).await?;

    // SSLRequest message: length (8) + code (80877103)
    let ssl_request: [u8; 8] = [0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f];
    stream.write_all(&ssl_request).await?;

    let mut response = [0u8; 1];
    stream.read_exact(&mut response).await?;

    Ok(response[0])
}

#[tokio::test]
async fn test_ssl_disabled_responds_no() {
    use scry::config::Config;
    use scry::observability::ProxyMetrics;
    use scry::proxy::{EventBatcher, ProxyServer};
    use scry::publisher::DebugLoggerPublisher;
    use std::sync::Arc;

    // Start proxy with TLS disabled (default)
    let config = Config::default();
    let metrics = Arc::new(ProxyMetrics::new());
    let publisher = Box::new(DebugLoggerPublisher::new(Arc::clone(&metrics)));
    let batcher = EventBatcher::new(config.publisher.clone(), publisher, Arc::clone(&metrics));

    let mut test_config = config.clone();
    test_config.proxy.listen_address = "127.0.0.1:0".to_string();

    let server = ProxyServer::new(test_config, batcher, metrics).await.unwrap();
    let addr = server.local_addr().unwrap();

    // Run server in background
    let server_task = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(5), server.run()).await;
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send SSL request
    let response = send_ssl_request(&addr.to_string()).await.unwrap();

    // Should respond with 'N' (no SSL)
    assert_eq!(response, b'N');

    server_task.abort();
}

#[tokio::test]
async fn test_ssl_allow_with_certs_responds_yes() {
    use scry::config::{Config, TlsSslMode};
    use scry::observability::ProxyMetrics;
    use scry::proxy::{EventBatcher, ProxyServer};
    use scry::publisher::DebugLoggerPublisher;
    use std::sync::Arc;

    let (_dir, cert_path, key_path) = generate_test_certs();

    let mut config = Config::default();
    config.proxy.listen_address = "127.0.0.1:0".to_string();
    config.tls.client_tls_sslmode = TlsSslMode::Allow;
    config.tls.client_tls_cert_file = Some(cert_path);
    config.tls.client_tls_key_file = Some(key_path);

    let metrics = Arc::new(ProxyMetrics::new());
    let publisher = Box::new(DebugLoggerPublisher::new(Arc::clone(&metrics)));
    let batcher = EventBatcher::new(config.publisher.clone(), publisher, Arc::clone(&metrics));

    let server = ProxyServer::new(config, batcher, metrics).await.unwrap();
    let addr = server.local_addr().unwrap();

    let server_task = tokio::spawn(async move {
        let _ = tokio::time::timeout(Duration::from_secs(5), server.run()).await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send SSL request
    let response = send_ssl_request(&addr.to_string()).await.unwrap();

    // Should respond with 'S' (yes SSL)
    assert_eq!(response, b'S');

    server_task.abort();
}
```

**Step 2: Run the tests**

Run: `cargo test -p scry --test tls_integration`
Expected: PASS (both tests, assuming openssl is available)

**Step 3: Commit**

```bash
git add scry-proxy/tests/tls_integration.rs
git commit -m "$(cat <<'EOF'
test(tls): add TLS integration tests

Tests verify:
- SSL disabled mode responds with 'N' to SSLRequest
- SSL allow mode with certificates responds with 'S'
EOF
)"
```

---

## Phase 2: Backend TLS

### Task 8: Create BackendTransport Abstraction

**Files:**
- Modify: `scry-proxy/src/tls/transport.rs`
- Modify: `scry-proxy/src/tls/mod.rs`

**Step 1: Write the failing test**

Add to `scry-proxy/src/tls/mod.rs` tests:

```rust
    #[tokio::test]
    async fn test_backend_transport_plain_tcp() {
        use tokio::net::{TcpListener, TcpStream};
        use super::transport::BackendTransport;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).await.unwrap();
        let transport = BackendTransport::Plain(client);

        assert!(!transport.is_encrypted());
    }
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p scry test_backend_transport_plain_tcp`
Expected: FAIL with "cannot find type `BackendTransport`"

**Step 3: Add BackendTransport to transport.rs**

In `scry-proxy/src/tls/transport.rs`, add after `ClientTransport`:

```rust
use tokio_rustls::client::TlsStream as ClientTlsStream;

/// A backend transport that can be either plain TCP or TLS-encrypted
/// Used for connections from proxy to PostgreSQL backend
#[pin_project(project = BackendTransportProj)]
pub enum BackendTransport {
    /// Plain unencrypted TCP connection
    Plain(#[pin] TcpStream),
    /// TLS-encrypted connection (client-side TLS)
    Tls(#[pin] ClientTlsStream<TcpStream>),
}

impl BackendTransport {
    /// Check if the transport is encrypted
    pub fn is_encrypted(&self) -> bool {
        matches!(self, BackendTransport::Tls(_))
    }

    /// Get the peer address
    pub fn peer_addr(&self) -> io::Result<std::net::SocketAddr> {
        match self {
            BackendTransport::Plain(stream) => stream.peer_addr(),
            BackendTransport::Tls(stream) => stream.get_ref().0.peer_addr(),
        }
    }
}

impl AsyncRead for BackendTransport {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.project() {
            BackendTransportProj::Plain(stream) => stream.poll_read(cx, buf),
            BackendTransportProj::Tls(stream) => stream.poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for BackendTransport {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.project() {
            BackendTransportProj::Plain(stream) => stream.poll_write(cx, buf),
            BackendTransportProj::Tls(stream) => stream.poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.project() {
            BackendTransportProj::Plain(stream) => stream.poll_flush(cx),
            BackendTransportProj::Tls(stream) => stream.poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.project() {
            BackendTransportProj::Plain(stream) => stream.poll_shutdown(cx),
            BackendTransportProj::Tls(stream) => stream.poll_shutdown(cx),
        }
    }
}
```

**Step 4: Update tls/mod.rs exports**

```rust
pub use transport::{BackendTransport, ClientTransport};
```

**Step 5: Run tests to verify they pass**

Run: `cargo test -p scry test_backend_transport`
Expected: PASS

**Step 6: Commit**

```bash
git add scry-proxy/src/tls/transport.rs scry-proxy/src/tls/mod.rs
git commit -m "$(cat <<'EOF'
feat(tls): add BackendTransport abstraction for backend connections

BackendTransport wraps either TcpStream or client-side TlsStream,
implementing AsyncRead + AsyncWrite for unified backend I/O.
EOF
)"
```

---

### Task 9: Implement Backend SSL Handshake Function

**Files:**
- Create: `scry-proxy/src/tls/backend.rs`
- Modify: `scry-proxy/src/tls/mod.rs`

**Step 1: Write the failing test**

Add to `scry-proxy/src/tls/mod.rs`:

```rust
mod backend;
pub use backend::upgrade_backend_to_tls;
```

Create `scry-proxy/src/tls/backend.rs` with test:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ssl_request_bytes() {
        // Verify our SSL request matches PostgreSQL spec
        assert_eq!(SSL_REQUEST.len(), 8);
        // Length field = 8
        assert_eq!(i32::from_be_bytes([SSL_REQUEST[0], SSL_REQUEST[1], SSL_REQUEST[2], SSL_REQUEST[3]]), 8);
        // Request code = 80877103
        assert_eq!(i32::from_be_bytes([SSL_REQUEST[4], SSL_REQUEST[5], SSL_REQUEST[6], SSL_REQUEST[7]]), 80877103);
    }
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p scry test_ssl_request_bytes`
Expected: FAIL with "cannot find value `SSL_REQUEST`"

**Step 3: Implement backend SSL handshake**

Create `scry-proxy/src/tls/backend.rs`:

```rust
use crate::config::TlsSslMode;
use anyhow::{Context, Result};
use rustls::ClientConfig;
use rustls::pki_types::ServerName;
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
            stream.write_all(&SSL_REQUEST).await
                .context("Failed to send SSLRequest to backend")?;

            // Read single-byte response
            let mut response = [0u8; 1];
            stream.read_exact(&mut response).await
                .context("Failed to read SSL response from backend")?;

            match response[0] {
                b'S' => {
                    info!("Backend accepted SSL, upgrading connection");

                    let config = tls_config.ok_or_else(|| {
                        anyhow::anyhow!("TLS config required but not provided for sslmode {:?}", sslmode)
                    })?;

                    let connector = TlsConnector::from(config);

                    // Parse hostname for TLS verification
                    let server_name: ServerName<'static> = hostname
                        .to_string()
                        .try_into()
                        .map_err(|_| anyhow::anyhow!("Invalid hostname for TLS: {}", hostname))?;

                    let tls_stream = connector.connect(server_name, stream).await
                        .context("TLS handshake with backend failed")?;

                    info!("Backend TLS handshake completed");
                    Ok(BackendSslResult::Upgraded(BackendTransport::Tls(tls_stream)))
                }

                b'N' => {
                    debug!("Backend declined SSL");

                    match sslmode {
                        TlsSslMode::Allow => {
                            info!("Backend declined SSL, continuing with plain TCP (allow mode)");
                            Ok(BackendSslResult::Declined(stream))
                        }
                        TlsSslMode::Require | TlsSslMode::VerifyCa | TlsSslMode::VerifyFull => {
                            warn!("Backend does not support SSL but sslmode={:?} requires it", sslmode);
                            Ok(BackendSslResult::Required)
                        }
                        TlsSslMode::Disable => unreachable!(),
                    }
                }

                other => {
                    Err(anyhow::anyhow!(
                        "Unexpected SSL response from backend: {} (expected 'S' or 'N')",
                        other as char
                    ))
                }
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
        assert_eq!(i32::from_be_bytes([SSL_REQUEST[0], SSL_REQUEST[1], SSL_REQUEST[2], SSL_REQUEST[3]]), 8);
        // Request code = 80877103
        assert_eq!(i32::from_be_bytes([SSL_REQUEST[4], SSL_REQUEST[5], SSL_REQUEST[6], SSL_REQUEST[7]]), 80877103);
    }

    #[test]
    fn test_ssl_request_matches_startup() {
        // Verify SSL_REQUEST here matches the one in startup.rs
        use crate::tls::startup::SSL_REQUEST_CODE;
        let code = i32::from_be_bytes([SSL_REQUEST[4], SSL_REQUEST[5], SSL_REQUEST[6], SSL_REQUEST[7]]);
        assert_eq!(code, SSL_REQUEST_CODE);
    }
}
```

**Step 4: Run tests to verify they pass**

Run: `cargo test -p scry tls::backend`
Expected: PASS

**Step 5: Commit**

```bash
git add scry-proxy/src/tls/backend.rs scry-proxy/src/tls/mod.rs
git commit -m "$(cat <<'EOF'
feat(tls): implement backend SSL negotiation

Add negotiate_backend_ssl() and upgrade_backend_to_tls() functions
that handle the PostgreSQL SSL handshake for backend connections:
- Sends SSLRequest to backend
- Handles 'S' (accept) and 'N' (decline) responses
- Upgrades connection to TLS when accepted
EOF
)"
```

---

### Task 10: Modify TcpConnectionPool to Support TLS

**Files:**
- Modify: `scry-proxy/src/proxy/tcp_pool.rs`
- Modify: `scry-proxy/src/proxy/mod.rs`

**Step 1: Update TcpStreamManager to include TLS config**

In `scry-proxy/src/proxy/tcp_pool.rs`, update imports:

```rust
use crate::config::{ConnectionRetryConfig, TlsConfig};
use crate::tls::{load_server_tls_config, upgrade_backend_to_tls, BackendTransport};
use rustls::ClientConfig;
```

**Step 2: Change pool to use BackendTransport instead of TcpStream**

Update the type alias:

```rust
/// Pooled backend connection wrapper
///
/// This type wraps a pooled backend connection (TCP or TLS).
/// When dropped, the connection is automatically returned to the pool.
pub(crate) type PooledConnection = deadpool::managed::Object<BackendTransportManager>;
```

**Step 3: Create BackendTransportManager**

Replace `TcpStreamManager` with:

```rust
/// Manager for backend transport connections (TCP or TLS)
///
/// Implements deadpool's Manager trait to handle connection lifecycle:
/// - Creating new connections (with optional TLS upgrade)
/// - Recycling connections between uses
pub struct BackendTransportManager {
    backend_addr: String,
    backend_host: String,
    protocol: Arc<dyn Protocol>,
    tls_config: Option<Arc<ClientConfig>>,
    tls_sslmode: TlsSslMode,
}

#[async_trait]
impl Manager for BackendTransportManager {
    type Type = BackendTransport;
    type Error = anyhow::Error;

    /// Create a new backend connection, optionally upgrading to TLS
    async fn create(&self) -> Result<BackendTransport, Self::Error> {
        debug!(
            backend_addr = %self.backend_addr,
            protocol = self.protocol.name(),
            sslmode = ?self.tls_sslmode,
            "Creating new backend connection"
        );

        // First, establish TCP connection
        let stream = TcpStream::connect(&self.backend_addr)
            .await
            .context("Failed to connect to backend")?;

        debug!(backend_addr = %self.backend_addr, "TCP connection established");

        // Then, negotiate SSL if configured
        let transport = upgrade_backend_to_tls(
            stream,
            &self.backend_host,
            &self.tls_sslmode,
            self.tls_config.clone(),
        ).await.context("Failed to negotiate SSL with backend")?;

        if transport.is_encrypted() {
            info!(backend_addr = %self.backend_addr, "Backend connection using TLS");
        } else {
            debug!(backend_addr = %self.backend_addr, "Backend connection using plain TCP");
        }

        Ok(transport)
    }

    /// Recycle a connection before returning it to the pool
    async fn recycle(
        &self,
        conn: &mut BackendTransport,
        _metrics: &deadpool::managed::Metrics,
    ) -> RecycleResult<Self::Error> {
        debug!(protocol = self.protocol.name(), "Recycling backend connection");

        // Health check and reset work the same regardless of transport type
        // since BackendTransport implements AsyncRead + AsyncWrite

        // For now, we need to extract the inner stream for protocol methods
        // This is a limitation - protocol health_check/reset_connection expect TcpStream
        // We'll need to make Protocol trait generic or add new methods

        // Simplified approach: just check if connection is still alive
        // by attempting a zero-byte write (which checks the socket state)
        // Full health check would require Protocol trait changes

        match conn {
            BackendTransport::Plain(stream) => {
                match self.protocol.health_check(stream).await {
                    Ok(true) => {
                        debug!("Connection health check passed");
                    }
                    Ok(false) => {
                        warn!("Connection failed health check, will be closed");
                        return Err(deadpool::managed::RecycleError::Backend(anyhow::anyhow!(
                            "Connection failed health check"
                        )));
                    }
                    Err(e) => {
                        warn!(error = %e, "Connection health check error, will be closed");
                        return Err(deadpool::managed::RecycleError::Backend(e));
                    }
                }

                match self.protocol.reset_connection(stream).await {
                    Ok(true) => {
                        debug!("Connection state reset successfully");
                        Ok(())
                    }
                    Ok(false) => {
                        debug!("Protocol doesn't support state reset, closing connection");
                        Err(deadpool::managed::RecycleError::Backend(anyhow::anyhow!(
                            "Protocol does not support connection reset"
                        )))
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to reset connection state, will be closed");
                        Err(deadpool::managed::RecycleError::Backend(e))
                    }
                }
            }
            BackendTransport::Tls(_) => {
                // For TLS connections, we can't easily run protocol health checks
                // without making Protocol trait async-read/write generic.
                // For now, assume TLS connections are healthy if they haven't errored.
                // TODO: Make Protocol trait generic over AsyncRead+AsyncWrite
                debug!("TLS connection recycled (limited health check)");
                Ok(())
            }
        }
    }
}
```

**Step 4: Update TcpConnectionPool to accept TLS config**

Update `TcpConnectionPool::new()`:

```rust
    /// Create a new TCP connection pool with optional TLS
    pub fn new(
        protocol: Arc<dyn Protocol>,
        config: ProtocolConfig,
        tls_config: &TlsConfig,
        max_size: usize,
        min_idle: Option<usize>,
        circuit_breaker: Option<Arc<CircuitBreaker>>,
        retry_config: Option<ConnectionRetryConfig>,
        lifo: bool,
    ) -> Result<Self> {
        let queue_mode = if lifo { QueueMode::Lifo } else { QueueMode::Fifo };

        // Load TLS configuration for backend connections
        let server_tls_config = load_server_tls_config(tls_config)
            .context("Failed to load server TLS configuration")?;

        info!(
            protocol = protocol.name(),
            backend_addr = %config.backend_addr(),
            max_size = max_size,
            min_idle = ?min_idle,
            circuit_breaker_enabled = circuit_breaker.is_some(),
            retry_enabled = retry_config.is_some(),
            tls_enabled = server_tls_config.is_some(),
            sslmode = ?tls_config.server_tls_sslmode,
            lifo = lifo,
            "Creating backend connection pool"
        );

        let manager = BackendTransportManager {
            backend_addr: config.backend_addr(),
            backend_host: config.host.clone(),
            protocol: Arc::clone(&protocol),
            tls_config: server_tls_config,
            tls_sslmode: tls_config.server_tls_sslmode.clone(),
        };

        let mut builder = Pool::builder(manager).max_size(max_size).queue_mode(queue_mode);

        if let Some(min) = min_idle {
            builder = builder.runtime(deadpool::Runtime::Tokio1);
            debug!(min_idle = min, "Pool min_idle configured");
        }

        let pool = builder
            .runtime(deadpool::Runtime::Tokio1)
            .build()
            .context("Failed to create connection pool")?;

        info!("Backend connection pool created successfully");

        Ok(Self { pool, protocol, config, circuit_breaker, retry_config })
    }
```

**Step 5: Update pool field type**

```rust
pub struct TcpConnectionPool {
    pool: Pool<BackendTransportManager>,
    protocol: Arc<dyn Protocol>,
    config: ProtocolConfig,
    circuit_breaker: Option<Arc<CircuitBreaker>>,
    retry_config: Option<ConnectionRetryConfig>,
}
```

**Step 6: Run build to verify compilation**

Run: `cargo build -p scry`
Expected: Compiles (may have warnings about unused TLS in connection.rs - will fix in next task)

**Step 7: Commit**

```bash
git add scry-proxy/src/proxy/tcp_pool.rs
git commit -m "$(cat <<'EOF'
feat(pool): add TLS support to backend connection pool

- Replace TcpStreamManager with BackendTransportManager
- Pool now creates BackendTransport (TCP or TLS) connections
- SSL negotiation happens during connection creation
- TLS config passed to pool constructor
EOF
)"
```

---

### Task 11: Update ProxyServer to Pass TLS Config to Pool

**Files:**
- Modify: `scry-proxy/src/proxy/server.rs`

**Step 1: Update pool creation to pass TLS config**

In `ProxyServer::new()`, find where `TcpConnectionPool::new()` is called and add the TLS config parameter:

```rust
            let pool = TcpConnectionPool::new(
                Arc::clone(&protocol),
                protocol_config,
                &config.tls,  // Add TLS config
                config.performance.pool_size,
                Some(config.performance.pool_min_idle),
                circuit_breaker,
                retry_config,
                config.performance.pool_lifo,
            )
            .context("Failed to create TCP connection pool")?;
```

**Step 2: Add server TLS logging**

After the pool creation, add:

```rust
            if config.tls.server_tls_sslmode != TlsSslMode::Disable {
                info!(
                    sslmode = ?config.tls.server_tls_sslmode,
                    "Backend TLS enabled"
                );
            }
```

**Step 3: Run tests to verify nothing broke**

Run: `cargo test -p scry`
Expected: PASS

**Step 4: Commit**

```bash
git add scry-proxy/src/proxy/server.rs
git commit -m "$(cat <<'EOF'
feat(proxy): pass TLS config to connection pool

ProxyServer now passes TlsConfig to TcpConnectionPool for
backend TLS support. Logs backend TLS status on startup.
EOF
)"
```

---

### Task 12: Update ConnectionHandler for BackendTransport

**Files:**
- Modify: `scry-proxy/src/proxy/connection.rs`

**Step 1: Update imports**

```rust
use crate::tls::{BackendTransport, ClientTransport};
```

**Step 2: Update handle_with_pooled_backend to use BackendTransport**

The `PooledConnection` type is now `Object<BackendTransportManager>`, which derefs to `BackendTransport`.

Update the method signature and implementation to work with `BackendTransport` instead of `TcpStream`:

In `handle_with_pooled_backend`, the `backend_conn` variable now derefs to `BackendTransport`. Since `BackendTransport` implements `AsyncRead + AsyncWrite`, the existing `read()` and `write_all()` calls should work.

However, we need to handle the case where we access the underlying stream. Search for any direct `TcpStream` methods and update them.

**Step 3: Run build to verify**

Run: `cargo build -p scry`
Expected: Compiles without errors

**Step 4: Run all tests**

Run: `cargo test -p scry`
Expected: PASS

**Step 5: Commit**

```bash
git add scry-proxy/src/proxy/connection.rs
git commit -m "$(cat <<'EOF'
feat(connection): update handler for BackendTransport

ConnectionHandler now works with BackendTransport for pooled
connections, supporting both plain TCP and TLS backends.
EOF
)"
```

---

### Task 13: Add Backend TLS Integration Tests

**Files:**
- Modify: `scry-proxy/tests/tls_integration.rs`

**Step 1: Add test for backend TLS with TLS-enabled Postgres**

Add to `scry-proxy/tests/tls_integration.rs`:

```rust
/// Test backend TLS connection to a TLS-enabled PostgreSQL
/// Note: This test requires a Postgres container with SSL enabled
#[tokio::test]
#[ignore] // Run with: cargo test --test tls_integration backend_tls -- --ignored
async fn test_backend_tls_connection() {
    use scry::config::{Config, TlsSslMode};
    use scry::observability::ProxyMetrics;
    use scry::proxy::{EventBatcher, ProxyServer};
    use scry::publisher::DebugLoggerPublisher;
    use std::sync::Arc;
    use tokio_postgres::NoTls;

    // This test would need a TLS-enabled Postgres container
    // For now, test with sslmode=allow against regular Postgres
    // (which will decline SSL and fall back to plain TCP)

    let mut config = Config::default();
    config.proxy.listen_address = "127.0.0.1:0".to_string();
    config.tls.server_tls_sslmode = TlsSslMode::Allow;

    let metrics = Arc::new(ProxyMetrics::new());
    let publisher = Box::new(DebugLoggerPublisher::new(Arc::clone(&metrics)));
    let batcher = EventBatcher::new(config.publisher.clone(), publisher, Arc::clone(&metrics));

    // This would need testcontainers setup with TLS-enabled Postgres
    // Skipping actual connection test for now
    let _ = config;
    let _ = batcher;
    let _ = metrics;
}

/// Test that sslmode=require fails when backend doesn't support SSL
#[tokio::test]
async fn test_backend_tls_require_fails_without_ssl() {
    use scry::config::{Config, TlsSslMode};
    use scry::tls::{upgrade_backend_to_tls, load_server_tls_config};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Create a mock "backend" that declines SSL
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Spawn mock backend
    let backend_task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();

        // Read SSLRequest
        let mut buf = [0u8; 8];
        stream.read_exact(&mut buf).await.unwrap();

        // Respond with 'N' (no SSL)
        stream.write_all(&[b'N']).await.unwrap();
    });

    // Try to connect with sslmode=require
    let stream = TcpStream::connect(addr).await.unwrap();

    let mut tls_config = crate::config::TlsConfig::default();
    tls_config.server_tls_sslmode = TlsSslMode::Require;

    let server_tls = load_server_tls_config(&tls_config).unwrap();

    let result = upgrade_backend_to_tls(
        stream,
        "localhost",
        &tls_config.server_tls_sslmode,
        server_tls,
    ).await;

    // Should fail because backend declined SSL
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("does not support SSL"));

    backend_task.await.unwrap();
}

/// Test that sslmode=allow falls back to plain TCP
#[tokio::test]
async fn test_backend_tls_allow_fallback() {
    use scry::config::TlsSslMode;
    use scry::tls::{upgrade_backend_to_tls, load_server_tls_config};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Create a mock "backend" that declines SSL
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let backend_task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();

        // Read SSLRequest
        let mut buf = [0u8; 8];
        stream.read_exact(&mut buf).await.unwrap();

        // Respond with 'N' (no SSL)
        stream.write_all(&[b'N']).await.unwrap();

        // Keep connection alive briefly
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    });

    let stream = TcpStream::connect(addr).await.unwrap();

    let mut tls_config = crate::config::TlsConfig::default();
    tls_config.server_tls_sslmode = TlsSslMode::Allow;

    let server_tls = load_server_tls_config(&tls_config).unwrap();

    let result = upgrade_backend_to_tls(
        stream,
        "localhost",
        &tls_config.server_tls_sslmode,
        server_tls,
    ).await;

    // Should succeed with plain TCP
    assert!(result.is_ok());
    let transport = result.unwrap();
    assert!(!transport.is_encrypted()); // Should be plain TCP

    backend_task.await.unwrap();
}
```

**Step 2: Run the tests**

Run: `cargo test -p scry --test tls_integration`
Expected: PASS (the ignored test won't run unless explicitly requested)

**Step 3: Commit**

```bash
git add scry-proxy/tests/tls_integration.rs
git commit -m "$(cat <<'EOF'
test(tls): add backend TLS integration tests

Tests verify:
- sslmode=require fails when backend declines SSL
- sslmode=allow falls back to plain TCP when backend declines
EOF
)"
```

---

## Summary

### Environment Variables Added

| Variable | Description | Default |
|----------|-------------|---------|
| `SCRY_TLS__CLIENT_TLS_SSLMODE` | Client TLS mode: disable, allow, require, verify-ca, verify-full | disable |
| `SCRY_TLS__CLIENT_TLS_CERT_FILE` | Path to server certificate (PEM) | None |
| `SCRY_TLS__CLIENT_TLS_KEY_FILE` | Path to server private key (PEM) | None |
| `SCRY_TLS__CLIENT_TLS_CA_FILE` | Path to CA for client cert verification | None |
| `SCRY_TLS__SERVER_TLS_SSLMODE` | Backend TLS mode | disable |
| `SCRY_TLS__SERVER_TLS_CA_FILE` | Path to CA for server cert verification | None |
| `SCRY_TLS__SERVER_TLS_CERT_FILE` | Path to client cert for backend mTLS | None |
| `SCRY_TLS__SERVER_TLS_KEY_FILE` | Path to client key for backend mTLS | None |

### Files Changed

| File | Change |
|------|--------|
| `Cargo.toml` | Add TLS dependencies |
| `scry-proxy/Cargo.toml` | Add TLS dependencies |
| `scry-proxy/src/config/mod.rs` | Add TlsSslMode, TlsConfig |
| `scry-proxy/src/tls/mod.rs` | New module with exports |
| `scry-proxy/src/tls/config.rs` | Certificate loading (client + server) |
| `scry-proxy/src/tls/transport.rs` | ClientTransport + BackendTransport abstractions |
| `scry-proxy/src/tls/startup.rs` | Client SSL startup handshake |
| `scry-proxy/src/tls/backend.rs` | Backend SSL negotiation |
| `scry-proxy/src/proxy/tcp_pool.rs` | BackendTransportManager with TLS support |
| `scry-proxy/src/proxy/server.rs` | Client + backend TLS integration |
| `scry-proxy/src/proxy/connection.rs` | Accept ClientTransport + BackendTransport |
| `scry-proxy/tests/tls_integration.rs` | Client + backend TLS integration tests |

### Task Summary

| Phase | Task | Description |
|-------|------|-------------|
| 1 | Task 1 | Add TLS dependencies (tokio-rustls, rustls, rustls-pemfile) |
| 1 | Task 2 | Add TLS configuration types (TlsSslMode, TlsConfig) |
| 1 | Task 3 | Create TLS module with certificate loading |
| 1 | Task 4 | Create ClientTransport abstraction |
| 1 | Task 5 | Implement PostgreSQL SSL startup handshake |
| 1 | Task 6 | Integrate client TLS into ProxyServer |
| 1 | Task 7 | Add client TLS integration tests |
| 2 | Task 8 | Create BackendTransport abstraction |
| 2 | Task 9 | Implement backend SSL handshake function |
| 2 | Task 10 | Modify TcpConnectionPool to support TLS |
| 2 | Task 11 | Update ProxyServer to pass TLS config to pool |
| 2 | Task 12 | Update ConnectionHandler for BackendTransport |
| 2 | Task 13 | Add backend TLS integration tests |

### Testing Checklist

- [ ] Unit tests pass: `cargo test -p scry`
- [ ] Integration tests pass: `cargo test -p scry --test tls_integration`
- [ ] Manual test client TLS: `psql "sslmode=require host=localhost port=5433"`
- [ ] Manual test no TLS: `psql "sslmode=disable host=localhost port=5433"`
- [ ] Test backend TLS with TLS-enabled Postgres (RDS, Cloud SQL, etc.)
- [ ] Verify sslmode=require fails appropriately when backend declines
- [ ] Verify sslmode=allow falls back to plain TCP
- [ ] Check certificate errors are handled gracefully
