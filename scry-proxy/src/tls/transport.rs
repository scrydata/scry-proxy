use pin_project::pin_project;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio_rustls::client::TlsStream as ClientTlsStream;
use tokio_rustls::server::TlsStream;

/// A client transport that can be plain TCP, TLS-encrypted, or UNIX socket
#[pin_project(project = ClientTransportProj)]
pub enum ClientTransport {
    /// Plain unencrypted TCP connection
    Plain(#[pin] TcpStream),
    /// TLS-encrypted connection (boxed to reduce enum size variance)
    Tls(#[pin] Box<TlsStream<TcpStream>>),
    /// UNIX socket connection (Unix platforms only)
    #[cfg(unix)]
    Unix(#[pin] UnixStream),
}

impl ClientTransport {
    /// Check if the transport is encrypted
    pub fn is_encrypted(&self) -> bool {
        matches!(self, ClientTransport::Tls(_))
    }

    /// Check if this is a UNIX socket connection
    #[cfg(unix)]
    pub fn is_unix(&self) -> bool {
        matches!(self, ClientTransport::Unix(_))
    }

    /// Get the peer address (returns error for UNIX sockets)
    pub fn peer_addr(&self) -> io::Result<std::net::SocketAddr> {
        match self {
            ClientTransport::Plain(stream) => stream.peer_addr(),
            ClientTransport::Tls(stream) => stream.get_ref().0.peer_addr(),
            #[cfg(unix)]
            ClientTransport::Unix(_) => Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "UNIX sockets don't have peer address",
            )),
        }
    }

    /// Returns true if the peer presented a certificate that rustls accepted
    /// during the TLS handshake.
    ///
    /// `rustls` only populates `peer_certificates()` when the client sent a
    /// certificate chain that the configured client-certificate verifier
    /// accepted, so a non-empty chain here means a verified client certificate
    /// was actually presented. Non-TLS transports (plain TCP, UNIX socket)
    /// always return `false`. Used by cert auth as a fail-closed defense-in-depth
    /// check (P1 §4.1) before trusting the connection identity.
    pub fn has_verified_peer_cert(&self) -> bool {
        match self {
            ClientTransport::Tls(stream) => {
                stream.get_ref().1.peer_certificates().is_some_and(|certs| !certs.is_empty())
            }
            ClientTransport::Plain(_) => false,
            #[cfg(unix)]
            ClientTransport::Unix(_) => false,
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
            #[cfg(unix)]
            ClientTransportProj::Unix(stream) => stream.poll_read(cx, buf),
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
            #[cfg(unix)]
            ClientTransportProj::Unix(stream) => stream.poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.project() {
            ClientTransportProj::Plain(stream) => stream.poll_flush(cx),
            ClientTransportProj::Tls(stream) => stream.poll_flush(cx),
            #[cfg(unix)]
            ClientTransportProj::Unix(stream) => stream.poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.project() {
            ClientTransportProj::Plain(stream) => stream.poll_shutdown(cx),
            ClientTransportProj::Tls(stream) => stream.poll_shutdown(cx),
            #[cfg(unix)]
            ClientTransportProj::Unix(stream) => stream.poll_shutdown(cx),
        }
    }
}

/// A backend transport that can be either plain TCP or TLS-encrypted
/// Used for connections from proxy to PostgreSQL backend
#[pin_project(project = BackendTransportProj)]
pub enum BackendTransport {
    /// Plain unencrypted TCP connection
    Plain(#[pin] TcpStream),
    /// TLS-encrypted connection (client-side TLS, boxed to reduce enum size variance)
    Tls(#[pin] Box<ClientTlsStream<TcpStream>>),
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    /// A plain (unencrypted) client transport never counts as having presented
    /// a verified client certificate — cert auth must fail closed on it.
    #[tokio::test]
    async fn plain_transport_has_no_verified_peer_cert() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            stream
        });
        let _client = TcpStream::connect(addr).await.unwrap();
        let server = accept.await.unwrap();

        let transport = ClientTransport::Plain(server);
        assert!(
            !transport.has_verified_peer_cert(),
            "plain TCP transport must not report a verified client certificate"
        );
    }
}
