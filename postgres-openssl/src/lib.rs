//! TLS support for `tokio-postgres` and `postgres` via `openssl`.
//!
//! # Examples
//!
//! ```no_run
//! use openssl::ssl::{SslConnector, SslMethod};
//! # #[cfg(feature = "runtime")]
//! use postgres_openssl::MakeTlsConnector;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # #[cfg(feature = "runtime")] {
//! let mut builder = SslConnector::builder(SslMethod::tls())?;
//! builder.set_ca_file("database_cert.pem")?;
//! let connector = MakeTlsConnector::new(builder.build());
//!
//! let connect_future = tokio_postgres::connect(
//!     "host=localhost user=postgres sslmode=require",
//!     connector,
//! );
//! # }
//!
//! // ...
//! # Ok(())
//! # }
//! ```
//!
//! ```no_run
//! use openssl::ssl::{SslConnector, SslMethod};
//! # #[cfg(feature = "runtime")]
//! use postgres_openssl::MakeTlsConnector;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # #[cfg(feature = "runtime")] {
//! let mut builder = SslConnector::builder(SslMethod::tls())?;
//! builder.set_ca_file("database_cert.pem")?;
//! let connector = MakeTlsConnector::new(builder.build());
//!
//! let client = postgres::Client::connect(
//!     "host=localhost user=postgres sslmode=require",
//!     connector,
//! )?;
//! # }
//!
//! // ...
//! # Ok(())
//! # }
//! ```
#![warn(rust_2018_idioms, clippy::all, missing_docs)]

#[cfg(feature = "runtime")]
use openssl::error::ErrorStack;
use openssl::hash::MessageDigest;
use openssl::nid::Nid;
#[cfg(feature = "runtime")]
use openssl::ssl::SslConnector;
use openssl::ssl::{self, ConnectConfiguration, SslConnectorBuilder, SslRef};
use openssl::x509::X509VerifyResult;
use std::error::Error;
use std::fmt::{self, Debug};
use std::future::Future;
use std::io;
use std::pin::Pin;
#[cfg(feature = "runtime")]
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, BufReader, ReadBuf};
use tokio_openssl::SslStream;
use tokio_postgres::tls;
#[cfg(feature = "runtime")]
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::tls::{ChannelBinding, TlsConnect};

#[cfg(test)]
mod test;

type ConfigCallback =
    dyn Fn(&mut ConnectConfiguration, &str) -> Result<(), ErrorStack> + Sync + Send;

/// A `MakeTlsConnect` implementation using the `openssl` crate.
///
/// Requires the `runtime` Cargo feature (enabled by default).
#[cfg(feature = "runtime")]
#[derive(Clone)]
pub struct MakeTlsConnector {
    connector: SslConnector,
    config: Arc<ConfigCallback>,
}

impl Debug for MakeTlsConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MakeTlsConnector")
        .field("connector", &self.connector)
        .finish()
    }
}

#[cfg(feature = "runtime")]
impl MakeTlsConnector {
    /// Creates a new connector.
    pub fn new(connector: SslConnector) -> MakeTlsConnector {
        MakeTlsConnector {
            connector,
            config: Arc::new(|_, _| Ok(())),
        }
    }

    /// Sets a callback used to apply per-connection configuration.
    ///
    /// The the callback is provided the domain name along with the `ConnectConfiguration`.
    pub fn set_callback<F>(&mut self, f: F)
    where
        F: Fn(&mut ConnectConfiguration, &str) -> Result<(), ErrorStack> + 'static + Sync + Send,
    {
        self.config = Arc::new(f);
    }
}

#[cfg(feature = "runtime")]
impl<S> MakeTlsConnect<S> for MakeTlsConnector
where
    S: AsyncRead + AsyncWrite + Unpin + Debug + 'static + Sync + Send,
{
    type Stream = TlsStream<S>;
    type TlsConnect = TlsConnector;
    type Error = ErrorStack;

    fn make_tls_connect(&mut self, domain: &str) -> Result<TlsConnector, ErrorStack> {
        let mut ssl = self.connector.configure()?;
        (self.config)(&mut ssl, domain)?;
        Ok(TlsConnector::new(ssl, domain))
    }
}

/// A `TlsConnect` implementation using the `openssl` crate.
pub struct TlsConnector {
    ssl: ConnectConfiguration,
    domain: String,
}

impl TlsConnector {
    /// Creates a new connector configured to connect to the specified domain.
    pub fn new(ssl: ConnectConfiguration, domain: &str) -> TlsConnector {
        TlsConnector {
            ssl,
            domain: domain.to_string(),
        }
    }
}

impl<S> TlsConnect<S> for TlsConnector
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = TlsStream<S>;
    type Error = Box<dyn Error + Send + Sync>;
    #[allow(clippy::type_complexity)]
    type Future = Pin<Box<dyn Future<Output = Result<TlsStream<S>, Self::Error>> + Send>>;

    fn connect(self, stream: S) -> Self::Future {
        let stream = BufReader::with_capacity(8192, stream);
        let future = async move {
            let ssl = self.ssl.into_ssl(&self.domain)?;
            let mut stream = SslStream::new(ssl, stream)?;
            match Pin::new(&mut stream).connect().await {
                Ok(()) => Ok(TlsStream(stream)),
                Err(error) => Err(Box::new(ConnectError {
                    error,
                    verify_result: stream.ssl().verify_result(),
                }) as _),
            }
        };

        Box::pin(future)
    }
}

#[derive(Debug)]
struct ConnectError {
    error: ssl::Error,
    verify_result: X509VerifyResult,
}

impl fmt::Display for ConnectError {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.error, fmt)?;

        if self.verify_result != X509VerifyResult::OK {
            fmt.write_str(": ")?;
            fmt::Display::fmt(&self.verify_result, fmt)?;
        }

        Ok(())
    }
}

impl Error for ConnectError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.error)
    }
}

/// The stream returned by `TlsConnector`.
pub struct TlsStream<S>(SslStream<BufReader<S>>);

impl<S> AsyncRead for TlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl<S> AsyncWrite for TlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

impl<S> tls::TlsStream for TlsStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn channel_binding(&self) -> ChannelBinding {
        match tls_server_end_point(self.0.ssl()) {
            Some(buf) => ChannelBinding::tls_server_end_point(buf),
            None => ChannelBinding::none(),
        }
    }
}

fn tls_server_end_point(ssl: &SslRef) -> Option<Vec<u8>> {
    let cert = ssl.peer_certificate()?;
    let algo_nid = cert.signature_algorithm().object().nid();
    let signature_algorithms = algo_nid.signature_algorithms()?;
    let md = match signature_algorithms.digest {
        Nid::MD5 | Nid::SHA1 => MessageDigest::sha256(),
        nid => MessageDigest::from_nid(nid)?,
    };
    cert.digest(md).ok().map(|b| b.to_vec())
}

/// Set ALPN for `SslConnectorBuilder`
///
/// This is required when using `sslnegotiation=direct`
pub fn set_postgresql_alpn(builder: &mut SslConnectorBuilder) -> Result<(), ErrorStack> {
    builder.set_alpn_protos(b"\x0apostgresql")
}
