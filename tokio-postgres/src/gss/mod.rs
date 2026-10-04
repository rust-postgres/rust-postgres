//! GSSAPI / SSPI (Kerberos) authentication.
//!
//! Enabled by the `gss` feature. With the feature off, `connect_raw` keeps its
//! original refusal of `AuthenticationGSS`/`AuthenticationSSPI` and this module is
//! not compiled at all.
//!
//! ## The wire exchange
//!
//! Postgres carries GSS tokens in the generic `'p'` frame — the same message type
//! password and SASL responses use, but with a RAW, unterminated body:
//!
//! ```text
//! server -> AuthenticationGSS            (or AuthenticationSSPI)
//! client -> 'p'(token_1)
//! server -> AuthenticationGSSContinue(token_2)
//! client -> 'p'(token_3)                 (repeat until the context completes)
//! server -> AuthenticationOk
//! ```
//!
//! "Raw" is the load-bearing word: a GSS token is binary and contains NUL bytes,
//! so the C-string body that `authenticate_password` writes cannot carry one. See
//! `send_token`.
//!
//! The number of round trips is mechanism-specific, so this is a loop, not a
//! fixed handshake. With mutual authentication (which we always request) the
//! client needs the server's reply token before its own context is complete.
//!
//! ## Platforms
//!
//! One trait, two `cfg` implementations, so `connect_raw` sees a single path:
//!
//! * Unix — `libgssapi`, which binds the *system* GSS library (MIT krb5 or
//!   Heimdal). That is the intended behaviour: it is the same library `kinit`
//!   populates, so an existing ticket cache just works.
//! * Windows — SSPI through `windows-sys`, i.e. the OS. No third-party TLS or
//!   crypto is pulled in on either platform.
//!
//! ## Ambient vs explicit credentials
//!
//! Both platforms support two credential sources, and **both are needed**:
//!
//! * *Ambient* — the user's existing ticket cache (`kinit`) or Windows logon
//!   session. This is the domain-joined production case and the default.
//! * *Explicit* — a principal plus password, acquired at connect time. Required
//!   on any machine that is not joined to the realm, where the ambient cache is
//!   simply empty. Verified 2026-09-01: on a workgroup Windows host with the
//!   realm registered via `ksetup`, `klist tickets` reports zero cached tickets
//!   and only the explicit path can authenticate.

use crate::codec::FrontendMessage;
use crate::config::Config;
use crate::connect_raw::StartupStream;
use crate::tls::TlsStream;
use crate::Error;
use bytes::BytesMut;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use tokio::io::{AsyncRead, AsyncWrite};

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

#[cfg(unix)]
use unix as imp;
#[cfg(windows)]
use windows as imp;

mod error;
pub(crate) use error::{GssError, GssFailure};

/// What the caller must send, and whether the context is finished.
pub(crate) struct GssStep {
    /// Token to hand to the server. May be empty on the final step.
    pub token: Vec<u8>,
    /// True once no further server token is needed.
    pub complete: bool,
}

/// Everything a platform needs to build a client context.
pub(crate) struct GssParams<'a> {
    /// `krbsrvname` — the service half of the SPN. Defaults to `postgres`.
    pub service: &'a str,
    /// The host half of the SPN, exactly as the user spelled it.
    pub host: &'a str,
    /// Explicit principal (`user@REALM`), or `None` for ambient credentials.
    pub principal: Option<&'a str>,
    /// Password for `principal`. Ignored when `principal` is `None`.
    pub password: Option<&'a str>,
    /// Unix only: a client keytab to acquire from instead of a password.
    #[cfg_attr(windows, allow(dead_code))]
    pub keytab: Option<&'a str>,
}

impl GssParams<'_> {
    /// The SPN in the canonical `service/host` spelling, for error messages.
    ///
    /// Short-vs-FQDN mismatch here is the single most common Kerberos
    /// misconfiguration, which is why every failure repeats this back.
    pub fn spn(&self) -> String {
        format!("{}/{}", self.service, self.host)
    }
}

/// A platform client context. One impl compiles per target.
pub(crate) trait GssClientContext: Sized {
    /// Acquire credentials and prepare (but do not start) the context.
    fn new(params: &GssParams<'_>) -> Result<Self, GssError>;

    /// Feed the server's token (`None` on the first call) and get ours back.
    fn step(&mut self, input: Option<&[u8]>) -> Result<GssStep, GssError>;
}

/// Run the GSS exchange to completion.
///
/// Returns `true` when the server's `AuthenticationOk` has **already been
/// consumed** here, so `authenticate` must not read it again. Normally this is
/// `false`: the client's context completes first and the trailing
/// `AuthenticationOk` is left for `authenticate`'s own tail — which is what
/// keeps the change to that function a two-line one.
pub(crate) async fn authenticate_gss<S, T>(
    stream: &mut StartupStream<S, T>,
    config: &Config,
    hostname: Option<&str>,
) -> Result<bool, Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: TlsStream + Unpin,
{
    // Without a hostname there is no SPN to target. This happens on
    // `Config::connect_raw`, which takes an already-connected stream of unknown
    // origin; say so plainly rather than building `postgres/`.
    let host = match hostname {
        Some(host) if !host.is_empty() => host,
        _ => {
            return Err(Error::authentication(Box::new(GssError::no_hostname())));
        }
    };

    let params = GssParams {
        service: config.krbsrvname.as_deref().unwrap_or("postgres"),
        host,
        principal: config.gss_principal.as_deref(),
        password: config.gss_password.as_deref(),
        keytab: config.gss_keytab.as_deref(),
    };

    let mut ctx = imp::ClientContext::new(&params)
        .map_err(|e| Error::authentication(Box::new(e.with_spn(&params))))?;

    let mut input: Option<Vec<u8>> = None;
    loop {
        let step = ctx
            .step(input.as_deref())
            .map_err(|e| Error::authentication(Box::new(e.with_spn(&params))))?;

        if !step.token.is_empty() {
            send_token(stream, &step.token).await?;
        }

        if step.complete {
            // The server's AuthenticationOk is still unread; leave it.
            return Ok(false);
        }

        match stream_next(stream).await? {
            Some(Message::AuthenticationGssContinue(body)) => {
                input = Some(body.data().to_vec());
            }
            // A server can declare success while our context still expects
            // another token — legal, and it means we consumed the Ok.
            Some(Message::AuthenticationOk) => return Ok(true),
            Some(Message::ErrorResponse(body)) => return Err(Error::db(body)),
            Some(_) => return Err(Error::unexpected_message()),
            None => return Err(Error::closed()),
        }
    }
}

/// Send one GSS token to the server.
///
/// **Not `authenticate_password`.** That builds its body with `write_cstr`, which
/// NUL-terminates and rejects an embedded NUL — and a GSS token is binary, so it
/// is full of them. Using it fails every real handshake with `string contains
/// embedded null` before a single byte reaches the server. (Found exactly that
/// way: it passes anything that does not run a live exchange.)
///
/// `frontend::sasl_response` is the right builder despite the name: it writes
/// message type `'p'` with a raw, unterminated body, which is byte-for-byte what
/// libpq sends for a GSS token (`pqPutMsgStart('p')` then `pqPutnchar`). The `'p'`
/// frame is generic — password, SASL and GSS all share it.
async fn send_token<S, T>(
    stream: &mut StartupStream<S, T>,
    token: &[u8],
) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: TlsStream + Unpin,
{
    use futures_util::SinkExt;
    let mut buf = BytesMut::new();
    frontend::sasl_response(token, &mut buf).map_err(Error::encode)?;
    stream
        .send(FrontendMessage::Raw(buf.freeze()))
        .await
        .map_err(Error::io)
}

async fn stream_next<S, T>(stream: &mut StartupStream<S, T>) -> Result<Option<Message>, Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: TlsStream + Unpin,
{
    use futures_util::TryStreamExt;
    stream.try_next().await.map_err(Error::io)
}
