use crate::codec::{BackendMessage, BackendMessages, FrontendMessage, PostgresCodec};
use crate::config::{self, Config};
use crate::connect_tls::connect_tls;
use crate::maybe_tls_stream::MaybeTlsStream;
use crate::tls::{TlsConnect, TlsStream};
use crate::{Client, Connection, Error};
use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use futures_channel::mpsc;
use futures_util::{Sink, SinkExt, Stream, TryStreamExt};
use postgres_protocol::authentication;
use postgres_protocol::authentication::sasl;
use postgres_protocol::authentication::sasl::{OAuthBearer, ScramSha256};
use postgres_protocol::message::backend::{AuthenticationSaslBody, Message};
use postgres_protocol::message::frontend;
use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

pub struct StartupStream<S, T> {
    inner: Framed<MaybeTlsStream<S, T>, PostgresCodec>,
    buf: BackendMessages,
    delayed: VecDeque<BackendMessage>,
}

impl<S, T> Sink<FrontendMessage> for StartupStream<S, T>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    type Error = io::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: FrontendMessage) -> io::Result<()> {
        Pin::new(&mut self.inner).start_send(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_close(cx)
    }
}

impl<S, T> Stream for StartupStream<S, T>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    type Item = io::Result<Message>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<io::Result<Message>>> {
        loop {
            match self.buf.next() {
                Ok(Some(message)) => return Poll::Ready(Some(Ok(message))),
                Ok(None) => {}
                Err(e) => return Poll::Ready(Some(Err(e))),
            }

            match ready!(Pin::new(&mut self.inner).poll_next(cx)) {
                Some(Ok(BackendMessage::Normal { messages, .. })) => self.buf = messages,
                Some(Ok(BackendMessage::Async(message))) => return Poll::Ready(Some(Ok(message))),
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                None => return Poll::Ready(None),
            }
        }
    }
}

pub async fn connect_raw<S, T>(
    stream: S,
    tls: T,
    has_hostname: bool,
    config: &Config,
) -> Result<(Client, Connection<S, T::Stream>), Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: TlsConnect<S>,
{
    let stream = connect_tls(
        stream,
        config.ssl_mode,
        config.ssl_negotiation,
        tls,
        has_hostname,
    )
    .await?;

    let mut stream = StartupStream {
        inner: Framed::new(stream, PostgresCodec),
        buf: BackendMessages::empty(),
        delayed: VecDeque::new(),
    };

    let user = match config.user.as_deref() {
        Some(user) => Cow::Borrowed(user),
        None => Cow::Owned(whoami::username().map_err(|err| Error::io(err.into()))?),
    };

    startup(&mut stream, config, &user).await?;
    authenticate(&mut stream, config, &user).await?;
    let (process_id, secret_key, parameters) = read_info(&mut stream).await?;

    let (sender, receiver) = mpsc::unbounded();
    let client = Client::new(
        sender,
        config.ssl_mode,
        config.ssl_negotiation,
        process_id,
        secret_key,
    );
    let connection = Connection::new(stream.inner, stream.delayed, parameters, receiver);

    Ok((client, connection))
}

async fn startup<S, T>(
    stream: &mut StartupStream<S, T>,
    config: &Config,
    user: &str,
) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut params = vec![("client_encoding", "UTF8")];
    params.push(("user", user));
    if let Some(dbname) = &config.dbname {
        params.push(("database", &**dbname));
    }
    if let Some(options) = &config.options {
        params.push(("options", &**options));
    }
    if let Some(application_name) = &config.application_name {
        params.push(("application_name", &**application_name));
    }

    let mut buf = BytesMut::new();
    frontend::startup_message(params, &mut buf).map_err(Error::encode)?;

    stream
        .send(FrontendMessage::Raw(buf.freeze()))
        .await
        .map_err(Error::io)
}

async fn authenticate<S, T>(
    stream: &mut StartupStream<S, T>,
    config: &Config,
    user: &str,
) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: TlsStream + Unpin,
{
    match stream.try_next().await.map_err(Error::io)? {
        Some(Message::AuthenticationOk) => {
            can_skip_channel_binding(config)?;
            return Ok(());
        }
        Some(Message::AuthenticationCleartextPassword) => {
            can_skip_channel_binding(config)?;

            let pass = config
                .password
                .as_ref()
                .ok_or_else(|| Error::config("password missing".into()))?;

            authenticate_password(stream, pass).await?;
        }
        Some(Message::AuthenticationMd5Password(body)) => {
            can_skip_channel_binding(config)?;

            let pass = config
                .password
                .as_ref()
                .ok_or_else(|| Error::config("password missing".into()))?;

            let output = authentication::md5_hash(user.as_bytes(), pass, body.salt());
            authenticate_password(stream, output.as_bytes()).await?;
        }
        Some(Message::AuthenticationSasl(body)) => {
            match authenticate_sasl(stream, body, config).await? {
                SaslProgress::Finished => {}
                SaslProgress::Authenticated => return Ok(()),
            }
        }
        Some(Message::AuthenticationKerberosV5)
        | Some(Message::AuthenticationScmCredential)
        | Some(Message::AuthenticationGss)
        | Some(Message::AuthenticationSspi) => {
            return Err(Error::authentication(
                "unsupported authentication method".into(),
            ));
        }
        Some(Message::ErrorResponse(body)) => return Err(Error::db(body)),
        Some(_) => return Err(Error::unexpected_message()),
        None => return Err(Error::closed()),
    }

    match stream.try_next().await.map_err(Error::io)? {
        Some(Message::AuthenticationOk) => Ok(()),
        Some(Message::ErrorResponse(body)) => Err(Error::db(body)),
        Some(_) => Err(Error::unexpected_message()),
        None => Err(Error::closed()),
    }
}

fn can_skip_channel_binding(config: &Config) -> Result<(), Error> {
    match config.channel_binding {
        config::ChannelBinding::Disable | config::ChannelBinding::Prefer => Ok(()),
        config::ChannelBinding::Require => Err(Error::authentication(
            "server did not use channel binding".into(),
        )),
    }
}

async fn authenticate_password<S, T>(
    stream: &mut StartupStream<S, T>,
    password: &[u8],
) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut buf = BytesMut::new();
    frontend::password_message(password, &mut buf).map_err(Error::encode)?;

    stream
        .send(FrontendMessage::Raw(buf.freeze()))
        .await
        .map_err(Error::io)
}

/// How far a SASL mechanism got before returning.
enum SaslProgress {
    /// The mechanism exchange is over; the caller reads the server's final message.
    Finished,
    /// The mechanism read `AuthenticationOk` itself; authentication is complete.
    Authenticated,
}

async fn authenticate_sasl<S, T>(
    stream: &mut StartupStream<S, T>,
    body: AuthenticationSaslBody,
    config: &Config,
) -> Result<SaslProgress, Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: TlsStream + Unpin,
{
    let mut has_scram = false;
    let mut has_scram_plus = false;
    let mut has_oauthbearer = false;
    let mut mechanisms = body.mechanisms();
    while let Some(mechanism) = mechanisms.next().map_err(Error::parse)? {
        match mechanism {
            sasl::SCRAM_SHA_256 => has_scram = true,
            sasl::SCRAM_SHA_256_PLUS => has_scram_plus = true,
            sasl::OAUTHBEARER => has_oauthbearer = true,
            _ => {}
        }
    }

    // the server's `oauth` and `scram-sha-256` HBA methods advertise disjoint mechanism lists, so
    // SCRAM is preferred where both somehow appear rather than a password configuration being
    // overridden by a token one
    if has_scram || has_scram_plus {
        return authenticate_scram(stream, has_scram, has_scram_plus, config).await;
    }

    if has_oauthbearer {
        return authenticate_oauthbearer(stream, config).await;
    }

    Err(Error::authentication("unsupported SASL mechanism".into()))
}

async fn authenticate_scram<S, T>(
    stream: &mut StartupStream<S, T>,
    has_scram: bool,
    has_scram_plus: bool,
    config: &Config,
) -> Result<SaslProgress, Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: TlsStream + Unpin,
{
    let password = config
        .password
        .as_ref()
        .ok_or_else(|| Error::config("password missing".into()))?;

    let channel_binding = stream
        .inner
        .get_ref()
        .channel_binding()
        .tls_server_end_point
        .filter(|_| config.channel_binding != config::ChannelBinding::Disable)
        .map(sasl::ChannelBinding::tls_server_end_point);

    let (channel_binding, mechanism) = if has_scram_plus {
        match channel_binding {
            Some(channel_binding) => (channel_binding, sasl::SCRAM_SHA_256_PLUS),
            None => (sasl::ChannelBinding::unsupported(), sasl::SCRAM_SHA_256),
        }
    } else if has_scram {
        match channel_binding {
            Some(_) => (sasl::ChannelBinding::unrequested(), sasl::SCRAM_SHA_256),
            None => (sasl::ChannelBinding::unsupported(), sasl::SCRAM_SHA_256),
        }
    } else {
        return Err(Error::authentication("unsupported SASL mechanism".into()));
    };

    if mechanism != sasl::SCRAM_SHA_256_PLUS {
        can_skip_channel_binding(config)?;
    }

    let mut scram = ScramSha256::new(password, channel_binding);

    let mut buf = BytesMut::new();
    frontend::sasl_initial_response(mechanism, scram.message(), &mut buf).map_err(Error::encode)?;
    stream
        .send(FrontendMessage::Raw(buf.freeze()))
        .await
        .map_err(Error::io)?;

    let body = match stream.try_next().await.map_err(Error::io)? {
        Some(Message::AuthenticationSaslContinue(body)) => body,
        Some(Message::ErrorResponse(body)) => return Err(Error::db(body)),
        Some(_) => return Err(Error::unexpected_message()),
        None => return Err(Error::closed()),
    };

    scram
        .update(body.data())
        .map_err(|e| Error::authentication(e.into()))?;

    let mut buf = BytesMut::new();
    frontend::sasl_response(scram.message(), &mut buf).map_err(Error::encode)?;
    stream
        .send(FrontendMessage::Raw(buf.freeze()))
        .await
        .map_err(Error::io)?;

    let body = match stream.try_next().await.map_err(Error::io)? {
        Some(Message::AuthenticationSaslFinal(body)) => body,
        Some(Message::ErrorResponse(body)) => return Err(Error::db(body)),
        Some(_) => return Err(Error::unexpected_message()),
        None => return Err(Error::closed()),
    };

    scram
        .finish(body.data())
        .map_err(|e| Error::authentication(e.into()))?;

    Ok(SaslProgress::Finished)
}

async fn authenticate_oauthbearer<S, T>(
    stream: &mut StartupStream<S, T>,
    config: &Config,
) -> Result<SaslProgress, Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: TlsStream + Unpin,
{
    // OAUTHBEARER defines no channel binding, so `channel_binding=require` cannot be satisfied
    can_skip_channel_binding(config)?;

    let token_provider = config
        .token_provider
        .as_ref()
        .ok_or_else(|| Error::config("token provider missing".into()))?;

    let token = token_provider
        .get_token()
        .await
        .map_err(Error::authentication)?;

    let mut buf = BytesMut::new();
    frontend::sasl_initial_response(
        sasl::OAUTHBEARER,
        OAuthBearer::new(&token).message(),
        &mut buf,
    )
    .map_err(Error::encode)?;
    stream
        .send(FrontendMessage::Raw(buf.freeze()))
        .await
        .map_err(Error::io)?;

    match stream.try_next().await.map_err(Error::io)? {
        // the token was accepted. OAUTHBEARER carries no additional data on success, so there is no
        // `AuthenticationSASLFinal` message and this is the end of authentication
        Some(Message::AuthenticationOk) => Ok(SaslProgress::Authenticated),
        // the server only challenges when it has decided to fail the exchange; the challenge holds
        // a JSON error document. RFC 7628, Sec. 3.2.3 requires a dummy response, and sending it is
        // what gets the server to report the failure as an `ErrorResponse` the caller then reads
        Some(Message::AuthenticationSaslContinue(_)) => {
            let mut buf = BytesMut::new();
            frontend::sasl_response(OAuthBearer::ERROR_RESPONSE, &mut buf)
                .map_err(Error::encode)?;
            stream
                .send(FrontendMessage::Raw(buf.freeze()))
                .await
                .map_err(Error::io)?;

            Ok(SaslProgress::Finished)
        }
        Some(Message::ErrorResponse(body)) => Err(Error::db(body)),
        Some(_) => Err(Error::unexpected_message()),
        None => Err(Error::closed()),
    }
}

async fn read_info<S, T>(
    stream: &mut StartupStream<S, T>,
) -> Result<(i32, i32, HashMap<String, String>), Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    let mut process_id = 0;
    let mut secret_key = 0;
    let mut parameters = HashMap::new();

    loop {
        match stream.try_next().await.map_err(Error::io)? {
            Some(Message::BackendKeyData(body)) => {
                process_id = body.process_id();
                secret_key = body.secret_key();
            }
            Some(Message::ParameterStatus(body)) => {
                parameters.insert(
                    body.name().map_err(Error::parse)?.to_string(),
                    body.value().map_err(Error::parse)?.to_string(),
                );
            }
            Some(msg @ Message::NoticeResponse(_)) => {
                stream.delayed.push_back(BackendMessage::Async(msg))
            }
            Some(Message::ReadyForQuery(_)) => return Ok((process_id, secret_key, parameters)),
            Some(Message::ErrorResponse(body)) => return Err(Error::db(body)),
            Some(_) => return Err(Error::unexpected_message()),
            None => return Err(Error::closed()),
        }
    }
}
