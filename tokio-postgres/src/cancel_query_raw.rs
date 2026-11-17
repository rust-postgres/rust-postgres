use crate::config::{SslMode, SslNegotiation};
use crate::tls::TlsConnect;
use crate::{Error, connect_tls};
use bytes::BytesMut;
use postgres_protocol::message::frontend;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub async fn cancel_query_raw<S, T>(
    stream: S,
    mode: SslMode,
    negotiation: SslNegotiation,
    tls: T,
    has_hostname: bool,
    process_id: i32,
    secret_key: i32,
) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: TlsConnect<S>,
{
    let mut stream = connect_tls::connect_tls(stream, mode, negotiation, tls, has_hostname).await?;

    let mut buf = BytesMut::new();
    frontend::cancel_request(process_id, secret_key, &mut buf);

    stream.write_all(&buf).await.map_err(Error::io)?;
    stream.flush().await.map_err(Error::io)?;
    stream.shutdown().await.map_err(Error::io)?;

    // The server closes the cancellation connection after processing the request.
    if stream.read(&mut [0]).await.map_err(Error::io)? != 0 {
        return Err(Error::unexpected_message());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NoTls;
    use futures_util::poll;
    use tokio::io::duplex;

    #[tokio::test]
    async fn waits_for_server_to_close_cancel_connection() {
        let (client, mut server) = duplex(64);
        let cancel = cancel_query_raw(
            client,
            SslMode::Disable,
            SslNegotiation::Postgres,
            NoTls,
            false,
            42,
            13,
        );
        tokio::pin!(cancel);
        assert!(poll!(&mut cancel).is_pending());

        let mut request = Vec::new();
        server.read_to_end(&mut request).await.unwrap();
        assert_eq!(request.len(), 16);
        assert!(poll!(&mut cancel).is_pending());
        drop(server);
        cancel.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_an_unexpected_cancel_response() {
        let (client, mut server) = duplex(64);
        server.write_all(b"X").await.unwrap();
        assert!(
            cancel_query_raw(
                client,
                SslMode::Disable,
                SslNegotiation::Postgres,
                NoTls,
                false,
                42,
                13,
            )
            .await
            .is_err()
        );
    }
}
