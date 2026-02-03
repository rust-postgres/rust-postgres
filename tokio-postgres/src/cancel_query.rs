use crate::client::SocketConfig;
use crate::config::{SslMode, SslNegotiation};
use crate::tls::MakeTlsConnect;
use crate::{cancel_query_raw, connect_socket, Error, Socket};
use std::io;

pub(crate) async fn cancel_query<T>(
    config: Option<SocketConfig>,
    ssl_mode: SslMode,
    ssl_negotiation: SslNegotiation,
    mut tls: T,
    process_id: i32,
    secret_key: i32,
) -> Result<(), Error>
where
    T: MakeTlsConnect<Socket>,
{
    let config = match config {
        Some(config) => config,
        None => {
            return Err(Error::connect(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unknown host",
            )))
        }
    };

    let tls = tls
        .make_tls_connect(config.hostname.as_deref().unwrap_or(""))
        .map_err(|e| Error::tls(e.into()))?;
    let has_hostname = config.hostname.is_some();

    let socket = connect_socket::connect_socket(
        &config.addr,
        config.port,
        config.connect_timeout,
        config.tcp_user_timeout,
        #[cfg(not(target_arch = "wasm32"))]
        config.keepalive.as_ref(),
    )
    .await?;

    cancel_query_raw::cancel_query_raw(
        socket,
        ssl_mode,
        ssl_negotiation,
        tls,
        has_hostname,
        process_id,
        secret_key,
    )
    .await
}
