use tokio::runtime;
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::{Error, Socket};

/// The capability to request cancellation of in-progress queries on a
/// connection.
#[derive(Clone, Debug)]
pub struct CancelToken(tokio_postgres::CancelToken);

impl CancelToken {
    pub(crate) fn new(inner: tokio_postgres::CancelToken) -> CancelToken {
        CancelToken(inner)
    }

    /// Attempts to cancel the in-progress query on the connection associated
    /// with this `CancelToken`.
    ///
    /// The server provides no information about whether a cancellation attempt was successful or not. An error will
    /// only be returned if the client was unable to connect to the database.
    ///
    /// Cancellation is inherently racy. There is no guarantee that the
    /// cancellation request will reach the server before the query terminates
    /// normally, or that the connection associated with this token is still
    /// active.
    pub fn cancel_query<T>(&self, tls: T) -> Result<(), Error>
    where
        T: MakeTlsConnect<Socket>,
    {
        runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap() // FIXME don't unwrap
            .block_on(self.0.cancel_query(tls))
    }
}
