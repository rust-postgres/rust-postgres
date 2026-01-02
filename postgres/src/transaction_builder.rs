use crate::connection::ConnectionRef;
use crate::{Error, IsolationLevel, Transaction};

/// A builder for database transactions.
#[derive(Debug)]
pub struct TransactionBuilder<'a> {
    connection: ConnectionRef<'a>,
    builder: tokio_postgres::TransactionBuilder<'a>,
}

impl<'a> TransactionBuilder<'a> {
    pub(crate) fn new(
        connection: ConnectionRef<'a>,
        builder: tokio_postgres::TransactionBuilder<'a>,
    ) -> TransactionBuilder<'a> {
        TransactionBuilder {
            connection,
            builder,
        }
    }

    /// Sets the isolation level of the transaction.
    pub fn isolation_level(mut self, isolation_level: IsolationLevel) -> Self {
        self.builder = self.builder.isolation_level(isolation_level);
        self
    }

    /// Sets the access mode of the transaction.
    pub fn read_only(mut self, read_only: bool) -> Self {
        self.builder = self.builder.read_only(read_only);
        self
    }

    /// Sets the deferrability of the transaction.
    ///
    /// If the transaction is also serializable and read only, creation of the transaction may block, but when it
    /// completes the transaction is able to run with less overhead and a guarantee that it will not be aborted due to
    /// serialization failure.
    pub fn deferrable(mut self, deferrable: bool) -> Self {
        self.builder = self.builder.deferrable(deferrable);
        self
    }

    /// Begins the transaction.
    ///
    /// The transaction will roll back by default - use the `commit` method to commit it.
    pub fn start(mut self) -> Result<Transaction<'a>, Error> {
        let transaction = self.connection.block_on(self.builder.start())?;
        Ok(Transaction::new(self.connection, transaction))
    }
}
