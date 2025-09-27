use crate::connection::ConnectionRef;
use crate::{CancelToken, CopyInWriter, CopyOutReader, Portal, RowIter, Statement, ToStatement};
use tokio_postgres::types::{BorrowToSql, ToSql, Type};
use tokio_postgres::{Error, Row, SimpleQueryMessage};

/// A representation of a PostgreSQL database transaction.
///
/// Transactions will implicitly roll back by default when dropped. Use the `commit` method to commit the changes made
/// in the transaction. Transactions can be nested, with inner transactions implemented via savepoints.
pub struct Transaction<'a> {
    connection: ConnectionRef<'a>,
    transaction: Option<tokio_postgres::Transaction<'a>>,
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if let Some(transaction) = self.transaction.take() {
            let _ = self.connection.block_on(transaction.rollback());
        }
    }
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(
        connection: ConnectionRef<'a>,
        transaction: tokio_postgres::Transaction<'a>,
    ) -> Transaction<'a> {
        Transaction {
            connection,
            transaction: Some(transaction),
        }
    }

    /// Consumes the transaction, committing all changes made within it.
    pub fn commit(mut self) -> Result<(), Error> {
        self.connection
            .block_on(self.transaction.take().unwrap().commit())
    }

    /// Rolls the transaction back, discarding all changes made within it.
    ///
    /// This is equivalent to `Transaction`'s `Drop` implementation, but provides any error encountered to the caller.
    pub fn rollback(mut self) -> Result<(), Error> {
        self.connection
            .block_on(self.transaction.take().unwrap().rollback())
    }

    /// Like `Client::prepare`.
    pub fn prepare(&mut self, query: &str) -> Result<Statement, Error> {
        self.connection
            .block_on(self.transaction.as_ref().unwrap().prepare(query))
    }

    /// Like `Client::prepare_typed`.
    pub fn prepare_typed(&mut self, query: &str, types: &[Type]) -> Result<Statement, Error> {
        self.connection.block_on(
            self.transaction
                .as_ref()
                .unwrap()
                .prepare_typed(query, types),
        )
    }

    /// Like `Client::execute`.
    pub fn execute<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.connection
            .block_on(self.transaction.as_ref().unwrap().execute(query, params))
    }

    /// Like `Client::query`.
    pub fn query<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.connection
            .block_on(self.transaction.as_ref().unwrap().query(query, params))
    }

    /// Like `Client::query_one`.
    pub fn query_one<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Row, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.connection
            .block_on(self.transaction.as_ref().unwrap().query_one(query, params))
    }

    /// Like `Client::query_opt`.
    pub fn query_opt<T>(
        &mut self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.connection
            .block_on(self.transaction.as_ref().unwrap().query_opt(query, params))
    }

    /// Like `Client::query_raw`.
    pub fn query_raw<T, P, I>(&mut self, query: &T, params: I) -> Result<RowIter<'_>, Error>
    where
        T: ?Sized + ToStatement,
        P: BorrowToSql,
        I: IntoIterator<Item = P>,
        I::IntoIter: ExactSizeIterator,
    {
        let stream = self
            .connection
            .block_on(self.transaction.as_ref().unwrap().query_raw(query, params))?;
        Ok(RowIter::new(self.connection.as_ref(), stream))
    }

    /// Like `Client::query_typed`.
    pub fn query_typed(
        &mut self,
        statement: &str,
        params: &[(&(dyn ToSql + Sync), Type)],
    ) -> Result<Vec<Row>, Error> {
        self.connection.block_on(
            self.transaction
                .as_ref()
                .unwrap()
                .query_typed(statement, params),
        )
    }

    /// Like `Client::query_typed_raw`.
    pub fn query_typed_raw<P, I>(&mut self, query: &str, params: I) -> Result<RowIter<'_>, Error>
    where
        P: BorrowToSql,
        I: IntoIterator<Item = (P, Type)>,
    {
        let stream = self.connection.block_on(
            self.transaction
                .as_ref()
                .unwrap()
                .query_typed_raw(query, params),
        )?;
        Ok(RowIter::new(self.connection.as_ref(), stream))
    }

    /// Binds parameters to a statement, creating a "portal".
    ///
    /// Portals can be used with the `query_portal` method to page through the results of a query without being forced
    /// to consume them all immediately.
    ///
    /// Portals are automatically closed when the transaction they were created in is closed.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    pub fn bind<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Portal, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.connection
            .block_on(self.transaction.as_ref().unwrap().bind(query, params))
    }

    /// Continues execution of a portal, returning the next set of rows.
    ///
    /// Unlike `query`, portals can be incrementally evaluated by limiting the number of rows returned in each call to
    /// `query_portal`. If the requested number is negative or 0, all remaining rows will be returned.
    pub fn query_portal(&mut self, portal: &Portal, max_rows: i32) -> Result<Vec<Row>, Error> {
        self.connection.block_on(
            self.transaction
                .as_ref()
                .unwrap()
                .query_portal(portal, max_rows),
        )
    }

    /// The maximally flexible version of `query_portal`.
    pub fn query_portal_raw(
        &mut self,
        portal: &Portal,
        max_rows: i32,
    ) -> Result<RowIter<'_>, Error> {
        let stream = self.connection.block_on(
            self.transaction
                .as_ref()
                .unwrap()
                .query_portal_raw(portal, max_rows),
        )?;
        Ok(RowIter::new(self.connection.as_ref(), stream))
    }

    /// Like `Client::copy_in`.
    pub fn copy_in<T>(&mut self, query: &T) -> Result<CopyInWriter<'_>, Error>
    where
        T: ?Sized + ToStatement,
    {
        let sink = self
            .connection
            .block_on(self.transaction.as_ref().unwrap().copy_in(query))?;
        Ok(CopyInWriter::new(self.connection.as_ref(), sink))
    }

    /// Like `Client::copy_out`.
    pub fn copy_out<T>(&mut self, query: &T) -> Result<CopyOutReader<'_>, Error>
    where
        T: ?Sized + ToStatement,
    {
        let stream = self
            .connection
            .block_on(self.transaction.as_ref().unwrap().copy_out(query))?;
        Ok(CopyOutReader::new(self.connection.as_ref(), stream))
    }

    /// Like `Client::simple_query`.
    pub fn simple_query(&mut self, query: &str) -> Result<Vec<SimpleQueryMessage>, Error> {
        self.connection
            .block_on(self.transaction.as_ref().unwrap().simple_query(query))
    }

    /// Like `Client::batch_execute`.
    pub fn batch_execute(&mut self, query: &str) -> Result<(), Error> {
        self.connection
            .block_on(self.transaction.as_ref().unwrap().batch_execute(query))
    }

    /// Like `Client::cancel_token`.
    pub fn cancel_token(&self) -> CancelToken {
        CancelToken::new(self.transaction.as_ref().unwrap().cancel_token())
    }

    /// Like `Client::transaction`, but creates a nested transaction via a savepoint.
    pub fn transaction(&mut self) -> Result<Transaction<'_>, Error> {
        let transaction = self
            .connection
            .block_on(self.transaction.as_mut().unwrap().transaction())?;
        Ok(Transaction::new(self.connection.as_ref(), transaction))
    }

    /// Like `Client::transaction`, but creates a nested transaction via a savepoint with the specified name.
    pub fn savepoint<I>(&mut self, name: I) -> Result<Transaction<'_>, Error>
    where
        I: Into<String>,
    {
        let transaction = self
            .connection
            .block_on(self.transaction.as_mut().unwrap().savepoint(name))?;
        Ok(Transaction::new(self.connection.as_ref(), transaction))
    }

    /// Like `Client::query_typed_one`.
    pub fn query_typed_one(
        &mut self,
        query: &str,
        params: &[(&(dyn ToSql + Sync), Type)],
    ) -> Result<Row, Error> {
        self.connection.block_on(
            self.transaction
                .as_ref()
                .unwrap()
                .query_typed_one(query, params),
        )
    }

    /// Like `Client::query_typed_opt`.
    pub fn query_typed_opt(
        &mut self,
        query: &str,
        params: &[(&(dyn ToSql + Sync), Type)],
    ) -> Result<Option<Row>, Error> {
        self.connection.block_on(
            self.transaction
                .as_ref()
                .unwrap()
                .query_typed_opt(query, params),
        )
    }
}
