use crate::query::RowStream;
use crate::types::{BorrowToSql, ToSql, Type};
use crate::{Client, Error, Row, SimpleQueryMessage, Statement, ToStatement, Transaction};

mod private {
    pub trait Sealed {}
}

/// A trait allowing abstraction over connections and transactions.
///
/// This trait is "sealed", and cannot be implemented outside of this crate.
pub trait GenericClient: private::Sealed {
    /// Like [`Client::execute`].
    fn execute<T>(
        &self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> impl Future<Output = Result<u64, Error>> + Send
    where
        T: ?Sized + ToStatement + Sync + Send;

    /// Like [`Client::execute_raw`].
    fn execute_raw<P, I, T>(
        &self,
        statement: &T,
        params: I,
    ) -> impl Future<Output = Result<u64, Error>> + Send
    where
        T: ?Sized + ToStatement + Sync + Send,
        P: BorrowToSql,
        I: IntoIterator<Item = P> + Sync + Send,
        I::IntoIter: ExactSizeIterator;

    /// Like [`Client::execute_typed`].
    fn execute_typed(
        &self,
        statement: &str,
        params: &[(&(dyn ToSql + Sync), Type)],
    ) -> impl Future<Output = Result<u64, Error>> + Send;

    /// Like [`Client::query`].
    fn query<T>(
        &self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> impl Future<Output = Result<Vec<Row>, Error>> + Send
    where
        T: ?Sized + ToStatement + Sync + Send;

    /// Like [`Client::query_one`].
    fn query_one<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> impl Future<Output = Result<Row, Error>> + Send
    where
        T: ?Sized + ToStatement + Sync + Send;

    /// Like [`Client::query_opt`].
    fn query_opt<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> impl Future<Output = Result<Option<Row>, Error>> + Send
    where
        T: ?Sized + ToStatement + Sync + Send;

    /// Like [`Client::query_raw`].
    fn query_raw<T, P, I>(
        &self,
        statement: &T,
        params: I,
    ) -> impl Future<Output = Result<RowStream, Error>> + Send
    where
        T: ?Sized + ToStatement + Sync + Send,
        P: BorrowToSql,
        I: IntoIterator<Item = P> + Sync + Send,
        I::IntoIter: ExactSizeIterator;

    /// Like [`Client::query_typed`]
    fn query_typed(
        &self,
        statement: &str,
        params: &[(&(dyn ToSql + Sync), Type)],
    ) -> impl Future<Output = Result<Vec<Row>, Error>> + Send;

    /// Like [`Client::query_typed_one`].
    fn query_typed_one(
        &self,
        statement: &str,
        params: &[(&(dyn ToSql + Sync), Type)],
    ) -> impl Future<Output = Result<Row, Error>> + Send;

    /// Like [`Client::query_opt_typed`].
    fn query_typed_opt(
        &self,
        statement: &str,
        params: &[(&(dyn ToSql + Sync), Type)],
    ) -> impl Future<Output = Result<Option<Row>, Error>> + Send;

    /// Like [`Client::query_typed_raw`]
    fn query_typed_raw<P, I>(
        &self,
        statement: &str,
        params: I,
    ) -> impl Future<Output = Result<RowStream, Error>> + Send
    where
        P: BorrowToSql,
        I: IntoIterator<Item = (P, Type)> + Sync + Send;

    /// Like [`Client::prepare`].
    fn prepare(&self, query: &str) -> impl Future<Output = Result<Statement, Error>> + Send;

    /// Like [`Client::prepare_typed`].
    fn prepare_typed(
        &self,
        query: &str,
        parameter_types: &[Type],
    ) -> impl Future<Output = Result<Statement, Error>> + Send;

    /// Like [`Client::transaction`].
    fn transaction<'a>(&'a mut self)
    -> impl Future<Output = Result<Transaction<'a>, Error>> + Send;

    /// Like [`Client::batch_execute`].
    fn batch_execute(&self, query: &str) -> impl Future<Output = Result<(), Error>> + Send;

    /// Like [`Client::simple_query`].
    fn simple_query(
        &self,
        query: &str,
    ) -> impl Future<Output = Result<Vec<SimpleQueryMessage>, Error>> + Send;

    /// Returns a reference to the underlying [`Client`].
    fn client(&self) -> &Client;
}

impl private::Sealed for Client {}

impl GenericClient for Client {
    async fn execute<T>(&self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement + Sync + Send,
    {
        self.execute(query, params).await
    }

    async fn execute_typed(
        &self,
        statement: &str,
        params: &[(&(dyn ToSql + Sync), Type)],
    ) -> Result<u64, Error> {
        self.execute_typed(statement, params).await
    }

    async fn execute_raw<P, I, T>(&self, statement: &T, params: I) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement + Sync + Send,
        P: BorrowToSql,
        I: IntoIterator<Item = P> + Sync + Send,
        I::IntoIter: ExactSizeIterator,
    {
        self.execute_raw(statement, params).await
    }

    async fn query<T>(&self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement + Sync + Send,
    {
        self.query(query, params).await
    }

    async fn query_one<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, Error>
    where
        T: ?Sized + ToStatement + Sync + Send,
    {
        self.query_one(statement, params).await
    }

    async fn query_opt<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error>
    where
        T: ?Sized + ToStatement + Sync + Send,
    {
        self.query_opt(statement, params).await
    }

    async fn query_raw<T, P, I>(&self, statement: &T, params: I) -> Result<RowStream, Error>
    where
        T: ?Sized + ToStatement + Sync + Send,
        P: BorrowToSql,
        I: IntoIterator<Item = P> + Sync + Send,
        I::IntoIter: ExactSizeIterator,
    {
        self.query_raw(statement, params).await
    }

    async fn query_typed(
        &self,
        statement: &str,
        params: &[(&(dyn ToSql + Sync), Type)],
    ) -> Result<Vec<Row>, Error> {
        self.query_typed(statement, params).await
    }

    async fn query_typed_one(
        &self,
        statement: &str,
        params: &[(&(dyn ToSql + Sync), Type)],
    ) -> Result<Row, Error> {
        self.query_typed_one(statement, params).await
    }

    /// Like [`Client::query_opt_typed`].
    async fn query_typed_opt(
        &self,
        statement: &str,
        params: &[(&(dyn ToSql + Sync), Type)],
    ) -> Result<Option<Row>, Error> {
        self.query_typed_opt(statement, params).await
    }

    async fn query_typed_raw<P, I>(&self, statement: &str, params: I) -> Result<RowStream, Error>
    where
        P: BorrowToSql,
        I: IntoIterator<Item = (P, Type)> + Sync + Send,
    {
        self.query_typed_raw(statement, params).await
    }

    async fn prepare(&self, query: &str) -> Result<Statement, Error> {
        self.prepare(query).await
    }

    async fn prepare_typed(
        &self,
        query: &str,
        parameter_types: &[Type],
    ) -> Result<Statement, Error> {
        self.prepare_typed(query, parameter_types).await
    }

    async fn transaction<'a>(&'a mut self) -> Result<Transaction<'a>, Error> {
        self.transaction().await
    }

    async fn batch_execute(&self, query: &str) -> Result<(), Error> {
        self.batch_execute(query).await
    }

    async fn simple_query(&self, query: &str) -> Result<Vec<SimpleQueryMessage>, Error> {
        self.simple_query(query).await
    }

    fn client(&self) -> &Client {
        self
    }
}

impl private::Sealed for Transaction<'_> {}

impl GenericClient for Transaction<'_> {
    async fn execute<T>(&self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement + Sync + Send,
    {
        self.execute(query, params).await
    }

    async fn execute_raw<P, I, T>(&self, statement: &T, params: I) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement + Sync + Send,
        P: BorrowToSql,
        I: IntoIterator<Item = P> + Sync + Send,
        I::IntoIter: ExactSizeIterator,
    {
        self.execute_raw(statement, params).await
    }

    async fn query<T>(&self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement + Sync + Send,
    {
        self.query(query, params).await
    }

    async fn query_one<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, Error>
    where
        T: ?Sized + ToStatement + Sync + Send,
    {
        self.query_one(statement, params).await
    }

    async fn query_opt<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error>
    where
        T: ?Sized + ToStatement + Sync + Send,
    {
        self.query_opt(statement, params).await
    }

    async fn query_raw<T, P, I>(&self, statement: &T, params: I) -> Result<RowStream, Error>
    where
        T: ?Sized + ToStatement + Sync + Send,
        P: BorrowToSql,
        I: IntoIterator<Item = P> + Sync + Send,
        I::IntoIter: ExactSizeIterator,
    {
        self.query_raw(statement, params).await
    }

    async fn query_typed(
        &self,
        statement: &str,
        params: &[(&(dyn ToSql + Sync), Type)],
    ) -> Result<Vec<Row>, Error> {
        self.query_typed(statement, params).await
    }

    async fn query_typed_one(
        &self,
        statement: &str,
        params: &[(&(dyn ToSql + Sync), Type)],
    ) -> Result<Row, Error> {
        self.query_typed_one(statement, params).await
    }

    /// Like [`Client::query_opt_typed`].
    async fn query_typed_opt(
        &self,
        statement: &str,
        params: &[(&(dyn ToSql + Sync), Type)],
    ) -> Result<Option<Row>, Error> {
        self.query_typed_opt(statement, params).await
    }

    async fn query_typed_raw<P, I>(&self, statement: &str, params: I) -> Result<RowStream, Error>
    where
        P: BorrowToSql,
        I: IntoIterator<Item = (P, Type)> + Sync + Send,
    {
        self.query_typed_raw(statement, params).await
    }

    async fn prepare(&self, query: &str) -> Result<Statement, Error> {
        self.prepare(query).await
    }

    async fn prepare_typed(
        &self,
        query: &str,
        parameter_types: &[Type],
    ) -> Result<Statement, Error> {
        self.prepare_typed(query, parameter_types).await
    }

    async fn transaction<'a>(&'a mut self) -> Result<Transaction<'a>, Error> {
        self.transaction().await
    }

    async fn batch_execute(&self, query: &str) -> Result<(), Error> {
        self.batch_execute(query).await
    }

    async fn simple_query(&self, query: &str) -> Result<Vec<SimpleQueryMessage>, Error> {
        self.simple_query(query).await
    }

    fn client(&self) -> &Client {
        self.client()
    }

    async fn execute_typed(
        &self,
        statement: &str,
        params: &[(&(dyn ToSql + Sync), Type)],
    ) -> Result<u64, Error> {
        self.client().execute_typed(statement, params).await
    }
}
