use crate::types::{BorrowToSql, ToSql, Type};
use crate::{
    Client, CopyInWriter, CopyOutReader, Error, Row, RowIter, SimpleQueryMessage, Statement,
    ToStatement, Transaction,
};

mod private {
    pub trait Sealed {}
}

/// A trait allowing abstraction over connections and transactions.
///
/// This trait is "sealed", and cannot be implemented outside of this crate.
pub trait GenericClient: private::Sealed {
    /// Like `Client::execute`.
    fn execute<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement;

    /// Like `Client::query`.
    fn query<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement;

    /// Like `Client::query_one`.
    fn query_one<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Row, Error>
    where
        T: ?Sized + ToStatement;

    /// Like `Client::query_opt`.
    fn query_opt<T>(
        &mut self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error>
    where
        T: ?Sized + ToStatement;

    /// Like `Client::query_raw`.
    fn query_raw<T, P, I>(&mut self, query: &T, params: I) -> Result<RowIter<'_>, Error>
    where
        T: ?Sized + ToStatement,
        P: BorrowToSql,
        I: IntoIterator<Item = P>,
        I::IntoIter: ExactSizeIterator;

    /// Like [`Client::query_typed`]
    fn query_typed(
        &mut self,
        statement: &str,
        params: &[(&(dyn ToSql + Sync), Type)],
    ) -> Result<Vec<Row>, Error>;

    /// Like [`Client::query_typed_raw`]
    fn query_typed_raw<P, I>(&mut self, statement: &str, params: I) -> Result<RowIter<'_>, Error>
    where
        P: BorrowToSql,
        I: IntoIterator<Item = (P, Type)> + Sync + Send;

    /// Like `Client::query_typed_one`.
    fn query_typed_one(
        &mut self,
        query: &str,
        params: &[(&(dyn ToSql + Sync), Type)],
    ) -> Result<Row, Error>;

    /// Like `Client::query_typed_opt`.
    fn query_typed_opt(
        &mut self,
        query: &str,
        params: &[(&(dyn ToSql + Sync), Type)],
    ) -> Result<Option<Row>, Error>;

    /// Like `Client::prepare`.
    fn prepare(&mut self, query: &str) -> Result<Statement, Error>;

    /// Like `Client::prepare_typed`.
    fn prepare_typed(&mut self, query: &str, types: &[Type]) -> Result<Statement, Error>;

    /// Like `Client::copy_in`.
    fn copy_in<T>(&mut self, query: &T) -> Result<CopyInWriter<'_>, Error>
    where
        T: ?Sized + ToStatement;

    /// Like `Client::copy_out`.
    fn copy_out<T>(&mut self, query: &T) -> Result<CopyOutReader<'_>, Error>
    where
        T: ?Sized + ToStatement;

    /// Like `Client::simple_query`.
    fn simple_query(&mut self, query: &str) -> Result<Vec<SimpleQueryMessage>, Error>;

    /// Like `Client::batch_execute`.
    fn batch_execute(&mut self, query: &str) -> Result<(), Error>;

    /// Like `Client::transaction`.
    fn transaction(&mut self) -> Result<Transaction<'_>, Error>;
}

impl private::Sealed for Client {}

impl GenericClient for Client {
    fn execute<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.execute(query, params)
    }

    fn query<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.query(query, params)
    }

    fn query_one<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Row, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.query_one(query, params)
    }

    fn query_opt<T>(
        &mut self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.query_opt(query, params)
    }

    fn query_raw<T, P, I>(&mut self, query: &T, params: I) -> Result<RowIter<'_>, Error>
    where
        T: ?Sized + ToStatement,
        P: BorrowToSql,
        I: IntoIterator<Item = P>,
        I::IntoIter: ExactSizeIterator,
    {
        self.query_raw(query, params)
    }

    fn query_typed(
        &mut self,
        statement: &str,
        params: &[(&(dyn ToSql + Sync), Type)],
    ) -> Result<Vec<Row>, Error> {
        self.query_typed(statement, params)
    }

    fn query_typed_raw<P, I>(&mut self, statement: &str, params: I) -> Result<RowIter<'_>, Error>
    where
        P: BorrowToSql,
        I: IntoIterator<Item = (P, Type)> + Sync + Send,
    {
        self.query_typed_raw(statement, params)
    }

    fn prepare(&mut self, query: &str) -> Result<Statement, Error> {
        self.prepare(query)
    }

    fn prepare_typed(&mut self, query: &str, types: &[Type]) -> Result<Statement, Error> {
        self.prepare_typed(query, types)
    }

    fn copy_in<T>(&mut self, query: &T) -> Result<CopyInWriter<'_>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.copy_in(query)
    }

    fn copy_out<T>(&mut self, query: &T) -> Result<CopyOutReader<'_>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.copy_out(query)
    }

    fn simple_query(&mut self, query: &str) -> Result<Vec<SimpleQueryMessage>, Error> {
        self.simple_query(query)
    }

    fn batch_execute(&mut self, query: &str) -> Result<(), Error> {
        self.batch_execute(query)
    }

    fn transaction(&mut self) -> Result<Transaction<'_>, Error> {
        self.transaction()
    }

    fn query_typed_one(
        &mut self,
        query: &str,

        params: &[(&(dyn ToSql + Sync), Type)],
    ) -> Result<Row, Error> {
        self.query_typed_one(query, params)
    }

    fn query_typed_opt(
        &mut self,
        query: &str,

        params: &[(&(dyn ToSql + Sync), Type)],
    ) -> Result<Option<Row>, Error> {
        self.query_typed_opt(query, params)
    }
}

impl private::Sealed for Transaction<'_> {}

impl GenericClient for Transaction<'_> {
    fn execute<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.execute(query, params)
    }

    fn query<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.query(query, params)
    }

    fn query_one<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Row, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.query_one(query, params)
    }

    fn query_opt<T>(
        &mut self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.query_opt(query, params)
    }

    fn query_raw<T, P, I>(&mut self, query: &T, params: I) -> Result<RowIter<'_>, Error>
    where
        T: ?Sized + ToStatement,
        P: BorrowToSql,
        I: IntoIterator<Item = P>,
        I::IntoIter: ExactSizeIterator,
    {
        self.query_raw(query, params)
    }

    fn query_typed(
        &mut self,
        statement: &str,
        params: &[(&(dyn ToSql + Sync), Type)],
    ) -> Result<Vec<Row>, Error> {
        self.query_typed(statement, params)
    }

    fn query_typed_raw<P, I>(&mut self, statement: &str, params: I) -> Result<RowIter<'_>, Error>
    where
        P: BorrowToSql,
        I: IntoIterator<Item = (P, Type)> + Sync + Send,
    {
        self.query_typed_raw(statement, params)
    }

    fn prepare(&mut self, query: &str) -> Result<Statement, Error> {
        self.prepare(query)
    }

    fn prepare_typed(&mut self, query: &str, types: &[Type]) -> Result<Statement, Error> {
        self.prepare_typed(query, types)
    }

    fn copy_in<T>(&mut self, query: &T) -> Result<CopyInWriter<'_>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.copy_in(query)
    }

    fn copy_out<T>(&mut self, query: &T) -> Result<CopyOutReader<'_>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.copy_out(query)
    }

    fn simple_query(&mut self, query: &str) -> Result<Vec<SimpleQueryMessage>, Error> {
        self.simple_query(query)
    }

    fn batch_execute(&mut self, query: &str) -> Result<(), Error> {
        self.batch_execute(query)
    }

    fn transaction(&mut self) -> Result<Transaction<'_>, Error> {
        self.transaction()
    }

    fn query_typed_one(
        &mut self,
        query: &str,
        params: &[(&(dyn ToSql + Sync), Type)],
    ) -> Result<Row, Error> {
        self.query_typed_one(query, params)
    }

    fn query_typed_opt(
        &mut self,
        query: &str,
        params: &[(&(dyn ToSql + Sync), Type)],
    ) -> Result<Option<Row>, Error> {
        self.query_typed_opt(query, params)
    }
}
