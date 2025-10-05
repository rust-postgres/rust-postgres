use crate::connection::Connection;
use crate::{
    CancelToken, Config, CopyInWriter, CopyOutReader, Notifications, RowIter, Statement,
    ToStatement, Transaction, TransactionBuilder,
};
use std::task::Poll;
use std::time::Duration;
use tokio_postgres::tls::{MakeTlsConnect, TlsConnect};
use tokio_postgres::types::{BorrowToSql, ToSql, Type};
use tokio_postgres::{Error, Row, SimpleQueryMessage, Socket};

/// A synchronous PostgreSQL client.
pub struct Client {
    connection: Connection,
    client: tokio_postgres::Client,
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.close_inner();
    }
}

impl Client {
    pub(crate) fn new(connection: Connection, client: tokio_postgres::Client) -> Client {
        Client { connection, client }
    }

    /// A convenience function which parses a configuration string into a `Config` and then connects to the database.
    ///
    /// See the documentation for [`Config`] for information about the connection syntax.
    ///
    /// [`Config`]: config/struct.Config.html
    pub fn connect<T>(params: &str, tls_mode: T) -> Result<Client, Error>
    where
        T: MakeTlsConnect<Socket> + 'static + Send,
        T::TlsConnect: Send,
        T::Stream: Send,
        <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
    {
        params.parse::<Config>()?.connect(tls_mode)
    }

    /// Returns a new `Config` object which can be used to configure and connect to a database.
    pub fn configure() -> Config {
        Config::new()
    }

    /// Executes a statement, returning the number of rows modified.
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the parameter of the list
    /// provided, 1-indexed.
    ///
    /// If the statement does not modify any rows (e.g. `SELECT`), 0 is returned.
    ///
    /// The `query` argument can either be a `Statement`, or a raw query string. If the same statement will be
    /// repeatedly executed (perhaps with different query parameters), consider preparing the statement up front
    /// with the `prepare` method.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use postgres::{Client, NoTls};
    ///
    /// # fn main() -> Result<(), postgres::Error> {
    /// let mut client = Client::connect("host=localhost user=postgres", NoTls)?;
    ///
    /// let bar = 1i32;
    /// let baz = true;
    /// let rows_updated = client.execute(
    ///     "UPDATE foo SET bar = $1 WHERE baz = $2",
    ///     &[&bar, &baz],
    /// )?;
    ///
    /// println!("{} rows updated", rows_updated);
    /// # Ok(())
    /// # }
    /// ```
    pub fn execute<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.connection.block_on(self.client.execute(query, params))
    }

    /// Executes a statement, returning the resulting rows.
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the parameter of the list
    /// provided, 1-indexed.
    ///
    /// The `query` argument can either be a `Statement`, or a raw query string. If the same statement will be
    /// repeatedly executed (perhaps with different query parameters), consider preparing the statement up front
    /// with the `prepare` method.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use postgres::{Client, NoTls};
    ///
    /// # fn main() -> Result<(), postgres::Error> {
    /// let mut client = Client::connect("host=localhost user=postgres", NoTls)?;
    ///
    /// let baz = true;
    /// for row in client.query("SELECT foo FROM bar WHERE baz = $1", &[&baz])? {
    ///     let foo: i32 = row.get("foo");
    ///     println!("foo: {}", foo);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn query<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.connection.block_on(self.client.query(query, params))
    }

    /// Executes a statement which returns a single row, returning it.
    ///
    /// Returns an error if the query does not return exactly one row.
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the parameter of the list
    /// provided, 1-indexed.
    ///
    /// The `query` argument can either be a `Statement`, or a raw query string. If the same statement will be
    /// repeatedly executed (perhaps with different query parameters), consider preparing the statement up front
    /// with the `prepare` method.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use postgres::{Client, NoTls};
    ///
    /// # fn main() -> Result<(), postgres::Error> {
    /// let mut client = Client::connect("host=localhost user=postgres", NoTls)?;
    ///
    /// let baz = true;
    /// let row = client.query_one("SELECT foo FROM bar WHERE baz = $1", &[&baz])?;
    /// let foo: i32 = row.get("foo");
    /// println!("foo: {}", foo);
    /// # Ok(())
    /// # }
    /// ```
    pub fn query_one<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Row, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.connection
            .block_on(self.client.query_one(query, params))
    }

    /// Executes a statement which returns zero or one rows, returning it.
    ///
    /// Returns an error if the query returns more than one row.
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the parameter of the list
    /// provided, 1-indexed.
    ///
    /// The `query` argument can either be a `Statement`, or a raw query string. If the same statement will be
    /// repeatedly executed (perhaps with different query parameters), consider preparing the statement up front
    /// with the `prepare` method.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use postgres::{Client, NoTls};
    ///
    /// # fn main() -> Result<(), postgres::Error> {
    /// let mut client = Client::connect("host=localhost user=postgres", NoTls)?;
    ///
    /// let baz = true;
    /// let row = client.query_opt("SELECT foo FROM bar WHERE baz = $1", &[&baz])?;
    /// match row {
    ///     Some(row) => {
    ///         let foo: i32 = row.get("foo");
    ///         println!("foo: {}", foo);
    ///     }
    ///     None => println!("no matching foo"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn query_opt<T>(
        &mut self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.connection
            .block_on(self.client.query_opt(query, params))
    }

    /// A maximally-flexible version of `query`.
    ///
    /// It takes an iterator of parameters rather than a slice, and returns an iterator of rows rather than collecting
    /// them into an array.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use postgres::{Client, NoTls};
    /// use fallible_iterator::FallibleIterator;
    /// use std::iter;
    ///
    /// # fn main() -> Result<(), postgres::Error> {
    /// let mut client = Client::connect("host=localhost user=postgres", NoTls)?;
    ///
    /// let baz = true;
    /// let mut it = client.query_raw("SELECT foo FROM bar WHERE baz = $1", iter::once(baz))?;
    ///
    /// while let Some(row) = it.next()? {
    ///     let foo: i32 = row.get("foo");
    ///     println!("foo: {}", foo);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// If you have a type like `Vec<T>` where `T: ToSql` Rust will not know how to use it as params. To get around
    /// this the type must explicitly be converted to `&dyn ToSql`.
    ///
    /// ```no_run
    /// # use postgres::{Client, NoTls};
    /// use postgres::types::ToSql;
    /// use fallible_iterator::FallibleIterator;
    /// # fn main() -> Result<(), postgres::Error> {
    /// # let mut client = Client::connect("host=localhost user=postgres", NoTls)?;
    ///
    /// let params: Vec<String> = vec![
    ///     "first param".into(),
    ///     "second param".into(),
    /// ];
    /// let mut it = client.query_raw(
    ///     "SELECT foo FROM bar WHERE biz = $1 AND baz = $2",
    ///     params,
    /// )?;
    ///
    /// while let Some(row) = it.next()? {
    ///     let foo: i32 = row.get("foo");
    ///     println!("foo: {}", foo);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn query_raw<T, P, I>(&mut self, query: &T, params: I) -> Result<RowIter<'_>, Error>
    where
        T: ?Sized + ToStatement,
        P: BorrowToSql,
        I: IntoIterator<Item = P>,
        I::IntoIter: ExactSizeIterator,
    {
        let stream = self
            .connection
            .block_on(self.client.query_raw(query, params))?;
        Ok(RowIter::new(self.connection.as_ref(), stream))
    }

    /// Like `query`, but requires the types of query parameters to be explicitly specified.
    ///
    /// Compared to `query`, this method allows performing queries without three round trips (for
    /// prepare, execute, and close) by requiring the caller to specify parameter values along with
    /// their Postgres type. Thus, this is suitable in environments where prepared statements aren't
    /// supported (such as Cloudflare Workers with Hyperdrive).
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the
    /// parameter of the list provided, 1-indexed.
    pub fn query_typed(
        &mut self,
        query: &str,
        params: &[(&(dyn ToSql + Sync), Type)],
    ) -> Result<Vec<Row>, Error> {
        self.connection
            .block_on(self.client.query_typed(query, params))
    }

    /// The maximally flexible version of [`query_typed`].
    ///
    /// Compared to `query`, this method allows performing queries without three round trips (for
    /// prepare, execute, and close) by requiring the caller to specify parameter values along with
    /// their Postgres type. Thus, this is suitable in environments where prepared statements aren't
    /// supported (such as Cloudflare Workers with Hyperdrive).
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the
    /// parameter of the list provided, 1-indexed.
    ///
    /// [`query_typed`]: #method.query_typed
    ///
    /// # Examples
    /// ```no_run
    /// # use postgres::{Client, NoTls};
    /// use postgres::types::{ToSql, Type};
    /// use fallible_iterator::FallibleIterator;
    /// # fn main() -> Result<(), postgres::Error> {
    /// # let mut client = Client::connect("host=localhost user=postgres", NoTls)?;
    ///
    /// let params: Vec<(String, Type)> = vec![
    ///     ("first param".into(), Type::TEXT),
    ///     ("second param".into(), Type::TEXT),
    /// ];
    /// let mut it = client.query_typed_raw(
    ///     "SELECT foo FROM bar WHERE biz = $1 AND baz = $2",
    ///     params,
    /// )?;
    ///
    /// while let Some(row) = it.next()? {
    ///     let foo: i32 = row.get("foo");
    ///     println!("foo: {}", foo);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn query_typed_raw<P, I>(&mut self, query: &str, params: I) -> Result<RowIter<'_>, Error>
    where
        P: BorrowToSql,
        I: IntoIterator<Item = (P, Type)>,
    {
        let stream = self
            .connection
            .block_on(self.client.query_typed_raw(query, params))?;
        Ok(RowIter::new(self.connection.as_ref(), stream))
    }

    /// Creates a new prepared statement.
    ///
    /// Prepared statements can be executed repeatedly, and may contain query parameters (indicated by `$1`, `$2`, etc),
    /// which are set when executed. Prepared statements can only be used with the connection that created them.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use postgres::{Client, NoTls};
    ///
    /// # fn main() -> Result<(), postgres::Error> {
    /// let mut client = Client::connect("host=localhost user=postgres", NoTls)?;
    ///
    /// let statement = client.prepare("SELECT name FROM people WHERE id = $1")?;
    ///
    /// for id in 0..10 {
    ///     let rows = client.query(&statement, &[&id])?;
    ///     let name: &str = rows[0].get(0);
    ///     println!("name: {}", name);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn prepare(&mut self, query: &str) -> Result<Statement, Error> {
        self.connection.block_on(self.client.prepare(query))
    }

    /// Like `prepare`, but allows the types of query parameters to be explicitly specified.
    ///
    /// The list of types may be smaller than the number of parameters - the types of the remaining parameters will be
    /// inferred. For example, `client.prepare_typed(query, &[])` is equivalent to `client.prepare(query)`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use postgres::{Client, NoTls};
    /// use postgres::types::Type;
    ///
    /// # fn main() -> Result<(), postgres::Error> {
    /// let mut client = Client::connect("host=localhost user=postgres", NoTls)?;
    ///
    /// let statement = client.prepare_typed(
    ///     "SELECT name FROM people WHERE id = $1",
    ///     &[Type::INT8],
    /// )?;
    ///
    /// for id in 0..10 {
    ///     let rows = client.query(&statement, &[&id])?;
    ///     let name: &str = rows[0].get(0);
    ///     println!("name: {}", name);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn prepare_typed(&mut self, query: &str, types: &[Type]) -> Result<Statement, Error> {
        self.connection
            .block_on(self.client.prepare_typed(query, types))
    }

    /// Executes a `COPY FROM STDIN` statement, returning the number of rows created.
    ///
    /// The `query` argument can either be a `Statement`, or a raw query string. The data in the provided reader is
    /// passed along to the server verbatim; it is the caller's responsibility to ensure it uses the proper format.
    /// PostgreSQL does not support parameters in `COPY` statements, so this method does not take any.
    ///
    /// The copy *must* be explicitly completed via the `finish` method. If it is not, the copy will be aborted.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use postgres::{Client, NoTls};
    /// use std::io::Write;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut client = Client::connect("host=localhost user=postgres", NoTls)?;
    ///
    /// let mut writer = client.copy_in("COPY people FROM stdin")?;
    /// writer.write_all(b"1\tjohn\n2\tjane\n")?;
    /// writer.finish()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn copy_in<T>(&mut self, query: &T) -> Result<CopyInWriter<'_>, Error>
    where
        T: ?Sized + ToStatement,
    {
        let sink = self.connection.block_on(self.client.copy_in(query))?;
        Ok(CopyInWriter::new(self.connection.as_ref(), sink))
    }

    /// Executes a `COPY TO STDOUT` statement, returning a reader of the resulting data.
    ///
    /// The `query` argument can either be a `Statement`, or a raw query string. PostgreSQL does not support parameters
    /// in `COPY` statements, so this method does not take any.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use postgres::{Client, NoTls};
    /// use std::io::Read;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut client = Client::connect("host=localhost user=postgres", NoTls)?;
    ///
    /// let mut reader = client.copy_out("COPY people TO stdout")?;
    /// let mut buf = vec![];
    /// reader.read_to_end(&mut buf)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn copy_out<T>(&mut self, query: &T) -> Result<CopyOutReader<'_>, Error>
    where
        T: ?Sized + ToStatement,
    {
        let stream = self.connection.block_on(self.client.copy_out(query))?;
        Ok(CopyOutReader::new(self.connection.as_ref(), stream))
    }

    /// Executes a sequence of SQL statements using the simple query protocol.
    ///
    /// Statements should be separated by semicolons. If an error occurs, execution of the sequence will stop at that
    /// point. The simple query protocol returns the values in rows as strings rather than in their binary encodings,
    /// so the associated row type doesn't work with the `FromSql` trait. Rather than simply returning the rows, this
    /// method returns a sequence of an enum which indicates either the completion of one of the commands, or a row of
    /// data. This preserves the framing between the separate statements in the request.
    ///
    /// This is a simple convenience method over `simple_query_iter`.
    ///
    /// # Warning
    ///
    /// Prepared statements should be used for any query which contains user-specified data, as they provided the
    /// functionality to safely embed that data in the request. Do not form statements via string concatenation and pass
    /// them to this method!
    pub fn simple_query(&mut self, query: &str) -> Result<Vec<SimpleQueryMessage>, Error> {
        self.connection.block_on(self.client.simple_query(query))
    }

    /// Validates the connection by performing a simple no-op query.
    ///
    /// If the specified timeout is reached before the backend responds, an error will be returned.
    pub fn is_valid(&mut self, timeout: Duration) -> Result<(), Error> {
        let inner_client = &self.client;
        self.connection.block_on(async {
            let trivial_query = inner_client.simple_query("");
            tokio::time::timeout(timeout, trivial_query)
                .await
                .map_err(|_| Error::__private_api_timeout())?
                .map(|_| ())
        })
    }

    /// Executes a sequence of SQL statements using the simple query protocol.
    ///
    /// Statements should be separated by semicolons. If an error occurs, execution of the sequence will stop at that
    /// point. This is intended for use when, for example, initializing a database schema.
    ///
    /// # Warning
    ///
    /// Prepared statements should be use for any query which contains user-specified data, as they provided the
    /// functionality to safely embed that data in the request. Do not form statements via string concatenation and pass
    /// them to this method!
    pub fn batch_execute(&mut self, query: &str) -> Result<(), Error> {
        self.connection.block_on(self.client.batch_execute(query))
    }

    /// Check the connection is alive and wait for the confirmation.
    pub fn check_connection(&mut self) -> Result<(), Error> {
        self.connection.block_on(self.client.check_connection())
    }

    /// Begins a new database transaction.
    ///
    /// The transaction will roll back by default - use the `commit` method to commit it.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use postgres::{Client, NoTls};
    ///
    /// # fn main() -> Result<(), postgres::Error> {
    /// let mut client = Client::connect("host=localhost user=postgres", NoTls)?;
    ///
    /// let mut transaction = client.transaction()?;
    /// transaction.execute("UPDATE foo SET bar = 10", &[])?;
    /// // ...
    ///
    /// transaction.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn transaction(&mut self) -> Result<Transaction<'_>, Error> {
        let transaction = self.connection.block_on(self.client.transaction())?;
        Ok(Transaction::new(self.connection.as_ref(), transaction))
    }

    /// Returns a builder for a transaction with custom settings.
    ///
    /// Unlike the `transaction` method, the builder can be used to control the transaction's isolation level and other
    /// attributes.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use postgres::{Client, IsolationLevel, NoTls};
    ///
    /// # fn main() -> Result<(), postgres::Error> {
    /// let mut client = Client::connect("host=localhost user=postgres", NoTls)?;
    ///
    /// let mut transaction = client.build_transaction()
    ///     .isolation_level(IsolationLevel::RepeatableRead)
    ///     .start()?;
    /// transaction.execute("UPDATE foo SET bar = 10", &[])?;
    /// // ...
    ///
    /// transaction.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn build_transaction(&mut self) -> TransactionBuilder<'_> {
        TransactionBuilder::new(self.connection.as_ref(), self.client.build_transaction())
    }

    /// Returns a structure providing access to asynchronous notifications.
    ///
    /// Use the `LISTEN` command to register this connection for notifications.
    pub fn notifications(&mut self) -> Notifications<'_> {
        Notifications::new(self.connection.as_ref())
    }

    /// Constructs a cancellation token that can later be used to request cancellation of a query running on this
    /// connection.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use postgres::{Client, NoTls};
    /// use postgres::error::SqlState;
    /// use std::thread;
    /// use std::time::Duration;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut client = Client::connect("host=localhost user=postgres", NoTls)?;
    ///
    /// let cancel_token = client.cancel_token();
    ///
    /// thread::spawn(move || {
    ///     // Abort the query after 5s.
    ///     thread::sleep(Duration::from_secs(5));
    ///     let _ = cancel_token.cancel_query(NoTls);
    /// });
    ///
    /// match client.simple_query("SELECT long_running_query()") {
    ///     Err(e) if e.code() == Some(&SqlState::QUERY_CANCELED) => {
    ///         // Handle canceled query.
    ///     }
    ///     Err(err) => return Err(err.into()),
    ///     Ok(rows) => {
    ///         // ...
    ///     }
    /// }
    /// // ...
    ///
    /// # Ok(())
    /// # }
    /// ```
    pub fn cancel_token(&self) -> CancelToken {
        CancelToken::new(self.client.cancel_token())
    }

    /// Clears the client's type information cache.
    ///
    /// When user-defined types are used in a query, the client loads their definitions from the database and caches
    /// them for the lifetime of the client. If those definitions are changed in the database, this method can be used
    /// to flush the local cache and allow the new, updated definitions to be loaded.
    pub fn clear_type_cache(&self) {
        self.client.clear_type_cache();
    }

    /// Determines if the client's connection has already closed.
    ///
    /// If this returns `true`, the client is no longer usable.
    pub fn is_closed(&self) -> bool {
        self.client.is_closed()
    }

    /// Closes the client's connection to the server.
    ///
    /// This is equivalent to `Client`'s `Drop` implementation, except that it returns any error encountered to the
    /// caller.
    pub fn close(mut self) -> Result<(), Error> {
        self.close_inner()
    }

    fn close_inner(&mut self) -> Result<(), Error> {
        self.client.__private_api_close();

        self.connection.poll_block_on(|_, _, done| {
            if done {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        })
    }
}
