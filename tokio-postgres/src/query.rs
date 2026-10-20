use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::prepare::get_type;
use crate::types::{BorrowToSql, IsNull};
use crate::{Column, Error, Portal, Row, Statement};
use bytes::{Bytes, BytesMut};
use fallible_iterator::FallibleIterator;
use futures_util::Stream;
use log::{Level, debug, log_enabled};
use pin_project_lite::pin_project;
use postgres_protocol::message::backend::{CommandCompleteBody, DataRowBody, Message};
use postgres_protocol::message::frontend;
use postgres_types::Type;
use std::fmt;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

struct BorrowToSqlParamsDebug<'a, T>(&'a [T]);

impl<T> fmt::Debug for BorrowToSqlParamsDebug<'_, T>
where
    T: BorrowToSql,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list()
            .entries(self.0.iter().map(|x| x.borrow_to_sql()))
            .finish()
    }
}

pub async fn query<P, I>(
    client: &InnerClient,
    statement: Statement,
    params: I,
) -> Result<RowStream, Error>
where
    P: BorrowToSql,
    I: IntoIterator<Item = P>,
    I::IntoIter: ExactSizeIterator,
{
    let buf = if log_enabled!(Level::Debug) {
        let params = params.into_iter().collect::<Vec<_>>();
        debug!(
            "executing statement {} with parameters: {:?}",
            statement.name(),
            BorrowToSqlParamsDebug(params.as_slice()),
        );
        encode(client, &statement, params)?
    } else {
        encode(client, &statement, params)?
    };
    let responses = start(client, buf).await?;
    Ok(RowStream {
        statement,
        responses,
        rows_affected: None,
    })
}

pub async fn query_typed<P, I>(
    client: &Arc<InnerClient>,
    query: &str,
    params: I,
) -> Result<RowStream, Error>
where
    P: BorrowToSql,
    I: IntoIterator<Item = (P, Type)>,
{
    let buf = {
        let params = params.into_iter().collect::<Vec<_>>();
        let param_oids = params.iter().map(|(_, t)| t.oid()).collect::<Vec<_>>();

        client.with_buf(|buf| {
            frontend::parse("", query, param_oids, buf).map_err(Error::parse)?;
            encode_bind_raw("", params, "", buf)?;
            frontend::describe(b'S', "", buf).map_err(Error::encode)?;
            frontend::execute("", 0, buf).map_err(Error::encode)?;
            frontend::sync(buf);

            Ok(buf.split().freeze())
        })?
    };

    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    loop {
        match responses.next().await? {
            Message::ParseComplete | Message::BindComplete | Message::ParameterDescription(_) => {}
            Message::NoData => {
                return Ok(RowStream {
                    statement: Statement::unnamed(vec![], vec![]),
                    responses,
                    rows_affected: None,
                });
            }
            Message::RowDescription(row_description) => {
                let mut columns: Vec<Column> = vec![];
                let mut it = row_description.fields();
                while let Some(field) = it.next().map_err(Error::parse)? {
                    let type_ = get_type(client, field.type_oid()).await?;
                    let column = Column {
                        name: field.name().to_string(),
                        table_oid: Some(field.table_oid()).filter(|n| *n != 0),
                        column_id: Some(field.column_id()).filter(|n| *n != 0),
                        type_modifier: field.type_modifier(),
                        r#type: type_,
                    };
                    columns.push(column);
                }
                return Ok(RowStream {
                    statement: Statement::unnamed(vec![], columns),
                    responses,
                    rows_affected: None,
                });
            }
            _ => return Err(Error::unexpected_message()),
        }
    }
}

pub async fn execute_typed<P, I>(
    client: &Arc<InnerClient>,
    query: &str,
    params: I,
) -> Result<u64, Error>
where
    P: BorrowToSql,
    I: IntoIterator<Item = (P, Type)>,
{
    let buf = {
        let params = params.into_iter().collect::<Vec<_>>();
        let param_oids = params.iter().map(|(_, t)| t.oid()).collect::<Vec<_>>();

        client.with_buf(|buf| {
            frontend::parse("", query, param_oids, buf).map_err(Error::parse)?;
            encode_bind_raw("", params, "", buf)?;
            frontend::describe(b'S', "", buf).map_err(Error::encode)?;
            frontend::execute("", 0, buf).map_err(Error::encode)?;
            frontend::sync(buf);

            Ok(buf.split().freeze())
        })?
    };

    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    let mut rows = 0;

    loop {
        match responses.next().await? {
            Message::ParseComplete
            | Message::BindComplete
            | Message::ParameterDescription(_)
            | Message::RowDescription(_) => {}
            Message::NoData => {
                rows = 0;
            }

            Message::DataRow(_) => {}
            Message::CommandComplete(body) => {
                rows = extract_row_affected(&body)?;
            }

            Message::EmptyQueryResponse => rows = 0,
            Message::ReadyForQuery(_) => return Ok(rows),
            _ => {
                return Err(Error::unexpected_message());
            }
        }
    }
}

pub async fn query_portal(
    client: &InnerClient,
    portal: &Portal,
    max_rows: i32,
) -> Result<RowStream, Error> {
    let buf = client.with_buf(|buf| {
        frontend::execute(portal.name(), max_rows, buf).map_err(Error::encode)?;
        frontend::sync(buf);
        Ok(buf.split().freeze())
    })?;

    let responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    Ok(RowStream {
        statement: portal.statement().clone(),
        responses,
        rows_affected: None,
    })
}

/// Extract the number of rows affected from [`CommandCompleteBody`].
pub fn extract_row_affected(body: &CommandCompleteBody) -> Result<u64, Error> {
    let rows = body
        .tag()
        .map_err(Error::parse)?
        .rsplit(' ')
        .next()
        .unwrap()
        .parse()
        .unwrap_or(0);
    Ok(rows)
}

pub async fn execute<P, I>(
    client: &InnerClient,
    statement: Statement,
    params: I,
) -> Result<u64, Error>
where
    P: BorrowToSql,
    I: IntoIterator<Item = P>,
    I::IntoIter: ExactSizeIterator,
{
    let buf = if log_enabled!(Level::Debug) {
        let params = params.into_iter().collect::<Vec<_>>();
        debug!(
            "executing statement {} with parameters: {:?}",
            statement.name(),
            BorrowToSqlParamsDebug(params.as_slice()),
        );
        encode(client, &statement, params)?
    } else {
        encode(client, &statement, params)?
    };
    let mut responses = start(client, buf).await?;

    let mut rows = 0;
    loop {
        match responses.next().await? {
            Message::DataRow(_) => {}
            Message::CommandComplete(body) => {
                rows = extract_row_affected(&body)?;
            }
            Message::EmptyQueryResponse => rows = 0,
            Message::ReadyForQuery(_) => return Ok(rows),
            _ => return Err(Error::unexpected_message()),
        }
    }
}

async fn start(client: &InnerClient, buf: Bytes) -> Result<Responses, Error> {
    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    match responses.next().await? {
        Message::BindComplete => {}
        _ => return Err(Error::unexpected_message()),
    }

    Ok(responses)
}

pub fn encode<P, I>(client: &InnerClient, statement: &Statement, params: I) -> Result<Bytes, Error>
where
    P: BorrowToSql,
    I: IntoIterator<Item = P>,
    I::IntoIter: ExactSizeIterator,
{
    client.with_buf(|buf| {
        encode_bind(statement, params, "", buf)?;
        frontend::execute("", 0, buf).map_err(Error::encode)?;
        frontend::sync(buf);
        Ok(buf.split().freeze())
    })
}

pub fn encode_bind<P, I>(
    statement: &Statement,
    params: I,
    portal: &str,
    buf: &mut BytesMut,
) -> Result<(), Error>
where
    P: BorrowToSql,
    I: IntoIterator<Item = P>,
    I::IntoIter: ExactSizeIterator,
{
    let params = params.into_iter();
    if params.len() != statement.params().len() {
        return Err(Error::parameters(params.len(), statement.params().len()));
    }

    encode_bind_raw(
        statement.name(),
        params.zip(statement.params().iter().cloned()),
        portal,
        buf,
    )
}

fn encode_bind_raw<P, I>(
    statement_name: &str,
    params: I,
    portal: &str,
    buf: &mut BytesMut,
) -> Result<(), Error>
where
    P: BorrowToSql,
    I: IntoIterator<Item = (P, Type)>,
    I::IntoIter: ExactSizeIterator,
{
    let (param_formats, params): (Vec<_>, Vec<_>) = params
        .into_iter()
        .map(|(p, ty)| (p.borrow_to_sql().encode_format(&ty) as i16, (p, ty)))
        .unzip();

    let mut error_idx = 0;
    let r = frontend::bind(
        portal,
        statement_name,
        param_formats,
        params.into_iter().enumerate(),
        |(idx, (param, ty)), buf| match param.borrow_to_sql().to_sql_checked(&ty, buf) {
            Ok(IsNull::No) => Ok(postgres_protocol::IsNull::No),
            Ok(IsNull::Yes) => Ok(postgres_protocol::IsNull::Yes),
            Err(e) => {
                error_idx = idx;
                Err(e)
            }
        },
        Some(1),
        buf,
    );
    match r {
        Ok(()) => Ok(()),
        Err(frontend::BindError::Conversion(e)) => Err(Error::to_sql(e, error_idx)),
        Err(frontend::BindError::Serialization(e)) => Err(Error::encode(e)),
    }
}

pin_project! {
    /// A stream of table rows.
    #[project(!Unpin)]
    pub struct RowStream {
        statement: Statement,
        responses: Responses,
        rows_affected: Option<u64>,
    }
}

impl Stream for RowStream {
    type Item = Result<Row, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match ready!(poll_data_row(this.responses, this.rows_affected, cx)?) {
            Some(body) => Poll::Ready(Some(Ok(Row::new(this.statement.clone(), body)?))),
            None => Poll::Ready(None),
        }
    }
}

/// The response loop shared by `RowStream::poll_next` and `RowStream::poll_next_body`: the next
/// `DataRow` body, or `None` once the request is complete.
fn poll_data_row(
    responses: &mut Responses,
    rows_affected: &mut Option<u64>,
    cx: &mut Context<'_>,
) -> Poll<Option<Result<DataRowBody, Error>>> {
    loop {
        match ready!(responses.poll_next(cx)?) {
            Message::DataRow(body) => return Poll::Ready(Some(Ok(body))),
            Message::CommandComplete(body) => {
                *rows_affected = Some(extract_row_affected(&body)?);
            }
            Message::EmptyQueryResponse | Message::PortalSuspended => {}
            Message::ReadyForQuery(_) => return Poll::Ready(None),
            _ => return Poll::Ready(Some(Err(Error::unexpected_message()))),
        }
    }
}

/// Accepts a `DataRow` body exactly when `Row::new` would build a row from it, and fails with the
/// error `Row::new` returns otherwise: every field's range is read (a malformed length fails, in
/// the iterator's order), then the field count must equal the statement's column count. Nothing
/// is allocated.
fn check_body(columns: usize, body: DataRowBody) -> Result<DataRowBody, Error> {
    let mut ranges = body.ranges();
    let mut fields = 0usize;
    while ranges.next().map_err(Error::parse)?.is_some() {
        fields += 1;
    }
    if fields != columns {
        return Err(Error::parse(io::Error::new(
            io::ErrorKind::InvalidData,
            "DataRow field count does not match the number of columns",
        )));
    }
    Ok(body)
}

impl RowStream {
    /// Returns the columns of the rows this stream yields.
    pub fn columns(&self) -> &[Column] {
        self.statement.columns()
    }

    /// Polls for the next row as its raw `DataRow` body, without building a [`Row`].
    ///
    /// Nothing is allocated per row: the body is a reference-counted slice of the received
    /// messages. A body is handed over only when [`Row`] would accept it: every field's range is
    /// within the body and there are exactly `self.columns().len()` fields; otherwise the error
    /// is the one `Row` returns. Its values are in the binary format that `query`, `query_raw`,
    /// `query_typed_raw` and `Transaction::query_portal` request; field ranges come from
    /// `DataRowBody::ranges()` and values decode with `FromSql` on the slices.
    ///
    /// [`rows_affected`](Self::rows_affected) is set as with `poll_next`, and the two may be
    /// mixed on one stream.
    pub fn poll_next_body(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<DataRowBody, Error>>> {
        let this = self.project();
        match ready!(poll_data_row(this.responses, this.rows_affected, cx)) {
            Some(Ok(body)) => Poll::Ready(Some(check_body(this.statement.columns().len(), body))),
            other => Poll::Ready(other),
        }
    }

    /// Returns the number of rows affected by the query.
    ///
    /// This function will return `None` until the stream has been exhausted.
    pub fn rows_affected(&self) -> Option<u64> {
        self.rows_affected
    }
}

pub async fn sync(client: &InnerClient) -> Result<(), Error> {
    let buf = Bytes::from_static(b"S\0\0\0\x04");
    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    match responses.next().await? {
        Message::ReadyForQuery(_) => Ok(()),
        _ => Err(Error::unexpected_message()),
    }
}

#[cfg(test)]
mod test {
    use bytes::BytesMut;
    use postgres_protocol::message::backend::{DataRowBody, Message};

    use super::*;

    fn data_row(field_count: u16, fields: &[Option<&[u8]>]) -> DataRowBody {
        let mut body = BytesMut::new();
        body.extend_from_slice(&field_count.to_be_bytes());
        for field in fields {
            match field {
                Some(field) => {
                    body.extend_from_slice(&(field.len() as i32).to_be_bytes());
                    body.extend_from_slice(field);
                }
                None => body.extend_from_slice(&(-1i32).to_be_bytes()),
            }
        }

        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"D");
        buf.extend_from_slice(&(body.len() as i32 + 4).to_be_bytes());
        buf.extend_from_slice(&body);

        match Message::parse(&mut buf).unwrap().unwrap() {
            Message::DataRow(body) => body,
            _ => unreachable!("expected DataRow"),
        }
    }

    fn column(name: &str) -> Column {
        Column {
            name: name.to_string(),
            table_oid: None,
            column_id: None,
            type_modifier: 0,
            r#type: Type::TEXT,
        }
    }

    /// A `DataRow` message with the given field count and raw field bytes (lengths included), for
    /// bodies whose fields do not match their lengths.
    fn raw_data_row(field_count: u16, fields: &[u8]) -> DataRowBody {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(b"D");
        buf.extend_from_slice(&(fields.len() as i32 + 6).to_be_bytes());
        buf.extend_from_slice(&field_count.to_be_bytes());
        buf.extend_from_slice(fields);
        match Message::parse(&mut buf).unwrap().unwrap() {
            Message::DataRow(body) => body,
            _ => unreachable!("expected DataRow"),
        }
    }

    fn assert_same_outcome(raw: Result<DataRowBody, Error>, row: Result<Row, Error>, case: &str) {
        assert_eq!(raw.is_ok(), row.is_ok(), "{case}");
        if let (Err(raw), Err(row)) = (&raw, &row) {
            assert_eq!(raw.to_string(), row.to_string(), "{case}");
            assert_eq!(
                std::error::Error::source(raw).map(|e| e.to_string()),
                std::error::Error::source(row).map(|e| e.to_string()),
                "{case}"
            );
        }
    }

    fn statement(columns: usize) -> Statement {
        Statement::unnamed(
            vec![],
            (0..columns).map(|i| column(&i.to_string())).collect(),
        )
    }

    /// The raw read accepts and refuses exactly the bodies `Row::new` accepts and refuses,
    /// with the same error text.
    /// A field count, the fields sent and the statement's column count.
    type CountCase<'a> = (u16, &'a [Option<&'a [u8]>], usize);

    #[test]
    fn field_count_check_agrees_with_row() {
        let cases: [CountCase<'_>; 5] = [
            (2, &[Some(&b"x"[..]), Some(&b"y"[..])], 2),
            (2, &[Some(&b"x"[..]), None], 2),
            (0, &[], 0),
            (1, &[Some(&b""[..])], 2),
            (3, &[Some(&b"a"[..]), Some(&b"b"[..]), Some(&b"c"[..])], 2),
        ];
        for (count, fields, columns) in cases {
            let raw = check_body(columns, data_row(count, fields));
            let row = Row::new(statement(columns), data_row(count, fields));
            assert_same_outcome(raw, row, &format!("{count} fields, {columns} columns"));
        }
    }

    /// Malformed bodies: a field longer than the rest of the body, a truncated length, bytes left
    /// after the last field, a count above the fields sent. Refused with `Row::new`'s error.
    #[test]
    fn malformed_bodies_fail_as_in_row() {
        let cases: [(&str, u16, Vec<u8>, usize); 5] = [
            (
                "field past the end",
                1,
                [&10i32.to_be_bytes()[..], &b"ab"[..]].concat(),
                1,
            ),
            ("truncated length", 1, vec![0, 0], 1),
            (
                "trailing bytes",
                1,
                [&1i32.to_be_bytes()[..], &b"ab"[..]].concat(),
                1,
            ),
            (
                "count above fields",
                2,
                [&1i32.to_be_bytes()[..], &b"a"[..]].concat(),
                2,
            ),
            (
                "negative length is NULL",
                1,
                (-1i32).to_be_bytes().to_vec(),
                1,
            ),
        ];
        for (case, count, fields, columns) in cases {
            let raw = check_body(columns, raw_data_row(count, &fields));
            let row = Row::new(statement(columns), raw_data_row(count, &fields));
            assert_eq!(raw.is_ok(), case == "negative length is NULL", "{case}");
            assert_same_outcome(raw, row, case);
        }
    }

    /// A body handed over keeps its fields: the ranges read from it select the values sent.
    #[test]
    fn accepted_body_keeps_its_fields() {
        let body = check_body(3, data_row(3, &[Some(&b"ab"[..]), None, Some(&b""[..])])).unwrap();
        let mut ranges = body.ranges();
        let mut values = Vec::new();
        while let Some(range) = ranges.next().unwrap() {
            values.push(range.map(|r| body.buffer()[r].to_vec()));
        }
        assert_eq!(values, vec![Some(b"ab".to_vec()), None, Some(Vec::new())]);
    }
}
