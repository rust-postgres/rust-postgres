use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::query::extract_row_affected;
use crate::types::Oid;
use crate::{Error, SimpleQueryMessage, SimpleQueryRow};
use bytes::Bytes;
use fallible_iterator::FallibleIterator;
use futures_util::Stream;
use log::debug;
use pin_project_lite::pin_project;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

/// Information about a column of a single query row.
#[derive(Debug)]
pub struct SimpleColumn {
    name: String,
    type_oid: Oid,
    type_modifier: i32,
}

impl SimpleColumn {
    pub(crate) fn new(name: String, type_oid: Oid, type_modifier: i32) -> SimpleColumn {
        SimpleColumn {
            name,
            type_oid,
            type_modifier,
        }
    }

    /// Returns the name of the column.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the OID of the column's type.
    pub fn type_oid(&self) -> Oid {
        self.type_oid
    }

    /// Returns the type modifier of the column, or -1 if it has none.
    ///
    /// The meaning of the value depends on the type; see `pg_attribute.atttypmod`. For example,
    /// a `VARCHAR(10)` column has a modifier of 14 (the length plus 4 bytes of header).
    pub fn type_modifier(&self) -> i32 {
        self.type_modifier
    }
}

/// The completion of a statement in a simple query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimpleCommandTag {
    tag: String,
    rows: u64,
}

impl SimpleCommandTag {
    /// Returns the command tag, such as `SELECT 3`, `INSERT 0 2` or `CREATE TABLE`.
    pub fn tag(&self) -> &str {
        &self.tag
    }

    /// Returns the number of rows modified or selected, or 0 if the command reports none.
    pub fn rows(&self) -> u64 {
        self.rows
    }
}

pub async fn simple_query(client: &InnerClient, query: &str) -> Result<SimpleQueryStream, Error> {
    debug!("executing simple query: {query}");

    let buf = encode(client, query)?;
    let responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    Ok(SimpleQueryStream {
        responses,
        columns: None,
        command_tags: false,
    })
}

pub async fn batch_execute(client: &InnerClient, query: &str) -> Result<(), Error> {
    debug!("executing statement batch: {query}");

    let buf = encode(client, query)?;
    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    loop {
        match responses.next().await? {
            Message::ReadyForQuery(_) => return Ok(()),
            Message::CommandComplete(_)
            | Message::EmptyQueryResponse
            | Message::RowDescription(_)
            | Message::DataRow(_) => {}
            _ => return Err(Error::unexpected_message()),
        }
    }
}

fn encode(client: &InnerClient, query: &str) -> Result<Bytes, Error> {
    client.with_buf(|buf| {
        frontend::query(query, buf).map_err(Error::encode)?;
        Ok(buf.split().freeze())
    })
}

pin_project! {
    /// A stream of simple query results.
    #[project(!Unpin)]
    pub struct SimpleQueryStream {
        responses: Responses,
        columns: Option<Arc<[SimpleColumn]>>,
        command_tags: bool,
    }
}

impl SimpleQueryStream {
    /// Reports each completed statement as [`SimpleQueryMessage::CommandTag`] and each empty
    /// statement as [`SimpleQueryMessage::EmptyQuery`], instead of `CommandComplete`.
    pub fn with_command_tags(mut self) -> SimpleQueryStream {
        self.command_tags = true;
        self
    }
}

impl Stream for SimpleQueryStream {
    type Item = Result<SimpleQueryMessage, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match ready!(this.responses.poll_next(cx)?) {
            Message::CommandComplete(body) => {
                let rows = extract_row_affected(&body)?;
                if *this.command_tags {
                    let tag = body.tag().map_err(Error::parse)?.to_string();
                    let tag = SimpleCommandTag { tag, rows };
                    return Poll::Ready(Some(Ok(SimpleQueryMessage::CommandTag(tag))));
                }
                Poll::Ready(Some(Ok(SimpleQueryMessage::CommandComplete(rows))))
            }
            Message::EmptyQueryResponse if *this.command_tags => {
                Poll::Ready(Some(Ok(SimpleQueryMessage::EmptyQuery)))
            }
            Message::EmptyQueryResponse => {
                Poll::Ready(Some(Ok(SimpleQueryMessage::CommandComplete(0))))
            }
            Message::RowDescription(body) => {
                let columns: Arc<[SimpleColumn]> = body
                    .fields()
                    .map(|f| {
                        Ok(SimpleColumn::new(
                            f.name().to_string(),
                            f.type_oid(),
                            f.type_modifier(),
                        ))
                    })
                    .collect::<Vec<_>>()
                    .map_err(Error::parse)?
                    .into();

                *this.columns = Some(columns.clone());
                Poll::Ready(Some(Ok(SimpleQueryMessage::RowDescription(columns))))
            }
            Message::DataRow(body) => {
                let row = match &this.columns {
                    Some(columns) => SimpleQueryRow::new(columns.clone(), body)?,
                    None => return Poll::Ready(Some(Err(Error::unexpected_message()))),
                };
                Poll::Ready(Some(Ok(SimpleQueryMessage::Row(row))))
            }
            Message::ReadyForQuery(_) => Poll::Ready(None),
            _ => Poll::Ready(Some(Err(Error::unexpected_message()))),
        }
    }
}
