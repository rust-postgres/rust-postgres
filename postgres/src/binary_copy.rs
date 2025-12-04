//! Utilities for working with the PostgreSQL binary copy format.

use crate::connection::ConnectionRef;
use crate::types::{BorrowToSql, ToSql, Type};
use crate::{CopyInWriter, CopyOutReader, Error};
use fallible_iterator::FallibleIterator;
use futures_util::StreamExt;
use std::pin::Pin;
#[doc(inline)]
pub use tokio_postgres::binary_copy::BinaryCopyOutRow;
use tokio_postgres::binary_copy::{self, BinaryCopyOutStream};

/// A type which serializes rows into the PostgreSQL binary copy format.
///
/// The copy *must* be explicitly completed via the `finish` method. If it is not, the copy will be aborted.
#[derive(Debug)]
pub struct BinaryCopyInWriter<'a> {
    connection: ConnectionRef<'a>,
    sink: Pin<Box<binary_copy::BinaryCopyInWriter>>,
}

impl<'a> BinaryCopyInWriter<'a> {
    /// Creates a new writer which will write rows of the provided types.
    pub fn new(writer: CopyInWriter<'a>, types: &[Type]) -> BinaryCopyInWriter<'a> {
        let stream = writer
            .sink
            .into_unpinned()
            .expect("writer has already been written to");

        BinaryCopyInWriter {
            connection: writer.connection,
            sink: Box::pin(binary_copy::BinaryCopyInWriter::new(stream, types)),
        }
    }

    /// Writes a single row.
    ///
    /// # Panics
    ///
    /// Panics if the number of values provided does not match the number expected.
    pub fn write(&mut self, values: &[&(dyn ToSql + Sync)]) -> Result<(), Error> {
        self.connection.block_on(self.sink.as_mut().write(values))
    }

    /// A maximally-flexible version of `write`.
    ///
    /// # Panics
    ///
    /// Panics if the number of values provided does not match the number expected.
    pub fn write_raw<P, I>(&mut self, values: I) -> Result<(), Error>
    where
        P: BorrowToSql,
        I: IntoIterator<Item = P>,
        I::IntoIter: ExactSizeIterator,
    {
        self.connection
            .block_on(self.sink.as_mut().write_raw(values))
    }

    /// Completes the copy, returning the number of rows added.
    ///
    /// This method *must* be used to complete the copy process. If it is not, the copy will be aborted.
    pub fn finish(mut self) -> Result<u64, Error> {
        self.connection.block_on(self.sink.as_mut().finish())
    }
}

/// An iterator of rows deserialized from the PostgreSQL binary copy format.
#[derive(Debug)]
pub struct BinaryCopyOutIter<'a> {
    connection: ConnectionRef<'a>,
    stream: Pin<Box<BinaryCopyOutStream>>,
}

impl<'a> BinaryCopyOutIter<'a> {
    /// Creates a new iterator from a raw copy out reader and the types of the columns being returned.
    pub fn new(reader: CopyOutReader<'a>, types: &[Type]) -> BinaryCopyOutIter<'a> {
        let stream = reader
            .stream
            .into_unpinned()
            .expect("reader has already been read from");

        BinaryCopyOutIter {
            connection: reader.connection,
            stream: Box::pin(BinaryCopyOutStream::new(stream, types)),
        }
    }
}

impl FallibleIterator for BinaryCopyOutIter<'_> {
    type Item = BinaryCopyOutRow;
    type Error = Error;

    fn next(&mut self) -> Result<Option<BinaryCopyOutRow>, Error> {
        let stream = &mut self.stream;
        self.connection
            .block_on(async { stream.next().await.transpose() })
    }
}
