use crate::connection::ConnectionRef;
use crate::lazy_pin::LazyPin;
use bytes::{Bytes, BytesMut};
use futures_util::SinkExt;
use std::io;
use std::io::Write;
use tokio_postgres::{CopyInSink, Error};

/// The writer returned by the `copy_in` method.
///
/// The copy *must* be explicitly completed via the `finish` method. If it is not, the copy will be aborted.
pub struct CopyInWriter<'a> {
    pub(crate) connection: ConnectionRef<'a>,
    pub(crate) sink: LazyPin<CopyInSink<Bytes>>,
    buf: BytesMut,
}

impl<'a> CopyInWriter<'a> {
    pub(crate) fn new(connection: ConnectionRef<'a>, sink: CopyInSink<Bytes>) -> CopyInWriter<'a> {
        CopyInWriter {
            connection,
            sink: LazyPin::new(sink),
            buf: BytesMut::new(),
        }
    }

    /// Completes the copy, returning the number of rows written.
    ///
    /// If this is not called, the copy will be aborted.
    pub fn finish(mut self) -> Result<u64, Error> {
        self.flush_inner()?;
        self.connection.block_on(self.sink.pinned().finish())
    }

    fn flush_inner(&mut self) -> Result<(), Error> {
        if self.buf.is_empty() {
            return Ok(());
        }

        self.connection
            .block_on(self.sink.pinned().send(self.buf.split().freeze()))
    }
}

impl Write for CopyInWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.buf.len() > 4096 {
            self.flush()?;
        }

        self.buf.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_inner().map_err(io::Error::other)
    }
}
