use crate::connection::ConnectionRef;
use crate::lazy_pin::LazyPin;
use bytes::{Buf, Bytes};
use futures_util::StreamExt;
use std::io::{self, BufRead, Read};
use tokio_postgres::CopyOutStream;

/// The reader returned by the `copy_out` method.
pub struct CopyOutReader<'a> {
    pub(crate) connection: ConnectionRef<'a>,
    pub(crate) stream: LazyPin<CopyOutStream>,
    cur: Bytes,
}

impl<'a> CopyOutReader<'a> {
    pub(crate) fn new(connection: ConnectionRef<'a>, stream: CopyOutStream) -> CopyOutReader<'a> {
        CopyOutReader {
            connection,
            stream: LazyPin::new(stream),
            cur: Bytes::new(),
        }
    }
}

impl Read for CopyOutReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let b = self.fill_buf()?;
        let len = usize::min(buf.len(), b.len());
        buf[..len].copy_from_slice(&b[..len]);
        self.consume(len);
        Ok(len)
    }
}

impl BufRead for CopyOutReader<'_> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        while !self.cur.has_remaining() {
            let mut stream = self.stream.pinned();
            match self
                .connection
                .block_on(async { stream.next().await.transpose() })
            {
                Ok(Some(cur)) => self.cur = cur,
                Err(e) => return Err(io::Error::other(e)),
                Ok(None) => break,
            };
        }

        Ok(&self.cur)
    }

    fn consume(&mut self, amt: usize) {
        self.cur.advance(amt);
    }
}
