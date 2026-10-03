use bytes::{Buf, Bytes, BytesMut};
use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend;
use postgres_protocol::message::frontend::CopyData;
use std::io;
use tokio_util::codec::{Decoder, Encoder};

pub enum FrontendMessage {
    Raw(Bytes),
    CopyData(CopyData<Box<dyn Buf + Send>>),
}

pub enum BackendMessage {
    Normal {
        messages: BackendMessages,
        request_complete: bool,
    },
    Async(backend::Message),
}

pub struct BackendMessages(BytesMut);

impl BackendMessages {
    pub fn with_capacity(capacity: usize) -> BackendMessages {
        BackendMessages(BytesMut::with_capacity(capacity))
    }

    fn with_buffer(buffer: BytesMut) -> BackendMessages {
        BackendMessages(buffer)
    }
}

impl FallibleIterator for BackendMessages {
    type Item = backend::Message;
    type Error = io::Error;

    fn next(&mut self) -> io::Result<Option<backend::Message>> {
        backend::Message::parse(&mut self.0)
    }
}

pub struct PostgresCodec {
    /// Upper bound (in bytes) the `Framed` read buffer is reclaimed back down
    /// to once a completed batch drains it, instead of pinning grown capacity.
    max_buffer_size: usize,
}

impl PostgresCodec {
    pub fn new(max_buffer_size: usize) -> PostgresCodec {
        PostgresCodec { max_buffer_size }
    }

    /// Upper bound (in bytes) the codec's `Framed` buffers should be reclaimed
    /// back down to once a connection goes idle.
    pub fn max_buffer_size(&self) -> usize {
        self.max_buffer_size
    }
}

impl Encoder<FrontendMessage> for PostgresCodec {
    type Error = io::Error;

    fn encode(&mut self, item: FrontendMessage, dst: &mut BytesMut) -> io::Result<()> {
        match item {
            FrontendMessage::Raw(buf) => dst.extend_from_slice(&buf),
            FrontendMessage::CopyData(data) => data.write(dst),
        }

        Ok(())
    }
}

impl Decoder for PostgresCodec {
    type Item = BackendMessage;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<BackendMessage>, io::Error> {
        let mut idx = 0;
        let mut request_complete = false;

        while let Some(header) = backend::Header::parse(&src[idx..])? {
            let len = header.len() as usize + 1;
            if src[idx..].len() < len {
                break;
            }

            match header.tag() {
                backend::NOTICE_RESPONSE_TAG
                | backend::NOTIFICATION_RESPONSE_TAG
                | backend::PARAMETER_STATUS_TAG => {
                    if idx == 0 {
                        let message = backend::Message::parse(src)?.unwrap();
                        return Ok(Some(BackendMessage::Async(message)));
                    } else {
                        break;
                    }
                }
                _ => {}
            }

            idx += len;

            if header.tag() == backend::READY_FOR_QUERY_TAG {
                request_complete = true;
                break;
            }
        }

        if idx == 0 {
            Ok(None)
        } else {
            let messages = BackendMessages::with_buffer(src.split_to(idx));

            // `Framed` keeps `src` grown to the largest batch ever read, pinning
            // multi-MB capacity for a pooled connection's lifetime. Once the
            // remaining tail fits the bound, move it into a freshly bounded
            // buffer. A single in-flight message larger than the bound is left
            // untouched so its bytes stay contiguous.
            if src.capacity() > self.max_buffer_size && src.len() <= self.max_buffer_size {
                let mut bounded = BytesMut::with_capacity(self.max_buffer_size);
                bounded.extend_from_slice(src);
                *src = bounded;
            }

            Ok(Some(BackendMessage::Normal {
                messages,
                request_complete,
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;
    use postgres_protocol::message::backend;

    /// Append a single backend protocol frame: tag, big-endian length (which
    /// includes the 4-byte length field but not the tag), then the body.
    fn push_frame(buf: &mut BytesMut, tag: u8, body: &[u8]) {
        buf.put_u8(tag);
        buf.put_i32((body.len() + 4) as i32);
        buf.put_slice(body);
    }

    /// A completed batch that fully drains an oversized read buffer must be
    /// reclaimed back down to the configured bound.
    #[test]
    fn decode_reclaims_oversized_drained_buffer() {
        let bound = 256;
        let mut codec = PostgresCodec::new(bound);

        let mut src = BytesMut::with_capacity(1024 * 1024);
        push_frame(&mut src, backend::COMMAND_COMPLETE_TAG, b"INSERT 0 1\0");
        push_frame(&mut src, backend::READY_FOR_QUERY_TAG, b"I");

        assert!(src.capacity() > bound);
        let message = codec.decode(&mut src).unwrap();
        assert!(matches!(
            message,
            Some(BackendMessage::Normal {
                request_complete: true,
                ..
            })
        ));
        // Buffer was fully drained, so it should be reclaimed to the bound.
        assert!(src.is_empty());
        assert!(src.capacity() <= bound);
    }

    /// A buffer that stays under the bound is left untouched (no realloc churn).
    #[test]
    fn decode_keeps_small_buffer() {
        let bound = 1024 * 1024;
        let mut codec = PostgresCodec::new(bound);

        let mut src = BytesMut::with_capacity(256);
        push_frame(&mut src, backend::COMMAND_COMPLETE_TAG, b"SELECT 1\0");
        push_frame(&mut src, backend::READY_FOR_QUERY_TAG, b"I");
        let capacity_before = src.capacity();

        let _ = codec.decode(&mut src).unwrap();

        // Under the bound: capacity is not forcibly reallocated upward.
        assert!(src.capacity() <= capacity_before.max(bound));
    }

    /// When a partial trailing message remains, its bytes must survive while
    /// the oversized backing buffer is still reclaimed (the tail is copied
    /// forward, not dropped).
    #[test]
    fn decode_preserves_partial_trailing_message() {
        let bound = 64;
        let mut codec = PostgresCodec::new(bound);

        let mut src = BytesMut::with_capacity(1024 * 1024);
        push_frame(&mut src, backend::COMMAND_COMPLETE_TAG, b"INSERT 0 1\0");
        push_frame(&mut src, backend::READY_FOR_QUERY_TAG, b"I");
        // A trailing frame header that promises 100 body bytes but supplies none.
        src.put_u8(backend::DATA_ROW_TAG);
        src.put_i32(104);

        let _ = codec.decode(&mut src).unwrap();

        // `ReadyForQuery` ends the batch before the trailing frame, so its
        // header bytes must survive...
        assert!(!src.is_empty());
        // ...exactly the 5-byte trailing header remains...
        assert_eq!(src.len(), 5);
        // ...and because that tail fits the bound, the oversized backing is
        // reclaimed rather than retained.
        assert!(src.capacity() <= bound);
    }

    /// Simulates the bounce-back: a small amount of new data sitting in a
    /// reclaimed oversized backing (capacity well past the bound) must be
    /// reclaimed on the next decode, preserving the in-flight bytes.
    #[test]
    fn decode_reclaims_oversized_backing_with_small_tail() {
        let bound = 64;
        let mut codec = PostgresCodec::new(bound);

        // Oversized backing holding a complete frame plus a small partial tail.
        let mut src = BytesMut::with_capacity(1024 * 1024);
        push_frame(&mut src, backend::COMMAND_COMPLETE_TAG, b"INSERT 0 1\0");
        // Partial trailing header (5 bytes) — no ReadyForQuery this time.
        src.put_u8(backend::DATA_ROW_TAG);
        src.put_i32(104);

        assert!(src.capacity() > bound);
        let _ = codec.decode(&mut src).unwrap();

        // The complete frame was consumed; the 5-byte partial tail survives and
        // the buffer is reclaimed to the bound.
        assert_eq!(src.len(), 5);
        assert!(src.capacity() <= bound);
    }

    /// A single in-flight message larger than the bound must not be reclaimed —
    /// its bytes have to stay contiguous, so the buffer may exceed the bound
    /// for exactly that case.
    #[test]
    fn decode_keeps_oversized_single_message() {
        let bound = 64;
        let mut codec = PostgresCodec::new(bound);

        let mut src = BytesMut::with_capacity(1024 * 1024);
        // A complete small frame to consume, then a large partial frame that
        // alone exceeds the bound and is still arriving.
        push_frame(&mut src, backend::COMMAND_COMPLETE_TAG, b"INSERT 0 1\0");
        src.put_u8(backend::DATA_ROW_TAG);
        src.put_i32(2004); // promises 2000 body bytes...
        src.put_slice(&[0u8; 200]); // ...but only 200 have arrived so far.

        let len_before = src.len();
        let _ = codec.decode(&mut src).unwrap();

        // The partial large message (> bound) remains intact and is not
        // truncated or reclaimed.
        assert!(src.len() > bound);
        assert!(src.len() < len_before);
    }
}
