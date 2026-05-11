use crate::codec::{BackendMessage, BackendMessages, FrontendMessage, PostgresCodec};
use crate::copy_in::CopyInReceiver;
use crate::error::DbError;
use crate::maybe_tls_stream::MaybeTlsStream;
use crate::{AsyncMessage, Error, Notification};
use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use futures_channel::mpsc;
use futures_util::{Sink, Stream, StreamExt, stream::FusedStream};
use log::{info, trace};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

pub enum RequestMessages {
    Single(FrontendMessage),
    CopyIn(CopyInReceiver),
}

pub struct Request {
    pub messages: RequestMessages,
    pub sender: mpsc::Sender<BackendMessages>,
}

pub struct Response {
    sender: mpsc::Sender<BackendMessages>,
    /// Frames the consumer's sender wasn't ready to accept when they
    /// arrived from the wire. Kept per-response so an in-flight typeinfo
    /// sub-query (whose response is queued behind the original query's
    /// DataRows on the same socket) can be routed to a *different*
    /// response while this one is congested. The `bool` tracks whether the
    /// frame carries `ReadyForQuery` — once that has been *delivered* (not
    /// just observed) the response is removed from the queue.
    parked: VecDeque<(BackendMessages, bool)>,
    /// Set true once a `ReadyForQuery` frame for this response has been
    /// seen on the wire (whether or not it's been pushed to `sender` yet).
    /// New wire frames after this point are routed to the *next*
    /// in-flight response, not this one.
    completion_seen: bool,
}

#[derive(PartialEq, Debug)]
enum State {
    Active,
    Terminating,
    Closing,
}

/// A connection to a PostgreSQL database.
///
/// This is one half of what is returned when a new connection is established. It performs the actual IO with the
/// server, and should generally be spawned off onto an executor to run in the background.
///
/// `Connection` implements `Future`, and only resolves when the connection is closed, either because a fatal error has
/// occurred, or because its associated `Client` has dropped and all outstanding work has completed.
#[must_use = "futures do nothing unless polled"]
pub struct Connection<S, T> {
    stream: Framed<MaybeTlsStream<S, T>, PostgresCodec>,
    parameters: HashMap<String, String>,
    receiver: mpsc::UnboundedReceiver<Request>,
    pending_request: Option<RequestMessages>,
    pending_responses: VecDeque<BackendMessage>,
    responses: VecDeque<Response>,
    state: State,
}

impl<S, T> Connection<S, T>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) fn new(
        stream: Framed<MaybeTlsStream<S, T>, PostgresCodec>,
        pending_responses: VecDeque<BackendMessage>,
        parameters: HashMap<String, String>,
        receiver: mpsc::UnboundedReceiver<Request>,
    ) -> Connection<S, T> {
        Connection {
            stream,
            parameters,
            receiver,
            pending_request: None,
            pending_responses,
            responses: VecDeque::new(),
            state: State::Active,
        }
    }

    fn poll_response(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<BackendMessage, Error>>> {
        if let Some(message) = self.pending_responses.pop_front() {
            trace!("retrying pending response");
            return Poll::Ready(Some(Ok(message)));
        }

        Pin::new(&mut self.stream)
            .poll_next(cx)
            .map(|o| o.map(|r| r.map_err(Error::io)))
    }

    /// Walk every in-flight response and try to flush its parked frames
    /// into its sender. We poll each sender independently so a congested
    /// response (e.g. a streaming query whose consumer is paused waiting
    /// on a typeinfo lookup) doesn't block delivery to other responses
    /// (e.g. that very typeinfo sub-query) — that's the whole point of
    /// per-response parking. The waker registered by each `poll_ready`
    /// will wake us back up when the corresponding consumer pulls.
    fn drain_all_parked(&mut self, cx: &mut Context<'_>) {
        // Track responses whose parked queue we exhausted *and* whose
        // completion frame is among the delivered. They can be removed.
        let mut idx = 0;
        while idx < self.responses.len() {
            let response = &mut self.responses[idx];
            let mut remove = false;
            while let Some((_, _)) = response.parked.front() {
                match response.sender.poll_ready(cx) {
                    Poll::Ready(Ok(())) => {
                        let (messages, frame_complete) = response.parked.pop_front().unwrap();
                        let _ = response.sender.start_send(messages);
                        if frame_complete {
                            // The completion frame just left for the
                            // consumer — this response is finished from
                            // the Connection's POV.
                            remove = true;
                            break;
                        }
                    }
                    Poll::Ready(Err(_)) => {
                        // Receiver hung up. Drain the rest of the parked
                        // queue silently so the wire stays in sync, then
                        // remove the response once we hit its completion
                        // frame (or, if it's not yet seen, leave it as a
                        // marker so subsequent wire frames are routed
                        // correctly).
                        let (_, frame_complete) = response.parked.pop_front().unwrap();
                        if frame_complete {
                            remove = true;
                            break;
                        }
                    }
                    Poll::Pending => break, // sender still backed up; try next response
                }
            }
            if remove {
                self.responses.remove(idx);
            } else {
                idx += 1;
            }
        }
    }

    /// Index of the response that the next wire frame belongs to. Frames
    /// arrive in strict FIFO order per request, so the target is the first
    /// response whose `ReadyForQuery` has *not* been observed yet. If a
    /// response's completion frame is parked (sent but not yet delivered to
    /// the consumer), it's still "done" from the wire's POV — the next
    /// frame is for a later response.
    fn target_response_idx(&self) -> Option<usize> {
        self.responses
            .iter()
            .position(|r| !r.completion_seen)
    }

    fn poll_read(&mut self, cx: &mut Context<'_>) -> Result<Option<AsyncMessage>, Error> {
        if self.state != State::Active {
            trace!("poll_read: done");
            return Ok(None);
        }

        loop {
            // Try to flush any backlog from a previous iteration before
            // pulling new bytes off the wire.
            self.drain_all_parked(cx);

            let message = match self.poll_response(cx)? {
                Poll::Ready(Some(message)) => message,
                Poll::Ready(None) => return Err(Error::closed()),
                Poll::Pending => {
                    trace!("poll_read: waiting on response");
                    return Ok(None);
                }
            };

            let (mut messages, request_complete) = match message {
                BackendMessage::Async(Message::NoticeResponse(body)) => {
                    let error = DbError::parse(&mut body.fields()).map_err(Error::parse)?;
                    return Ok(Some(AsyncMessage::Notice(error)));
                }
                BackendMessage::Async(Message::NotificationResponse(body)) => {
                    let notification = Notification {
                        process_id: body.process_id(),
                        channel: body.channel().map_err(Error::parse)?.to_string(),
                        payload: body.message().map_err(Error::parse)?.to_string(),
                    };
                    return Ok(Some(AsyncMessage::Notification(notification)));
                }
                BackendMessage::Async(Message::ParameterStatus(body)) => {
                    self.parameters.insert(
                        body.name().map_err(Error::parse)?.to_string(),
                        body.value().map_err(Error::parse)?.to_string(),
                    );
                    continue;
                }
                BackendMessage::Async(_) => unreachable!(),
                BackendMessage::Normal {
                    messages,
                    request_complete,
                } => (messages, request_complete),
            };

            let Some(target_idx) = self.target_response_idx() else {
                // No in-flight request — only acceptable if the wire sent
                // us an `ErrorResponse` to surface as a connection error.
                return match messages.next().map_err(Error::parse)? {
                    Some(Message::ErrorResponse(error)) => Err(Error::db(error)),
                    _ => Err(Error::unexpected_message()),
                };
            };

            let response = &mut self.responses[target_idx];

            if !response.parked.is_empty() {
                // Must preserve protocol order: a later wire frame for the
                // same response can't be sent before an earlier parked one.
                if request_complete {
                    response.completion_seen = true;
                }
                response.parked.push_back((messages, request_complete));
                continue;
            }

            match response.sender.poll_ready(cx) {
                Poll::Ready(Ok(())) => {
                    let _ = response.sender.start_send(messages);
                    if request_complete {
                        // Completion frame delivered straight through to
                        // the consumer — drop the response.
                        self.responses.remove(target_idx);
                    }
                }
                Poll::Ready(Err(_)) => {
                    if request_complete {
                        self.responses.remove(target_idx);
                    }
                }
                Poll::Pending => {
                    // Park this frame. New wire frames for *this* response
                    // will continue to be parked (preserving order), while
                    // frames for *later* responses can still be delivered
                    // directly via `target_response_idx()`.
                    if request_complete {
                        response.completion_seen = true;
                    }
                    response.parked.push_back((messages, request_complete));
                    trace!("poll_read: parking frame for congested response");
                }
            }
        }
    }

    fn poll_request(&mut self, cx: &mut Context<'_>) -> Poll<Option<RequestMessages>> {
        if let Some(messages) = self.pending_request.take() {
            trace!("retrying pending request");
            return Poll::Ready(Some(messages));
        }

        if self.receiver.is_terminated() {
            return Poll::Ready(None);
        }

        match self.receiver.poll_next_unpin(cx) {
            Poll::Ready(Some(request)) => {
                trace!("polled new request");
                self.responses.push_back(Response {
                    sender: request.sender,
                    parked: VecDeque::new(),
                    completion_seen: false,
                });
                Poll::Ready(Some(request.messages))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_write(&mut self, cx: &mut Context<'_>) -> Result<bool, Error> {
        loop {
            if self.state == State::Closing {
                trace!("poll_write: done");
                return Ok(false);
            }

            if Pin::new(&mut self.stream)
                .poll_ready(cx)
                .map_err(Error::io)?
                .is_pending()
            {
                trace!("poll_write: waiting on socket");
                return Ok(false);
            }

            let request = match self.poll_request(cx) {
                Poll::Ready(Some(request)) => request,
                Poll::Ready(None) if self.responses.is_empty() && self.state == State::Active => {
                    trace!("poll_write: at eof, terminating");
                    self.state = State::Terminating;
                    let mut request = BytesMut::new();
                    frontend::terminate(&mut request);
                    RequestMessages::Single(FrontendMessage::Raw(request.freeze()))
                }
                Poll::Ready(None) => {
                    trace!(
                        "poll_write: at eof, pending responses {}",
                        self.responses.len()
                    );
                    return Ok(true);
                }
                Poll::Pending => {
                    trace!("poll_write: waiting on request");
                    return Ok(true);
                }
            };

            match request {
                RequestMessages::Single(request) => {
                    Pin::new(&mut self.stream)
                        .start_send(request)
                        .map_err(Error::io)?;
                    if self.state == State::Terminating {
                        trace!("poll_write: sent eof, closing");
                        self.state = State::Closing;
                    }
                }
                RequestMessages::CopyIn(mut receiver) => {
                    let message = match receiver.poll_next_unpin(cx) {
                        Poll::Ready(Some(message)) => message,
                        Poll::Ready(None) => {
                            trace!("poll_write: finished copy_in request");
                            continue;
                        }
                        Poll::Pending => {
                            trace!("poll_write: waiting on copy_in stream");
                            self.pending_request = Some(RequestMessages::CopyIn(receiver));
                            return Ok(true);
                        }
                    };
                    Pin::new(&mut self.stream)
                        .start_send(message)
                        .map_err(Error::io)?;
                    self.pending_request = Some(RequestMessages::CopyIn(receiver));
                }
            }
        }
    }

    fn poll_flush(&mut self, cx: &mut Context<'_>) -> Result<(), Error> {
        match Pin::new(&mut self.stream)
            .poll_flush(cx)
            .map_err(Error::io)?
        {
            Poll::Ready(()) => trace!("poll_flush: flushed"),
            Poll::Pending => trace!("poll_flush: waiting on socket"),
        }
        Ok(())
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        if self.state != State::Closing {
            return Poll::Pending;
        }

        match Pin::new(&mut self.stream)
            .poll_close(cx)
            .map_err(Error::io)?
        {
            Poll::Ready(()) => {
                trace!("poll_shutdown: complete");
                Poll::Ready(Ok(()))
            }
            Poll::Pending => {
                trace!("poll_shutdown: waiting on socket");
                Poll::Pending
            }
        }
    }

    /// Returns the value of a runtime parameter for this connection.
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.parameters.get(name).map(|s| &**s)
    }

    fn poll_message_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<AsyncMessage, Error>>> {
        let message = self.poll_read(cx)?;
        let want_flush = self.poll_write(cx)?;
        if want_flush {
            self.poll_flush(cx)?;
        }
        match message {
            Some(message) => Poll::Ready(Some(Ok(message))),
            None => match self.poll_shutdown(cx) {
                Poll::Ready(Ok(())) => Poll::Ready(None),
                Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
                Poll::Pending => Poll::Pending,
            },
        }
    }

    /// Polls for asynchronous messages from the server.
    ///
    /// The server can send notices as well as notifications asynchronously to the client. Applications that wish to
    /// examine those messages should use this method to drive the connection rather than its `Future` implementation.
    ///
    /// Return values of `None` or `Some(Err(_))` are "terminal"; callers should not invoke this method again after
    /// receiving one of those values.
    pub fn poll_message(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<AsyncMessage, Error>>> {
        match self.poll_message_inner(cx) {
            nominal @ (Poll::Pending | Poll::Ready(Some(Ok(_)))) => nominal,
            terminal @ (Poll::Ready(None) | Poll::Ready(Some(Err(_)))) => {
                self.receiver.close();
                terminal
            }
        }
    }
}

impl<S, T> Future for Connection<S, T>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        while let Some(message) = ready!(self.poll_message(cx)?) {
            if let AsyncMessage::Notice(notice) = message {
                info!("{}: {}", notice.severity(), notice.message());
            }
        }
        Poll::Ready(Ok(()))
    }
}
