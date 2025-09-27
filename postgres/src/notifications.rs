//! Asynchronous notifications.

use crate::connection::ConnectionRef;
use crate::{Error, Notification};
use fallible_iterator::FallibleIterator;
use futures_util::FutureExt;
use std::pin::Pin;
use std::task::{ready, Poll};
use std::time::Duration;
use tokio::time::{self, Instant, Sleep};

/// Notifications from a PostgreSQL backend.
pub struct Notifications<'a> {
    connection: ConnectionRef<'a>,
}

impl<'a> Notifications<'a> {
    pub(crate) fn new(connection: ConnectionRef<'a>) -> Notifications<'a> {
        Notifications { connection }
    }

    /// Returns the number of already buffered pending notifications.
    pub fn len(&self) -> usize {
        self.connection.notifications().len()
    }

    /// Determines if there are any already buffered pending notifications.
    pub fn is_empty(&self) -> bool {
        self.connection.notifications().is_empty()
    }

    /// Returns a nonblocking iterator over notifications.
    ///
    /// If there are no already buffered pending notifications, this iterator will poll the connection but will not
    /// block waiting on notifications over the network. A return value of `None` either indicates that there are no
    /// pending notifications or that the server has disconnected.
    ///
    /// # Note
    ///
    /// This iterator may start returning `Some` after previously returning `None` if more notifications are received.
    pub fn iter(&mut self) -> Iter<'_> {
        Iter {
            connection: self.connection.as_ref(),
        }
    }

    /// Returns a blocking iterator over notifications.
    ///
    /// If there are no already buffered pending notifications, this iterator will block indefinitely waiting on the
    /// PostgreSQL backend server to send one. It will only return `None` if the server has disconnected.
    pub fn blocking_iter(&mut self) -> BlockingIter<'_> {
        BlockingIter {
            connection: self.connection.as_ref(),
        }
    }

    /// Returns an iterator over notifications which blocks a limited amount of time.
    ///
    /// If there are no already buffered pending notifications, this iterator will block waiting on the PostgreSQL
    /// backend server to send one up to the provided timeout. A return value of `None` either indicates that there are
    /// no pending notifications or that the server has disconnected.
    ///
    /// # Note
    ///
    /// This iterator may start returning `Some` after previously returning `None` if more notifications are received.
    pub fn timeout_iter(&mut self, timeout: Duration) -> TimeoutIter<'_> {
        TimeoutIter {
            delay: Box::pin(self.connection.enter(|| time::sleep(timeout))),
            timeout,
            connection: self.connection.as_ref(),
        }
    }
}

/// A nonblocking iterator over pending notifications.
pub struct Iter<'a> {
    connection: ConnectionRef<'a>,
}

impl FallibleIterator for Iter<'_> {
    type Item = Notification;
    type Error = Error;

    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        if let Some(notification) = self.connection.notifications_mut().pop_front() {
            return Ok(Some(notification));
        }

        self.connection
            .poll_block_on(|_, notifications, _| Poll::Ready(Ok(notifications.pop_front())))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.connection.notifications().len(), None)
    }
}

/// A blocking iterator over pending notifications.
pub struct BlockingIter<'a> {
    connection: ConnectionRef<'a>,
}

impl FallibleIterator for BlockingIter<'_> {
    type Item = Notification;
    type Error = Error;

    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        if let Some(notification) = self.connection.notifications_mut().pop_front() {
            return Ok(Some(notification));
        }

        self.connection
            .poll_block_on(|_, notifications, done| match notifications.pop_front() {
                Some(notification) => Poll::Ready(Ok(Some(notification))),
                None if done => Poll::Ready(Ok(None)),
                None => Poll::Pending,
            })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.connection.notifications().len(), None)
    }
}

/// A time-limited blocking iterator over pending notifications.
pub struct TimeoutIter<'a> {
    connection: ConnectionRef<'a>,
    delay: Pin<Box<Sleep>>,
    timeout: Duration,
}

impl FallibleIterator for TimeoutIter<'_> {
    type Item = Notification;
    type Error = Error;

    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        if let Some(notification) = self.connection.notifications_mut().pop_front() {
            self.delay.as_mut().reset(Instant::now() + self.timeout);
            return Ok(Some(notification));
        }

        let delay = &mut self.delay;
        let timeout = self.timeout;
        self.connection.poll_block_on(|cx, notifications, done| {
            match notifications.pop_front() {
                Some(notification) => {
                    delay.as_mut().reset(Instant::now() + timeout);
                    return Poll::Ready(Ok(Some(notification)));
                }
                None if done => return Poll::Ready(Ok(None)),
                None => {}
            }

            ready!(delay.poll_unpin(cx));
            Poll::Ready(Ok(None))
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.connection.notifications().len(), None)
    }
}
