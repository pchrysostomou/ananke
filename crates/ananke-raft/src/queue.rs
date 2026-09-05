//! A queue between two tasks of one server (RAFT.md §3): the `net` task fills the
//! `raft` task's inbox, the `raft` task hands entries to the `apply` task. It is an
//! async queue with no runtime behind it, like the engine's turnstile: a consumer
//! that finds it empty parks its waker, and the next push wakes it. One consumer at a
//! time; any number of producers.
//!
//! The queue is unbounded. Bounding, with the policy of which message to drop, is
//! the inbox's business in `node.rs`, done through [`Queue::count`] and
//! [`Queue::remove_first`] before a push.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

struct Inner<T> {
    items: VecDeque<T>,
    waker: Option<Waker>,
    closed: bool,
}

/// See the module documentation.
pub struct Queue<T> {
    inner: Arc<Mutex<Inner<T>>>,
}

impl<T> Clone for Queue<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Default for Queue<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Queue<T> {
    /// An empty, open queue.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                items: VecDeque::new(),
                waker: None,
                closed: false,
            })),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner<T>> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Appends `item` and wakes the consumer.
    pub fn push(&self, item: T) {
        let waker = {
            let mut inner = self.lock();
            inner.items.push_back(item);
            inner.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// The next item, once there is one; `None` once the queue is closed and empty.
    pub fn pop(&self) -> Pop<'_, T> {
        Pop(self)
    }

    /// Closes the queue: what is in it is still delivered, then `pop` returns `None`.
    pub fn close(&self) {
        let waker = {
            let mut inner = self.lock();
            inner.closed = true;
            inner.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// How many items are queued.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().items.len()
    }

    /// Whether nothing is queued.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// How many queued items satisfy `pred`.
    pub fn count(&self, pred: impl Fn(&T) -> bool) -> usize {
        self.lock().items.iter().filter(|item| pred(item)).count()
    }

    /// Removes and returns the oldest queued item satisfying `pred`.
    pub fn remove_first(&self, pred: impl Fn(&T) -> bool) -> Option<T> {
        let mut inner = self.lock();
        let at = inner.items.iter().position(pred)?;
        inner.items.remove(at)
    }
}

/// See [`Queue::pop`].
#[must_use = "futures do nothing unless polled"]
pub struct Pop<'a, T>(&'a Queue<T>);

impl<T> Future for Pop<'_, T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut inner = self.0.lock();
        if let Some(item) = inner.items.pop_front() {
            return Poll::Ready(Some(item));
        }
        if inner.closed {
            return Poll::Ready(None);
        }
        inner.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn items_come_out_in_order_and_a_closed_queue_drains_then_ends() {
        let queue: Queue<u32> = Queue::new();
        let mut cx = Context::from_waker(Waker::noop());
        assert!(Pin::new(&mut queue.pop()).poll(&mut cx).is_pending());
        queue.push(1);
        queue.push(2);
        queue.push(3);
        assert_eq!(queue.len(), 3);
        assert_eq!(queue.count(|&x| x > 1), 2);
        assert_eq!(queue.remove_first(|&x| x > 1), Some(2));
        assert_eq!(
            Pin::new(&mut queue.pop()).poll(&mut cx),
            Poll::Ready(Some(1))
        );
        queue.close();
        assert_eq!(
            Pin::new(&mut queue.pop()).poll(&mut cx),
            Poll::Ready(Some(3))
        );
        assert_eq!(Pin::new(&mut queue.pop()).poll(&mut cx), Poll::Ready(None));
        assert!(queue.is_empty());
    }
}
