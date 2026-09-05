//! Small future combinators every crate needs and that must not pull in a runtime.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// The outcome of [`race`]: which future finished first, with its output.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Either<A, B> {
    /// The first future finished first.
    Left(A),
    /// The second future finished first.
    Right(B),
}

/// Resolves with whichever of `a` and `b` completes first; the other is dropped.
///
/// `a` is polled first on every turn, so a race between a receive and a timer prefers
/// the message when both are ready. Pin the futures with `std::pin::pin!`:
///
/// ```
/// use std::pin::pin;
/// use ananke_env::{Either, race};
///
/// # futures_lite_free_block_on(async {
/// let message = pin!(async { "message" });
/// let timer = pin!(std::future::pending::<()>());
/// assert_eq!(race(message, timer).await, Either::Left("message"));
/// # });
/// # fn futures_lite_free_block_on<F: std::future::Future<Output = ()>>(f: F) {
/// #     let mut f = std::pin::pin!(f);
/// #     let waker = std::task::Waker::noop();
/// #     let mut cx = std::task::Context::from_waker(waker);
/// #     while f.as_mut().poll(&mut cx).is_pending() {}
/// # }
/// ```
pub fn race<A: Future + Unpin, B: Future + Unpin>(a: A, b: B) -> Race<A, B> {
    Race { a, b }
}

/// See [`race`].
#[derive(Debug)]
#[must_use = "futures do nothing unless polled"]
pub struct Race<A, B> {
    a: A,
    b: B,
}

impl<A: Future + Unpin, B: Future + Unpin> Future for Race<A, B> {
    type Output = Either<A::Output, B::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Poll::Ready(a) = Pin::new(&mut this.a).poll(cx) {
            return Poll::Ready(Either::Left(a));
        }
        if let Poll::Ready(b) = Pin::new(&mut this.b).poll(cx) {
            return Poll::Ready(Either::Right(b));
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use std::future::{pending, ready};
    use std::pin::pin;
    use std::task::{Context, Waker};

    use super::*;

    fn poll_once<F: Future>(f: Pin<&mut F>) -> Poll<F::Output> {
        f.poll(&mut Context::from_waker(Waker::noop()))
    }

    #[test]
    fn left_wins_when_ready() {
        assert_eq!(
            poll_once(pin!(race(pin!(ready(1)), pin!(pending::<u8>())))),
            Poll::Ready(Either::Left(1))
        );
    }

    #[test]
    fn right_wins_when_left_is_pending() {
        assert_eq!(
            poll_once(pin!(race(pin!(pending::<u8>()), pin!(ready(2))))),
            Poll::Ready(Either::Right(2))
        );
    }

    #[test]
    fn left_is_preferred_when_both_are_ready() {
        assert_eq!(
            poll_once(pin!(race(pin!(ready(1)), pin!(ready(2))))),
            Poll::Ready(Either::Left(1))
        );
    }

    #[test]
    fn pending_when_neither_is_ready() {
        assert_eq!(
            poll_once(pin!(race(pin!(pending::<u8>()), pin!(pending::<u8>())))),
            Poll::Pending
        );
    }
}
