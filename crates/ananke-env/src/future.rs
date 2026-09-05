//! Small future combinators every crate needs and that must not pull in a runtime.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::{Environment, Rng};

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
/// Which side is polled first is decided on every poll by one bit from the
/// environment's scheduling stream, [`Environment::sched_rng`], so neither side can
/// starve the other: once both are ready, each wins the next poll with probability one
/// half. A fixed bias would let a node under a steady stream of ready messages never
/// fire its timer, and in Phase 2 that is a Raft follower flooded with `AppendEntries`
/// that never times out, a bug the simulator must expose rather than mask (D-016).
/// Taking the environment rather than a bare [`Rng`] means a caller cannot draw the bit
/// from the protocol stream by mistake (D-017). Under `SimEnv` the bit is reproducible
/// from the seed; under `RealEnv` it is OS entropy.
///
/// Pin the futures with `std::pin::pin!`:
///
/// ```
/// use std::pin::pin;
/// use ananke_env::sim::{Sim, SimConfig};
/// use ananke_env::{Either, Environment, Instant, race};
///
/// let mut sim = Sim::new(SimConfig::new(1));
/// let node = sim.add_node();
/// let env = sim.env(node);
/// env.clone().spawn("race", async move {
///     let message = pin!(async { "message" });
///     let timer = pin!(std::future::pending::<()>());
///     assert_eq!(race(&env, message, timer).await, Either::Left("message"));
/// });
/// sim.run_until(Instant::ZERO);
/// ```
pub fn race<E, A, B>(env: &E, a: A, b: B) -> Race<'_, E::Rng, A, B>
where
    E: Environment + ?Sized,
    A: Future + Unpin,
    B: Future + Unpin,
{
    race_with(env.sched_rng(), a, b)
}

/// [`race`] over an explicit stream. Crate-private so callers outside cannot pick the
/// wrong stream; tests use it with a scripted generator.
pub(crate) fn race_with<'r, R, A, B>(rng: &'r R, a: A, b: B) -> Race<'r, R, A, B>
where
    R: Rng + ?Sized,
    A: Future + Unpin,
    B: Future + Unpin,
{
    Race { rng, a, b }
}

/// See [`race`].
#[must_use = "futures do nothing unless polled"]
pub struct Race<'r, R: ?Sized, A, B> {
    rng: &'r R,
    a: A,
    b: B,
}

impl<R, A, B> Future for Race<'_, R, A, B>
where
    R: Rng + ?Sized,
    A: Future + Unpin,
    B: Future + Unpin,
{
    type Output = Either<A::Output, B::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let left_first = this.rng.next_u64() & 1 == 0;
        if left_first {
            if let Poll::Ready(a) = Pin::new(&mut this.a).poll(cx) {
                return Poll::Ready(Either::Left(a));
            }
            if let Poll::Ready(b) = Pin::new(&mut this.b).poll(cx) {
                return Poll::Ready(Either::Right(b));
            }
        } else {
            if let Poll::Ready(b) = Pin::new(&mut this.b).poll(cx) {
                return Poll::Ready(Either::Right(b));
            }
            if let Poll::Ready(a) = Pin::new(&mut this.a).poll(cx) {
                return Poll::Ready(Either::Left(a));
            }
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
    use crate::rng::tests::Scripted;

    fn poll_once<F: Future>(f: Pin<&mut F>) -> Poll<F::Output> {
        f.poll(&mut Context::from_waker(Waker::noop()))
    }

    const LEFT_FIRST: u64 = 0;
    const RIGHT_FIRST: u64 = 1;

    #[test]
    fn the_stream_decides_who_wins_when_both_are_ready() {
        let rng = Scripted::new([LEFT_FIRST]);
        assert_eq!(
            poll_once(pin!(race_with(&rng, pin!(ready(1)), pin!(ready(2))))),
            Poll::Ready(Either::Left(1))
        );
        let rng = Scripted::new([RIGHT_FIRST]);
        assert_eq!(
            poll_once(pin!(race_with(&rng, pin!(ready(1)), pin!(ready(2))))),
            Poll::Ready(Either::Right(2))
        );
    }

    #[test]
    fn the_only_ready_side_wins_whatever_the_draw() {
        for draw in [LEFT_FIRST, RIGHT_FIRST] {
            let rng = Scripted::new([draw]);
            assert_eq!(
                poll_once(pin!(race_with(&rng, pin!(ready(1)), pin!(pending::<u8>())))),
                Poll::Ready(Either::Left(1))
            );
            let rng = Scripted::new([draw]);
            assert_eq!(
                poll_once(pin!(race_with(&rng, pin!(pending::<u8>()), pin!(ready(2))))),
                Poll::Ready(Either::Right(2))
            );
        }
    }

    #[test]
    fn pending_when_neither_is_ready() {
        let rng = Scripted::new([LEFT_FIRST]);
        assert_eq!(
            poll_once(pin!(race_with(
                &rng,
                pin!(pending::<u8>()),
                pin!(pending::<u8>())
            ))),
            Poll::Pending
        );
    }
}
