//! A turnstile: one maintenance operation at a time. A flush and a compaction each
//! read the manifest in force, write files, write the next manifest and install it;
//! two of them interleaved would each build their manifest from the same base and
//! one would lose the other's tables. So each holds the turnstile from its first
//! look at the tables to its last file deletion (D-023). It is an async lock with no
//! runtime behind it: a guard released on drop wakes the next waiter.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

/// The lock.
#[derive(Debug, Default)]
pub struct Turnstile {
    state: Mutex<State>,
}

#[derive(Debug, Default)]
struct State {
    held: bool,
    waiters: VecDeque<Waker>,
}

fn lock(mutex: &Mutex<State>) -> MutexGuard<'_, State> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Turnstile {
    /// Resolves with the guard once the turnstile is free.
    pub fn acquire(&self) -> Acquire<'_> {
        Acquire(self)
    }
}

/// The future [`Turnstile::acquire`] returns.
#[must_use = "the guard is what holds the turnstile"]
pub struct Acquire<'a>(&'a Turnstile);

impl<'a> Future for Acquire<'a> {
    type Output = Guard<'a>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Guard<'a>> {
        let mut state = lock(&self.0.state);
        if state.held {
            state.waiters.push_back(cx.waker().clone());
            return Poll::Pending;
        }
        state.held = true;
        Poll::Ready(Guard(self.0))
    }
}

/// Holds the turnstile; dropping it lets the next waiter in.
pub struct Guard<'a>(&'a Turnstile);

impl Drop for Guard<'_> {
    fn drop(&mut self) {
        let next = {
            let mut state = lock(&self.0.state);
            state.held = false;
            state.waiters.pop_front()
        };
        if let Some(waker) = next {
            waker.wake();
        }
    }
}
