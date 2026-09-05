//! The virtual clock (SPEC.md §1.2).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use super::state::Shared;
use crate::{Clock, Instant, NodeId, WallTime};

/// One node's view of virtual time: global time transformed by the node's skew and
/// drift. Timers resolve when the scheduler advances time past them.
#[derive(Clone)]
pub struct SimClock {
    pub(super) shared: Arc<Shared>,
    pub(super) node: NodeId,
}

impl std::fmt::Debug for SimClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimClock")
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}

impl Clock for SimClock {
    fn now(&self) -> Instant {
        let st = self.shared.lock();
        st.node_time(self.node, st.now)
    }

    fn wall(&self) -> WallTime {
        let st = self.shared.lock();
        let local = st.node_time(self.node, st.now);
        st.config
            .wall_epoch
            .checked_add(Duration::from_nanos(local.as_nanos()))
            .expect("simulated wall time overflowed")
    }

    fn sleep_until(&self, deadline: Instant) -> impl Future<Output = ()> + Send {
        let at = self.shared.lock().global_time(self.node, deadline);
        SimSleep {
            shared: self.shared.clone(),
            at,
            registered: false,
        }
    }
}

/// A timer on the virtual clock.
pub struct SimSleep {
    shared: Arc<Shared>,
    at: Instant,
    registered: bool,
}

impl Future for SimSleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        let mut st = this.shared.lock();
        if st.now >= this.at {
            return Poll::Ready(());
        }
        if !this.registered {
            st.register_timer(this.at, cx.waker().clone());
            this.registered = true;
        }
        Poll::Pending
    }
}
