use std::future::Future;
use std::time::{Duration, Instant as StdInstant, SystemTime};

use crate::{Clock, Instant, WallTime};

/// The system clock. Instants count from the moment the environment was created.
#[derive(Clone, Copy, Debug)]
pub struct RealClock {
    epoch: StdInstant,
}

impl RealClock {
    pub(super) fn new() -> Self {
        Self {
            epoch: StdInstant::now(),
        }
    }

    fn to_std(self, t: Instant) -> StdInstant {
        self.epoch + Duration::from_nanos(t.as_nanos())
    }
}

impl Clock for RealClock {
    fn now(&self) -> Instant {
        let since_epoch = StdInstant::now().duration_since(self.epoch);
        Instant::from_nanos(
            u64::try_from(since_epoch.as_nanos()).expect("process uptime exceeds u64 nanoseconds"),
        )
    }

    fn wall(&self) -> WallTime {
        WallTime::try_from(SystemTime::now())
            .expect("system clock is before 1970 or beyond the u64 nanosecond range")
    }

    fn sleep_until(&self, deadline: Instant) -> impl Future<Output = ()> + Send {
        tokio::time::sleep_until(tokio::time::Instant::from_std(self.to_std(deadline)))
    }
}
