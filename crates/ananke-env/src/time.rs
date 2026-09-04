//! Monotonic and wall-clock time types (DECISIONS.md D-013).
//!
//! Both are plain nanosecond counters. They exist so that a simulated clock can produce
//! them from a virtual timeline and so that nothing outside `ananke-env` can reach the
//! real clock through `std::time::Instant::elapsed` and friends.

use std::fmt;
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::time::Duration;

/// A point on a monotonic clock, as nanoseconds since an arbitrary epoch.
///
/// The epoch is the moment the environment was created under [`RealEnv`](crate::RealEnv)
/// and the start of the run under the simulator, so instants are only comparable within
/// one node's clock. Arithmetic mirrors `std::time::Instant`: adding a `Duration` panics
/// on overflow, subtracting two instants saturates at zero.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Instant {
    nanos: u64,
}

impl Instant {
    /// The epoch itself.
    pub const ZERO: Instant = Instant { nanos: 0 };

    /// An instant `nanos` nanoseconds after the epoch.
    #[must_use]
    pub const fn from_nanos(nanos: u64) -> Self {
        Self { nanos }
    }

    /// Nanoseconds since the epoch.
    #[must_use]
    pub const fn as_nanos(self) -> u64 {
        self.nanos
    }

    /// Time elapsed from `earlier` to `self`, or zero if `earlier` is later.
    #[must_use]
    pub fn duration_since(self, earlier: Instant) -> Duration {
        Duration::from_nanos(self.nanos.saturating_sub(earlier.nanos))
    }

    /// Time elapsed from `earlier` to `self`, or `None` if `earlier` is later.
    #[must_use]
    pub fn checked_duration_since(self, earlier: Instant) -> Option<Duration> {
        self.nanos
            .checked_sub(earlier.nanos)
            .map(Duration::from_nanos)
    }

    /// `self + duration`, or `None` on overflow.
    #[must_use]
    pub fn checked_add(self, duration: Duration) -> Option<Instant> {
        self.nanos
            .checked_add(nanos_of(duration)?)
            .map(Instant::from_nanos)
    }

    /// `self - duration`, or `None` if the result would precede the epoch.
    #[must_use]
    pub fn checked_sub(self, duration: Duration) -> Option<Instant> {
        self.nanos
            .checked_sub(nanos_of(duration)?)
            .map(Instant::from_nanos)
    }
}

impl Add<Duration> for Instant {
    type Output = Instant;
    fn add(self, rhs: Duration) -> Instant {
        self.checked_add(rhs)
            .expect("overflow when adding Duration to Instant")
    }
}

impl AddAssign<Duration> for Instant {
    fn add_assign(&mut self, rhs: Duration) {
        *self = *self + rhs;
    }
}

impl Sub<Duration> for Instant {
    type Output = Instant;
    fn sub(self, rhs: Duration) -> Instant {
        self.checked_sub(rhs)
            .expect("underflow when subtracting Duration from Instant")
    }
}

impl SubAssign<Duration> for Instant {
    fn sub_assign(&mut self, rhs: Duration) {
        *self = *self - rhs;
    }
}

impl Sub<Instant> for Instant {
    type Output = Duration;
    fn sub(self, rhs: Instant) -> Duration {
        self.duration_since(rhs)
    }
}

impl fmt::Debug for Instant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Instant({:?})", Duration::from_nanos(self.nanos))
    }
}

/// A point on the wall clock, as nanoseconds since the Unix epoch.
///
/// Wall time is what humans and certificates care about. It may jump in either
/// direction, and nothing in ananke may assume it is synchronised across nodes
/// (SPEC.md §1.2). Arithmetic follows the same rules as [`Instant`].
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct WallTime {
    nanos: u64,
}

impl WallTime {
    /// 1970-01-01T00:00:00Z.
    pub const UNIX_EPOCH: WallTime = WallTime { nanos: 0 };

    /// A wall time `nanos` nanoseconds after the Unix epoch.
    #[must_use]
    pub const fn from_unix_nanos(nanos: u64) -> Self {
        Self { nanos }
    }

    /// Nanoseconds since the Unix epoch.
    #[must_use]
    pub const fn as_unix_nanos(self) -> u64 {
        self.nanos
    }

    /// Time elapsed from `earlier` to `self`, or zero if `earlier` is later.
    #[must_use]
    pub fn duration_since(self, earlier: WallTime) -> Duration {
        Duration::from_nanos(self.nanos.saturating_sub(earlier.nanos))
    }

    /// `self + duration`, or `None` on overflow.
    #[must_use]
    pub fn checked_add(self, duration: Duration) -> Option<WallTime> {
        self.nanos
            .checked_add(nanos_of(duration)?)
            .map(WallTime::from_unix_nanos)
    }

    /// `self - duration`, or `None` if the result would precede the Unix epoch.
    #[must_use]
    pub fn checked_sub(self, duration: Duration) -> Option<WallTime> {
        self.nanos
            .checked_sub(nanos_of(duration)?)
            .map(WallTime::from_unix_nanos)
    }
}

impl Add<Duration> for WallTime {
    type Output = WallTime;
    fn add(self, rhs: Duration) -> WallTime {
        self.checked_add(rhs)
            .expect("overflow when adding Duration to WallTime")
    }
}

impl AddAssign<Duration> for WallTime {
    fn add_assign(&mut self, rhs: Duration) {
        *self = *self + rhs;
    }
}

impl Sub<Duration> for WallTime {
    type Output = WallTime;
    fn sub(self, rhs: Duration) -> WallTime {
        self.checked_sub(rhs)
            .expect("underflow when subtracting Duration from WallTime")
    }
}

impl SubAssign<Duration> for WallTime {
    fn sub_assign(&mut self, rhs: Duration) {
        *self = *self - rhs;
    }
}

impl Sub<WallTime> for WallTime {
    type Output = Duration;
    fn sub(self, rhs: WallTime) -> Duration {
        self.duration_since(rhs)
    }
}

impl fmt::Debug for WallTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "WallTime({:?} after Unix epoch)",
            Duration::from_nanos(self.nanos)
        )
    }
}

/// A `std::time::SystemTime` that [`WallTime`] cannot represent: before 1970, or more
/// than 2^64 nanoseconds (about 584 years) after it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WallTimeOutOfRange;

impl fmt::Display for WallTimeOutOfRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("wall time is before 1970 or beyond the u64 nanosecond range")
    }
}

impl std::error::Error for WallTimeOutOfRange {}

fn nanos_of(duration: Duration) -> Option<u64> {
    u64::try_from(duration.as_nanos()).ok()
}

/// Conversions to and from `std::time::SystemTime`, for the process edges only:
/// certificate validity, log timestamps, and the real clock implementation.
///
/// This module is one of the three sanctioned mentions of a banned `std::time` type
/// outside `real` (see clippy.toml and `scripts/check-direct-io.sh`).
#[allow(clippy::disallowed_types)]
mod std_conversions {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::{WallTime, WallTimeOutOfRange};

    impl From<WallTime> for SystemTime {
        fn from(t: WallTime) -> SystemTime {
            UNIX_EPOCH + Duration::from_nanos(t.as_unix_nanos())
        }
    }

    impl TryFrom<SystemTime> for WallTime {
        type Error = WallTimeOutOfRange;

        fn try_from(t: SystemTime) -> Result<WallTime, WallTimeOutOfRange> {
            let since_epoch = t
                .duration_since(UNIX_EPOCH)
                .map_err(|_| WallTimeOutOfRange)?;
            u64::try_from(since_epoch.as_nanos())
                .map(WallTime::from_unix_nanos)
                .map_err(|_| WallTimeOutOfRange)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn round_trips_through_system_time() {
            let w = WallTime::from_unix_nanos(1_700_000_000_123_456_789);
            let s: SystemTime = w.into();
            assert_eq!(WallTime::try_from(s), Ok(w));
        }

        #[test]
        fn rejects_times_before_the_epoch() {
            let before = UNIX_EPOCH - Duration::from_secs(1);
            assert_eq!(WallTime::try_from(before), Err(WallTimeOutOfRange));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instant_arithmetic_mirrors_std() {
        let a = Instant::from_nanos(100);
        let b = a + Duration::from_nanos(50);
        assert_eq!(b.as_nanos(), 150);
        assert_eq!(b - a, Duration::from_nanos(50));
        assert_eq!(a - b, Duration::ZERO, "subtraction saturates");
        assert_eq!(a.checked_duration_since(b), None);
        assert_eq!(b.checked_duration_since(a), Some(Duration::from_nanos(50)));
        assert_eq!(Instant::ZERO.checked_sub(Duration::from_nanos(1)), None);
        assert_eq!(
            Instant::from_nanos(u64::MAX).checked_add(Duration::from_nanos(1)),
            None
        );
        let mut c = a;
        c += Duration::from_nanos(1);
        c -= Duration::from_nanos(2);
        assert_eq!(c, Instant::from_nanos(99));
    }

    #[test]
    fn debug_formats_are_stable() {
        assert_eq!(
            format!("{:?}", Instant::from_nanos(1_500_000_000)),
            "Instant(1.5s)"
        );
        assert_eq!(
            format!("{:?}", WallTime::from_unix_nanos(250)),
            "WallTime(250ns after Unix epoch)"
        );
    }

    #[test]
    fn wall_time_arithmetic() {
        let t = WallTime::from_unix_nanos(1_000);
        assert_eq!((t + Duration::from_nanos(5)).as_unix_nanos(), 1_005);
        assert_eq!((t - Duration::from_nanos(5)).as_unix_nanos(), 995);
        assert_eq!(WallTime::UNIX_EPOCH - t, Duration::ZERO);
        assert_eq!(t - WallTime::UNIX_EPOCH, Duration::from_nanos(1_000));
        assert_eq!(
            WallTime::UNIX_EPOCH.checked_sub(Duration::from_nanos(1)),
            None
        );
    }

    #[test]
    fn durations_beyond_u64_nanos_do_not_fit() {
        assert_eq!(Instant::ZERO.checked_add(Duration::MAX), None);
    }
}
