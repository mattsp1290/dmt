//! Host clocks. Stores use caller-supplied timestamps and never read a clock.
use dmt_core::Micros;
use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};
use std::time::{SystemTime, UNIX_EPOCH};

/// A host's source of microsecond timestamps.
pub trait Clock: Send + Sync {
    fn now(&self) -> Micros;
}
/// Wall-clock time, saturated to the timestamp range; pre-epoch time is zero.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;
impl Clock for SystemClock {
    fn now(&self) -> Micros {
        let micros = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_micros());
        Micros(i64::try_from(micros).unwrap_or(i64::MAX))
    }
}
/// Controllable clock whose clones share one atomic timestamp.
#[derive(Debug, Clone)]
pub struct ManualClock(Arc<AtomicI64>);
impl ManualClock {
    #[must_use]
    pub fn new(now: Micros) -> Self {
        Self(Arc::new(AtomicI64::new(now.0)))
    }
    pub fn set(&self, now: Micros) {
        self.0.store(now.0, Ordering::SeqCst);
    }
    #[must_use]
    pub fn advance(&self, delta: i64) -> Micros {
        let previous = self
            .0
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |v| {
                Some(v.saturating_add(delta))
            })
            .unwrap_or_else(|v| v);
        Micros(previous.saturating_add(delta))
    }
}
impl Clock for ManualClock {
    fn now(&self) -> Micros {
        Micros(self.0.load(Ordering::SeqCst))
    }
}
impl<C: Clock + ?Sized> Clock for Arc<C> {
    fn now(&self) -> Micros {
        (**self).now()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn shared_manual_clock() {
        let clock = ManualClock::new(Micros(10));
        let other = clock.clone();
        clock.set(Micros(20));
        assert_eq!(other.now(), Micros(20));
        assert_eq!(other.advance(5), Micros(25));
        assert_eq!(clock.now(), Micros(25));
    }
    #[test]
    fn saturates() {
        let clock = ManualClock::new(Micros(i64::MAX));
        assert_eq!(clock.advance(1), Micros(i64::MAX));
        clock.set(Micros(i64::MIN));
        assert_eq!(clock.advance(-1), Micros(i64::MIN));
    }
    #[test]
    fn system_time() {
        let first = SystemClock.now();
        assert!(first > Micros::from_secs(1_700_000_000));
        assert!(SystemClock.now() >= first);
    }
}
