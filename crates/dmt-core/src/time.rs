use serde::{Deserialize, Serialize};
/// Unix microseconds UTC. Hosts supply timestamps; the core never reads a clock.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct Micros(pub i64);
impl Micros {
    #[must_use]
    pub const fn from_secs(seconds: i64) -> Self {
        Self(seconds.saturating_mul(1_000_000))
    }
    #[must_use]
    pub const fn saturating_add(self, delta: i64) -> Self {
        Self(self.0.saturating_add(delta))
    }
    #[must_use]
    pub const fn as_i64(self) -> i64 {
        self.0
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn seconds_and_saturation() {
        assert_eq!(Micros::from_secs(2).as_i64(), 2_000_000);
        assert_eq!(Micros(i64::MAX).saturating_add(1), Micros(i64::MAX));
        assert_eq!(Micros::from_secs(i64::MAX), Micros(i64::MAX));
    }
}
