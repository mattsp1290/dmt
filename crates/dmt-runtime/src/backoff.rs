use std::time::Duration;
pub(crate) fn replan_delay(base: Duration, retry: u32) -> Duration {
    let ceiling = base
        .saturating_mul(2_u32.checked_pow(retry).unwrap_or(u32::MAX))
        .min(Duration::from_millis(200));
    let nanos = u64::try_from(ceiling.as_nanos()).expect("capped at 200 ms");
    Duration::from_nanos(rand::random_range(nanos / 2..=nanos))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn delay_is_jittered_and_capped() {
        for retry in 0..=20 {
            let ceiling = Duration::from_millis(5)
                .saturating_mul(2_u32.pow(retry))
                .min(Duration::from_millis(200));
            for _ in 0..100 {
                let delay = replan_delay(Duration::from_millis(5), retry);
                assert!(delay >= ceiling / 2 && delay <= ceiling);
            }
        }
        assert_eq!(replan_delay(Duration::ZERO, u32::MAX), Duration::ZERO);
    }
}
