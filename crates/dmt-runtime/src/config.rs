use crate::BuildError;
use dmt_core::WorkerId;
use std::time::Duration;

/// Engine concurrency, timing, and retry settings.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Claim loops; zero disables all background work.
    pub workers: usize,
    /// Maximum concurrent dispatches per worker.
    pub claim_limit: usize,
    /// Idle claim and quiescence polling interval.
    pub poll_interval: Duration,
    /// Persistence lease duration.
    pub lease: Duration,
    /// Dispatch heartbeat cadence.
    pub heartbeat_every: Duration,
    /// Fallback handler timeout when the node has none.
    pub handler_timeout: Option<Duration>,
    /// Commit retries after the first attempt.
    pub max_replan_attempts: u32,
    /// Initial jittered retry delay, doubling up to 200 ms.
    pub replan_backoff: Duration,
    /// Deadline and exhaustion sweep cadence.
    pub sweep_interval: Duration,
    /// Cooperative cancellation wait after the shutdown drain.
    pub cancel_grace: Duration,
    /// Identity shared by every worker of this engine.
    pub worker_id: WorkerId,
    /// Test-only crash injection; never enable in production.
    #[cfg(feature = "test-faults")]
    #[doc(hidden)]
    pub fault: Option<crate::FaultPoint>,
}
impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            workers: 4,
            claim_limit: 8,
            poll_interval: Duration::from_millis(250),
            lease: Duration::from_secs(30),
            heartbeat_every: Duration::from_secs(10),
            handler_timeout: None,
            max_replan_attempts: 16,
            replan_backoff: Duration::from_millis(5),
            sweep_interval: Duration::from_secs(1),
            cancel_grace: Duration::from_secs(5),
            worker_id: WorkerId::new(),
            #[cfg(feature = "test-faults")]
            fault: None,
        }
    }
}
impl EngineConfig {
    pub(crate) fn lease_micros(&self) -> i64 {
        i64::try_from(self.lease.as_micros()).unwrap_or(i64::MAX)
    }
    pub(crate) fn validate(&self) -> Result<(), BuildError> {
        for (duration, name) in [
            (self.poll_interval, "poll_interval"),
            (self.lease, "lease"),
            (self.heartbeat_every, "heartbeat_every"),
            (self.sweep_interval, "sweep_interval"),
        ] {
            if duration.is_zero() {
                return Err(BuildError::InvalidConfig(name));
            }
        }
        if self.workers > 0 && self.claim_limit == 0 {
            return Err(BuildError::InvalidConfig("claim_limit"));
        }
        if self.max_replan_attempts == 0 {
            return Err(BuildError::InvalidConfig("max_replan_attempts"));
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn defaults_match_the_plan() {
        let c = EngineConfig::default();
        assert_eq!(
            (c.workers, c.claim_limit, c.max_replan_attempts),
            (4, 8, 16)
        );
        assert_eq!(c.poll_interval, Duration::from_millis(250));
        assert_eq!(c.lease_micros(), 30_000_000);
        assert_eq!(c.heartbeat_every, Duration::from_secs(10));
        assert_eq!(c.handler_timeout, None);
        assert_eq!(c.replan_backoff, Duration::from_millis(5));
        assert_eq!(c.sweep_interval, Duration::from_secs(1));
        assert_eq!(c.cancel_grace, Duration::from_secs(5));
        assert_ne!(c.worker_id, EngineConfig::default().worker_id);
    }
    #[test]
    fn validate_rejects_zero_values() {
        for field in 0..6 {
            let mut c = EngineConfig::default();
            match field {
                0 => c.poll_interval = Duration::ZERO,
                1 => c.lease = Duration::ZERO,
                2 => c.heartbeat_every = Duration::ZERO,
                3 => c.sweep_interval = Duration::ZERO,
                4 => c.claim_limit = 0,
                _ => c.max_replan_attempts = 0,
            }
            assert!(matches!(c.validate(), Err(BuildError::InvalidConfig(_))));
        }
        assert!(
            EngineConfig {
                workers: 0,
                claim_limit: 0,
                ..EngineConfig::default()
            }
            .validate()
            .is_ok()
        );
    }
}
