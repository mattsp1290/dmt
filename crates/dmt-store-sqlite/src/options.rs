use std::time::Duration;

/// Connection settings for a file-backed store.
#[derive(Debug, Clone)]
pub struct SqliteOptions {
    pub(crate) busy_timeout: Duration,
    pub(crate) synchronous_full: bool,
    #[cfg(feature = "test-faults")]
    pub(crate) fault: Option<crate::FaultPoint>,
}

impl Default for SqliteOptions {
    fn default() -> Self {
        Self {
            busy_timeout: Duration::from_secs(5),
            synchronous_full: false,
            #[cfg(feature = "test-faults")]
            fault: None,
        }
    }
}

impl SqliteOptions {
    /// Set the SQLite lock timeout (default: five seconds).
    #[must_use]
    pub fn busy_timeout(mut self, timeout: Duration) -> Self {
        self.busy_timeout = timeout;
        self
    }

    /// Select FULL synchronization for durability across power loss.
    #[must_use]
    pub fn synchronous_full(mut self, enabled: bool) -> Self {
        self.synchronous_full = enabled;
        self
    }
}

#[cfg(feature = "test-faults")]
impl SqliteOptions {
    /// Arm a one-shot fault shared by store clones.
    #[doc(hidden)]
    #[must_use]
    pub fn fault(mut self, fault: crate::FaultPoint) -> Self {
        self.fault = Some(fault);
        self
    }
}
