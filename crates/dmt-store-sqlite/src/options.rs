use std::time::Duration;

/// Connection settings for a file-backed store.
#[derive(Debug, Clone)]
pub struct SqliteOptions {
    pub(crate) busy_timeout: Duration,
    pub(crate) synchronous_full: bool,
}

impl Default for SqliteOptions {
    fn default() -> Self {
        Self {
            busy_timeout: Duration::from_secs(5),
            synchronous_full: false,
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
