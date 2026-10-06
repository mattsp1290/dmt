//! Backend-neutral persistence contract and host clocks for dmt.
pub mod clock;
pub mod error;
pub mod store;
pub mod types;
pub use clock::{Clock, ManualClock, SystemClock};
pub use error::StoreError;
pub use store::Store;
pub use types::*;
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    fn assert_send_sync<T: Send + Sync>() {}
    #[test]
    fn object_safety() {
        let _: Option<Arc<dyn Store>> = None;
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let _ = clock.now();
        assert_send_sync::<Box<dyn Store>>();
    }
    #[test]
    fn error_display() {
        let errors = [
            StoreError::VersionConflict {
                expected: 1,
                actual: 2,
            },
            StoreError::JoinDrift {
                join_id: "join".into(),
            },
            StoreError::LeaseLost {
                task_id: "task".into(),
            },
            StoreError::RunTerminal {
                run_id: "run".into(),
            },
            StoreError::NotFound("task".into()),
            StoreError::AlreadyResolved {
                signal_id: "signal".into(),
            },
            StoreError::GraphMismatch {
                graph_id: "graph".into(),
                version: 1,
            },
            StoreError::InvalidCommit("shape".into()),
            StoreError::Busy,
            StoreError::Backend("io".into()),
        ];
        let expected = [
            "run version conflict: expected 1, actual 2",
            "join join drifted from the planned counters or is already satisfied",
            "lease lost for task task",
            "run run is terminal",
            "not found: task",
            "signal signal already resolved",
            "graph graph@1 is registered with a different definition",
            "invalid commit: shape",
            "store busy",
            "backend error: io",
        ];
        for (error, text) in errors.iter().zip(expected) {
            assert_eq!(error.to_string(), text);
        }
    }
}
