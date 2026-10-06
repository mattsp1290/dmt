use dmt_core::{GraphId, JoinId, RunId, SignalId, TaskId};
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StoreError {
    #[error("run version conflict: expected {expected}, actual {actual}")]
    VersionConflict { expected: u64, actual: u64 },
    #[error("join {join_id} drifted from the planned counters or is already satisfied")]
    JoinDrift { join_id: JoinId },
    #[error("lease lost for task {task_id}")]
    LeaseLost { task_id: TaskId },
    #[error("run {run_id} is terminal")]
    RunTerminal { run_id: RunId },
    #[error("not found: {0}")]
    NotFound(String),
    #[error("signal {signal_id} already resolved")]
    AlreadyResolved { signal_id: SignalId },
    #[error("graph {graph_id}@{version} is registered with a different definition")]
    GraphMismatch { graph_id: GraphId, version: u32 },
    #[error("invalid commit: {0}")]
    InvalidCommit(String),
    #[error("store busy")]
    Busy,
    #[error("backend error: {0}")]
    Backend(String),
}
