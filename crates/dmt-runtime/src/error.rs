use dmt_core::{GraphId, NodeId, PlanError, RunId, RunStatus};
use dmt_store::StoreError;

/// Invalid engine construction.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum BuildError {
    /// No persistence backend was supplied.
    #[error("store is required")]
    MissingStore,
    /// Multiple versions or definitions of one graph id were supplied.
    #[error("duplicate graph {graph_id}")]
    DuplicateGraph { graph_id: GraphId },
    /// A handler node id was registered twice.
    #[error("duplicate handler {node_id}")]
    DuplicateHandler { node_id: NodeId },
    /// An executable node has no handler.
    #[error("missing handler for {node_id} of graph {graph_id}")]
    MissingHandler { graph_id: GraphId, node_id: NodeId },
    /// A configuration field violates an engine invariant.
    #[error("invalid configuration: {0}")]
    InvalidConfig(&'static str),
}

/// Failure of an engine or host operation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EngineError {
    /// Persistence failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The planner rejected the operation.
    #[error(transparent)]
    Plan(#[from] PlanError),
    /// Starting runs requires a graph registered with this engine.
    #[error("graph {graph_id} is not registered with this engine")]
    GraphNotRegistered { graph_id: GraphId },
    /// The referenced definition is absent from the store.
    #[error("graph {graph_id}@{version} is unavailable")]
    GraphUnavailable { graph_id: GraphId, version: u32 },
    /// The requested run does not exist.
    #[error("run {run_id} not found")]
    RunNotFound { run_id: RunId },
    /// The run already ended.
    #[error("run {run_id} is terminal: {status}")]
    RunTerminal { run_id: RunId, status: RunStatus },
    /// No matching open signal exists.
    #[error("signal {name} not found for run {run_id}")]
    SignalNotFound { run_id: RunId, name: String },
    /// The bounded retry budget was exhausted.
    #[error("contention after {attempts} attempts: {last}")]
    Contention { attempts: u32, last: StoreError },
    /// A signal may have committed behind a backend error; inspect the run.
    #[error("operation may have committed: {last}")]
    Indeterminate { last: StoreError },
    /// Waiting for quiescence exceeded its deadline.
    #[error("wait timed out")]
    Timeout,
    /// Shutdown needed to stop unfinished dispatches hard.
    #[error("shutdown timed out; abandoned {abandoned} dispatches")]
    ShutdownTimedOut { abandoned: usize },
}
