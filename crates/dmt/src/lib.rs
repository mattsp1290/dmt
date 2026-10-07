//! Host-facing facade over `core`, `store`, and `runtime`.
//! Enable `sqlite` for durable local runs.
//! The `test-faults` feature is only for tests that deliberately crash processes.
pub use async_trait::async_trait;
pub use dmt_core as core;
pub use dmt_core::{
    BranchRef, BranchResult, Edge, EndStatus, ExhaustReason, Graph, GraphBuilder, GraphError,
    GraphId, Guard, JoinId, JoinInput, JoinPolicy, JoinRecord, Micros, NodeDef, NodeId, NodeKind,
    NodeOutcome, Outcome, RetryPolicy, RunEvent, RunId, RunSnapshot, RunStatus, SignalId,
    SignalKey, SignalPayload, SignalRecord, StepKey, TaskId, TaskRecord, TaskStatus, WorkerId,
};
pub use dmt_runtime as runtime;
pub use dmt_runtime::{
    BuildError, CancellationToken, Engine, EngineBuilder, EngineConfig, EngineError, EngineHandle,
    HandlerError, HandlerRegistry, NodeContext, NodeHandler, NodeInput, Quiescent,
};
pub use dmt_store as store;
pub use dmt_store::{
    Clock, EventRecord, ManualClock, MemoryStore, RunFilter, RunSummary, Store, StoreError,
    SystemClock,
};
#[cfg(feature = "sqlite")]
pub use dmt_store_sqlite as sqlite;
#[cfg(feature = "sqlite")]
pub use dmt_store_sqlite::{SqliteOptions, SqliteStore};
