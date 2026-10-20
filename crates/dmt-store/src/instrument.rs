//! Store instrumentation with bounded backend, method, and result labels.
use crate::{
    ApplyResult, ClaimRequest, ClaimedTask, EventRecord, HeartbeatResult, LeaseProof, RunFilter,
    RunSummary, Store, StoreError,
};
use async_trait::async_trait;
use dmt_core::{
    Commit, Graph, GraphId, Micros, RunId, RunSnapshot, SignalRecord, TaskId, TaskRecord,
};
use std::{future::Future, sync::Arc, time::Instant};
use tracing::Instrument;

/// Store metric names.
pub mod names {
    /// Store calls by backend, method, and result.
    pub const CALLS: &str = "dmt.store.calls_total";
    /// Store call latency in seconds.
    pub const DURATION: &str = "dmt.store.call.duration_seconds";
}
/// Register descriptions with the current recorder. Safe to repeat.
pub fn describe() {
    metrics::describe_counter!(names::CALLS, "Store calls by backend, method, and result");
    metrics::describe_histogram!(
        names::DURATION,
        metrics::Unit::Seconds,
        "Store call latency"
    );
}
/// Decorates any store without changing its results or cancellation semantics.
/// Engines wrap stores by default; disable `instrument_store` when wrapping manually.
pub struct InstrumentedStore {
    inner: Arc<dyn Store>,
    backend: &'static str,
}
impl std::fmt::Debug for InstrumentedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstrumentedStore")
            .field("backend", &self.backend)
            .finish_non_exhaustive()
    }
}
impl InstrumentedStore {
    /// Wrap a store, preserving its bounded backend name.
    #[must_use]
    pub fn new(inner: Arc<dyn Store>) -> Self {
        describe();
        Self {
            backend: inner.backend_name(),
            inner,
        }
    }
    async fn call<T>(
        &self,
        method: &'static str,
        future: impl Future<Output = Result<T, StoreError>>,
    ) -> Result<T, StoreError> {
        let span = tracing::debug_span!(
            "store",
            backend = self.backend,
            method,
            "operation.name" = "dmt.store",
            "resource.name" = method,
            "span.type" = "db",
            "db.system" = self.backend,
            result = tracing::field::Empty
        );
        let started = Instant::now();
        let result = future.instrument(span.clone()).await;
        let label = result_label(&result);
        span.record("result", label);
        metrics::counter!(names::CALLS, "backend" => self.backend, "method" => method, "result" => label).increment(1);
        metrics::histogram!(names::DURATION, "backend" => self.backend, "method" => method, "result" => label).record(started.elapsed().as_secs_f64());
        result
    }
}
/// Stable result vocabulary; error messages never become labels.
#[must_use]
pub fn result_label<T>(result: &Result<T, StoreError>) -> &'static str {
    match result {
        Ok(_) => "ok",
        Err(StoreError::VersionConflict { .. }) => "version_conflict",
        Err(StoreError::JoinDrift { .. }) => "join_drift",
        Err(StoreError::LeaseLost { .. }) => "lease_lost",
        Err(StoreError::RunTerminal { .. }) => "run_terminal",
        Err(StoreError::NotFound(_)) => "not_found",
        Err(StoreError::AlreadyResolved { .. }) => "already_resolved",
        Err(StoreError::GraphMismatch { .. }) => "graph_mismatch",
        Err(StoreError::InvalidCommit(_)) => "invalid_commit",
        Err(StoreError::Busy) => "busy",
        Err(StoreError::Backend(_)) => "backend",
    }
}
#[async_trait]
impl Store for InstrumentedStore {
    fn backend_name(&self) -> &'static str {
        self.backend
    }
    async fn register_graph(&self, graph: &Graph) -> Result<(), StoreError> {
        self.call("register_graph", self.inner.register_graph(graph))
            .await
    }
    async fn load_graph(&self, id: &GraphId, version: u32) -> Result<Option<Graph>, StoreError> {
        self.call("load_graph", self.inner.load_graph(id, version))
            .await
    }
    async fn create_run(&self, commit: Commit) -> Result<RunId, StoreError> {
        self.call("create_run", self.inner.create_run(commit)).await
    }
    async fn load_run(&self, run_id: &RunId) -> Result<Option<RunSnapshot>, StoreError> {
        self.call("load_run", self.inner.load_run(run_id)).await
    }
    async fn load_task(&self, task_id: &TaskId) -> Result<Option<TaskRecord>, StoreError> {
        self.call("load_task", self.inner.load_task(task_id)).await
    }
    async fn claim_ready(&self, req: ClaimRequest) -> Result<Vec<ClaimedTask>, StoreError> {
        self.call("claim_ready", self.inner.claim_ready(req)).await
    }
    async fn heartbeat(
        &self,
        proof: &LeaseProof,
        now: Micros,
        lease_micros: i64,
    ) -> Result<HeartbeatResult, StoreError> {
        self.call("heartbeat", self.inner.heartbeat(proof, now, lease_micros))
            .await
    }
    async fn apply(
        &self,
        commit: Commit,
        by: Option<LeaseProof>,
    ) -> Result<ApplyResult, StoreError> {
        self.call("apply", self.inner.apply(commit, by)).await
    }
    async fn find_open_signal(
        &self,
        run_id: &RunId,
        name: &str,
    ) -> Result<Option<SignalRecord>, StoreError> {
        self.call(
            "find_open_signal",
            self.inner.find_open_signal(run_id, name),
        )
        .await
    }
    async fn due_signals(
        &self,
        now: Micros,
        limit: usize,
    ) -> Result<Vec<SignalRecord>, StoreError> {
        self.call("due_signals", self.inner.due_signals(now, limit))
            .await
    }
    async fn exhausted_tasks(&self, limit: usize) -> Result<Vec<TaskRecord>, StoreError> {
        self.call("exhausted_tasks", self.inner.exhausted_tasks(limit))
            .await
    }
    async fn events(
        &self,
        run_id: &RunId,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<EventRecord>, StoreError> {
        self.call("events", self.inner.events(run_id, after_seq, limit))
            .await
    }
    async fn list_runs(&self, filter: RunFilter) -> Result<Vec<RunSummary>, StoreError> {
        self.call("list_runs", self.inner.list_runs(filter)).await
    }
}

#[cfg(test)]
mod tests;
