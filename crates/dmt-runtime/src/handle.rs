use crate::{EngineError, commit_loop, shared::Shared, shutdown};
use dmt_core::{GraphId, RunId, RunSnapshot, RunStatus, WorkerId, plan_start};
use dmt_store::{EventRecord, StoreError};
use serde_json::Value;
use std::{sync::Arc, time::Duration};
use tokio::time::sleep;

/// Engine control and store queries. Dropping the last clone stops every background task.
/// Handlers must not own a handle clone or call `abort`/`shutdown`: those wait for handlers.
#[derive(Clone)]
pub struct EngineHandle {
    pub(crate) inner: Arc<HandleInner>,
}
pub(crate) struct HandleInner {
    pub shared: Arc<Shared>,
}
impl Drop for HandleInner {
    fn drop(&mut self) {
        self.shared.kill.cancel();
    }
}
impl std::fmt::Debug for EngineHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineHandle")
            .field("worker_id", self.worker_id())
            .finish()
    }
}
/// A durable pause or the end of a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quiescent {
    /// Waiting for an external signal or deadline.
    Parked,
    /// Completed, failed, or cancelled.
    Terminal(RunStatus),
}
impl EngineHandle {
    /// Create a new run using this engine's registered graph version.
    /// # Errors
    /// Returns an unknown graph, planner, store, or retry-budget error.
    pub async fn start_run(&self, graph_id: &GraphId, input: Value) -> Result<RunId, EngineError> {
        let shared = &self.inner.shared;
        let graph =
            shared
                .catalog
                .registered(graph_id)
                .ok_or_else(|| EngineError::GraphNotRegistered {
                    graph_id: graph_id.clone(),
                })?;
        let run_id = RunId::new();
        let mut last = StoreError::Busy;
        for retry in 0..=shared.config.max_replan_attempts {
            commit_loop::delay(shared, retry).await;
            let commit = plan_start(graph, run_id.clone(), input.clone(), shared.clock.now())?;
            match shared.store.create_run(commit).await {
                Ok(_) => {
                    shared.wake.notify_waiters();
                    return Ok(run_id);
                }
                Err(StoreError::VersionConflict { .. }) if retry > 0 => {
                    shared.wake.notify_waiters();
                    return Ok(run_id);
                }
                Err(e) if commit_loop::transient(&e) => last = e,
                Err(e) => return Err(e.into()),
            }
        }
        Err(EngineError::Contention {
            attempts: shared.config.max_replan_attempts.saturating_add(1),
            last,
        })
    }
    /// Read a run snapshot.
    /// # Errors
    /// Returns a store error.
    pub async fn run(&self, run_id: &RunId) -> Result<Option<RunSnapshot>, EngineError> {
        Ok(self.inner.shared.store.load_run(run_id).await?)
    }
    /// Read persisted events strictly after `after_seq`.
    /// # Errors
    /// Returns a store error.
    pub async fn events(
        &self,
        run_id: &RunId,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<EventRecord>, EngineError> {
        Ok(self
            .inner
            .shared
            .store
            .events(run_id, after_seq, limit)
            .await?)
    }
    /// Wait in Tokio time until the run parks or ends.
    /// # Errors
    /// Returns an absent run, store error, or timeout.
    pub async fn wait_quiescent(
        &self,
        run_id: &RunId,
        timeout: Duration,
    ) -> Result<Quiescent, EngineError> {
        tokio::time::timeout(timeout, self.quiescent(run_id))
            .await
            .map_err(|_| EngineError::Timeout)?
    }
    async fn quiescent(&self, run_id: &RunId) -> Result<Quiescent, EngineError> {
        let shared = &self.inner.shared;
        loop {
            let wake = shared.wake.notified();
            tokio::pin!(wake);
            wake.as_mut().enable();
            match self.run(run_id).await {
                Ok(Some(run)) if run.status.is_terminal() => {
                    return Ok(Quiescent::Terminal(run.status));
                }
                Ok(Some(run)) if run.status == RunStatus::Parked => return Ok(Quiescent::Parked),
                Ok(Some(_)) => {}
                Ok(None) => {
                    return Err(EngineError::RunNotFound {
                        run_id: run_id.clone(),
                    });
                }
                Err(EngineError::Store(e)) if commit_loop::transient(&e) => {}
                Err(e) => return Err(e),
            }
            tokio::select! { () = wake => {}, () = sleep(shared.config.poll_interval) => {} }
        }
    }
    /// Stop all engine tasks and wait for their exit at await points. Idempotent.
    /// A backend write already handed off may still commit; close the store before reusing its file.
    pub async fn abort(&self) {
        shutdown::abort(&self.inner.shared).await;
    }
    /// This engine's persistence worker identity.
    #[must_use]
    pub fn worker_id(&self) -> &WorkerId {
        &self.inner.shared.config.worker_id
    }
}
