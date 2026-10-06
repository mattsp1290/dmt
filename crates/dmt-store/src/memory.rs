//! Shared, non-durable reference backend. Every operation holds one mutex.
mod apply;
mod claims;
use crate::{
    ApplyResult, ClaimRequest, ClaimedTask, EventRecord, HeartbeatResult, LeaseProof, RunFilter,
    RunSummary, Store, StoreError,
};
use async_trait::async_trait;
use dmt_core::{
    Commit, Graph, GraphId, JoinId, JoinRecord, Micros, RunEvent, RunId, RunSnapshot, SignalId,
    SignalRecord, StepKey, TaskId, TaskRecord, TaskStatus,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// An in-memory store. Clones share state; a new instance starts empty.
#[derive(Debug, Clone, Default)]
pub struct MemoryStore {
    inner: Arc<Mutex<Inner>>,
}
#[derive(Debug, Default)]
struct Inner {
    graphs: BTreeMap<(GraphId, u32), Graph>,
    runs: BTreeMap<RunId, RunRow>,
    tasks: BTreeMap<TaskId, TaskRecord>,
    by_step: BTreeMap<StepKey, TaskId>,
    joins: BTreeMap<JoinId, JoinRecord>,
    signals: BTreeMap<SignalId, SignalRecord>,
}
#[derive(Debug)]
struct RunRow {
    snapshot: RunSnapshot,
    events: Vec<EventRecord>,
    created_at: Micros,
    updated_at: Micros,
}
impl MemoryStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
fn append_event(row: &mut RunRow, event: RunEvent, now: Micros) {
    row.events.push(EventRecord {
        seq: row.events.len() as u64 + 1,
        recorded_at: now,
        event,
    });
}
fn lease_matches(task: &TaskRecord, proof: &LeaseProof) -> bool {
    task.status == TaskStatus::Running
        && task.lease_owner.as_ref() == Some(&proof.worker_id)
        && task.attempt == proof.attempt
}
fn clear_lease(task: &mut TaskRecord) {
    task.lease_owner = None;
    task.lease_until = None;
}

#[async_trait]
impl Store for MemoryStore {
    async fn register_graph(&self, graph: &Graph) -> Result<(), StoreError> {
        let mut inner = self.lock();
        let key = (graph.id().clone(), graph.version());
        if let Some(existing) = inner.graphs.get(&key) {
            if existing.definition_hash() != graph.definition_hash() {
                return Err(StoreError::GraphMismatch {
                    graph_id: key.0,
                    version: key.1,
                });
            }
        } else {
            inner.graphs.insert(key, graph.clone());
        }
        Ok(())
    }
    async fn load_graph(&self, id: &GraphId, version: u32) -> Result<Option<Graph>, StoreError> {
        Ok(self.lock().graphs.get(&(id.clone(), version)).cloned())
    }
    async fn create_run(&self, commit: Commit) -> Result<RunId, StoreError> {
        apply::create(&mut self.lock(), commit)
    }
    async fn apply(
        &self,
        commit: Commit,
        by: Option<LeaseProof>,
    ) -> Result<ApplyResult, StoreError> {
        apply::apply(&mut self.lock(), commit, by.as_ref())
    }
    async fn load_run(&self, run_id: &RunId) -> Result<Option<RunSnapshot>, StoreError> {
        let inner = self.lock();
        Ok(inner.runs.get(run_id).map(|row| {
            let mut snapshot = row.snapshot.clone();
            snapshot.tasks = inner
                .tasks
                .values()
                .filter(|t| {
                    &t.run_id == run_id
                        && matches!(
                            t.status,
                            TaskStatus::Ready
                                | TaskStatus::Running
                                | TaskStatus::Awaiting
                                | TaskStatus::Exhausted
                        )
                })
                .cloned()
                .collect();
            snapshot.joins = inner
                .joins
                .values()
                .filter(|j| &j.run_id == run_id)
                .cloned()
                .collect();
            snapshot.signals = inner
                .signals
                .values()
                .filter(|s| &s.run_id == run_id && s.resolved_at.is_none())
                .cloned()
                .collect();
            snapshot
        }))
    }
    async fn load_task(&self, task_id: &TaskId) -> Result<Option<TaskRecord>, StoreError> {
        Ok(self.lock().tasks.get(task_id).cloned())
    }
    async fn claim_ready(&self, req: ClaimRequest) -> Result<Vec<ClaimedTask>, StoreError> {
        Ok(claims::claim(&mut self.lock(), &req))
    }
    async fn heartbeat(
        &self,
        proof: &LeaseProof,
        now: Micros,
        lease_micros: i64,
    ) -> Result<HeartbeatResult, StoreError> {
        let mut inner = self.lock();
        let task = inner
            .tasks
            .get_mut(&proof.task_id)
            .ok_or_else(|| StoreError::NotFound(format!("task {}", proof.task_id)))?;
        if !lease_matches(task, proof) {
            return Ok(HeartbeatResult::Lost);
        }
        let lease_until = now.saturating_add(lease_micros);
        task.lease_until = Some(lease_until);
        Ok(HeartbeatResult::Extended { lease_until })
    }
    async fn find_open_signal(
        &self,
        run_id: &RunId,
        name: &str,
    ) -> Result<Option<SignalRecord>, StoreError> {
        Ok(self
            .lock()
            .signals
            .values()
            .find(|s| &s.run_id == run_id && s.name == name && s.resolved_at.is_none())
            .cloned())
    }
    async fn due_signals(
        &self,
        now: Micros,
        limit: usize,
    ) -> Result<Vec<SignalRecord>, StoreError> {
        let inner = self.lock();
        let mut rows: Vec<_> = inner
            .signals
            .values()
            .filter(|s| {
                s.resolved_at.is_none()
                    && s.deadline_at.is_some_and(|d| d <= now)
                    && !inner.runs[&s.run_id].snapshot.status.is_terminal()
            })
            .cloned()
            .collect();
        rows.sort_by(|a, b| (a.deadline_at, &a.signal_id).cmp(&(b.deadline_at, &b.signal_id)));
        rows.truncate(limit);
        Ok(rows)
    }
    async fn exhausted_tasks(&self, limit: usize) -> Result<Vec<TaskRecord>, StoreError> {
        let inner = self.lock();
        let mut rows: Vec<_> = inner
            .tasks
            .values()
            .filter(|t| {
                t.status == TaskStatus::Exhausted
                    && t.planned_at.is_none()
                    && !inner.runs[&t.run_id].snapshot.status.is_terminal()
            })
            .cloned()
            .collect();
        rows.sort_by(|a, b| (a.run_at, &a.task_id).cmp(&(b.run_at, &b.task_id)));
        rows.truncate(limit);
        Ok(rows)
    }
    async fn events(
        &self,
        run_id: &RunId,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<EventRecord>, StoreError> {
        Ok(self.lock().runs.get(run_id).map_or_else(Vec::new, |r| {
            r.events
                .iter()
                .filter(|e| e.seq > after_seq)
                .take(limit)
                .cloned()
                .collect()
        }))
    }
    async fn list_runs(&self, filter: RunFilter) -> Result<Vec<RunSummary>, StoreError> {
        let inner = self.lock();
        let mut rows: Vec<_> = inner
            .runs
            .values()
            .filter(|r| {
                filter.status.is_none_or(|s| r.snapshot.status == s)
                    && filter
                        .graph_id
                        .as_ref()
                        .is_none_or(|id| &r.snapshot.graph_id == id)
            })
            .map(|r| RunSummary {
                run_id: r.snapshot.run_id.clone(),
                graph_id: r.snapshot.graph_id.clone(),
                graph_version: r.snapshot.graph_version,
                status: r.snapshot.status,
                version: r.snapshot.version,
                created_at: r.created_at,
                updated_at: r.updated_at,
            })
            .collect();
        rows.sort_by(|a, b| (a.created_at, &a.run_id).cmp(&(b.created_at, &b.run_id)));
        rows.truncate(filter.limit);
        Ok(rows)
    }
}
