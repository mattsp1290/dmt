//! Store contract.
//!
//!
//!
//! Every backend, `MemoryStore` included, must satisfy the following. The `dmt-store-conformance` crate exports named cases for these rules.
//!
//! ### Graphs and runs
//!
//! - `register_graph`: upsert keyed by `(graph.id(), graph.version())`. Same key and same `definition_hash()` → `Ok(())` (idempotent). Same key and different hash → `GraphMismatch` (rule 11). The store keeps the full `Graph` so `load_graph` can return it.
//! - `load_graph`: `Ok(None)` when unregistered.
//! - `create_run(commit)`: requires `commit.new_run.is_some()` and `commit.expected_run_version == 0`, else `InvalidCommit`. The graph `(new_run.graph_id, new_run.graph_version)` must be registered, else `NotFound` (rule 12); its hash must equal `new_run.definition_hash`, else `GraphMismatch`. A run with `commit.run_id` must not exist, else `VersionConflict { expected: 0, actual: existing.version }`. Then the store performs the same mutation as `apply` (below) with the run row created from `new_run` and `run_state` (status, `machine_json`, `output`), `created_at = updated_at = commit.now`, version 1, events `seq` 1..n. Returns the run id.
//! - `load_run`: `Ok(None)` for an unknown run. Otherwise a `RunSnapshot` with `version`, `status`, `machine_json`, `input`, `output`, `node_occurrences`, `tasks` including every task in Ready, Running, Awaiting, or Exhausted (planned or not; extra rows permitted), `joins` including every join of the run, `signals` including every open signal (`resolved_at.is_none()`) and no resolved signal.
//! - `load_task`: any status; `Ok(None)` when unknown.
//! - `list_runs(filter)`: runs matching `status` and `graph_id` when set, ordered by `(created_at, run_id)`, at most `limit`.
//!
//! ### `apply(commit, by)`
//!
//! Checks before any write (rule 1: all-or-nothing). The precedence of checks 1, 2, 3, and 5 (`NotFound` for the run, `RunTerminal`, `VersionConflict`, `LeaseLost`) is part of the contract and is tested by the suite (`case_version_conflict`, `case_run_terminal`, `case_lease_proof`); the position of check 4 and the relative order of checks 6 to 9 are `MemoryStore`'s order and backends may differ.
//!
//! 1. Run `commit.run_id` exists → else `NotFound`.
//! 2. Run status not terminal → else `RunTerminal` (rule 4).
//! 3. `run.version == commit.expected_run_version` → else `VersionConflict { expected: commit.expected_run_version, actual: run.version }` (rule 2).
//! 4. `commit.new_run.is_none()` → else `InvalidCommit`.
//! 5. If `by` is `Some(proof)`: task `proof.task_id` exists → else `NotFound`; `task.run_id == commit.run_id` and `task.lease_owner == Some(proof.worker_id)` and `task.attempt == proof.attempt` and `task.status == Running` → else `LeaseLost` (rule 3). `by == None` skips this check by design; it is for sweep, signal, timeout, and cancel commits, which no handler dispatch owns.
//! 6. Every `task_updates[i].task_id` exists and its `run_id == commit.run_id` → else `NotFound` (the poisoned-commit case of rule 1).
//! 7. If `join_contribution` is `Some(c)`: join `c.join_id` exists and belongs to the run → else `NotFound`; `join.satisfied_at.is_none()`; `join.received + (c.result is Done) == c.expected_received`; `join.failed + (c.result is Failed) == c.expected_failed` → else `JoinDrift` (rule 6).
//! 8. Every `resolved_signals[i].signal_id` exists and belongs to the run → else `NotFound`; `resolved_at.is_none()` → else `AlreadyResolved` (rule 10).
//! 9. If `new_signal` is `Some(s)`: no signal with `(run_id, s.key)` exists → else `InvalidCommit`. If `new_join` is `Some(j)`: no join `j.join_id` exists → else `InvalidCommit`. A `NewTask` whose `task_id` already exists with a different `step_key`, or whose `task_id` differs from its own `step_key` string, is `InvalidCommit` (insert-or-ignore applies to step keys only).
//!
//! Writes, all stamped with `commit.now` where a timestamp is needed:
//!
//! 1. If `run_state` is `Some`: set `status`, `machine_json`, `output`.
//! 2. Replace `node_occurrences` with `commit.node_occurrences`.
//! 3. For each `TaskUpdate` in order: if `status == Cancelled` and the row is not Ready or Awaiting, skip it (a concurrent claim moved it to Running without a version bump). Otherwise set `status`, `attempt`, `run_at`, `outcome`, `planned_at`; if `status != Running`, set `lease_owner = None` and `lease_until = None`.
//! 4. For each `NewTask` in order: if a task with the same `step_key` exists in the store or earlier in this commit, push the key to `ignored_tasks` and write nothing for it; else insert a `TaskRecord` with `run_id = commit.run_id`, `lease_owner = None`, `lease_until = None`, `outcome = None`, `planned_at = None`, and push the id to `inserted_tasks` (rule 5).
//! 5. Insert `new_join` as given.
//! 6. Apply `join_contribution`: set `received = expected_received`, `failed = expected_failed`, push `result` to `results`, and when `satisfied` set `satisfied_at = Some(commit.now)` and `join_satisfied = Some(join_id)`.
//! 7. Insert `new_signal` as a `SignalRecord` with `run_id = commit.run_id`, `resolved_at = None`.
//! 8. For each `SignalResolution`: set `resolved_at = Some(commit.now)`; a backend may also keep `label` and `payload` in its own row (SQLite stores `payload_json`); `SignalRecord` does not expose them.
//! 9. Append every `commit.events[i]` exactly as given (including a `TaskScheduled` whose `NewTask` was ignored) as `EventRecord { seq, recorded_at: commit.now, event }` using the run's sequence allocator (first event of a run has `seq` 1, each append increments by one; rule 9). A rejected commit must not consume sequence numbers: the next successful commit continues gap-free.
//! 10. Set `run.version = expected_run_version + 1` and `updated_at = commit.now`.
//! 11. Return `ApplyResult { run_version, inserted_tasks, ignored_tasks, join_satisfied }`.
//!
//! The store does not call `Commit::check`; the planner owns that validation.
//!
//! ### `claim_ready(req)` (rule 7)
//!
//! - `req.limit == 0` or `req.graphs.is_empty()` → `Ok(vec![])`, no writes.
//! - Candidates: tasks whose run's `graph_id` is in `req.graphs` and either (`status == Ready` and `run_at <= req.now`) or (`status == Running` and `lease_until < req.now`, an expired lease). Ordered by `(run_at, task_id)`.
//! - Walk the candidates in order and stop after `req.limit` tasks have been returned. For each candidate:
//!   - run terminal → `TaskStatus::next(RunTerminal)` = Cancelled; clear the lease; no event; not counted toward `limit`.
//!   - expired lease and `attempt >= max_attempts` → `next(LeaseExhausted)` = Exhausted; clear the lease; append `TaskExhausted { task_id, reason: LeaseReclaimsExceeded }` with the run's `seq` allocator and `recorded_at = req.now`; not counted.
//!   - expired lease otherwise → `next(Reclaim)` = Running with `attempt += 1`; Ready → `next(Claim)` = Running with `attempt` unchanged. Set `lease_owner = Some(req.worker_id)`, `lease_until = Some(req.now.saturating_add(req.lease_micros))`; append `TaskClaimed { task_id, worker_id, attempt, lease_until }`; push `ClaimedTask { task, graph_id, graph_version, proof: LeaseProof { task_id, worker_id, attempt } }`; counted.
//! - The run version never changes. Candidates not reached because `limit` was hit are untouched until the next call. Two concurrent callers never receive the same task (one critical section in `MemoryStore`; a write transaction in SQL stores).
//!
//! ### `heartbeat(proof, now, lease_micros)` (rule 8)
//!
//! Task missing → `NotFound`. `lease_owner == Some(worker_id)` and `attempt == proof.attempt` and `status == Running` → set `lease_until = now + lease_micros`, return `Extended { lease_until }`. Otherwise return `Lost` and write nothing. No event, no version change.
//!
//! ### Signals and sweeps
//!
//! - `find_open_signal(run_id, name)`: the open signal of that run with `name`; `Ok(None)` when none or the run is unknown. At most one is open per name by construction (graph validation rule 12 plus one occurrence at a time), so the multiple-open case is unreachable through planner-built commits and the suite does not test it; a backend may return any one of them (rule 13).
//! - `due_signals(now, limit)`: open signals with `deadline_at <= now` whose run is not terminal, ordered by `(deadline_at, signal_id)`, at most `limit`. The terminal-run clause is defensive and untested: `plan_cancel` resolves every open signal and no planner path completes a run while a wait is open, so planner-built commits cannot create an open signal on a terminal run.
//! - `exhausted_tasks(limit)`: tasks with `status == Exhausted`, `planned_at.is_none()`, run not terminal, ordered by `(run_at, task_id)`, at most `limit` (rule 15). The terminal-run clause is tested by `case_exhausted_sweep_idempotent` part B (a store-exhausted task of a cancelled run is not returned).
//! - `events(run_id, after_seq, limit)`: events with `seq > after_seq` in `seq` order, at most `limit`; unknown run → empty vector.
//!
//! ### Rule 14
//!
//! All timestamps are `Micros`. Ordering by `run_at` is exact to the microsecond; the suite schedules tasks 1 µs apart and asserts claim order.
//!
use crate::{
    ApplyResult, ClaimRequest, ClaimedTask, EventRecord, HeartbeatResult, LeaseProof, RunFilter,
    RunSummary, StoreError,
};
use async_trait::async_trait;
use dmt_core::{
    Commit, Graph, GraphId, Micros, RunId, RunSnapshot, SignalRecord, TaskId, TaskRecord,
};

#[async_trait]
pub trait Store: Send + Sync {
    /// Register graph.
    /// # Errors
    /// Returns `StoreError` on a contract violation or backend failure; any method may return `Busy`.
    async fn register_graph(&self, graph: &Graph) -> Result<(), StoreError>;
    /// Load graph.
    /// # Errors
    /// Returns `StoreError` on a contract violation or backend failure; any method may return `Busy`.
    async fn load_graph(&self, id: &GraphId, version: u32) -> Result<Option<Graph>, StoreError>;
    /// Create run.
    /// # Errors
    /// Returns `StoreError` on a contract violation or backend failure; any method may return `Busy`.
    async fn create_run(&self, commit: Commit) -> Result<RunId, StoreError>;
    /// Load run.
    /// # Errors
    /// Returns `StoreError` on a contract violation or backend failure; any method may return `Busy`.
    async fn load_run(&self, run_id: &RunId) -> Result<Option<RunSnapshot>, StoreError>;
    /// Load task.
    /// # Errors
    /// Returns `StoreError` on a contract violation or backend failure; any method may return `Busy`.
    async fn load_task(&self, task_id: &TaskId) -> Result<Option<TaskRecord>, StoreError>;
    /// Claim ready.
    /// # Errors
    /// Returns `StoreError` on a contract violation or backend failure; any method may return `Busy`.
    async fn claim_ready(&self, req: ClaimRequest) -> Result<Vec<ClaimedTask>, StoreError>;
    /// Heartbeat.
    /// # Errors
    /// Returns `StoreError` on a contract violation or backend failure; any method may return `Busy`.
    async fn heartbeat(
        &self,
        proof: &LeaseProof,
        now: Micros,
        lease_micros: i64,
    ) -> Result<HeartbeatResult, StoreError>;
    /// Apply.
    /// # Errors
    /// Returns `StoreError` on a contract violation or backend failure; any method may return `Busy`.
    async fn apply(
        &self,
        commit: Commit,
        by: Option<LeaseProof>,
    ) -> Result<ApplyResult, StoreError>;
    /// Find open signal.
    /// # Errors
    /// Returns `StoreError` on a contract violation or backend failure; any method may return `Busy`.
    async fn find_open_signal(
        &self,
        run_id: &RunId,
        name: &str,
    ) -> Result<Option<SignalRecord>, StoreError>;
    /// Due signals.
    /// # Errors
    /// Returns `StoreError` on a contract violation or backend failure; any method may return `Busy`.
    async fn due_signals(&self, now: Micros, limit: usize)
    -> Result<Vec<SignalRecord>, StoreError>;
    /// Exhausted tasks.
    /// # Errors
    /// Returns `StoreError` on a contract violation or backend failure; any method may return `Busy`.
    async fn exhausted_tasks(&self, limit: usize) -> Result<Vec<TaskRecord>, StoreError>;
    /// Events.
    /// # Errors
    /// Returns `StoreError` on a contract violation or backend failure; any method may return `Busy`.
    async fn events(
        &self,
        run_id: &RunId,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<EventRecord>, StoreError>;
    /// List runs.
    /// # Errors
    /// Returns `StoreError` on a contract violation or backend failure; any method may return `Busy`.
    async fn list_runs(&self, filter: RunFilter) -> Result<Vec<RunSummary>, StoreError>;
}
