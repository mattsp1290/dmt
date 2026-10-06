use async_trait::async_trait;
use dmt_core::{
    Commit, Graph, GraphId, Micros, RunId, RunSnapshot, SignalRecord, TaskId, TaskRecord,
};
use dmt_store::{
    ApplyResult, ClaimRequest, ClaimedTask, EventRecord, HeartbeatResult, LeaseProof, MemoryStore,
    RunFilter, RunSummary, Store, StoreError,
};
use dmt_store_conformance::{
    StoreFactory, case_concurrent_claims, case_crash_between_claim_and_apply, case_event_sequence,
    case_heartbeat, case_insert_or_ignore, case_join_guard, case_lease_proof, case_run_terminal,
};
use std::sync::{Arc, Mutex};
#[derive(Clone, Copy)]
enum Fault {
    IgnoreProof,
    HeartbeatIgnoresAttempt,
    HideIgnored,
    NoReclaimIncrement,
    SwallowJoinDrift,
    TerminalBeforeVersionSwapped,
    DropClaimEvents,
    DuplicateClaim,
    HideSignalLookup,
    HideSnapshotSignals,
    HideJoins,
}
struct Mutant {
    inner: MemoryStore,
    fault: Fault,
    previous: Arc<Mutex<Option<ClaimedTask>>>,
}
struct MutantFactory(Fault);
#[async_trait]
impl StoreFactory for MutantFactory {
    type S = Mutant;
    async fn fresh(&self) -> Mutant {
        Mutant {
            inner: MemoryStore::new(),
            fault: self.0,
            previous: Arc::new(Mutex::new(None)),
        }
    }
}
#[async_trait]
impl Store for Mutant {
    async fn register_graph(&self, graph: &Graph) -> Result<(), StoreError> {
        self.inner.register_graph(graph).await
    }
    async fn load_graph(&self, id: &GraphId, version: u32) -> Result<Option<Graph>, StoreError> {
        self.inner.load_graph(id, version).await
    }
    async fn create_run(&self, commit: Commit) -> Result<RunId, StoreError> {
        self.inner.create_run(commit).await
    }
    async fn load_run(&self, run_id: &RunId) -> Result<Option<RunSnapshot>, StoreError> {
        let mut snapshot = self.inner.load_run(run_id).await?;
        if let Some(view) = &mut snapshot {
            if matches!(self.fault, Fault::HideSnapshotSignals) {
                view.signals.clear();
            }
            if matches!(self.fault, Fault::HideJoins) {
                view.joins.clear();
            }
        }
        Ok(snapshot)
    }
    async fn load_task(&self, task_id: &TaskId) -> Result<Option<TaskRecord>, StoreError> {
        self.inner.load_task(task_id).await
    }
    async fn claim_ready(&self, req: ClaimRequest) -> Result<Vec<ClaimedTask>, StoreError> {
        let mut claimed = self.inner.claim_ready(req).await?;
        if matches!(self.fault, Fault::NoReclaimIncrement) {
            for c in &mut claimed {
                if c.task.attempt > 1 {
                    c.task.attempt -= 1;
                    c.proof.attempt -= 1;
                }
            }
        }
        if matches!(self.fault, Fault::DuplicateClaim) {
            // Seed only branch claims, so deterministic setup can complete normally.
            let mut previous = self.previous.lock().unwrap();
            let own = claimed.iter().find(|c| c.task.branch.is_some()).cloned();
            if let Some(c) = previous.as_ref() {
                claimed.push(c.clone());
            }
            if own.is_some() {
                *previous = own;
            }
        }
        Ok(claimed)
    }
    async fn heartbeat(
        &self,
        proof: &LeaseProof,
        now: Micros,
        lease_micros: i64,
    ) -> Result<HeartbeatResult, StoreError> {
        let mut proof = proof.clone();
        if matches!(self.fault, Fault::HeartbeatIgnoresAttempt)
            && let Some(task) = self.inner.load_task(&proof.task_id).await?
        {
            proof.attempt = task.attempt;
        }
        self.inner.heartbeat(&proof, now, lease_micros).await
    }
    async fn apply(
        &self,
        commit: Commit,
        by: Option<LeaseProof>,
    ) -> Result<ApplyResult, StoreError> {
        if matches!(self.fault, Fault::TerminalBeforeVersionSwapped)
            && let Some(run) = self.inner.load_run(&commit.run_id).await?
            && run.version != commit.expected_run_version
        {
            return Err(StoreError::VersionConflict {
                expected: commit.expected_run_version,
                actual: run.version,
            });
        }
        let by = if matches!(self.fault, Fault::IgnoreProof) {
            None
        } else {
            by
        };
        let result = self.inner.apply(commit, by).await;
        match result {
            Ok(mut r) => {
                if matches!(self.fault, Fault::HideIgnored) {
                    r.ignored_tasks.clear();
                }
                Ok(r)
            }
            Err(StoreError::JoinDrift { .. }) if matches!(self.fault, Fault::SwallowJoinDrift) => {
                Ok(ApplyResult::default())
            }
            other => other,
        }
    }
    async fn find_open_signal(
        &self,
        run_id: &RunId,
        name: &str,
    ) -> Result<Option<SignalRecord>, StoreError> {
        if matches!(self.fault, Fault::HideSignalLookup) {
            return Ok(None);
        }
        self.inner.find_open_signal(run_id, name).await
    }
    async fn due_signals(
        &self,
        now: Micros,
        limit: usize,
    ) -> Result<Vec<SignalRecord>, StoreError> {
        self.inner.due_signals(now, limit).await
    }
    async fn exhausted_tasks(&self, limit: usize) -> Result<Vec<TaskRecord>, StoreError> {
        self.inner.exhausted_tasks(limit).await
    }
    async fn events(
        &self,
        run_id: &RunId,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<EventRecord>, StoreError> {
        let mut events = self.inner.events(run_id, after_seq, limit).await?;
        if matches!(self.fault, Fault::DropClaimEvents) {
            events.retain(|e| e.event.kind() != "TaskClaimed");
            for (i, event) in events.iter_mut().enumerate() {
                event.seq = i as u64 + 1;
            }
        }
        Ok(events)
    }
    async fn list_runs(&self, filter: RunFilter) -> Result<Vec<RunSummary>, StoreError> {
        self.inner.list_runs(filter).await
    }
}
#[tokio::test]
#[should_panic(expected = "lease_proof")]
async fn mutant_lease_proof() {
    case_lease_proof(&MutantFactory(Fault::IgnoreProof)).await;
}
#[tokio::test]
#[should_panic(expected = "heartbeat")]
async fn mutant_heartbeat() {
    case_heartbeat(&MutantFactory(Fault::HeartbeatIgnoresAttempt)).await;
}
#[tokio::test]
#[should_panic(expected = "insert_or_ignore")]
async fn mutant_insert_or_ignore() {
    case_insert_or_ignore(&MutantFactory(Fault::HideIgnored)).await;
}
#[tokio::test]
#[should_panic(expected = "crash_between_claim_and_apply")]
async fn mutant_crash_between_claim_and_apply() {
    case_crash_between_claim_and_apply(&MutantFactory(Fault::NoReclaimIncrement)).await;
}
#[tokio::test]
#[should_panic(expected = "join_guard")]
async fn mutant_join_guard() {
    case_join_guard(&MutantFactory(Fault::SwallowJoinDrift)).await;
}
#[tokio::test]
#[should_panic(expected = "run_terminal")]
async fn mutant_run_terminal() {
    case_run_terminal(&MutantFactory(Fault::TerminalBeforeVersionSwapped)).await;
}
#[tokio::test]
#[should_panic(expected = "event_sequence")]
async fn mutant_event_sequence() {
    case_event_sequence(&MutantFactory(Fault::DropClaimEvents)).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[should_panic(expected = "concurrent_claims")]
async fn mutant_concurrent_claims() {
    case_concurrent_claims(&MutantFactory(Fault::DuplicateClaim)).await;
}

#[tokio::test]
#[should_panic(expected = "wait_loop")]
async fn hidden_signal_lookup_names_case() {
    dmt_store_conformance::case_wait_loop(&MutantFactory(Fault::HideSignalLookup)).await;
}
#[tokio::test]
#[should_panic(expected = "micros_ordering")]
async fn hidden_snapshot_signal_names_case() {
    dmt_store_conformance::case_micros_ordering(&MutantFactory(Fault::HideSnapshotSignals)).await;
}
#[tokio::test]
#[should_panic(expected = "join_guard")]
async fn hidden_join_names_case() {
    case_join_guard(&MutantFactory(Fault::HideJoins)).await;
}
