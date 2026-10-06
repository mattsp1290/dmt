//! Planner-driven helpers shared by backend conformance tests.
use dmt_core::{
    Commit, EndStatus, Graph, GraphBuilder, Micros, NodeOutcome, Outcome, RetryPolicy, RunId,
    RunSnapshot, SignalId, SignalKey, StepKey, TaskId, TaskRecord, WorkerId, plan_outcome,
    plan_start,
};
use dmt_store::{ApplyResult, ClaimRequest, ClaimedTask, EventRecord, Store, StoreError};
use serde_json::{Value, json};
use std::future::Future;

pub const T0: Micros = Micros(1_000_000_000);
pub const LEASE: i64 = 10_000;
#[must_use]
pub fn worker(name: &str) -> WorkerId {
    name.into()
}
#[must_use]
pub fn run(name: &str) -> RunId {
    name.into()
}
#[must_use]
pub fn task_id(run: &RunId, node: &str, occ: u32) -> TaskId {
    StepKey::task(run, &node.into(), occ).as_str().into()
}
#[must_use]
pub fn branch_id(run: &RunId, node: &str, occ: u32, index: u32) -> TaskId {
    StepKey::branch(run, &node.into(), occ, index)
        .as_str()
        .into()
}
#[must_use]
pub fn signal_id(run: &RunId, name: &str, occ: u32) -> SignalId {
    SignalId::for_run(run, &SignalKey::new(name, occ))
}
/// Graph whose exhausted task schedules another task.
/// # Panics
/// Panics if this static graph becomes invalid.
#[must_use]
pub fn exhaust_then_continue() -> Graph {
    GraphBuilder::new("exhaust-continue", 1)
        .start("a")
        .task_with(
            "a",
            RetryPolicy {
                max_attempts: 2,
                ..RetryPolicy::default()
            },
            None,
        )
        .task("b")
        .end("done", EndStatus::Completed)
        .edge("a", "done")
        .edge_on("a", "b", "failed")
        .edge("b", "done")
        .build()
        .expect("exhaust_then_continue: invalid fixture")
}
#[must_use]
pub fn claim_request(
    worker_name: &str,
    now: Micros,
    graphs: &[&Graph],
    limit: usize,
) -> ClaimRequest {
    ClaimRequest {
        worker_id: worker(worker_name),
        now,
        lease_micros: LEASE,
        limit,
        graphs: graphs.iter().map(|g| g.id().clone()).collect(),
    }
}
/// Retry lock contention up to 50 times with a 1 ms delay.
/// # Errors
/// Returns non-`Busy` errors immediately, or `Busy` after retries are exhausted.
pub async fn retry_busy<T, Fut, Op>(mut op: Op) -> Result<T, StoreError>
where
    Op: FnMut() -> Fut,
    Fut: Future<Output = Result<T, StoreError>>,
{
    for attempt in 0..=50 {
        match op().await {
            Err(StoreError::Busy) if attempt < 50 => {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            result => return result,
        }
    }
    unreachable!("bounded retry always returns")
}
/// Unwrap a result with the owning case's name.
/// # Panics
/// Panics with the case name when the result is an error.
pub fn ok<T, E: std::fmt::Debug>(case: &str, result: Result<T, E>) -> T {
    result.unwrap_or_else(|e| panic!("{case}: unexpected error {e:?}"))
}
/// Register a graph and create a planner-built run.
/// # Panics
/// Panics with the case name on any setup failure.
pub async fn start_run<S: Store + ?Sized>(
    case: &str,
    store: &S,
    graph: &Graph,
    name: &str,
    now: Micros,
) -> RunId {
    ok(case, retry_busy(|| store.register_graph(graph)).await);
    let commit = ok(case, plan_start(graph, run(name), Value::Null, now));
    ok(case, retry_busy(|| store.create_run(commit.clone())).await)
}
/// Load a required run.
/// # Panics
/// Panics with the case name if loading fails or the run is absent.
pub async fn snapshot<S: Store + ?Sized>(case: &str, store: &S, run: &RunId) -> RunSnapshot {
    ok(case, retry_busy(|| store.load_run(run)).await)
        .unwrap_or_else(|| panic!("{case}: missing run {run}"))
}
/// Load a required task of any status.
/// # Panics
/// Panics with the case name if loading fails or the task is absent.
pub async fn task<S: Store + ?Sized>(case: &str, store: &S, id: &TaskId) -> TaskRecord {
    ok(case, retry_busy(|| store.load_task(id)).await)
        .unwrap_or_else(|| panic!("{case}: missing task {id}"))
}
/// Claim exactly the expected next task, without claiming its peers.
/// # Panics
/// Panics with the case name if the next candidate differs or claiming fails.
pub async fn claim_one<S: Store + ?Sized>(
    case: &str,
    store: &S,
    worker: &str,
    now: Micros,
    graph: &Graph,
    id: &TaskId,
) -> ClaimedTask {
    let req = claim_request(worker, now, &[graph], 1);
    let mut claimed = ok(case, retry_busy(|| store.claim_ready(req.clone())).await);
    assert_eq!(claimed.len(), 1, "{case}: expected one claim for {id}");
    let claimed = claimed.remove(0);
    assert_eq!(&claimed.task.task_id, id, "{case}: wrong next candidate");
    claimed
}
/// Claim up to 64 eligible tasks.
/// # Panics
/// Panics with the case name on a store error.
pub async fn claim_all<S: Store + ?Sized>(
    case: &str,
    store: &S,
    worker: &str,
    now: Micros,
    graph: &Graph,
) -> Vec<ClaimedTask> {
    let req = claim_request(worker, now, &[graph], 64);
    ok(case, retry_busy(|| store.claim_ready(req.clone())).await)
}
/// Plan a successful outcome from a fresh snapshot.
/// # Panics
/// Panics with the case name on load or planner errors.
pub async fn plan_done<S: Store + ?Sized>(
    case: &str,
    store: &S,
    graph: &Graph,
    id: &TaskId,
    label: &str,
    now: Micros,
) -> Commit {
    let task = task(case, store, id).await;
    let snapshot = snapshot(case, store, &task.run_id).await;
    ok(
        case,
        plan_outcome(
            graph,
            &snapshot,
            id,
            NodeOutcome::Done(Outcome::with_payload(label, json!(1))),
            now,
        ),
    )
}
/// Plan and apply a dispatched outcome.
/// # Panics
/// Panics with the case name on snapshot or planner errors.
/// # Errors
/// Returns store errors from applying the commit.
pub async fn complete<S: Store + ?Sized>(
    case: &str,
    store: &S,
    graph: &Graph,
    claimed: &ClaimedTask,
    outcome: NodeOutcome,
    now: Micros,
) -> Result<ApplyResult, StoreError> {
    let snapshot = snapshot(case, store, &claimed.task.run_id).await;
    let commit = ok(
        case,
        plan_outcome(graph, &snapshot, &claimed.task.task_id, outcome, now),
    );
    retry_busy(|| store.apply(commit.clone(), Some(claimed.proof.clone()))).await
}
/// Read all events of a fixture run.
/// # Panics
/// Panics with the case name on a store error.
pub async fn events<S: Store + ?Sized>(case: &str, store: &S, run: &RunId) -> Vec<EventRecord> {
    ok(case, retry_busy(|| store.events(run, 0, 10_000)).await)
}
#[must_use]
pub fn kinds(events: &[EventRecord]) -> Vec<&'static str> {
    events.iter().map(|e| e.event.kind()).collect()
}
#[must_use]
pub fn count(events: &[EventRecord], kind: &str) -> usize {
    events.iter().filter(|e| e.event.kind() == kind).count()
}
/// Assert per-run gap-free event allocation.
/// # Panics
/// Panics with the case name on a sequence gap.
pub fn assert_contiguous(case: &str, events: &[EventRecord]) {
    for (i, event) in events.iter().enumerate() {
        assert_eq!(event.seq, i as u64 + 1, "{case}: event sequence gap");
    }
}

pub(crate) fn done() -> NodeOutcome {
    NodeOutcome::Done(Outcome::with_payload("ok", json!(1)))
}
pub(crate) async fn branches<S: Store + ?Sized>(
    case: &str,
    store: &S,
    graph: &Graph,
    name: &str,
    count: usize,
) -> RunId {
    let run = start_run(case, store, graph, name, T0).await;
    let a = claim_one(case, store, "w1", T0, graph, &task_id(&run, "a", 0)).await;
    ok(case, complete(case, store, graph, &a, done(), T0).await);
    let fo = claim_one(case, store, "w1", T0, graph, &task_id(&run, "fo", 0)).await;
    ok(
        case,
        complete(
            case,
            store,
            graph,
            &fo,
            NodeOutcome::FanOut((0..count).map(|i| json!(i)).collect()),
            T0,
        )
        .await,
    );
    run
}
pub(crate) async fn exhaust<S: Store + ?Sized>(
    case: &str,
    store: &S,
    graph: &Graph,
    name: &str,
) -> (RunId, ClaimedTask) {
    let run = start_run(case, store, graph, name, T0).await;
    let first = claim_one(case, store, "w1", T0, graph, &task_id(&run, "a", 0)).await;
    let second = claim_one(
        case,
        store,
        "w1",
        T0.saturating_add(LEASE + 1),
        graph,
        &first.task.task_id,
    )
    .await;
    assert_eq!(
        second.task.attempt, 2,
        "{case}: reclaim must increment attempt"
    );
    assert!(
        claim_all(case, store, "w1", T0.saturating_add(2 * LEASE + 2), graph)
            .await
            .is_empty(),
        "{case}: exhausted task was claimed"
    );
    (run, first)
}

/// Extract a required backend value with its owning case's name.
/// # Panics
/// Panics with the case name when the value is absent.
pub fn required<T>(case: &str, value: Option<T>) -> T {
    value.unwrap_or_else(|| panic!("{case}: missing required value"))
}
/// Index backend data with a case-named diagnostic.
/// # Panics
/// Panics with the case name when the row is absent.
#[must_use]
pub fn at<'a, T>(case: &str, rows: &'a [T], index: usize) -> &'a T {
    rows.get(index)
        .unwrap_or_else(|| panic!("{case}: missing row at index {index}"))
}
