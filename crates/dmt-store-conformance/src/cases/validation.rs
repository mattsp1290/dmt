//! Additional negative subcases, owned by the original public contract cases.
use crate::{
    StoreFactory,
    harness::{
        T0, at, branches, claim_all, claim_one, complete, done, events, ok, plan_done, required,
        snapshot, start_run, task_id,
    },
};
use dmt_core::{
    BranchResult, Commit, Graph, JoinPolicy, NodeOutcome, SignalPayload, fixtures, plan_outcome,
    plan_signal,
};
use dmt_store::{LeaseProof, Store, StoreError};
use serde_json::Value;

async fn invalid<S: Store>(case: &str, store: &S, commit: Commit, proof: Option<LeaseProof>) {
    let run = commit.run_id.clone();
    let before = snapshot(case, store, &run).await;
    let ev = events(case, store, &run).await;
    assert!(
        matches!(
            store.apply(commit, proof).await,
            Err(StoreError::InvalidCommit(_))
        ),
        "{case}: invalid identity/uniqueness accepted"
    );
    assert_eq!(
        snapshot(case, store, &run).await,
        before,
        "{case}: rejected commit changed snapshot"
    );
    assert_eq!(
        events(case, store, &run).await,
        ev,
        "{case}: rejected commit changed events"
    );
}
pub(super) async fn task_identity<S: Store>(
    case: &str,
    store: &S,
    commit: &Commit,
    proof: &LeaseProof,
) {
    let mut bad = commit.clone();
    bad.new_tasks[0].task_id = "wrong-id".into();
    invalid(case, store, bad, Some(proof.clone())).await;
}
pub(super) async fn create_version<S: Store>(
    case: &str,
    store: &S,
    graph: &Graph,
    commit: &Commit,
) {
    ok(case, store.register_graph(graph).await);
    let mut bad = commit.clone();
    bad.expected_run_version = 1;
    assert!(
        matches!(
            store.create_run(bad).await,
            Err(StoreError::InvalidCommit(_))
        ),
        "{case}: nonzero create version"
    );
    assert!(
        ok(case, store.load_run(&commit.run_id).await).is_none(),
        "{case}: failed start wrote run"
    );
    assert!(
        events(case, store, &commit.run_id).await.is_empty(),
        "{case}: failed start wrote events"
    );
}
pub(super) async fn duplicate_join<F: StoreFactory>(factory: &F) {
    const CASE: &str = "join_guard";
    let store = factory.fresh().await;
    let graph = fixtures::fan_out(JoinPolicy::All);
    let run = branches(CASE, &store, &graph, "run-join", 2).await;
    let claimed = claim_all(CASE, &store, "w1", T0, &graph).await;
    let mut c = plan_done(
        CASE,
        &store,
        &graph,
        &at(CASE, &claimed, 0).task.task_id,
        "ok",
        T0,
    )
    .await;
    c.new_join = Some(at(CASE, &snapshot(CASE, &store, &run).await.joins, 0).clone());
    invalid(CASE, &store, c, Some(at(CASE, &claimed, 0).proof.clone())).await;
}
pub(super) async fn failed_join<F: StoreFactory>(factory: &F) {
    const CASE: &str = "join_guard";
    let store = factory.fresh().await;
    let graph = fixtures::fan_out(JoinPolicy::Quorum(1));
    let run = branches(CASE, &store, &graph, "run-failed", 2).await;
    let claimed = claim_all(CASE, &store, "w1", T0, &graph).await;
    let first = at(CASE, &claimed, 0);
    let s = snapshot(CASE, &store, &run).await;
    let c = ok(
        CASE,
        plan_outcome(
            &graph,
            &s,
            &first.task.task_id,
            NodeOutcome::Fail {
                message: "failed branch".into(),
                retryable: false,
            },
            T0,
        ),
    );
    let mut bad = c.clone();
    required(CASE, bad.join_contribution.as_mut()).expected_failed += 1;
    let ev = events(CASE, &store, &run).await;
    assert!(
        matches!(
            store.apply(bad, Some(first.proof.clone())).await,
            Err(StoreError::JoinDrift { .. })
        ),
        "{CASE}: failed counter drift accepted"
    );
    assert_eq!(
        snapshot(CASE, &store, &run).await,
        s,
        "{CASE}: failed drift wrote snapshot"
    );
    assert_eq!(
        events(CASE, &store, &run).await,
        ev,
        "{CASE}: failed drift wrote events"
    );
    ok(CASE, store.apply(c, Some(first.proof.clone())).await);
    let s = snapshot(CASE, &store, &run).await;
    let join = at(CASE, &s.joins, 0);
    assert_eq!(
        (join.received, join.failed),
        (0, 1),
        "{CASE}: failed counters"
    );
    assert_eq!(
        join.results,
        vec![BranchResult::Failed {
            index: 0,
            message: "failed branch".into()
        }],
        "{CASE}: failed result persisted"
    );
    ok(
        CASE,
        complete(CASE, &store, &graph, at(CASE, &claimed, 1), done(), T0).await,
    );
    let s = snapshot(CASE, &store, &run).await;
    let join = at(CASE, &s.joins, 0);
    assert_eq!(
        (join.received, join.failed, join.results.len()),
        (1, 1, 2),
        "{CASE}: mixed counters"
    );
}
pub(super) async fn duplicate_signals<F: StoreFactory>(factory: &F) {
    const CASE: &str = "signal_resolution";
    let store = factory.fresh().await;
    let graph = fixtures::loop_via_wait();
    let run = start_run(CASE, &store, &graph, "run-signals", T0).await;
    let a = claim_one(CASE, &store, "w1", T0, &graph, &task_id(&run, "plan", 0)).await;
    let first = plan_done(CASE, &store, &graph, &a.task.task_id, "ok", T0).await;
    let old = required(CASE, first.new_signal.clone());
    ok(CASE, store.apply(first, Some(a.proof)).await);
    let c = ok(
        CASE,
        plan_signal(
            &graph,
            &snapshot(CASE, &store, &run).await,
            &old.signal_id,
            SignalPayload {
                label: "changes_requested".into(),
                payload: Value::Null,
            },
            T0,
        ),
    );
    ok(CASE, store.apply(c, None).await);
    let a = claim_one(CASE, &store, "w1", T0, &graph, &task_id(&run, "plan", 1)).await;
    let c = plan_done(CASE, &store, &graph, &a.task.task_id, "ok", T0).await;
    let mut bad_key = c.clone();
    required(CASE, bad_key.new_signal.as_mut()).key = old.key;
    invalid(CASE, &store, bad_key, Some(a.proof.clone())).await;
    let mut bad_id = c;
    required(CASE, bad_id.new_signal.as_mut()).signal_id = old.signal_id;
    invalid(CASE, &store, bad_id, Some(a.proof)).await;
}
