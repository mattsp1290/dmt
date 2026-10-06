use crate::harness::{at, required};
use crate::{
    StoreFactory,
    harness::{
        T0, assert_contiguous, branches, claim_all, claim_one, complete, count, done, events,
        kinds, ok, plan_done, snapshot, start_run, task, task_id, worker,
    },
};
use dmt_core::{
    BranchResult, JoinContribution, JoinPolicy, Outcome, RunStatus, SignalPayload,
    SignalResolution, TaskStatus, TaskUpdate, fixtures, plan_cancel, plan_outcome, plan_signal,
};
use dmt_store::{LeaseProof, Store, StoreError};
use serde_json::{Value, json};

/// Prove transactional rule 1.
/// # Panics
/// Panics with the case name when the backend violates the contract.
pub async fn case_apply_atomic<F: StoreFactory>(factory: &F) {
    const CASE: &str = "apply_atomic";
    let store = factory.fresh().await;
    let graph = fixtures::linear();
    let run = start_run(CASE, &store, &graph, "run-1", T0).await;
    let a = claim_one(CASE, &store, "w1", T0, &graph, &task_id(&run, "a", 0)).await;
    let mut commit = plan_done(
        CASE,
        &store,
        &graph,
        &a.task.task_id,
        "ok",
        T0.saturating_add(1),
    )
    .await;
    commit.task_updates.push(TaskUpdate {
        task_id: "run-1/missing/0".into(),
        status: TaskStatus::Completed,
        attempt: 1,
        run_at: T0,
        outcome: None,
        planned_at: None,
    });
    let before = snapshot(CASE, &store, &run).await;
    let ev = events(CASE, &store, &run).await;
    assert!(
        matches!(
            store.apply(commit, Some(a.proof.clone())).await,
            Err(StoreError::NotFound(_))
        ),
        "{CASE}: poison must reject"
    );
    assert_eq!(
        snapshot(CASE, &store, &run).await,
        before,
        "{CASE}: partial writes"
    );
    assert_eq!(
        events(CASE, &store, &run).await,
        ev,
        "{CASE}: rejected events persisted"
    );
    assert!(
        ok(CASE, store.load_task(&task_id(&run, "b", 0)).await).is_none(),
        "{CASE}: child inserted"
    );
    assert_eq!(before.version, 1, "{CASE}: initial version");
    ok(CASE, complete(CASE, &store, &graph, &a, done(), T0).await);
    assert_contiguous(CASE, &events(CASE, &store, &run).await);
}
/// Prove transactional rule 2.
/// # Panics
/// Panics with the case name when the backend violates the contract.
pub async fn case_version_conflict<F: StoreFactory>(factory: &F) {
    const CASE: &str = "version_conflict";
    let store = factory.fresh().await;
    let graph = fixtures::linear();
    let run = start_run(CASE, &store, &graph, "run-1", T0).await;
    let a = claim_one(CASE, &store, "w1", T0, &graph, &task_id(&run, "a", 0)).await;
    let good = plan_done(CASE, &store, &graph, &a.task.task_id, "ok", T0).await;
    let mut bad = good.clone();
    bad.expected_run_version = 5;
    let before = snapshot(CASE, &store, &run).await;
    let ev = events(CASE, &store, &run).await;
    assert_eq!(
        store.apply(bad, Some(a.proof.clone())).await,
        Err(StoreError::VersionConflict {
            expected: 5,
            actual: 1
        }),
        "{CASE}: version guard"
    );
    assert_eq!(
        snapshot(CASE, &store, &run).await,
        before,
        "{CASE}: changed snapshot"
    );
    assert_eq!(
        events(CASE, &store, &run).await,
        ev,
        "{CASE}: changed events"
    );
    let mut missing = good.clone();
    missing.run_id = "run-missing".into();
    assert!(
        matches!(
            store.apply(missing, None).await,
            Err(StoreError::NotFound(_))
        ),
        "{CASE}: missing run"
    );
    let result = ok(CASE, store.apply(good, Some(a.proof)).await);
    assert_eq!(result.run_version, 2, "{CASE}: new version");
    assert_eq!(
        result.inserted_tasks,
        vec![task_id(&run, "b", 0)],
        "{CASE}: inserted tasks"
    );
    assert_contiguous(CASE, &events(CASE, &store, &run).await);
}
/// Prove transactional rule 3.
/// # Panics
/// Panics with the case name when the backend violates the contract.
pub async fn case_lease_proof<F: StoreFactory>(factory: &F) {
    const CASE: &str = "lease_proof";
    let store = factory.fresh().await;
    let graph = fixtures::linear();
    let run = start_run(CASE, &store, &graph, "run-1", T0).await;
    let a = claim_one(CASE, &store, "w1", T0, &graph, &task_id(&run, "a", 0)).await;
    let commit = plan_done(CASE, &store, &graph, &a.task.task_id, "ok", T0).await;
    let before = snapshot(CASE, &store, &run).await;
    for proof in [
        LeaseProof {
            worker_id: worker("w2"),
            ..a.proof.clone()
        },
        LeaseProof {
            attempt: 2,
            ..a.proof.clone()
        },
    ] {
        assert_eq!(
            store.apply(commit.clone(), Some(proof)).await,
            Err(StoreError::LeaseLost {
                task_id: a.task.task_id.clone()
            }),
            "{CASE}: invalid proof accepted"
        );
    }
    assert!(
        matches!(
            store
                .apply(
                    commit.clone(),
                    Some(LeaseProof {
                        task_id: task_id(&run, "b", 0),
                        ..a.proof.clone()
                    })
                )
                .await,
            Err(StoreError::NotFound(_))
        ),
        "{CASE}: missing proof task"
    );
    let mut stale = commit.clone();
    stale.expected_run_version = 5;
    assert!(
        matches!(
            store
                .apply(
                    stale,
                    Some(LeaseProof {
                        worker_id: worker("w2"),
                        ..a.proof.clone()
                    })
                )
                .await,
            Err(StoreError::VersionConflict { .. })
        ),
        "{CASE}: version precedes lease"
    );
    let run2 = start_run(CASE, &store, &graph, "run-2", T0).await;
    let a2 = claim_one(CASE, &store, "w1", T0, &graph, &task_id(&run2, "a", 0)).await;
    assert!(
        matches!(
            store.apply(commit.clone(), Some(a2.proof.clone())).await,
            Err(StoreError::LeaseLost { .. })
        ),
        "{CASE}: cross-run proof"
    );
    assert_eq!(
        snapshot(CASE, &store, &run).await,
        before,
        "{CASE}: rejected proofs changed run"
    );
    ok(CASE, store.apply(commit, Some(a.proof)).await);
    let c2 = plan_done(CASE, &store, &graph, &a2.task.task_id, "ok", T0).await;
    ok(CASE, store.apply(c2, None).await);
    assert_contiguous(CASE, &events(CASE, &store, &run).await);
}
/// Prove transactional rule 4.
/// # Panics
/// Panics with the case name when the backend violates the contract.
pub async fn case_run_terminal<F: StoreFactory>(factory: &F) {
    const CASE: &str = "run_terminal";
    let store = factory.fresh().await;
    let graph = fixtures::linear();
    let run = start_run(CASE, &store, &graph, "run-1", T0).await;
    let a = claim_one(CASE, &store, "w1", T0, &graph, &task_id(&run, "a", 0)).await;
    let stale = plan_done(CASE, &store, &graph, &a.task.task_id, "ok", T0).await;
    let cancel = ok(
        CASE,
        plan_cancel(
            &graph,
            &snapshot(CASE, &store, &run).await,
            "stop".into(),
            T0.saturating_add(1),
        ),
    );
    assert_eq!(
        ok(CASE, store.apply(cancel, None).await).run_version,
        2,
        "{CASE}: cancel version"
    );
    assert_eq!(
        snapshot(CASE, &store, &run).await.status,
        RunStatus::Cancelled,
        "{CASE}: terminal status"
    );
    assert_eq!(
        task(CASE, &store, &a.task.task_id).await,
        a.task,
        "{CASE}: running task changed"
    );
    let ev = events(CASE, &store, &run).await;
    assert_eq!(
        store.apply(stale, Some(a.proof)).await,
        Err(StoreError::RunTerminal {
            run_id: run.clone()
        }),
        "{CASE}: terminal must precede version"
    );
    assert_eq!(
        events(CASE, &store, &run).await,
        ev,
        "{CASE}: rejected terminal events"
    );
    assert_eq!(
        kinds(&ev).last(),
        Some(&"RunCancelled"),
        "{CASE}: cancel event"
    );
}
/// Prove transactional rule 5.
/// # Panics
/// Panics with the case name when the backend violates the contract.
pub async fn case_insert_or_ignore<F: StoreFactory>(factory: &F) {
    const CASE: &str = "insert_or_ignore";
    let store = factory.fresh().await;
    let graph = fixtures::linear();
    let run = start_run(CASE, &store, &graph, "run-1", T0).await;
    let start = ok(
        CASE,
        dmt_core::plan_start(&graph, run.clone(), Value::Null, T0),
    );
    let a = claim_one(CASE, &store, "w1", T0, &graph, &task_id(&run, "a", 0)).await;
    let mut commit = plan_done(CASE, &store, &graph, &a.task.task_id, "ok", T0).await;
    super::validation::task_identity(CASE, &store, &commit, &a.proof).await;
    let child = at(CASE, &commit.new_tasks, 0).clone();
    let mut duplicate = child.clone();
    duplicate.input = json!("second copy must not overwrite");
    commit.new_tasks.push(duplicate);
    commit.new_tasks.extend(start.new_tasks);
    commit.events.push(start.events[1].clone());
    let result = ok(CASE, store.apply(commit, Some(a.proof)).await);
    assert_eq!(
        result.ignored_tasks,
        vec![child.step_key, a.task.step_key],
        "{CASE}: ignored keys"
    );
    assert_eq!(
        result.inserted_tasks,
        vec![task_id(&run, "b", 0)],
        "{CASE}: inserted tasks"
    );
    assert_eq!(result.run_version, 2, "{CASE}: version");
    assert_eq!(
        task(CASE, &store, &child.task_id).await.input,
        child.input,
        "{CASE}: first insert must win"
    );
    let a = task(CASE, &store, &a.task.task_id).await;
    assert_eq!(
        a.status,
        TaskStatus::Completed,
        "{CASE}: ignored insert overwrote row"
    );
    assert!(
        a.lease_owner.is_none() && a.lease_until.is_none(),
        "{CASE}: lease not cleared"
    );
    assert_eq!(
        task(CASE, &store, &task_id(&run, "b", 0)).await.status,
        TaskStatus::Ready,
        "{CASE}: child status"
    );
    assert_eq!(
        count(&events(CASE, &store, &run).await, "TaskScheduled"),
        3,
        "{CASE}: events must be appended verbatim"
    );
}
/// Prove transactional rule 6.
/// # Panics
/// Panics with the case name when the backend violates the contract.
pub async fn case_join_guard<F: StoreFactory>(factory: &F) {
    join_all(factory).await;
    join_late(factory).await;
    super::validation::failed_join(factory).await;
    super::validation::duplicate_join(factory).await;
}
async fn join_all<F: StoreFactory>(factory: &F) {
    const CASE: &str = "join_guard";
    let store = factory.fresh().await;
    let graph = fixtures::fan_out(JoinPolicy::All);
    let run = branches(CASE, &store, &graph, "run-1", 2).await;
    let claimed = claim_all(CASE, &store, "w1", T0.saturating_add(1), &graph).await;
    assert_eq!(claimed.len(), 2, "{CASE}: branches");
    let s = snapshot(CASE, &store, &run).await;
    let c0 = ok(
        CASE,
        plan_outcome(&graph, &s, &claimed[0].task.task_id, done(), T0),
    );
    let c1 = ok(
        CASE,
        plan_outcome(&graph, &s, &claimed[1].task.task_id, done(), T0),
    );
    let mut drift = c0.clone();
    required(CASE, drift.join_contribution.as_mut()).expected_received = 2;
    assert!(
        matches!(
            store.apply(drift, Some(claimed[0].proof.clone())).await,
            Err(StoreError::JoinDrift { .. })
        ),
        "{CASE}: counter drift accepted"
    );
    assert_eq!(
        snapshot(CASE, &store, &run).await,
        s,
        "{CASE}: drift wrote state"
    );
    assert!(
        ok(CASE, store.apply(c0, Some(claimed[0].proof.clone())).await)
            .join_satisfied
            .is_none(),
        "{CASE}: premature satisfaction"
    );
    let s1 = snapshot(CASE, &store, &run).await;
    assert_eq!(
        (
            at(CASE, &s1.joins, 0).received,
            at(CASE, &s1.joins, 0).results.len(),
            at(CASE, &s1.joins, 0).satisfied_at
        ),
        (1, 1, None),
        "{CASE}: first counters"
    );
    assert_eq!(
        store.apply(c1, Some(claimed[1].proof.clone())).await,
        Err(StoreError::VersionConflict {
            expected: 3,
            actual: 4
        }),
        "{CASE}: stale join version"
    );
    let c = plan_done(CASE, &store, &graph, &claimed[1].task.task_id, "ok", T0).await;
    assert!(
        c.join_contribution
            .as_ref()
            .is_some_and(|c| c.expected_received == 2 && c.satisfied),
        "{CASE}: replanned counters"
    );
    assert_eq!(
        ok(CASE, store.apply(c, Some(claimed[1].proof.clone())).await).join_satisfied,
        Some(at(CASE, &s.joins, 0).join_id.clone()),
        "{CASE}: satisfaction result"
    );
    let s2 = snapshot(CASE, &store, &run).await;
    assert_eq!(
        (
            at(CASE, &s2.joins, 0).received,
            at(CASE, &s2.joins, 0).satisfied_at
        ),
        (2, Some(T0)),
        "{CASE}: satisfaction timestamp"
    );
    assert_eq!(
        task(CASE, &store, &task_id(&run, "jn", 0)).await.status,
        TaskStatus::Ready,
        "{CASE}: join task"
    );
    let ev = events(CASE, &store, &run).await;
    assert_eq!(
        count(&ev, "JoinSatisfied"),
        1,
        "{CASE}: duplicate satisfaction"
    );
    assert_contiguous(CASE, &ev);
}
async fn join_late<F: StoreFactory>(factory: &F) {
    const CASE: &str = "join_guard";
    let store = factory.fresh().await;
    let graph = fixtures::fan_out(JoinPolicy::Quorum(1));
    let run = branches(CASE, &store, &graph, "run-2", 2).await;
    let claimed = claim_all(CASE, &store, "w1", T0, &graph).await;
    assert_eq!(claimed.len(), 2, "{CASE}: branches");
    ok(
        CASE,
        complete(CASE, &store, &graph, &claimed[0], done(), T0).await,
    );
    let late = plan_done(CASE, &store, &graph, &claimed[1].task.task_id, "ok", T0).await;
    assert!(
        late.join_contribution.is_none(),
        "{CASE}: late planner contribution"
    );
    let before = snapshot(CASE, &store, &run).await;
    let mut forced = late.clone();
    forced.join_contribution = Some(JoinContribution {
        join_id: at(CASE, &before.joins, 0).join_id.clone(),
        result: BranchResult::Done {
            index: 1,
            outcome: Outcome::with_payload("ok", json!(1)),
        },
        expected_received: 2,
        expected_failed: 0,
        satisfied: false,
    });
    assert!(
        matches!(
            store.apply(forced, Some(claimed[1].proof.clone())).await,
            Err(StoreError::JoinDrift { .. })
        ),
        "{CASE}: satisfied join accepted contribution"
    );
    assert_eq!(
        snapshot(CASE, &store, &run).await,
        before,
        "{CASE}: forced contribution wrote"
    );
    ok(
        CASE,
        store.apply(late, Some(claimed[1].proof.clone())).await,
    );
    let after = snapshot(CASE, &store, &run).await;
    assert_eq!(
        (
            at(CASE, &after.joins, 0).received,
            at(CASE, &after.joins, 0).results.len()
        ),
        (1, 1),
        "{CASE}: late counters"
    );
    let ev = events(CASE, &store, &run).await;
    assert_eq!(count(&ev, "BranchContributed"), 2, "{CASE}: late event");
    assert_eq!(
        count(&ev, "JoinSatisfied"),
        1,
        "{CASE}: repeated satisfaction"
    );
    task(CASE, &store, &task_id(&run, "jn", 0)).await;
    assert!(
        ok(CASE, store.load_task(&task_id(&run, "jn", 1)).await).is_none(),
        "{CASE}: duplicate join task"
    );
}
/// Prove transactional rule 10.
/// # Panics
/// Panics with the case name when the backend violates the contract.
pub async fn case_signal_resolution<F: StoreFactory>(factory: &F) {
    const CASE: &str = "signal_resolution";
    super::validation::duplicate_signals(factory).await;
    let store = factory.fresh().await;
    let graph = fixtures::loop_via_wait();
    let run = start_run(CASE, &store, &graph, "run-1", T0).await;
    let a = claim_one(CASE, &store, "w1", T0, &graph, &task_id(&run, "plan", 0)).await;
    ok(
        CASE,
        complete(CASE, &store, &graph, &a, done(), T0.saturating_add(1)).await,
    );
    let before = snapshot(CASE, &store, &run).await;
    assert_eq!(before.status, RunStatus::Parked, "{CASE}: parked");
    assert_eq!(
        task(CASE, &store, &task_id(&run, "signoff", 0))
            .await
            .status,
        TaskStatus::Awaiting,
        "{CASE}: awaiting task"
    );
    let signal = required(
        CASE,
        ok(CASE, store.find_open_signal(&run, "signoff").await),
    );
    assert_eq!(
        signal.key,
        dmt_core::SignalKey::new("signoff", 0),
        "{CASE}: key"
    );
    let resolve = ok(
        CASE,
        plan_signal(
            &graph,
            &before,
            &signal.signal_id,
            SignalPayload {
                label: "approved".into(),
                payload: json!(true),
            },
            T0.saturating_add(2),
        ),
    );
    ok(CASE, store.apply(resolve, None).await);
    assert!(
        ok(CASE, store.find_open_signal(&run, "signoff").await).is_none(),
        "{CASE}: still open"
    );
    assert_resolved_wait(CASE, &store, &run, &signal.task_id).await;
    let a = claim_one(
        CASE,
        &store,
        "w1",
        T0.saturating_add(2),
        &graph,
        &task_id(&run, "implement", 0),
    )
    .await;
    let mut bad = plan_done(
        CASE,
        &store,
        &graph,
        &a.task.task_id,
        "ok",
        T0.saturating_add(2),
    )
    .await;
    bad.resolved_signals.push(SignalResolution {
        signal_id: signal.signal_id.clone(),
        label: "x".into(),
        payload: Value::Null,
    });
    let before = snapshot(CASE, &store, &run).await;
    assert_eq!(
        store.apply(bad, Some(a.proof.clone())).await,
        Err(StoreError::AlreadyResolved {
            signal_id: signal.signal_id
        }),
        "{CASE}: double resolution"
    );
    assert_eq!(
        snapshot(CASE, &store, &run).await,
        before,
        "{CASE}: resolution partial writes"
    );
    ok(
        CASE,
        complete(CASE, &store, &graph, &a, done(), T0.saturating_add(2)).await,
    );
    assert_eq!(
        snapshot(CASE, &store, &run).await.status,
        RunStatus::Completed,
        "{CASE}: terminal"
    );
    let ev = events(CASE, &store, &run).await;
    assert!(
        kinds(&ev).contains(&"RunResumed") && kinds(&ev).contains(&"SignalReceived"),
        "{CASE}: signal events"
    );
    assert_contiguous(CASE, &ev);
}

async fn assert_resolved_wait<S: Store>(
    case: &str,
    store: &S,
    run: &dmt_core::RunId,
    id: &dmt_core::TaskId,
) {
    let s = snapshot(case, store, run).await;
    assert!(s.signals.is_empty(), "{case}: resolved signal in snapshot");
    assert_eq!(s.status, RunStatus::Active, "{case}: resumed");
    assert_eq!(
        task(case, store, id).await.status,
        TaskStatus::Completed,
        "{case}: wait completed"
    );
}
