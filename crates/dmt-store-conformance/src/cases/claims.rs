use crate::{
    StoreFactory,
    harness::{
        LEASE, T0, assert_contiguous, branch_id, branches, claim_all, claim_one, claim_request,
        complete, count, done, events, kinds, ok, plan_done, run, snapshot, start_run, task,
        task_id, worker,
    },
};
use dmt_core::{
    ExhaustReason, JoinPolicy, NodeOutcome, RunEvent, RunStatus, TaskStatus, fixtures, plan_cancel,
    plan_exhausted,
};
use dmt_store::{Clock, HeartbeatResult, LeaseProof, ManualClock, Store, StoreError};
use serde_json::json;

/// Prove transactional rule 7.
/// # Panics
/// Panics with the case name when the backend violates the contract.
pub async fn case_claim_ready<F: StoreFactory>(factory: &F) {
    const CASE: &str = "claim_ready";
    let store = factory.fresh().await;
    let graph = fixtures::linear();
    let retry = fixtures::retry_chain(3);
    for (name, offset) in [("run-1", 2), ("run-2", 1), ("run-3", 1)] {
        start_run(CASE, &store, &graph, name, T0.saturating_add(offset)).await;
    }
    start_run(CASE, &store, &retry, "run-4", T0).await;
    for req in [
        claim_request("w1", T0.saturating_add(10), &[], 8),
        claim_request("w1", T0.saturating_add(10), &[&graph], 0),
    ] {
        assert!(
            ok(CASE, store.claim_ready(req).await).is_empty(),
            "{CASE}: empty filter/limit wrote"
        );
    }
    for name in ["run-1", "run-2", "run-3", "run-4"] {
        assert_eq!(
            task(CASE, &store, &task_id(&run(name), "a", 0))
                .await
                .status,
            TaskStatus::Ready,
            "{CASE}: no-op changed task"
        );
        assert_eq!(
            count(&events(CASE, &store, &run(name)).await, "TaskClaimed"),
            0,
            "{CASE}: no-op event"
        );
    }
    let claimed = ok(
        CASE,
        store
            .claim_ready(claim_request("w1", T0.saturating_add(1), &[&graph], 8))
            .await,
    );
    assert_eq!(
        claimed
            .iter()
            .map(|c| c.task.task_id.clone())
            .collect::<Vec<_>>(),
        vec!["run-2/a/0".into(), "run-3/a/0".into()],
        "{CASE}: candidate ordering/filter"
    );
    assert_claim_metadata(CASE, &store, &graph, &claimed).await;
    for expected in ["run-4/a/0", "run-1/a/0"] {
        let got = ok(
            CASE,
            store
                .claim_ready(claim_request(
                    "w1",
                    T0.saturating_add(2),
                    &[&graph, &retry],
                    1,
                ))
                .await,
        );
        assert_eq!(got.len(), 1, "{CASE}: limit");
        assert_eq!(
            got[0].task.task_id.as_str(),
            expected,
            "{CASE}: cross-graph ordering"
        );
    }
    assert!(
        claim_all(CASE, &store, "w1", T0.saturating_add(2), &graph)
            .await
            .is_empty(),
        "{CASE}: active lease claimed"
    );
    assert!(
        claim_all(CASE, &store, "w1", T0.saturating_add(1 + LEASE), &graph)
            .await
            .is_empty(),
        "{CASE}: expiry must be strict"
    );
    let reclaimed = claim_all(CASE, &store, "w1", T0.saturating_add(2 + LEASE), &graph).await;
    assert_eq!(
        reclaimed
            .iter()
            .map(|c| (c.task.task_id.as_str(), c.task.attempt))
            .collect::<Vec<_>>(),
        [("run-2/a/0", 2), ("run-3/a/0", 2)],
        "{CASE}: boundary reclaims"
    );
}
/// Prove transactional rule 8.
/// # Panics
/// Panics with the case name when the backend violates the contract.
pub async fn case_heartbeat<F: StoreFactory>(factory: &F) {
    const CASE: &str = "heartbeat";
    let store = factory.fresh().await;
    let graph = fixtures::linear();
    let run = start_run(CASE, &store, &graph, "run-1", T0).await;
    let a = claim_one(CASE, &store, "w1", T0, &graph, &task_id(&run, "a", 0)).await;
    let ev = events(CASE, &store, &run).await;
    let until = T0.saturating_add(5 + LEASE);
    assert_eq!(
        ok(
            CASE,
            store.heartbeat(&a.proof, T0.saturating_add(5), LEASE).await
        ),
        HeartbeatResult::Extended { lease_until: until },
        "{CASE}: extension"
    );
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
            ok(
                CASE,
                store.heartbeat(&proof, T0.saturating_add(10), LEASE).await
            ),
            HeartbeatResult::Lost,
            "{CASE}: invalid heartbeat proof"
        );
    }
    assert_eq!(
        task(CASE, &store, &a.task.task_id).await.lease_until,
        Some(until),
        "{CASE}: rejected heartbeat changed deadline"
    );
    assert!(
        matches!(
            store
                .heartbeat(
                    &LeaseProof {
                        task_id: task_id(&run, "b", 0),
                        ..a.proof.clone()
                    },
                    T0,
                    LEASE
                )
                .await,
            Err(StoreError::NotFound(_))
        ),
        "{CASE}: missing task"
    );
    assert_eq!(
        events(CASE, &store, &run).await,
        ev,
        "{CASE}: heartbeat appended events"
    );
    assert_eq!(
        snapshot(CASE, &store, &run).await.version,
        1,
        "{CASE}: heartbeat changed version"
    );
    ok(CASE, complete(CASE, &store, &graph, &a, done(), T0).await);
    let ev = events(CASE, &store, &run).await;
    assert_eq!(
        ok(CASE, store.heartbeat(&a.proof, T0, LEASE).await),
        HeartbeatResult::Lost,
        "{CASE}: completed heartbeat"
    );
    assert_eq!(
        events(CASE, &store, &run).await,
        ev,
        "{CASE}: lost heartbeat event"
    );
}
/// Prove crash between claim and apply.
/// # Panics
/// Panics with the case name when the backend violates the contract.
pub async fn case_crash_between_claim_and_apply<F: StoreFactory>(factory: &F) {
    const CASE: &str = "crash_between_claim_and_apply";
    let store = factory.fresh().await;
    let graph = fixtures::linear();
    let clock = ManualClock::new(T0);
    let run = start_run(CASE, &store, &graph, "run-1", clock.now()).await;
    let id = task_id(&run, "a", 0);
    let a = claim_one(CASE, &store, "w1", clock.now(), &graph, &id).await;
    assert_eq!(a.task.attempt, 1, "{CASE}: first attempt");
    let _ = clock.advance(LEASE);
    assert!(
        claim_all(CASE, &store, "w1", clock.now(), &graph)
            .await
            .is_empty(),
        "{CASE}: boundary reclaimed"
    );
    let _ = clock.advance(1);
    let a = claim_one(CASE, &store, "w1", clock.now(), &graph, &id).await;
    assert_eq!(a.task.attempt, 2, "{CASE}: reclaim attempt");
    assert_eq!(a.proof.attempt, 2, "{CASE}: proof attempt");
    assert_eq!(
        a.task.lease_until,
        Some(clock.now().saturating_add(LEASE)),
        "{CASE}: reclaim deadline"
    );
    assert_eq!(
        a.task.lease_owner,
        Some(worker("w1")),
        "{CASE}: reclaim owner"
    );
    let ev = events(CASE, &store, &run).await;
    assert_eq!(
        ev.iter()
            .filter_map(|e| if let RunEvent::TaskClaimed { attempt, .. } = e.event {
                Some(attempt)
            } else {
                None
            })
            .collect::<Vec<_>>(),
        [1, 2],
        "{CASE}: claim attempts"
    );
    assert_contiguous(CASE, &ev);
    ok(
        CASE,
        complete(CASE, &store, &graph, &a, done(), clock.now()).await,
    );
    assert!(
        events(CASE, &store, &run)
            .await
            .iter()
            .any(|e| matches!(e.event, RunEvent::TaskCompleted { attempt: 2, .. })),
        "{CASE}: completion attempt"
    );
}
/// Prove stale dispatch same worker.
/// # Panics
/// Panics with the case name when the backend violates the contract.
pub async fn case_stale_dispatch_same_worker<F: StoreFactory>(factory: &F) {
    const CASE: &str = "stale_dispatch_same_worker";
    let store = factory.fresh().await;
    let graph = fixtures::linear();
    let run = start_run(CASE, &store, &graph, "run-1", T0).await;
    let id = task_id(&run, "a", 0);
    let old = claim_one(CASE, &store, "w1", T0, &graph, &id).await;
    let stale = plan_done(CASE, &store, &graph, &id, "ok", T0).await;
    let now = T0.saturating_add(LEASE + 1);
    let a = claim_one(CASE, &store, "w1", now, &graph, &id).await;
    assert_eq!(a.task.attempt, 2, "{CASE}: reclaim");
    assert!(
        matches!(
            store.apply(stale, Some(old.proof.clone())).await,
            Err(StoreError::LeaseLost { .. })
        ),
        "{CASE}: same worker stale outcome"
    );
    assert_eq!(
        ok(CASE, store.heartbeat(&old.proof, now, LEASE).await),
        HeartbeatResult::Lost,
        "{CASE}: stale heartbeat"
    );
    assert!(
        matches!(
            ok(CASE, store.heartbeat(&a.proof, now, LEASE).await),
            HeartbeatResult::Extended { .. }
        ),
        "{CASE}: fresh heartbeat"
    );
    ok(CASE, complete(CASE, &store, &graph, &a, done(), now).await);
    let ev = events(CASE, &store, &run).await;
    assert_eq!(
        count(&ev, "TaskCompleted"),
        1,
        "{CASE}: duplicated completion"
    );
    assert!(
        ev.iter()
            .any(|e| matches!(e.event, RunEvent::TaskCompleted { attempt: 2, .. })),
        "{CASE}: completion attempt"
    );
}
/// Prove cancel skips claimed.
/// # Panics
/// Panics with the case name when the backend violates the contract.
pub async fn case_cancel_skips_claimed<F: StoreFactory>(factory: &F) {
    const CASE: &str = "cancel_skips_claimed";
    let store = factory.fresh().await;
    let graph = fixtures::fan_out(JoinPolicy::All);
    let run = branches(CASE, &store, &graph, "run-1", 2).await;
    let cancel = ok(
        CASE,
        plan_cancel(
            &graph,
            &snapshot(CASE, &store, &run).await,
            "stop".into(),
            T0.saturating_add(1),
        ),
    );
    let a = claim_one(CASE, &store, "w1", T0, &graph, &branch_id(&run, "br", 0, 0)).await;
    assert_eq!(
        snapshot(CASE, &store, &run).await.version,
        3,
        "{CASE}: claim version"
    );
    ok(CASE, store.apply(cancel, None).await);
    assert_eq!(
        snapshot(CASE, &store, &run).await.status,
        RunStatus::Cancelled,
        "{CASE}: cancellation"
    );
    assert_eq!(
        task(CASE, &store, &a.task.task_id).await,
        a.task,
        "{CASE}: cancelled concurrent claim"
    );
    assert_eq!(
        task(CASE, &store, &branch_id(&run, "br", 0, 1))
            .await
            .status,
        TaskStatus::Cancelled,
        "{CASE}: ready peer not cancelled"
    );
    terminal_cleanup(
        CASE,
        &store,
        &graph,
        &run,
        &a.task.task_id,
        T0.saturating_add(LEASE + 1),
        "RunCancelled",
    )
    .await;
}
async fn terminal_cleanup<S: Store>(
    case: &str,
    store: &S,
    graph: &dmt_core::Graph,
    run: &dmt_core::RunId,
    id: &dmt_core::TaskId,
    now: dmt_core::Micros,
    last: &str,
) {
    assert!(
        claim_all(case, store, "w1", now, graph).await.is_empty(),
        "{case}: terminal run claimed"
    );
    let t = task(case, store, id).await;
    assert_eq!(
        t.status,
        TaskStatus::Cancelled,
        "{case}: terminal candidate not cancelled"
    );
    assert!(
        t.lease_owner.is_none() && t.lease_until.is_none(),
        "{case}: terminal lease retained"
    );
    assert_eq!(t.attempt, 1, "{case}: terminal reclaim incremented attempt");
    assert_eq!(
        kinds(&events(case, store, run).await).last().copied(),
        Some(last),
        "{case}: terminal cleanup appended event"
    );
}
/// Prove no claim after terminal.
/// # Panics
/// Panics with the case name when the backend violates the contract.
pub async fn case_no_claim_after_terminal<F: StoreFactory>(factory: &F) {
    no_claim_cancel(factory).await;
    no_claim_completed(factory).await;
}
async fn no_claim_cancel<F: StoreFactory>(factory: &F) {
    const CASE: &str = "no_claim_after_terminal";
    let store = factory.fresh().await;
    let graph = fixtures::fan_out(JoinPolicy::All);
    let run = branches(CASE, &store, &graph, "run-1", 2).await;
    let a = claim_one(CASE, &store, "w1", T0, &graph, &branch_id(&run, "br", 0, 0)).await;
    let cancel = ok(
        CASE,
        plan_cancel(
            &graph,
            &snapshot(CASE, &store, &run).await,
            "stop".into(),
            T0.saturating_add(1),
        ),
    );
    ok(CASE, store.apply(cancel, None).await);
    assert_eq!(
        task(CASE, &store, &a.task.task_id).await,
        a.task,
        "{CASE}: running row cancelled early"
    );
    assert_eq!(
        task(CASE, &store, &branch_id(&run, "br", 0, 1))
            .await
            .status,
        TaskStatus::Cancelled,
        "{CASE}: peer cancel"
    );
    terminal_cleanup(
        CASE,
        &store,
        &graph,
        &run,
        &a.task.task_id,
        T0.saturating_add(LEASE + 1),
        "RunCancelled",
    )
    .await;
    assert!(
        ok(CASE, store.exhausted_tasks(10).await).is_empty(),
        "{CASE}: terminal sweep"
    );
    assert!(
        ok(CASE, store.due_signals(T0.saturating_add(1000), 10).await).is_empty(),
        "{CASE}: terminal timeout"
    );
}
async fn no_claim_completed<F: StoreFactory>(factory: &F) {
    const CASE: &str = "no_claim_after_terminal";
    let store = factory.fresh().await;
    let graph = fixtures::fan_out(JoinPolicy::Quorum(1));
    let run = start_run(CASE, &store, &graph, "run-2", T0).await;
    let a = claim_one(CASE, &store, "w1", T0, &graph, &task_id(&run, "a", 0)).await;
    ok(CASE, complete(CASE, &store, &graph, &a, done(), T0).await);
    let fo = claim_one(CASE, &store, "w1", T0, &graph, &task_id(&run, "fo", 0)).await;
    ok(
        CASE,
        complete(
            CASE,
            &store,
            &graph,
            &fo,
            NodeOutcome::FanOut(vec![json!(1), json!(2)]),
            T0.saturating_add(10),
        )
        .await,
    );
    let br = claim_one(
        CASE,
        &store,
        "w1",
        T0.saturating_add(10),
        &graph,
        &branch_id(&run, "br", 0, 0),
    )
    .await;
    ok(
        CASE,
        complete(CASE, &store, &graph, &br, done(), T0.saturating_add(1)).await,
    );
    let jn = claim_one(
        CASE,
        &store,
        "w1",
        T0.saturating_add(1),
        &graph,
        &task_id(&run, "jn", 0),
    )
    .await;
    ok(
        CASE,
        complete(CASE, &store, &graph, &jn, done(), T0.saturating_add(1)).await,
    );
    let id = branch_id(&run, "br", 0, 1);
    assert_eq!(
        snapshot(CASE, &store, &run).await.status,
        RunStatus::Completed,
        "{CASE}: completion"
    );
    assert_eq!(
        task(CASE, &store, &id).await.status,
        TaskStatus::Ready,
        "{CASE}: straggler not ready"
    );
    terminal_cleanup(
        CASE,
        &store,
        &graph,
        &run,
        &id,
        T0.saturating_add(20),
        "RunCompleted",
    )
    .await;
}
/// Prove reclaim exhausts.
/// # Panics
/// Panics with the case name when the backend violates the contract.
pub async fn case_reclaim_exhausts<F: StoreFactory>(factory: &F) {
    const CASE: &str = "reclaim_exhausts";
    let store = factory.fresh().await;
    let graph = fixtures::retry_chain(2);
    let run = start_run(CASE, &store, &graph, "run-1", T0).await;
    let id = task_id(&run, "a", 0);
    let first = claim_one(CASE, &store, "w1", T0, &graph, &id).await;
    let stale = plan_done(CASE, &store, &graph, &id, "ok", T0).await;
    let second = claim_one(
        CASE,
        &store,
        "w1",
        T0.saturating_add(LEASE + 1),
        &graph,
        &id,
    )
    .await;
    assert_eq!(second.task.attempt, 2, "{CASE}: reclaim attempt");
    assert!(
        claim_all(CASE, &store, "w1", T0.saturating_add(2 * LEASE + 2), &graph)
            .await
            .is_empty(),
        "{CASE}: overclaimed budget"
    );
    let t = task(CASE, &store, &id).await;
    assert_eq!(
        (t.status, t.attempt, t.planned_at),
        (TaskStatus::Exhausted, 2, None),
        "{CASE}: exhausted state"
    );
    assert!(
        t.lease_owner.is_none() && t.lease_until.is_none(),
        "{CASE}: exhausted lease"
    );
    let ev = events(CASE, &store, &run).await;
    assert_eq!(count(&ev, "TaskClaimed"), 2, "{CASE}: budget claims");
    assert!(
        matches!(
            ev.last().map(|e| &e.event),
            Some(RunEvent::TaskExhausted {
                reason: ExhaustReason::LeaseReclaimsExceeded,
                ..
            })
        ),
        "{CASE}: exhaustion event"
    );
    assert_contiguous(CASE, &ev);
    assert_eq!(
        ok(CASE, store.exhausted_tasks(10).await),
        vec![t],
        "{CASE}: pending sweep"
    );
    assert_eq!(
        ok(CASE, store.heartbeat(&second.proof, T0, LEASE).await),
        HeartbeatResult::Lost,
        "{CASE}: exhausted heartbeat"
    );
    assert!(
        matches!(
            store.apply(stale, Some(first.proof)).await,
            Err(StoreError::LeaseLost { .. })
        ),
        "{CASE}: exhausted outcome"
    );
    let c = ok(
        CASE,
        plan_exhausted(
            &graph,
            &snapshot(CASE, &store, &run).await,
            &id,
            T0.saturating_add(3 * LEASE),
        ),
    );
    ok(CASE, store.apply(c, None).await);
    assert_eq!(
        snapshot(CASE, &store, &run).await.status,
        RunStatus::Failed,
        "{CASE}: exhausted route"
    );
    assert!(
        ok(CASE, store.exhausted_tasks(10).await).is_empty(),
        "{CASE}: repeat sweep"
    );
    assert_eq!(
        count(&events(CASE, &store, &run).await, "TaskExhausted"),
        1,
        "{CASE}: duplicated exhaustion"
    );
}

async fn assert_claim_metadata<S: Store>(
    case: &str,
    store: &S,
    graph: &dmt_core::Graph,
    claimed: &[dmt_store::ClaimedTask],
) {
    for c in claimed {
        assert_eq!(c.task.status, TaskStatus::Running, "{case}: claim status");
        assert_eq!(c.task.lease_owner, Some(worker("w1")), "{case}: owner");
        assert_eq!(
            c.task.lease_until,
            Some(T0.saturating_add(1 + LEASE)),
            "{case}: deadline"
        );
        assert_eq!(c.task.attempt, 1, "{case}: initial attempt");
        assert_eq!(
            c.proof,
            LeaseProof {
                task_id: c.task.task_id.clone(),
                worker_id: worker("w1"),
                attempt: 1
            },
            "{case}: proof"
        );
        assert_eq!(
            (&c.graph_id, c.graph_version),
            (graph.id(), 1),
            "{case}: graph metadata"
        );
        assert_eq!(
            snapshot(case, store, &c.task.run_id).await.version,
            1,
            "{case}: claim bumped version"
        );
        assert!(
            matches!(events(case, store, &c.task.run_id).await.last().map(|e| &e.event), Some(RunEvent::TaskClaimed { worker_id, attempt: 1, lease_until, .. }) if worker_id == &worker("w1") && *lease_until == T0.saturating_add(1 + LEASE)),
            "{case}: claim event fields"
        );
    }
}
