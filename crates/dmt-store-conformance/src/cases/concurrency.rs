use crate::harness::at;
use crate::{
    StoreFactory,
    harness::{
        T0, assert_contiguous, branch_id, branches, claim_all, claim_one, claim_request, complete,
        count, done, events, ok, retry_busy, snapshot, task, task_id, worker,
    },
};
use dmt_core::{JoinPolicy, RunStatus, TaskStatus, fixtures, plan_outcome};
use dmt_store::{Store, StoreError};
use std::{collections::BTreeSet, sync::Arc};
use tokio::sync::Barrier;

/// Prove concurrent claims.
/// # Panics
/// Panics with the case name when the backend violates the contract.
pub async fn case_concurrent_claims<F: StoreFactory>(factory: &F) {
    const CASE: &str = "concurrent_claims";
    let store = factory.fresh().await;
    let graph = fixtures::fan_out(JoinPolicy::All);
    let run = branches(CASE, &store, &graph, "run-1", 16).await;
    // Count only the contending branch claims, after the two setup claims.
    let before = events(CASE, &store, &run).await;
    let store = Arc::new(store);
    let barrier = Arc::new(Barrier::new(4));
    let mut handles = Vec::new();
    for i in 0..4 {
        let store = store.clone();
        let barrier = barrier.clone();
        let graph = graph.clone();
        handles.push(tokio::spawn(async move {
            let name = format!("w{i}");
            let req = claim_request(&name, T0.saturating_add(1), &[&graph], 8);
            barrier.wait().await;
            let claimed = ok(CASE, retry_busy(|| store.claim_ready(req.clone())).await);
            (worker(&name), claimed)
        }));
    }
    let mut seen = BTreeSet::new();
    let mut total = 0;
    for handle in handles {
        let (owner, claimed) = ok(CASE, handle.await);
        total += claimed.len();
        for c in claimed {
            assert!(
                seen.insert(c.task.task_id.clone()),
                "{CASE}: overlapping claim {}",
                c.task.task_id
            );
            assert_eq!(
                task(CASE, store.as_ref(), &c.task.task_id)
                    .await
                    .lease_owner,
                Some(owner.clone()),
                "{CASE}: claim owner"
            );
        }
    }
    let expected = (0..16).map(|i| branch_id(&run, "br", 0, i)).collect();
    assert_eq!(seen, expected, "{CASE}: incomplete union");
    assert_eq!(total, 16, "{CASE}: total claims");
    let ev = events(CASE, store.as_ref(), &run).await;
    assert_eq!(
        count(&ev, "TaskClaimed") - count(&before, "TaskClaimed"),
        16,
        "{CASE}: branch claim events"
    );
    assert_contiguous(CASE, &ev);
}
/// Prove concurrent join.
/// # Panics
/// Panics with the case name when the backend violates the contract.
pub async fn case_concurrent_join<F: StoreFactory>(factory: &F) {
    const CASE: &str = "concurrent_join";
    let store = factory.fresh().await;
    let graph = fixtures::fan_out(JoinPolicy::All);
    let run = branches(CASE, &store, &graph, "run-1", 3).await;
    let claimed = claim_all(CASE, &store, "w1", T0, &graph).await;
    assert_eq!(claimed.len(), 3, "{CASE}: branch setup");
    let store = Arc::new(store);
    let barrier = Arc::new(Barrier::new(3));
    let mut handles = Vec::new();
    for a in claimed {
        let store = store.clone();
        let barrier = barrier.clone();
        let graph = graph.clone();
        let run = run.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            for _ in 0..100 {
                let s = snapshot(CASE, store.as_ref(), &run).await;
                let c = ok(CASE, plan_outcome(&graph, &s, &a.task.task_id, done(), T0));
                match store.apply(c, Some(a.proof.clone())).await {
                    Ok(_) => return,
                    Err(
                        StoreError::VersionConflict { .. }
                        | StoreError::JoinDrift { .. }
                        | StoreError::Busy,
                    ) => tokio::task::yield_now().await,
                    Err(e) => panic!("{CASE}: unexpected contention error {e:?}"),
                }
            }
            panic!("{CASE}: contention retries exhausted");
        }));
    }
    for handle in handles {
        ok(CASE, handle.await);
    }
    assert_eq!(
        task(CASE, store.as_ref(), &task_id(&run, "jn", 0))
            .await
            .status,
        TaskStatus::Ready,
        "{CASE}: join task"
    );
    assert!(
        ok(CASE, store.load_task(&task_id(&run, "jn", 1)).await).is_none(),
        "{CASE}: duplicate join task"
    );
    let s = snapshot(CASE, store.as_ref(), &run).await;
    assert_eq!(
        (
            at(CASE, &s.joins, 0).received,
            at(CASE, &s.joins, 0).results.len()
        ),
        (3, 3),
        "{CASE}: join counters"
    );
    assert!(
        at(CASE, &s.joins, 0).satisfied_at.is_some(),
        "{CASE}: join unsatisfied"
    );
    let ev = events(CASE, store.as_ref(), &run).await;
    for (kind, n) in [
        ("JoinSatisfied", 1),
        ("BranchContributed", 3),
        ("TaskCompleted", 5),
    ] {
        assert_eq!(count(&ev, kind), n, "{CASE}: count {kind}");
    }
    assert_contiguous(CASE, &ev);
    let jn = claim_one(
        CASE,
        store.as_ref(),
        "w1",
        T0,
        &graph,
        &task_id(&run, "jn", 0),
    )
    .await;
    ok(
        CASE,
        complete(CASE, store.as_ref(), &graph, &jn, done(), T0).await,
    );
    assert_eq!(
        snapshot(CASE, store.as_ref(), &run).await.status,
        RunStatus::Completed,
        "{CASE}: join completion"
    );
}
