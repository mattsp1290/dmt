use crate::{
    StoreFactory,
    harness::{
        LEASE, T0, claim_one, complete, done, exhaust, exhaust_then_continue, ok, snapshot, task,
        task_id,
    },
};
use dmt_core::{PlanError, RunStatus, TaskStatus, plan_cancel, plan_exhausted};
use dmt_store::{Store, StoreError};

/// Prove transactional rule 15.
/// # Panics
/// Panics with the case name when the backend violates the contract.
pub async fn case_exhausted_sweep_idempotent<F: StoreFactory>(factory: &F) {
    const CASE: &str = "exhausted_sweep_idempotent";
    let store = factory.fresh().await;
    let graph = exhaust_then_continue();
    let (run, a) = exhaust(CASE, &store, &graph, "run-1").await;
    let now = T0.saturating_add(2 * LEASE + 3);
    let view = snapshot(CASE, &store, &run).await;
    let c1 = ok(CASE, plan_exhausted(&graph, &view, &a.task.task_id, now));
    let c2 = ok(CASE, plan_exhausted(&graph, &view, &a.task.task_id, now));
    assert_eq!(c1, c2, "{CASE}: deterministic sweep");
    ok(CASE, store.apply(c1, None).await);
    assert_eq!(
        task(CASE, &store, &a.task.task_id).await.planned_at,
        Some(now),
        "{CASE}: planning stamp"
    );
    assert_eq!(
        task(CASE, &store, &task_id(&run, "b", 0)).await.status,
        TaskStatus::Ready,
        "{CASE}: failure successor"
    );
    let view = snapshot(CASE, &store, &run).await;
    assert_eq!(
        view.status,
        RunStatus::Active,
        "{CASE}: continuation status"
    );
    assert!(
        matches!(
            store.apply(c2, None).await,
            Err(StoreError::VersionConflict { .. })
        ),
        "{CASE}: duplicate sweep committed"
    );
    assert!(
        matches!(
            plan_exhausted(&graph, &view, &a.task.task_id, now),
            Err(PlanError::AlreadyPlanned { .. })
        ),
        "{CASE}: planned row replanned"
    );
    assert!(
        ok(CASE, store.exhausted_tasks(10).await).is_empty(),
        "{CASE}: planned row swept"
    );
    let b = claim_one(CASE, &store, "w1", now, &graph, &task_id(&run, "b", 0)).await;
    ok(CASE, complete(CASE, &store, &graph, &b, done(), now).await);
    assert_eq!(
        snapshot(CASE, &store, &run).await.status,
        RunStatus::Completed,
        "{CASE}: continuation completion"
    );
    let (run, a) = exhaust(CASE, &store, &graph, "run-2").await;
    assert!(
        ok(CASE, store.exhausted_tasks(10).await)
            .iter()
            .any(|record| record.task_id == a.task.task_id),
        "{CASE}: unplanned row absent"
    );
    let c = ok(
        CASE,
        plan_cancel(
            &graph,
            &snapshot(CASE, &store, &run).await,
            "stop".into(),
            now,
        ),
    );
    ok(CASE, store.apply(c, None).await);
    assert_eq!(
        snapshot(CASE, &store, &run).await.status,
        RunStatus::Cancelled,
        "{CASE}: terminal sweep filter setup"
    );
    assert!(
        ok(CASE, store.exhausted_tasks(10).await).is_empty(),
        "{CASE}: terminal exhaustion returned"
    );
    let record = task(CASE, &store, &a.task.task_id).await;
    assert_eq!(
        (record.status, record.planned_at),
        (TaskStatus::Exhausted, None),
        "{CASE}: terminal filter changed row"
    );
}
