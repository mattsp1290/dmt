use crate::{
    StoreFactory,
    harness::{
        T0, assert_contiguous, claim_one, complete, count, done, events, ok, signal_id, snapshot,
        start_run, task, task_id,
    },
};
use dmt_core::{RunStatus, SignalKey, SignalPayload, TaskStatus, fixtures, plan_signal};
use dmt_store::Store;
use serde_json::json;

/// Prove transactional rule 13.
/// # Panics
/// Panics with the case name when the backend violates the contract.
pub async fn case_wait_loop<F: StoreFactory>(factory: &F) {
    const CASE: &str = "wait_loop";
    let store = factory.fresh().await;
    let graph = fixtures::loop_via_wait();
    let run = start_run(CASE, &store, &graph, "run-1", T0).await;
    for i in 0..3 {
        let a = claim_one(CASE, &store, "w1", T0, &graph, &task_id(&run, "plan", i)).await;
        ok(CASE, complete(CASE, &store, &graph, &a, done(), T0).await);
        let s = snapshot(CASE, &store, &run).await;
        assert_eq!(s.status, RunStatus::Parked, "{CASE}: park occurrence {i}");
        let signal = ok(CASE, store.find_open_signal(&run, "signoff").await).unwrap();
        assert_eq!(
            signal.key,
            SignalKey::new("signoff", i),
            "{CASE}: occurrence key"
        );
        assert_eq!(
            signal.signal_id,
            signal_id(&run, "signoff", i),
            "{CASE}: occurrence id"
        );
        let label = if i < 2 {
            "changes_requested"
        } else {
            "approved"
        };
        let c = ok(
            CASE,
            plan_signal(
                &graph,
                &s,
                &signal.signal_id,
                SignalPayload {
                    label: label.into(),
                    payload: json!(true),
                },
                T0,
            ),
        );
        ok(CASE, store.apply(c, None).await);
        assert!(
            ok(CASE, store.find_open_signal(&run, "signoff").await).is_none(),
            "{CASE}: old signal open"
        );
        assert_eq!(
            snapshot(CASE, &store, &run).await.status,
            RunStatus::Active,
            "{CASE}: resume"
        );
    }
    for i in 0..3 {
        for node in ["signoff", "plan"] {
            assert_eq!(
                task(CASE, &store, &task_id(&run, node, i)).await.status,
                TaskStatus::Completed,
                "{CASE}: completed occurrence {node}/{i}"
            );
        }
    }
    let s = snapshot(CASE, &store, &run).await;
    assert_eq!(
        s.node_occurrences,
        [
            ("plan".into(), 3),
            ("signoff".into(), 3),
            ("implement".into(), 1)
        ]
        .into(),
        "{CASE}: occurrences"
    );
    let a = claim_one(
        CASE,
        &store,
        "w1",
        T0,
        &graph,
        &task_id(&run, "implement", 0),
    )
    .await;
    ok(CASE, complete(CASE, &store, &graph, &a, done(), T0).await);
    assert_eq!(
        snapshot(CASE, &store, &run).await.status,
        RunStatus::Completed,
        "{CASE}: completion"
    );
    let ev = events(CASE, &store, &run).await;
    for kind in ["WaitOpened", "SignalReceived", "RunParked", "RunResumed"] {
        assert_eq!(count(&ev, kind), 3, "{CASE}: count {kind}");
    }
    assert_contiguous(CASE, &ev);
}
