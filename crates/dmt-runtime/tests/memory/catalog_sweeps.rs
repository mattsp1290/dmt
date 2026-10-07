use super::support::*;
use crate::common::store::Fault;
use crate::common::*;
use dmt_core::*;
use dmt_runtime::*;
use dmt_store::*;
use serde_json::Value;
use std::sync::Mutex;
use std::{sync::Arc, time::Duration};

#[tokio::test(start_paused = true)]
async fn old_graph_version_resolves_from_store() {
    let store = Arc::new(MemoryStore::new());
    let clock = Arc::new(ManualClock::new(T0));
    let old = GraphBuilder::new("g", 1)
        .start("a")
        .task("a")
        .task("legacy")
        .end("done", EndStatus::Completed)
        .edge("a", "legacy")
        .edge("legacy", "done")
        .build()
        .unwrap();
    let a = start(
        store.clone(),
        clock.clone(),
        old,
        EngineConfig {
            workers: 0,
            ..fast_config()
        },
        &[],
    )
    .await;
    let run = a.start_run(&"g".into(), Value::Null).await.unwrap();
    let graph = GraphBuilder::new("g", 2)
        .start("a")
        .task("a")
        .end("done", EndStatus::Completed)
        .edge("a", "done")
        .build()
        .unwrap();
    let versions = Arc::new(Mutex::new(Vec::new()));
    let v = versions.clone();
    let b = start(
        store,
        clock,
        graph,
        fast_config(),
        &[(
            "a",
            handler(move |ctx, _| {
                v.lock().unwrap().push(ctx.graph_version);
                async { done() }
            }),
        )],
    )
    .await;
    assert_eq!(
        b.wait_quiescent(&run, Duration::from_secs(2))
            .await
            .unwrap(),
        Quiescent::Terminal(RunStatus::Failed)
    );
    assert!(events(&b, &run).await.iter().any(|e| matches!(e, RunEvent::TaskFailed { retryable: false, message, .. } if message.contains("no handler registered"))));
    let new = b.start_run(&"g".into(), Value::Null).await.unwrap();
    completed(&b, &new).await;
    assert_eq!(*versions.lock().unwrap(), [1, 2]);
    a.abort().await;
    b.abort().await;
}

#[tokio::test(start_paused = true)]
async fn engine_claims_only_registered_graphs() {
    let store = Arc::new(MemoryStore::new());
    let clock = Arc::new(ManualClock::new(T0));
    let a = start(
        store.clone(),
        clock.clone(),
        fixtures::retry_chain(1),
        EngineConfig {
            workers: 0,
            ..fast_config()
        },
        &[],
    )
    .await;
    let other = a.start_run(&"retry".into(), Value::Null).await.unwrap();
    let h = start(
        store.clone(),
        clock,
        fixtures::linear(),
        fast_config(),
        &[
            ("a", handler(|_, _| async { done() })),
            ("b", handler(|_, _| async { done() })),
        ],
    )
    .await;
    let run = h.start_run(&"linear".into(), Value::Null).await.unwrap();
    completed(&h, &run).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        store
            .load_task(&task_id(&other, "a"))
            .await
            .unwrap()
            .unwrap()
            .status,
        TaskStatus::Ready
    );
    assert_eq!(kind_count(&h, &other, "TaskClaimed").await, 0);
    a.abort().await;
    h.abort().await;
}

#[tokio::test(start_paused = true)]
async fn sweeps_act_on_unregistered_graphs() {
    let store = Arc::new(MemoryStore::new());
    let clock = Arc::new(ManualClock::new(T0));
    let a = start(
        store.clone(),
        clock.clone(),
        wait_start_graph(Some(5_000_000)),
        EngineConfig {
            workers: 0,
            ..fast_config()
        },
        &[],
    )
    .await;
    let run = a.start_run(&"x".into(), Value::Null).await.unwrap();
    parked(&a, &run).await;
    let b = start(
        store.clone(),
        clock.clone(),
        fixtures::linear(),
        fast_config(),
        &[
            ("a", handler(|_, _| async { done() })),
            ("b", handler(|_, _| async { done() })),
        ],
    )
    .await;
    let _ = clock.advance(5_000_000);
    eventually("foreign sweep", || async {
        kind_count(&b, &run, "SignalTimedOut").await == 1
    })
    .await;
    assert_eq!(
        store
            .load_task(&task_id(&run, "t"))
            .await
            .unwrap()
            .unwrap()
            .status,
        TaskStatus::Ready
    );
    assert_eq!(kind_count(&b, &run, "TaskClaimed").await, 0);
    a.abort().await;
    b.abort().await;
}

#[tokio::test(start_paused = true)]
async fn store_errors_do_not_stop_workers() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    store.script(
        "claim_ready",
        vec![Fault::Instead(StoreError::Busy); 3]
            .into_iter()
            .chain(vec![Fault::Instead(StoreError::Backend("io".into())); 2])
            .collect(),
    );
    store.script(
        "due_signals",
        vec![Fault::Instead(StoreError::Backend("io".into()))],
    );
    store.script("apply", vec![Fault::Instead(StoreError::Busy); 2]);
    let clock = Arc::new(ManualClock::new(T0));
    let r = Recorder::default();
    let rec = r.clone();
    let h = start(
        store.clone(),
        clock.clone(),
        fixtures::wait_with_deadline(5_000_000),
        fast_config(),
        &[(
            "a",
            handler(move |ctx, _| {
                rec.record(ctx);
                async { done() }
            }),
        )],
    )
    .await;
    let run = h.start_run(&"wait".into(), Value::Null).await.unwrap();
    parked(&h, &run).await;
    let _ = clock.advance(5_000_000);
    eventually("deadline recovered", || async {
        h.run(&run).await.unwrap().unwrap().status == RunStatus::Failed
    })
    .await;
    assert_eq!(r.count(), 1);
    assert!(store.calls("claim_ready") > 5);
    h.abort().await;
}
