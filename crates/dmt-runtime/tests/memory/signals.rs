use super::support::*;
use crate::common::store::Fault;
use crate::common::*;
use dmt_core::*;
use dmt_runtime::*;
use dmt_store::*;
use serde_json::Value;
use std::{sync::Arc, time::Duration};

#[tokio::test(start_paused = true)]
async fn loop_via_signoff_uses_occurrence_keys() {
    let r = Recorder::default();
    let rec = r.clone();
    let hnd = handler(move |ctx, _| {
        rec.record(ctx);
        async { done() }
    });
    let h = start(
        Arc::new(MemoryStore::new()),
        Arc::new(ManualClock::new(T0)),
        fixtures::loop_via_wait(),
        fast_config(),
        &[("plan", hnd.clone()), ("implement", hnd)],
    )
    .await;
    let run = h.start_run(&"loop".into(), Value::Null).await.unwrap();
    for _ in 0..2 {
        parked(&h, &run).await;
        h.signal(&run, "signoff", payload("changes_requested"))
            .await
            .unwrap();
    }
    parked(&h, &run).await;
    h.signal(&run, "signoff", payload("approved"))
        .await
        .unwrap();
    completed(&h, &run).await;
    assert_eq!(
        r.entries()
            .iter()
            .filter(|c| c.node_id.as_str() == "plan")
            .map(|c| c.step_key.clone())
            .collect::<Vec<_>>(),
        (0..3)
            .map(|i| StepKey::task(&run, &"plan".into(), i))
            .collect::<Vec<_>>()
    );
    let keys: Vec<_> = events(&h, &run)
        .await
        .into_iter()
        .filter_map(|e| {
            if let RunEvent::WaitOpened { signal_key, .. } = e {
                Some(signal_key.to_string())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(keys, ["signoff/0", "signoff/1", "signoff/2"]);
    assert_eq!(kind_count(&h, &run, "RunParked").await, 3);
    assert_eq!(kind_count(&h, &run, "RunResumed").await, 3);
    h.abort().await;
}

#[tokio::test(start_paused = true)]
async fn wait_deadline_follows_timeout_edge() {
    let clock = Arc::new(ManualClock::new(T0));
    let h = start(
        Arc::new(MemoryStore::new()),
        clock.clone(),
        fixtures::wait_with_deadline(5_000_000),
        fast_config(),
        &[("a", handler(|_, _| async { done() }))],
    )
    .await;
    let run = h.start_run(&"wait".into(), Value::Null).await.unwrap();
    parked(&h, &run).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    parked(&h, &run).await;
    let _ = clock.advance(5_000_000);
    assert_eq!(
        h.wait_quiescent(&run, Duration::from_secs(2))
            .await
            .unwrap(),
        Quiescent::Parked
    );
    eventually("timeout ends run", || async {
        h.run(&run).await.unwrap().unwrap().status == RunStatus::Failed
    })
    .await;
    assert_eq!(kind_count(&h, &run, "SignalTimedOut").await, 1);
    assert_eq!(kind_count(&h, &run, "SignalReceived").await, 0);
    assert!(matches!(
        h.signal(&run, "gate", payload("ok")).await,
        Err(EngineError::SignalNotFound { .. })
    ));
    h.abort().await;
}

#[tokio::test(start_paused = true)]
async fn cancel_while_parked_resolves_signal() {
    let store = Arc::new(MemoryStore::new());
    let h = loop_engine(store.clone(), Arc::new(ManualClock::new(T0)), fast_config()).await;
    let run = h.start_run(&"loop".into(), Value::Null).await.unwrap();
    parked(&h, &run).await;
    h.cancel(&run, "stop").await.unwrap();
    assert_eq!(
        h.wait_quiescent(&run, Duration::from_secs(1))
            .await
            .unwrap(),
        Quiescent::Terminal(RunStatus::Cancelled)
    );
    assert!(matches!(
        h.signal(&run, "signoff", payload("approved")).await,
        Err(EngineError::SignalNotFound { .. })
    ));
    assert!(matches!(
        h.cancel(&run, "again").await,
        Err(EngineError::RunTerminal {
            status: RunStatus::Cancelled,
            ..
        })
    ));
    assert_eq!(h.run(&run).await.unwrap().unwrap().signals, []);
    assert!(
        store
            .find_open_signal(&run, "signoff")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .load_task(&task_id(&run, "signoff"))
            .await
            .unwrap()
            .unwrap()
            .status,
        TaskStatus::Cancelled
    );
    h.abort().await;
}

#[tokio::test(start_paused = true)]
async fn worker_less_handle_signals_and_cancels() {
    let store = Arc::new(MemoryStore::new());
    let clock = Arc::new(ManualClock::new(T0));
    let h = start(
        store.clone(),
        clock.clone(),
        wait_start_graph(None),
        EngineConfig {
            workers: 0,
            ..fast_config()
        },
        &[],
    )
    .await;
    let run = h.start_run(&"x".into(), Value::Null).await.unwrap();
    parked(&h, &run).await;
    h.signal(&run, "go", payload("ok")).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        store
            .load_task(&task_id(&run, "t"))
            .await
            .unwrap()
            .unwrap()
            .status,
        TaskStatus::Ready
    );
    assert_eq!(kind_count(&h, &run, "TaskClaimed").await, 0);
    assert_eq!(
        h.wait_quiescent(&run, Duration::from_millis(100)).await,
        Err(EngineError::Timeout)
    );
    h.cancel(&run, "stop").await.unwrap();
    h.shutdown(Duration::from_secs(1)).await.unwrap();
    let cli = Engine::builder()
        .store(store)
        .clock(clock)
        .config(EngineConfig {
            workers: 0,
            ..fast_config()
        })
        .build()
        .unwrap()
        .start()
        .await
        .unwrap();
    assert!(matches!(
        cli.start_run(&"x".into(), Value::Null).await,
        Err(EngineError::GraphNotRegistered { .. })
    ));
    let run = h.start_run(&"x".into(), Value::Null).await.unwrap();
    cli.signal(&run, "go", payload("ok")).await.unwrap();
    cli.cancel(&run, "stop").await.unwrap();
    cli.shutdown(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn concurrent_duplicate_signal_resolves_once() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let h = loop_engine(store.clone(), Arc::new(ManualClock::new(T0)), fast_config()).await;
    let run = h.start_run(&"loop".into(), Value::Null).await.unwrap();
    parked(&h, &run).await;
    store.hold_next_load_runs(2);
    let (a, b) = tokio::join!(
        h.signal(&run, "signoff", payload("approved")),
        h.signal(&run, "signoff", payload("approved"))
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    assert!(matches!(
        a.as_ref().err().or(b.as_ref().err()),
        Some(EngineError::SignalNotFound { .. })
    ));
    completed(&h, &run).await;
    assert_eq!(kind_count(&h, &run, "SignalReceived").await, 1);
    h.abort().await;
}

#[tokio::test(start_paused = true)]
async fn signal_after_ambiguous_backend_error_is_indeterminate() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let h = loop_engine(store.clone(), Arc::new(ManualClock::new(T0)), fast_config()).await;
    let run = h.start_run(&"loop".into(), Value::Null).await.unwrap();
    parked(&h, &run).await;
    store.script("apply", vec![Fault::After]);
    assert!(matches!(
        h.signal(&run, "signoff", payload("approved")).await,
        Err(EngineError::Indeterminate { .. })
    ));
    completed(&h, &run).await;
    assert_eq!(kind_count(&h, &run, "SignalReceived").await, 1);
    h.abort().await;
}

#[tokio::test(start_paused = true)]
async fn cancel_after_ambiguous_backend_error_succeeds() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let h = loop_engine(store.clone(), Arc::new(ManualClock::new(T0)), fast_config()).await;
    let run = h.start_run(&"loop".into(), Value::Null).await.unwrap();
    parked(&h, &run).await;
    store.script("apply", vec![Fault::After]);
    h.cancel(&run, "stop").await.unwrap();
    assert_eq!(kind_count(&h, &run, "RunCancelled").await, 1);
    h.abort().await;
}

#[tokio::test(start_paused = true)]
async fn progress_is_wake_driven() {
    let h = loop_engine(
        Arc::new(MemoryStore::new()),
        Arc::new(ManualClock::new(T0)),
        EngineConfig {
            poll_interval: Duration::from_secs(3600),
            ..fast_config()
        },
    )
    .await;
    let begin = tokio::time::Instant::now();
    let run = h.start_run(&"loop".into(), Value::Null).await.unwrap();
    parked(&h, &run).await;
    h.signal(&run, "signoff", payload("approved"))
        .await
        .unwrap();
    completed(&h, &run).await;
    assert!(begin.elapsed() < Duration::from_secs(1));
    h.abort().await;
}

fn repeat_wait_graph() -> Graph {
    GraphBuilder::new("repeat-wait", 1)
        .start("w")
        .wait("w", "go", None)
        .end("done", EndStatus::Completed)
        .edge_on("w", "w", "repeat")
        .edge_on("w", "done", "ok")
        .build()
        .unwrap()
}

#[tokio::test(start_paused = true)]
async fn ambiguous_signal_does_not_consume_next_wait_occurrence() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let h = start(
        store.clone(),
        Arc::new(ManualClock::new(T0)),
        repeat_wait_graph(),
        EngineConfig {
            workers: 0,
            ..fast_config()
        },
        &[],
    )
    .await;
    let run = h
        .start_run(&"repeat-wait".into(), Value::Null)
        .await
        .unwrap();
    store.script("apply", vec![Fault::After]);
    assert!(matches!(
        h.signal(&run, "go", payload("repeat")).await,
        Err(EngineError::Indeterminate { .. })
    ));
    assert_eq!(kind_count(&h, &run, "SignalReceived").await, 1);
    assert_eq!(
        store
            .find_open_signal(&run, "go")
            .await
            .unwrap()
            .unwrap()
            .key
            .as_str(),
        "go/1"
    );
    h.signal(&run, "go", payload("ok")).await.unwrap();
    completed(&h, &run).await;
    h.abort().await;
}

#[tokio::test(start_paused = true)]
async fn concurrent_signals_do_not_consume_next_wait_occurrence() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let h = start(
        store.clone(),
        Arc::new(ManualClock::new(T0)),
        repeat_wait_graph(),
        EngineConfig {
            workers: 0,
            ..fast_config()
        },
        &[],
    )
    .await;
    let run = h
        .start_run(&"repeat-wait".into(), Value::Null)
        .await
        .unwrap();
    store.hold_next_load_runs(2);
    let (a, b) = tokio::join!(
        h.signal(&run, "go", payload("repeat")),
        h.signal(&run, "go", payload("repeat"))
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    assert!(matches!(
        a.as_ref().err().or(b.as_ref().err()),
        Some(EngineError::SignalNotFound { .. })
    ));
    assert_eq!(kind_count(&h, &run, "SignalReceived").await, 1);
    assert_eq!(
        store
            .find_open_signal(&run, "go")
            .await
            .unwrap()
            .unwrap()
            .key
            .as_str(),
        "go/1"
    );
    h.abort().await;
}

#[tokio::test(start_paused = true)]
async fn signal_retries_backend_error_without_committed_resolution() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let h = start(
        store.clone(),
        Arc::new(ManualClock::new(T0)),
        repeat_wait_graph(),
        EngineConfig {
            workers: 0,
            ..fast_config()
        },
        &[],
    )
    .await;
    let run = h
        .start_run(&"repeat-wait".into(), Value::Null)
        .await
        .unwrap();
    store.script(
        "apply",
        vec![Fault::Instead(StoreError::Backend("not committed".into()))],
    );
    h.signal(&run, "go", payload("repeat")).await.unwrap();
    assert_eq!(kind_count(&h, &run, "SignalReceived").await, 1);
    assert_eq!(
        store
            .find_open_signal(&run, "go")
            .await
            .unwrap()
            .unwrap()
            .key
            .as_str(),
        "go/1"
    );
    h.abort().await;
}
