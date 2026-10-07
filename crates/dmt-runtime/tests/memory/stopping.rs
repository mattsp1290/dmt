use crate::common::*;
use dmt_core::*;
use dmt_runtime::*;
use dmt_store::*;
use serde_json::Value;
use std::sync::Mutex;
use std::{sync::Arc, time::Duration};

#[tokio::test(start_paused = true)]
async fn abort_stops_every_engine_task() {
    for drop_handle in [false, true] {
        let store = Arc::new(MemoryStore::new());
        let clock = Arc::new(ManualClock::new(T0));
        let witness = Arc::new(());
        let w = witness.clone();
        let h = start(
            store.clone(),
            clock.clone(),
            fixtures::retry_chain(2),
            fast_config(),
            &[(
                "a",
                handler(move |_, _| {
                    let w = w.clone();
                    async move {
                        std::future::pending::<()>().await;
                        drop(w);
                        done()
                    }
                }),
            )],
        )
        .await;
        let run = h.start_run(&"retry".into(), Value::Null).await.unwrap();
        eventually("handler witness", || async {
            Arc::strong_count(&witness) == 3
        })
        .await;
        if drop_handle {
            drop(h);
        } else {
            h.abort().await;
            drop(h);
        }
        eventually("witness released", || async {
            Arc::strong_count(&witness) == 1
        })
        .await;
        let _ = clock.advance(10_000_001);
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(
            store
                .events(&run, 0, 100)
                .await
                .unwrap()
                .iter()
                .filter(|e| e.event.kind() == "TaskClaimed")
                .count(),
            1
        );
    }
}

#[tokio::test(start_paused = true)]
async fn cancel_during_running_handler_drops_outcome() {
    let store = Arc::new(MemoryStore::new());
    let clock = Arc::new(ManualClock::new(T0));
    let gate = Gate::new();
    let g = gate.clone();
    let r = Recorder::default();
    let rec = r.clone();
    let h = start(
        store.clone(),
        clock.clone(),
        fixtures::linear(),
        fast_config(),
        &[
            (
                "a",
                handler(move |ctx, _| {
                    let g = g.clone();
                    let r = rec.clone();
                    async move {
                        r.record(ctx.clone());
                        g.wait().await;
                        assert!(ctx.cancel.is_cancelled());
                        done()
                    }
                }),
            ),
            ("b", handler(|_, _| async { panic!("b must never run") })),
        ],
    )
    .await;
    let run = h.start_run(&"linear".into(), Value::Null).await.unwrap();
    eventually("running a", || async { r.count() == 1 }).await;
    h.cancel(&run, "stop").await.unwrap();
    assert!(r.entries()[0].cancel.is_cancelled());
    gate.open();
    tokio::time::sleep(Duration::from_millis(10)).await;
    let _ = clock.advance(10_000_001);
    eventually("terminal lease cleanup", || async {
        store
            .load_task(&task_id(&run, "a"))
            .await
            .unwrap()
            .unwrap()
            .status
            == TaskStatus::Cancelled
    })
    .await;
    assert_eq!(kind_count(&h, &run, "TaskCompleted").await, 0);
    let records = h.events(&run, 0, 100).await.unwrap();
    let cancel_seq = records
        .iter()
        .find(|e| matches!(e.event, RunEvent::RunCancelled { .. }))
        .unwrap()
        .seq;
    assert!(
        !records
            .iter()
            .any(|e| e.seq > cancel_seq && matches!(e.event, RunEvent::TaskClaimed { .. }))
    );
    h.abort().await;
}

#[tokio::test(start_paused = true)]
async fn shutdown_waits_for_in_flight_handler() {
    let store = Arc::new(MemoryStore::new());
    let gate = Gate::new();
    let g = gate.clone();
    let r = Recorder::default();
    let rec = r.clone();
    let h = start(
        store.clone(),
        Arc::new(ManualClock::new(T0)),
        fixtures::linear(),
        fast_config(),
        &[
            (
                "a",
                handler(move |ctx, _| {
                    let g = g.clone();
                    let r = rec.clone();
                    async move {
                        r.record(ctx);
                        g.wait().await;
                        done()
                    }
                }),
            ),
            (
                "b",
                handler(|_, _| async { panic!("no new claim after shutdown") }),
            ),
        ],
    )
    .await;
    let run = h.start_run(&"linear".into(), Value::Null).await.unwrap();
    eventually("handler started", || async { r.count() == 1 }).await;
    let drain = h.clone();
    let shutdown = tokio::spawn(async move { drain.shutdown(Duration::from_secs(10)).await });
    tokio::task::yield_now().await;
    gate.open();
    shutdown.await.unwrap().unwrap();
    assert_eq!(kind_count(&h, &run, "TaskCompleted").await, 1);
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        store
            .load_task(&task_id(&run, "b"))
            .await
            .unwrap()
            .unwrap()
            .status,
        TaskStatus::Ready
    );
}

#[tokio::test(start_paused = true)]
async fn shutdown_cancels_tokens_after_drain() {
    let r = Recorder::default();
    let rec = r.clone();
    let cancelled = Arc::new(Mutex::new(None));
    let t = cancelled.clone();
    let h = start(
        Arc::new(MemoryStore::new()),
        Arc::new(ManualClock::new(T0)),
        fixtures::retry_chain(2),
        fast_config(),
        &[(
            "a",
            handler(move |ctx, _| {
                let r = rec.clone();
                let t = t.clone();
                async move {
                    r.record(ctx.clone());
                    ctx.cancel.cancelled().await;
                    *t.lock().unwrap() = Some(tokio::time::Instant::now());
                    Err(HandlerError::Retryable("cancelled".into()))
                }
            }),
        )],
    )
    .await;
    let run = h.start_run(&"retry".into(), Value::Null).await.unwrap();
    eventually("started", || async { r.count() == 1 }).await;
    let begin = tokio::time::Instant::now();
    h.shutdown(Duration::from_millis(100)).await.unwrap();
    assert!(cancelled.lock().unwrap().unwrap() - begin >= Duration::from_millis(100));
    assert!(events(&h, &run).await.iter().any(|e| matches!(
        e,
        RunEvent::TaskFailed {
            retryable: true,
            ..
        }
    )));
}

#[tokio::test(start_paused = true)]
async fn shutdown_reports_abandoned_handlers() {
    let store = Arc::new(MemoryStore::new());
    let r = Recorder::default();
    let rec = r.clone();
    let h = start(
        store.clone(),
        Arc::new(ManualClock::new(T0)),
        fixtures::retry_chain(2),
        EngineConfig {
            cancel_grace: Duration::from_millis(100),
            ..fast_config()
        },
        &[(
            "a",
            handler(move |ctx, _| {
                rec.record(ctx);
                async {
                    std::future::pending::<()>().await;
                    done()
                }
            }),
        )],
    )
    .await;
    let run = h.start_run(&"retry".into(), Value::Null).await.unwrap();
    eventually("started", || async { r.count() == 1 }).await;
    assert_eq!(
        h.shutdown(Duration::from_millis(100)).await,
        Err(EngineError::ShutdownTimedOut { abandoned: 1 })
    );
    assert_eq!(
        store
            .load_task(&task_id(&run, "a"))
            .await
            .unwrap()
            .unwrap()
            .status,
        TaskStatus::Running
    );
    h.shutdown(Duration::from_secs(1)).await.unwrap();
}
