use crate::common::store::Fault;
use crate::common::*;
use dmt_core::*;
use dmt_runtime::*;
use dmt_store::*;
use serde_json::Value;
use std::{sync::Arc, time::Duration};

async fn lease_loss(same_engine: bool) {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let clock = Arc::new(ManualClock::new(T0));
    let gate1 = Gate::new();
    let g1 = gate1.clone();
    let gate2 = Gate::new();
    let g2 = gate2.clone();
    let r = Recorder::default();
    let rec = r.clone();
    let a = handler(move |ctx, _| {
        let g1 = g1.clone();
        let g2 = g2.clone();
        let rec = rec.clone();
        async move {
            let attempt = ctx.attempt;
            rec.record(ctx);
            if attempt == 1 {
                g1.wait().await;
            } else {
                g2.wait().await;
            }
            done()
        }
    });
    let hs = [("a", a), ("b", handler(|_, _| async { done() }))];
    let first = start(
        store.clone(),
        clock.clone(),
        fixtures::linear(),
        EngineConfig {
            workers: if same_engine { 2 } else { 1 },
            claim_limit: 1,
            ..fast_config()
        },
        &hs,
    )
    .await;
    let run = first
        .start_run(&"linear".into(), Value::Null)
        .await
        .unwrap();
    eventually("attempt 1", || async { r.count() == 1 }).await;
    let _ = clock.advance(10_000_001);
    let second = if same_engine {
        None
    } else {
        Some(
            start(
                store.clone(),
                clock,
                fixtures::linear(),
                EngineConfig {
                    workers: 1,
                    claim_limit: 1,
                    ..fast_config()
                },
                &hs,
            )
            .await,
        )
    };
    eventually("attempt 2", || async { r.count() == 2 }).await;
    gate1.open();
    eventually("lease rejection", || async {
        store
            .records()
            .iter()
            .any(|r| matches!(r.result, Err(StoreError::LeaseLost { .. })))
    })
    .await;
    let rejected = store
        .records()
        .into_iter()
        .find(|r| matches!(r.result, Err(StoreError::LeaseLost { .. })))
        .unwrap()
        .proof
        .unwrap();
    assert_eq!(&rejected.worker_id, first.worker_id());
    assert_eq!(rejected.attempt, 1);
    if let Some(second) = &second {
        assert!(events(&first, &run).await.iter().any(|e| matches!(e, RunEvent::TaskClaimed { worker_id, attempt: 2, .. } if worker_id == second.worker_id())));
    }
    gate2.open();
    completed(&first, &run).await;
    assert_reclaimed_completion(&first, &run).await;
    assert_eq!(r.entries()[0].step_key, r.entries()[1].step_key);
    first.abort().await;
    if let Some(second) = second {
        second.abort().await;
    }
}

#[tokio::test(start_paused = true)]
async fn lease_loss_across_engines_drops_stale_commit() {
    lease_loss(false).await;
}

#[tokio::test(start_paused = true)]
async fn lease_loss_within_one_engine_checks_attempt() {
    lease_loss(true).await;
}

async fn assert_reclaimed_completion(first: &EngineHandle, run: &RunId) {
    let completions: Vec<_> = events(first, run)
        .await
        .into_iter()
        .filter_map(|e| {
            if let RunEvent::TaskCompleted {
                task_id: id,
                attempt,
                ..
            } = e
            {
                (id == task_id(run, "a")).then_some(attempt)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(completions, [2]);
}

#[tokio::test(start_paused = true)]
async fn heartbeat_extends_lease_past_original_expiry() {
    let store = Arc::new(MemoryStore::new());
    let clock = Arc::new(ManualClock::new(T0));
    let gate = Gate::new();
    let g = gate.clone();
    let r = Recorder::default();
    let rec = r.clone();
    let h = start(
        store.clone(),
        clock.clone(),
        fixtures::retry_chain(3),
        EngineConfig {
            heartbeat_every: Duration::from_millis(50),
            ..fast_config()
        },
        &[(
            "a",
            handler(move |ctx, _| {
                let g = g.clone();
                let rec = rec.clone();
                async move {
                    rec.record(ctx);
                    g.wait().await;
                    done()
                }
            }),
        )],
    )
    .await;
    let run = h.start_run(&"retry".into(), Value::Null).await.unwrap();
    eventually("handler started", || async { r.count() == 1 }).await;
    for _ in 0..3 {
        let _ = clock.advance(6_000_000);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            store
                .load_task(&task_id(&run, "a"))
                .await
                .unwrap()
                .unwrap()
                .lease_until,
            Some(clock.now().saturating_add(10_000_000))
        );
    }
    assert_eq!(r.count(), 1);
    gate.open();
    completed(&h, &run).await;
    h.abort().await;
}

#[tokio::test(start_paused = true)]
async fn heartbeat_lost_cancels_handler_token() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    store.script("heartbeat", vec![Fault::Lost]);
    let h = start(
        store.clone(),
        Arc::new(ManualClock::new(T0)),
        fixtures::retry_chain(2),
        EngineConfig {
            heartbeat_every: Duration::from_millis(50),
            ..fast_config()
        },
        &[(
            "a",
            handler(|ctx, _| async move {
                ctx.cancel.cancelled().await;
                tokio::time::sleep(Duration::from_millis(500)).await;
                done()
            }),
        )],
    )
    .await;
    let run = h.start_run(&"retry".into(), Value::Null).await.unwrap();
    completed(&h, &run).await;
    assert_eq!(store.calls("heartbeat"), 1);
    assert_eq!(kind_count(&h, &run, "TaskCompleted").await, 1);
    h.abort().await;
}

#[tokio::test(start_paused = true)]
async fn lease_reclaim_exhaustion_follows_failed_edge() {
    let store = Arc::new(MemoryStore::new());
    let clock = Arc::new(ManualClock::new(T0));
    let r = Recorder::default();
    let rec = r.clone();
    let h = start(
        store.clone(),
        clock.clone(),
        fixtures::retry_chain(2),
        EngineConfig {
            workers: 1,
            claim_limit: 4,
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
    eventually("first attempt", || async { r.count() == 1 }).await;
    let _ = clock.advance(10_000_001);
    eventually("reclaim", || async { r.count() == 2 }).await;
    let _ = clock.advance(10_000_001);
    assert_eq!(
        h.wait_quiescent(&run, Duration::from_secs(5))
            .await
            .unwrap(),
        Quiescent::Terminal(RunStatus::Failed)
    );
    assert_eq!(
        r.entries().iter().map(|c| c.attempt).collect::<Vec<_>>(),
        [1, 2]
    );
    assert!(
        store
            .load_task(&task_id(&run, "a"))
            .await
            .unwrap()
            .unwrap()
            .planned_at
            .is_some()
    );
    assert!(events(&h, &run).await.iter().any(|e| matches!(
        e,
        RunEvent::TaskExhausted {
            reason: ExhaustReason::LeaseReclaimsExceeded,
            ..
        }
    )));
    h.abort().await;
}
