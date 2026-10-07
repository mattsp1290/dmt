use crate::common::store::Fault;
use crate::common::*;
use dmt_core::*;
use dmt_runtime::*;
use dmt_store::*;
use serde_json::Value;
use std::{sync::Arc, time::Duration};

#[tokio::test(start_paused = true)]
async fn handler_panic_becomes_retryable_failure() {
    let clock = Arc::new(ManualClock::new(T0));
    let h = start(
        Arc::new(MemoryStore::new()),
        clock.clone(),
        fixtures::retry_chain(2),
        fast_config(),
        &[(
            "a",
            handler(|ctx, _| async move {
                assert_ne!(ctx.attempt, 1, "handler panic");
                done()
            }),
        )],
    )
    .await;
    for _ in 0..2 {
        let run = h.start_run(&"retry".into(), Value::Null).await.unwrap();
        eventually("panic committed", || async { events(&h, &run).await.iter().any(|e| matches!(e, RunEvent::TaskFailed { retryable: true, message, .. } if message.contains("panicked"))) }).await;
        let _ = clock.advance(1_000_000);
        completed(&h, &run).await;
    }
    h.abort().await;
}

#[tokio::test(start_paused = true)]
async fn handler_timeout_uses_node_then_config() {
    for (node_timeout, config_timeout) in [
        (Some(50_000), None),
        (None, Some(Duration::from_millis(50))),
        (Some(50_000), Some(Duration::from_secs(3600))),
    ] {
        let graph = GraphBuilder::new("timeout", 1)
            .start("p")
            .task_with(
                "p",
                RetryPolicy {
                    max_attempts: 2,
                    ..RetryPolicy::default()
                },
                node_timeout,
            )
            .end("done", EndStatus::Completed)
            .edge("p", "done")
            .build()
            .unwrap();
        let clock = Arc::new(ManualClock::new(T0));
        let witness = Arc::new(());
        let w = witness.clone();
        let h = start(
            Arc::new(MemoryStore::new()),
            clock.clone(),
            graph,
            EngineConfig {
                handler_timeout: config_timeout,
                ..fast_config()
            },
            &[(
                "p",
                handler(move |ctx, _| {
                    let w = w.clone();
                    async move {
                        if ctx.attempt == 1 {
                            std::future::pending::<()>().await;
                            drop(w);
                        }
                        done()
                    }
                }),
            )],
        )
        .await;
        let run = h.start_run(&"timeout".into(), Value::Null).await.unwrap();
        let started = tokio::time::Instant::now();
        eventually("timeout committed", || async { events(&h, &run).await.iter().any(|e| matches!(e, RunEvent::TaskFailed { retryable: true, message, .. } if message.contains("timed out"))) }).await;
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(Arc::strong_count(&witness), 2);
        let _ = clock.advance(1_000_000);
        completed(&h, &run).await;
        h.abort().await;
    }
}

#[tokio::test(start_paused = true)]
async fn invalid_handler_outcome_fails_permanently() {
    for empty in [false, true] {
        let graph = if empty {
            fixtures::fan_out(JoinPolicy::All)
        } else {
            fixtures::linear()
        };
        let id = graph.id().clone();
        let mut builder = Engine::builder()
            .store(Arc::new(MemoryStore::new()))
            .clock(Arc::new(ManualClock::new(T0)))
            .config(fast_config())
            .graph(graph.clone());
        for (node, def) in graph.nodes() {
            if !matches!(def.kind, NodeKind::End { .. }) {
                let bad = if empty {
                    node.as_str() == "fo"
                } else {
                    node.as_str() == "a"
                };
                builder = builder.handler(
                    node.clone(),
                    handler(move |_, _| async move {
                        if bad {
                            Ok(NodeOutcome::FanOut(if empty {
                                vec![]
                            } else {
                                vec![Value::Null]
                            }))
                        } else {
                            done()
                        }
                    }),
                );
            }
        }
        let h = builder.build().unwrap().start().await.unwrap();
        let run = h.start_run(&id, Value::Null).await.unwrap();
        assert_eq!(
            h.wait_quiescent(&run, Duration::from_secs(2))
                .await
                .unwrap(),
            Quiescent::Terminal(RunStatus::Failed)
        );
        assert!(events(&h, &run).await.iter().any(|e| matches!(
            e,
            RunEvent::TaskFailed {
                retryable: false,
                ..
            }
        )));
        h.abort().await;
    }
}

#[tokio::test(start_paused = true)]
async fn pending_heartbeat_does_not_delay_handler_timeout() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    store.script("heartbeat", vec![Fault::Delay(Duration::from_secs(3600))]);
    let recorder = Recorder::default();
    let r = recorder.clone();
    let witness = Arc::new(());
    let w = witness.clone();
    let h = start(
        store.clone(),
        Arc::new(ManualClock::new(T0)),
        fixtures::retry_chain(2),
        EngineConfig {
            heartbeat_every: Duration::from_millis(10),
            handler_timeout: Some(Duration::from_millis(50)),
            ..fast_config()
        },
        &[(
            "a",
            handler(move |ctx, _| {
                let w = w.clone();
                let r = r.clone();
                async move {
                    r.record(ctx);
                    std::future::pending::<()>().await;
                    drop(w);
                    done()
                }
            }),
        )],
    )
    .await;
    let run = h.start_run(&"retry".into(), Value::Null).await.unwrap();
    eventually("handler started", || async { recorder.count() == 1 }).await;
    let begin = tokio::time::Instant::now();
    eventually("timeout during heartbeat", || async {
        kind_count(&h, &run, "TaskFailed").await == 1
    })
    .await;
    assert!(begin.elapsed() < Duration::from_millis(100));
    assert!(recorder.entries()[0].cancel.is_cancelled());
    assert_eq!(Arc::strong_count(&witness), 2);
    assert_eq!(store.calls("heartbeat"), 1);
    h.abort().await;
}

#[tokio::test(start_paused = true)]
async fn pending_heartbeat_does_not_delay_completion_or_drain() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    store.script("heartbeat", vec![Fault::Delay(Duration::from_secs(3600))]);
    let recorder = Recorder::default();
    let r = recorder.clone();
    let gate = Gate::new();
    let g = gate.clone();
    let h = start(
        store.clone(),
        Arc::new(ManualClock::new(T0)),
        fixtures::retry_chain(2),
        EngineConfig {
            heartbeat_every: Duration::from_millis(10),
            ..fast_config()
        },
        &[(
            "a",
            handler(move |ctx, _| {
                let g = g.clone();
                let r = r.clone();
                async move {
                    r.record(ctx);
                    g.wait().await;
                    done()
                }
            }),
        )],
    )
    .await;
    let run = h.start_run(&"retry".into(), Value::Null).await.unwrap();
    eventually("heartbeat started", || async {
        store.calls("heartbeat") == 1
    })
    .await;
    let drain = h.clone();
    let shutdown = tokio::spawn(async move { drain.shutdown(Duration::from_millis(100)).await });
    tokio::task::yield_now().await;
    gate.open();
    let begin = tokio::time::Instant::now();
    shutdown.await.unwrap().unwrap();
    assert!(begin.elapsed() < Duration::from_millis(100));
    completed(&h, &run).await;
    assert!(!recorder.entries()[0].cancel.is_cancelled());
}
