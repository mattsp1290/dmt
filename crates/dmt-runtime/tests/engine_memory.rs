mod common;
use common::*;
use dmt_core::*;
use dmt_runtime::*;
use dmt_store::*;
use serde_json::{Value, json};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[tokio::test(start_paused = true)]
async fn linear_run_completes_in_planner_order() {
    let recorder = Recorder::default();
    let r = recorder.clone();
    let hnd = handler(move |ctx, _| {
        r.record(ctx);
        async { done() }
    });
    let h = start(
        Arc::new(MemoryStore::new()),
        Arc::new(ManualClock::new(T0)),
        fixtures::linear(),
        fast_config(),
        &[("a", hnd.clone()), ("b", hnd)],
    )
    .await;
    let run = h
        .start_run(&"linear".into(), json!("secret"))
        .await
        .unwrap();
    completed(&h, &run).await;
    let records = h.events(&run, 0, 100).await.unwrap();
    assert_eq!(
        records.iter().map(|e| e.event.kind()).collect::<Vec<_>>(),
        [
            "RunStarted",
            "TaskScheduled",
            "TaskClaimed",
            "TaskCompleted",
            "TaskScheduled",
            "TaskClaimed",
            "TaskCompleted",
            "RunCompleted"
        ]
    );
    assert_eq!(
        records.iter().map(|e| e.seq).collect::<Vec<_>>(),
        (1..=8).collect::<Vec<_>>()
    );
    for ctx in recorder.entries() {
        assert_eq!(ctx.run_id, run);
        assert_eq!(ctx.graph_id, GraphId::from("linear"));
        assert_eq!(ctx.graph_version, 1);
        assert_eq!(ctx.attempt, 1);
        assert_eq!(ctx.step_key, StepKey::task(&run, &ctx.node_id, 0));
        assert_eq!(ctx.task_id.as_str(), ctx.step_key.as_str());
    }
    h.abort().await;
}
async fn fan_out_case(policy: JoinPolicy, failed: bool) {
    let joins = Arc::new(Mutex::new(Vec::new()));
    let j = joins.clone();
    let h = start(
        Arc::new(MemoryStore::new()),
        Arc::new(ManualClock::new(T0)),
        fixtures::fan_out(policy),
        fast_config(),
        &[
            ("a", handler(|_, _| async { done() })),
            (
                "fo",
                handler(|_, _| async {
                    Ok(NodeOutcome::FanOut(vec![json!(0), json!(1), json!(2)]))
                }),
            ),
            (
                "br",
                handler(move |_, input| async move {
                    let NodeInput::Branch { index, value } = input else {
                        panic!()
                    };
                    assert_eq!(value, json!(index));
                    if failed && index == 1 {
                        Err(HandlerError::Permanent("branch failed".into()))
                    } else {
                        done()
                    }
                }),
            ),
            (
                "jn",
                handler(move |_, input| {
                    let j = j.clone();
                    async move {
                        let NodeInput::Join(input) = input else {
                            panic!()
                        };
                        j.lock().unwrap().push(input);
                        done()
                    }
                }),
            ),
        ],
    )
    .await;
    let run = h.start_run(&"fan-out".into(), Value::Null).await.unwrap();
    completed(&h, &run).await;
    let inputs = joins.lock().unwrap().clone();
    assert_eq!(inputs.len(), 1);
    let input = &inputs[0];
    assert_eq!(input.expected, 3);
    assert_eq!(input.quorum_met, !failed);
    assert_eq!(
        input
            .results
            .iter()
            .map(BranchResult::index)
            .collect::<Vec<_>>(),
        [0, 1, 2]
    );
    if failed {
        assert!(matches!(
            &input.results[1],
            BranchResult::Failed { index: 1, .. }
        ));
    }
    drop(inputs);
    assert_eq!(kind_count(&h, &run, "JoinSatisfied").await, 1);
    h.abort().await;
}
#[tokio::test(start_paused = true)]
async fn fan_out_all_joins_in_branch_order() {
    fan_out_case(JoinPolicy::All, false).await;
}
#[tokio::test(start_paused = true)]
async fn exhausted_branch_reaches_join_as_failed() {
    fan_out_case(JoinPolicy::All, true).await;
}
#[tokio::test(start_paused = true)]
async fn quorum_join_runs_once_and_records_straggler() {
    let branch_gate = Gate::new();
    let b = branch_gate.clone();
    let join_gate = Gate::new();
    let j = join_gate.clone();
    let joins = Recorder::default();
    let r = joins.clone();
    let h = start(
        Arc::new(MemoryStore::new()),
        Arc::new(ManualClock::new(T0)),
        fixtures::fan_out(JoinPolicy::Quorum(2)),
        fast_config(),
        &[
            ("a", handler(|_, _| async { done() })),
            (
                "fo",
                handler(|_, _| async {
                    Ok(NodeOutcome::FanOut(vec![json!(0), json!(1), json!(2)]))
                }),
            ),
            (
                "br",
                handler(move |_, input| {
                    let b = b.clone();
                    async move {
                        if matches!(input, NodeInput::Branch { index: 2, .. }) {
                            b.wait().await;
                        }
                        done()
                    }
                }),
            ),
            (
                "jn",
                handler(move |ctx, input| {
                    let j = j.clone();
                    let r = r.clone();
                    async move {
                        let NodeInput::Join(input) = input else {
                            panic!()
                        };
                        assert_eq!(input.results.len(), 2);
                        assert!(input.quorum_met);
                        r.record(ctx);
                        j.wait().await;
                        done()
                    }
                }),
            ),
        ],
    )
    .await;
    let run = h.start_run(&"fan-out".into(), Value::Null).await.unwrap();
    eventually("join started", || async { joins.count() == 1 }).await;
    branch_gate.open();
    eventually("straggler contributed", || async {
        kind_count(&h, &run, "BranchContributed").await == 3
    })
    .await;
    join_gate.open();
    completed(&h, &run).await;
    assert_eq!(joins.count(), 1);
    assert_eq!(kind_count(&h, &run, "JoinSatisfied").await, 1);
    h.abort().await;
}
#[tokio::test(start_paused = true)]
async fn retry_backoff_then_exhaustion_follows_failed_edge() {
    let clock = Arc::new(ManualClock::new(T0));
    let r = Recorder::default();
    let rec = r.clone();
    let h = start(
        Arc::new(MemoryStore::new()),
        clock.clone(),
        fixtures::retry_chain(3),
        fast_config(),
        &[(
            "a",
            handler(move |ctx, _| {
                rec.record(ctx);
                async { Err(HandlerError::Retryable("retry".into())) }
            }),
        )],
    )
    .await;
    let run = h.start_run(&"retry".into(), Value::Null).await.unwrap();
    eventually("first retry", || async {
        kind_count(&h, &run, "TaskRetryScheduled").await == 1
    })
    .await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(r.count(), 1);
    let _ = clock.advance(1_000_000);
    eventually("second retry", || async {
        kind_count(&h, &run, "TaskRetryScheduled").await == 2
    })
    .await;
    let _ = clock.advance(2_000_000);
    assert_eq!(
        h.wait_quiescent(&run, Duration::from_secs(5))
            .await
            .unwrap(),
        Quiescent::Terminal(RunStatus::Failed)
    );
    let retry_times: Vec<_> = events(&h, &run)
        .await
        .into_iter()
        .filter_map(|e| {
            if let RunEvent::TaskRetryScheduled { run_at, .. } = e {
                Some(run_at)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(
        retry_times,
        [T0.saturating_add(1_000_000), T0.saturating_add(3_000_000)]
    );
    assert_eq!(
        r.entries().iter().map(|c| c.attempt).collect::<Vec<_>>(),
        [1, 2, 3]
    );
    h.abort().await;
}
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
#[tokio::test(start_paused = true)]
async fn replan_under_contention_stays_bounded() {
    let (store, h, run) = run_contention().await;
    let mut counts = std::collections::BTreeMap::<TaskId, (usize, usize)>::new();
    for record in store.records() {
        if let Some(proof) = record.proof.filter(|p| p.task_id.as_str().contains("/br/")) {
            let count = counts.entry(proof.task_id).or_default();
            match record.result {
                Ok(_) => count.0 += 1,
                Err(StoreError::VersionConflict { .. }) => count.1 += 1,
                other => panic!("unexpected {other:?}"),
            }
        }
    }
    assert_eq!(counts.len(), 8);
    assert!(counts.values().map(|c| c.1).sum::<usize>() >= 7);
    for (successes, conflicts) in counts.values() {
        assert_eq!(*successes, 1);
        assert!(*conflicts <= 7);
    }
    assert_eq!(kind_count(&h, &run, "BranchContributed").await, 8);
    assert_eq!(kind_count(&h, &run, "JoinSatisfied").await, 1);
    h.abort().await;
}
#[tokio::test(start_paused = true)]
async fn sliding_window_bounds_in_flight_and_refills_slots() {
    let gates: Vec<_> = (0..10).map(|_| Gate::new()).collect();
    let waits = gates.clone();
    let started = Recorder::default();
    let r = started.clone();
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let h = start(
        store.clone(),
        Arc::new(ManualClock::new(T0)),
        fixtures::fan_out(JoinPolicy::All),
        EngineConfig {
            workers: 2,
            claim_limit: 2,
            ..fast_config()
        },
        &[
            ("a", handler(|_, _| async { done() })),
            (
                "fo",
                handler(|_, _| async {
                    Ok(NodeOutcome::FanOut((0..10).map(|i| json!(i)).collect()))
                }),
            ),
            (
                "br",
                handler(move |ctx, input| {
                    let waits = waits.clone();
                    let r = r.clone();
                    async move {
                        let NodeInput::Branch { index, .. } = input else {
                            panic!()
                        };
                        r.record(ctx);
                        waits[index as usize].wait().await;
                        done()
                    }
                }),
            ),
            ("jn", handler(|_, _| async { done() })),
        ],
    )
    .await;
    let run = h.start_run(&"fan-out".into(), Value::Null).await.unwrap();
    eventually("four slots full", || async { started.count() == 4 }).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(started.count(), 4);
    let first = started.entries()[0]
        .step_key
        .as_str()
        .rsplit('/')
        .next()
        .unwrap()
        .parse::<usize>()
        .unwrap();
    gates[first].open();
    eventually("one refill", || async { started.count() == 5 }).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(started.count(), 5);
    assert!(
        store
            .limits
            .lock()
            .unwrap()
            .iter()
            .all(|n| (1..=2).contains(n))
    );
    for gate in gates {
        gate.open();
    }
    completed(&h, &run).await;
    h.abort().await;
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
async fn start_run_survives_backend_error_after_commit() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    store.script("create_run", vec![Fault::After]);
    let h = start(
        store.clone(),
        Arc::new(ManualClock::new(T0)),
        fixtures::retry_chain(2),
        fast_config(),
        &[("a", handler(|_, _| async { done() }))],
    )
    .await;
    let run = h.start_run(&"retry".into(), Value::Null).await.unwrap();
    completed(&h, &run).await;
    assert_eq!(store.list_runs(RunFilter::all(100)).await.unwrap().len(), 1);
    h.abort().await;
}
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
fn payload(label: &str) -> SignalPayload {
    SignalPayload {
        label: label.into(),
        payload: Value::Null,
    }
}
async fn loop_engine(
    store: Arc<dyn Store>,
    clock: Arc<dyn Clock>,
    config: EngineConfig,
) -> EngineHandle {
    let done = handler(|_, _| async { done() });
    start(
        store,
        clock,
        fixtures::loop_via_wait(),
        config,
        &[("plan", done.clone()), ("implement", done)],
    )
    .await
}
async fn parked(h: &EngineHandle, run: &RunId) {
    assert_eq!(
        h.wait_quiescent(run, Duration::from_secs(5)).await.unwrap(),
        Quiescent::Parked
    );
}
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
fn wait_start_graph(deadline: Option<i64>) -> Graph {
    let builder = GraphBuilder::new("x", 1)
        .start("w")
        .wait("w", "go", deadline)
        .task("t")
        .end("done", EndStatus::Completed)
        .edge_on("w", "t", "ok")
        .edge("t", "done");
    if deadline.is_some() {
        builder.edge_on("w", "t", "timeout").build().unwrap()
    } else {
        builder.build().unwrap()
    }
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
