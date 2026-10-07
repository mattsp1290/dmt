use crate::common::store::Fault;
use crate::common::*;
use dmt_core::*;
use dmt_runtime::*;
use dmt_store::*;
use serde_json::Value;
use serde_json::json;
use std::sync::Mutex;
use std::{sync::Arc, time::Duration};

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
