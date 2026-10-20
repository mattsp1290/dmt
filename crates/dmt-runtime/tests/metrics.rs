mod common;
use common::*;
use dmt_core::*;
use dmt_runtime::{Quiescent, telemetry::names};
use dmt_store::*;
use metrics_util::debugging::{DebugValue, Snapshotter};
use serde_json::Value;
use std::{sync::Arc, time::Duration};

#[derive(Eq, PartialEq, Ord, PartialOrd)]
struct Series {
    name: String,
    labels: Vec<(String, String)>,
}
struct Observations {
    source: Snapshotter,
    counts: std::cell::RefCell<std::collections::BTreeMap<Series, u64>>,
}
impl Observations {
    fn capture(
        &self,
    ) -> Vec<(
        metrics_util::CompositeKey,
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    )> {
        let entries = self.source.snapshot().into_vec();
        let mut totals = self.counts.borrow_mut();
        // Debug snapshots drain every series: retain counters and histogram sample counts.
        for (metric, _, _, value) in &entries {
            let count = match value {
                DebugValue::Counter(value) => *value,
                DebugValue::Histogram(values) => u64::try_from(values.len()).unwrap(),
                DebugValue::Gauge(_) => continue,
            };
            let mut labels: Vec<_> = metric
                .key()
                .labels()
                .map(|l| (l.key().to_owned(), l.value().to_owned()))
                .collect();
            labels.sort_unstable();
            *totals
                .entry(Series {
                    name: metric.key().name().to_owned(),
                    labels,
                })
                .or_default() += count;
        }
        entries
    }
    fn count(&self, name: &str, labels: &[(&str, &str)]) -> u64 {
        self.capture();
        self.counts
            .borrow()
            .iter()
            .filter(|(series, _)| {
                series.name == name
                    && labels.iter().all(|(k, v)| {
                        series
                            .labels
                            .iter()
                            .any(|(key, value)| key == k && value == v)
                    })
            })
            .map(|(_, count)| count)
            .sum()
    }
}
fn counter(snapshot: &Observations, name: &str, labels: &[(&str, &str)]) -> u64 {
    snapshot.count(name, labels)
}

#[tokio::test(start_paused = true)]
async fn metrics_follow_engine_activity() {
    let recorder = metrics_util::debugging::DebuggingRecorder::new();
    let source = recorder.snapshotter();
    let snapshot = Observations {
        source,
        counts: std::cell::RefCell::default(),
    };
    metrics::set_global_recorder(recorder).unwrap();
    linear_metrics(&snapshot).await;
    host_metrics(&snapshot).await;
    lease_metric(&snapshot).await;
    retry_metrics(&snapshot).await;
    input_and_registration_metrics(&snapshot).await;
}
async fn linear_metrics(snapshot: &Observations) {
    let h = start(
        Arc::new(MemoryStore::new()),
        Arc::new(ManualClock::new(T0)),
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
    h.abort().await;
    let entries = snapshot.capture();
    for name in [
        names::DISPATCH_DURATION,
        names::HANDLER_DURATION,
        names::COMMIT_DURATION,
    ] {
        let samples: usize = entries
            .iter()
            .filter(|(key, _, _, _)| key.key().name() == name)
            .map(|(_, _, _, v)| match v {
                DebugValue::Histogram(v) => v.len(),
                _ => 0,
            })
            .sum();
        assert_eq!(samples, 2, "{name}");
    }
    assert!(
        entries
            .iter()
            .any(|(key, _, _, v)| key.key().name() == names::IN_FLIGHT
                && matches!(v, DebugValue::Gauge(g) if f64::from(*g) == 0.0))
    );
    for (name, labels, expected) in [
        (names::RUNS_STARTED, vec![("graph_id", "linear")], 1),
        (names::TASKS_CLAIMED, vec![("graph_id", "linear")], 2),
        (names::EVENTS, vec![("kind", "RunStarted")], 1),
        (names::EVENTS, vec![("kind", "TaskCompleted")], 2),
        (names::EVENTS, vec![("kind", "RunCompleted")], 1),
        (
            names::DISPATCH_OUTCOMES,
            vec![("node_id", "a"), ("outcome", "done")],
            1,
        ),
        (
            names::COMMIT_ATTEMPTS,
            vec![("kind", "outcome"), ("result", "applied")],
            2,
        ),
    ] {
        assert_eq!(counter(snapshot, name, &labels), expected, "{name}");
    }
}
async fn host_metrics(snapshot: &Observations) {
    let (_, h, _) = run_contention().await;
    h.abort().await;
    assert!(counter(snapshot, names::COMMIT_ATTEMPTS, &[("result", "conflict")]) > 0);
    let clock = Arc::new(ManualClock::new(T0));
    let h = start(
        Arc::new(MemoryStore::new()),
        clock.clone(),
        fixtures::wait_with_deadline(1_000_000),
        fast_config(),
        &[("a", handler(|_, _| async { done() }))],
    )
    .await;
    let run = h.start_run(&"wait".into(), Value::Null).await.unwrap();
    assert_eq!(
        h.wait_quiescent(&run, Duration::from_secs(5))
            .await
            .unwrap(),
        Quiescent::Parked
    );
    assert!(
        h.signal(
            &run,
            "unknown",
            SignalPayload {
                label: "ok".into(),
                payload: Value::Null
            }
        )
        .await
        .is_err()
    );
    assert_eq!(
        counter(snapshot, names::SIGNALS, &[("result", "not_found")]),
        1
    );
    let _ = clock.advance(1_000_000);
    eventually("timeout", || async {
        h.run(&run).await.unwrap().unwrap().status.is_terminal()
    })
    .await;
    assert_eq!(
        counter(
            snapshot,
            names::SWEEPS,
            &[("kind", "timeout"), ("result", "applied")]
        ),
        1
    );
    assert_eq!(
        counter(snapshot, names::EVENTS, &[("kind", "SignalTimedOut")]),
        1
    );
    let run = h.start_run(&"wait".into(), Value::Null).await.unwrap();
    h.wait_quiescent(&run, Duration::from_secs(5))
        .await
        .unwrap();
    h.signal(
        &run,
        "gate",
        SignalPayload {
            label: "ok".into(),
            payload: Value::Null,
        },
    )
    .await
    .unwrap();
    assert_eq!(counter(snapshot, names::SIGNALS, &[("result", "ok")]), 1);
    assert_eq!(
        counter(snapshot, names::EVENTS, &[("kind", "SignalReceived")]),
        1
    );
    let run = h.start_run(&"wait".into(), Value::Null).await.unwrap();
    h.wait_quiescent(&run, Duration::from_secs(5))
        .await
        .unwrap();
    h.cancel(&run, "test").await.unwrap();
    assert!(h.cancel(&run, "test").await.is_err());
    assert_eq!(counter(snapshot, names::CANCELS, &[("result", "ok")]), 1);
    assert_eq!(
        counter(snapshot, names::CANCELS, &[("result", "terminal")]),
        1
    );
    assert_eq!(
        counter(snapshot, names::EVENTS, &[("kind", "RunCancelled")]),
        1
    );
    h.abort().await;
}
async fn lease_metric(snapshot: &Observations) {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let clock = Arc::new(ManualClock::new(T0));
    let old_gate = Gate::new();
    let new_gate = Gate::new();
    let old_wait = old_gate.clone();
    let new_wait = new_gate.clone();
    let records = Recorder::default();
    let rec = records.clone();
    let handlers = [(
        "a",
        handler(move |ctx, _| {
            let gate = if ctx.attempt == 1 {
                old_wait.clone()
            } else {
                new_wait.clone()
            };
            let rec = rec.clone();
            async move {
                rec.record(ctx);
                gate.wait().await;
                done()
            }
        }),
    )];
    let before = counter(snapshot, names::TASKS_CLAIMED, &[("graph_id", "retry")]);
    let first = start(
        store.clone(),
        clock.clone(),
        fixtures::retry_chain(2),
        fast_config(),
        &handlers,
    )
    .await;
    let run = first.start_run(&"retry".into(), Value::Null).await.unwrap();
    eventually("original claim", || async { records.count() == 1 }).await;
    assert_eq!(
        counter(snapshot, names::TASKS_CLAIMED, &[("graph_id", "retry")]),
        before + 1
    );
    let _ = clock.advance(10_000_001);
    let second = start(
        store.clone(),
        clock,
        fixtures::retry_chain(2),
        fast_config(),
        &handlers,
    )
    .await;
    eventually("reclaimed task", || async { records.count() == 2 }).await;
    assert_eq!(records.entries()[1].attempt, 2);
    assert_eq!(
        counter(snapshot, names::TASKS_CLAIMED, &[("graph_id", "retry")]),
        before + 2
    );
    old_gate.open();
    eventually("reclaimed lease rejects old commit", || async {
        counter(snapshot, names::LEASE_LOST, &[("source", "commit")]) == 1
    })
    .await;
    assert!(
        store
            .records()
            .iter()
            .any(|r| matches!(r.result, Err(StoreError::LeaseLost { .. })))
    );
    new_gate.open();
    completed(&second, &run).await;
    first.abort().await;
    second.abort().await;
}

async fn retry_metrics(snapshot: &Observations) {
    let clock = Arc::new(ManualClock::new(T0));
    let h = start(
        Arc::new(MemoryStore::new()),
        clock.clone(),
        fixtures::retry_chain(2),
        fast_config(),
        &[(
            "a",
            handler(|ctx, _| async move {
                if ctx.attempt == 1 {
                    Ok(NodeOutcome::Fail {
                        message: "retry".into(),
                        retryable: true,
                    })
                } else {
                    done()
                }
            }),
        )],
    )
    .await;
    let run = h.start_run(&"retry".into(), Value::Null).await.unwrap();
    eventually("retry scheduled", || async {
        kind_count(&h, &run, "TaskRetryScheduled").await == 1
    })
    .await;
    let _ = clock.advance(1_000_000);
    completed(&h, &run).await;
    h.abort().await;
    assert!(
        counter(
            snapshot,
            names::DISPATCH_OUTCOMES,
            &[("outcome", "fail_retryable")]
        ) > 0
    );
    for kind in ["TaskRetryScheduled", "TaskFailed"] {
        assert!(counter(snapshot, names::EVENTS, &[("kind", kind)]) > 0);
    }
    exhausted_metric(snapshot).await;
}
async fn exhausted_metric(snapshot: &Observations) {
    let store = Arc::new(MemoryStore::new());
    let clock = Arc::new(ManualClock::new(T0));
    let graph = fixtures::retry_chain(1);
    store.register_graph(&graph).await.unwrap();
    let run: RunId = "exhausted-test".into();
    store
        .create_run(plan_start(&graph, run.clone(), Value::Null, T0).unwrap())
        .await
        .unwrap();
    let request = |now| ClaimRequest {
        worker_id: "test".into(),
        now,
        lease_micros: 1,
        limit: 1,
        graphs: vec![graph.id().clone()],
    };
    store.claim_ready(request(T0)).await.unwrap();
    let now = clock.advance(2);
    store.claim_ready(request(now)).await.unwrap();
    let h = start(
        store,
        clock,
        graph,
        fast_config(),
        &[("a", handler(|_, _| async { done() }))],
    )
    .await;
    eventually("exhaustion sweep", || async {
        h.run(&run).await.unwrap().unwrap().status.is_terminal()
    })
    .await;
    h.abort().await;
    assert!(
        counter(
            snapshot,
            names::SWEEPS,
            &[("kind", "exhausted"), ("result", "applied")]
        ) > 0
    );
}

async fn input_and_registration_metrics(snapshot: &Observations) {
    for (node, expected) in [("jn", "bad_input"), ("a", "no_handler")] {
        let labels = [
            ("graph_id", "fan-out"),
            ("node_id", node),
            ("result", "applied"),
        ];
        let before = snapshot.count(names::DISPATCH_DURATION, &labels);
        let store = Arc::new(MemoryStore::new());
        let graph = fixtures::fan_out(JoinPolicy::All);
        store.register_graph(&graph).await.unwrap();
        let run: RunId = format!("invalid-{node}").into();
        let mut commit = plan_start(&graph, run.clone(), Value::Null, T0).unwrap();
        // A foreign backend can return malformed join input; create that row explicitly.
        commit.new_tasks[0].node_id = node.into();
        store.create_run(commit).await.unwrap();
        let graph = GraphBuilder::new("fan-out", 2)
            .start("x")
            .task("x")
            .end("done", EndStatus::Completed)
            .edge("x", "done")
            .build()
            .unwrap();
        let mut handlers = vec![("x", handler(|_, _| async { done() }))];
        if node == "jn" {
            handlers.push(("jn", handler(|_, _| async { done() })));
        }
        let h = start(
            store,
            Arc::new(ManualClock::new(T0)),
            graph,
            fast_config(),
            &handlers,
        )
        .await;
        eventually("input or handler rejected", || async {
            counter(snapshot, names::DISPATCH_OUTCOMES, &[("outcome", expected)]) == 1
        })
        .await;
        assert_eq!(
            h.wait_quiescent(&run, Duration::from_secs(5))
                .await
                .unwrap(),
            Quiescent::Terminal(RunStatus::Failed)
        );
        h.abort().await;
        assert_eq!(
            snapshot.count(names::DISPATCH_DURATION, &labels),
            before + 1
        );
    }
}
